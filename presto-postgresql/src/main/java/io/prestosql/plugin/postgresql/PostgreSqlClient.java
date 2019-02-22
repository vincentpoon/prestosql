/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.plugin.postgresql;

import io.airlift.slice.Slice;
import io.prestosql.plugin.jdbc.BaseJdbcClient;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.BlockReadFunction;
import io.prestosql.plugin.jdbc.BlockWriteFunction;
import io.prestosql.plugin.jdbc.BooleanReadFunction;
import io.prestosql.plugin.jdbc.ColumnMapping;
import io.prestosql.plugin.jdbc.DoubleReadFunction;
import io.prestosql.plugin.jdbc.DriverConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcClient;
import io.prestosql.plugin.jdbc.JdbcColumnHandle;
import io.prestosql.plugin.jdbc.JdbcConnectorId;
import io.prestosql.plugin.jdbc.JdbcOutputTableHandle;
import io.prestosql.plugin.jdbc.JdbcTableHandle;
import io.prestosql.plugin.jdbc.JdbcTypeHandle;
import io.prestosql.plugin.jdbc.LongReadFunction;
import io.prestosql.plugin.jdbc.ReadFunction;
import io.prestosql.plugin.jdbc.SliceReadFunction;
import io.prestosql.plugin.jdbc.StandardColumnMappings;
import io.prestosql.plugin.jdbc.WriteMapping;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.ArrayBlock;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.ByteArrayBlock;
import io.prestosql.spi.block.IntArrayBlock;
import static io.prestosql.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import io.prestosql.spi.block.LongArrayBlock;
import io.prestosql.spi.block.ShortArrayBlock;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.TinyintType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeUtils;
import io.prestosql.spi.type.VarbinaryType;
import io.prestosql.spi.type.VarcharType;

import org.postgresql.Driver;
import org.postgresql.jdbc.PgConnection;

import com.google.common.base.Preconditions;

import static io.prestosql.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR;
import javax.inject.Inject;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.prestosql.plugin.jdbc.StandardColumnMappings.arrayColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.arrayWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.tinyintWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varbinaryWriteFunction;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.lang.reflect.Array.get;
import static java.lang.reflect.Array.getLength;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class PostgreSqlClient
        extends BaseJdbcClient
{
    @Inject
    public PostgreSqlClient(JdbcConnectorId connectorId, BaseJdbcConfig config)
    {
        super(connectorId, config, "\"", new DriverConnectionFactory(new Driver(), config));
    }

    @Override
    public void commitCreateTable(JdbcOutputTableHandle handle)
    {
        // PostgreSQL does not allow qualifying the target of a rename
        String sql = format(
                "ALTER TABLE %s RENAME TO %s",
                quoted(handle.getCatalogName(), handle.getSchemaName(), handle.getTemporaryTableName()),
                quoted(handle.getTableName()));

        try (Connection connection = getConnection(handle)) {
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public PreparedStatement getPreparedStatement(Connection connection, String sql)
            throws SQLException
    {
        connection.setAutoCommit(false);
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setFetchSize(1000);
        return statement;
    }

    @Override
    protected ResultSet getTables(Connection connection, String schemaName, String tableName)
            throws SQLException
    {
        DatabaseMetaData metadata = connection.getMetaData();
        String escape = metadata.getSearchStringEscape();
        return metadata.getTables(
                connection.getCatalog(),
                escapeNamePattern(schemaName, escape),
                escapeNamePattern(tableName, escape),
                new String[] {"TABLE", "VIEW", "MATERIALIZED VIEW", "FOREIGN TABLE"});
    }

    @Override
    public List<JdbcColumnHandle> getColumns(ConnectorSession session, JdbcTableHandle tableHandle) {
        List<JdbcColumnHandle> columns = super.getColumns(session, tableHandle);
        // get the dimensions of ARRAY type (which isn't included in JDBC DatabaseMetadata results)
        Map<String, Integer> arrayColumns = columns
                .stream()
                .filter(column -> column.getColumnType() instanceof ArrayType)
                .collect(toMap(arrayColumn -> arrayColumn.getColumnName(), arrayColumn -> 0));

        if (arrayColumns.size() > 0) {
            try (Connection connection = connectionFactory.openConnection()) {
                String sql = String.format(
                        "SELECT att.attname, att.attndims " + 
                                "FROM pg_attribute att " + 
                                "  JOIN pg_class tbl ON tbl.oid = att.attrelid " + 
                                "  JOIN pg_namespace ns ON tbl.relnamespace = ns.oid " + 
                                "WHERE ns.nspname = '%s' " + 
                                "AND tbl.relname = '%s' " + 
                                "AND att.attname in ('%s')",
                        tableHandle.getSchemaName(),
                        tableHandle.getTableName(),
                        arrayColumns.keySet().stream().collect(joining("','"))
                        );
                try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
                    while (resultSet.next()) {
                        arrayColumns.put(resultSet.getString(1), resultSet.getInt(2));
                    }
                }
            } catch (SQLException e) {
                throw new PrestoException(JDBC_ERROR, e);
            }
            // set ArrayType and JdbcTypeHandle with the correct # of dimensions
            columns = columns.stream().map(column -> {
                if (column.getColumnType() instanceof ArrayType) {
                    Integer numDimensions = arrayColumns.get(column.getColumnName());
                    if (numDimensions <= 0) {
                        throw new PrestoException(JDBC_ERROR, "Didn't find dimensions for column: " + column);
                    }
                    else if (numDimensions > 0) {
                        JdbcTypeHandle jdbcTypeHandle = column.getJdbcTypeHandle();
                        JdbcTypeHandle typeHandleCopy = new JdbcTypeHandle(
                                jdbcTypeHandle.getJdbcType(),
                                jdbcTypeHandle.getJdbcTypeName(),
                                jdbcTypeHandle.getColumnSize(),
                                jdbcTypeHandle.getDecimalDigits(),
                                numDimensions);
                        Type arrayType = column.getColumnType();
                        while (numDimensions-- > 1) {
                            arrayType = new ArrayType(arrayType);
                        }
                        return new JdbcColumnHandle(column.getConnectorId(), column.getColumnName(), typeHandleCopy, arrayType);
                    }
                }
                return column;
                }).collect(toList());
        }
        return columns;
    }

    @Override
    public Optional<ColumnMapping> toPrestoType(ConnectorSession session, JdbcTypeHandle typeHandle)
    {
        switch (typeHandle.getJdbcType()) {
            case Types.ARRAY:
                try (Connection connection = connectionFactory.openConnection()) {
                    String jdbcTypeName = typeHandle.getJdbcTypeName();
                    //throw if no leading _
                    String baseJdbcTypeName = jdbcTypeName.replaceAll("_", "");
                    //TODO better way for getting base
//                    int jdbcType = connection.unwrap(PgConnection.class).getTypeInfo().getPGArrayElement();
                    PgConnection unwrap = connection.unwrap(PgConnection.class);
                    int baseJdbcType = unwrap.getTypeInfo().getSQLType(baseJdbcTypeName);
                    int arrayDimensions = typeHandle.getArrayDimensions();
                    JdbcTypeHandle baseTypeHandle = new JdbcTypeHandle(baseJdbcType, baseJdbcTypeName, typeHandle.getColumnSize(), typeHandle.getDecimalDigits(), arrayDimensions);

                    ColumnMapping elementMapping = toPrestoType(session, baseTypeHandle).get();
                    Type elementType = elementMapping.getType();
                    ArrayType arrayType = new ArrayType(elementType);
                    while (arrayDimensions-- > 1) {
                        arrayType = new ArrayType(arrayType);
                    }
                    
                    return Optional.of(
                            arrayColumnMapping(
                                    this,
                                    arrayType,
                                    baseTypeHandle
                                    ));
                } catch (SQLException e) {
                    throw new PrestoException(JDBC_ERROR, e);
                }
        }
        // TODO support PostgreSQL's TIMESTAMP WITH TIME ZONE and TIME WITH TIME ZONE explicitly, otherwise predicate pushdown for these types may be incorrect
        return super.toPrestoType(session, typeHandle);
    }



    


    
    // PostgreSql requires multidimensional arrays to have sub-arrays with matching dimensions, including arrays of null
    @Override
    protected void processArrayNulls(Object[] valuesArray, int lengthSeen) {
        for (int i = 0; i < valuesArray.length; i++) {
            if (valuesArray[i] == null) {
                valuesArray[i] = new Object[lengthSeen];
            }
        }
    }

    // PostgreSql jdbc array element names understood by org.postgresql.jdbc2.TypeInfoCache#getPGArrayType
    @Override
    protected String toArrayElementName(Type elementType) {
        String baseJdbcTypeName = super.toArrayElementName(elementType);
        if (DOUBLE.equals(elementType)) {
            baseJdbcTypeName = "float";
        }
        else if (REAL.equals(elementType)) {
            baseJdbcTypeName = "float4";
        }
        else if (TINYINT.equals(elementType)) {
            baseJdbcTypeName = "smallint";
        }
        else if (elementType instanceof VarcharType) {
            baseJdbcTypeName = "varchar";
        }
        else if (elementType instanceof CharType) {
            baseJdbcTypeName = "char";
        }
        else if (elementType instanceof DecimalType) {
            baseJdbcTypeName = "decimal";
        }
        else if (elementType instanceof ArrayType) {
            baseJdbcTypeName = toArrayElementName(((ArrayType) elementType).getElementType());
        }
        return baseJdbcTypeName;
    }

    @Override
    public WriteMapping toWriteMapping(Type type)
    {
        if (type instanceof ArrayType) {
            Type elementType = ((ArrayType) type).getElementType();
            String baseJdbcTypeName = toWriteMapping(elementType).getDataType();
            return WriteMapping.blockMapping(baseJdbcTypeName + "[]", arrayWriteFunction(this, elementType));
        }
        if (VARBINARY.equals(type)) {
            return WriteMapping.sliceMapping("bytea", varbinaryWriteFunction());
        }
        if (TINYINT.equals(type)) {
            return WriteMapping.longMapping("smallint", tinyintWriteFunction());
        }
        return super.toWriteMapping(type);
    }
}
