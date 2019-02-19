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

import io.prestosql.plugin.jdbc.BaseJdbcClient;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.BlockWriteFunction;
import io.prestosql.plugin.jdbc.ColumnMapping;
import io.prestosql.plugin.jdbc.DriverConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcConnectorId;
import io.prestosql.plugin.jdbc.JdbcOutputTableHandle;
import io.prestosql.plugin.jdbc.JdbcTypeHandle;
import io.prestosql.plugin.jdbc.StandardColumnMappings;
import io.prestosql.plugin.jdbc.WriteMapping;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.IntArrayBlock;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.Type;
import org.postgresql.Driver;
import org.postgresql.jdbc.PgConnection;

import javax.inject.Inject;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Optional;

import static io.prestosql.plugin.jdbc.StandardColumnMappings.varbinaryWriteFunction;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static java.lang.String.format;
import static java.lang.reflect.Array.get;
import static java.lang.reflect.Array.getLength;

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
    public Optional<ColumnMapping> toPrestoType(ConnectorSession session, JdbcTypeHandle typeHandle)
    {
        switch (typeHandle.getJdbcType()) {
            case Types.ARRAY:
                try (Connection connection = connectionFactory.openConnection()) {
                    String jdbcTypeName = typeHandle.getJdbcTypeName();
                    //throw if no leading _
                    String baseJdbcTypeName = jdbcTypeName.replaceFirst("_", "");
                    //TODO better way for getting base
//                    int jdbcType = connection.unwrap(PgConnection.class).getTypeInfo().getPGArrayElement();
                    int baseJdbcType = connection.unwrap(PgConnection.class).getTypeInfo().getSQLType(baseJdbcTypeName);
                    JdbcTypeHandle baseTypeHandle = new JdbcTypeHandle(baseJdbcType, baseJdbcTypeName, typeHandle.getColumnSize(), typeHandle.getDecimalDigits());
                    ColumnMapping elementMapping = toPrestoType(session, baseTypeHandle).get();
                    Type elementType = elementMapping.getType();
                    return Optional.of(StandardColumnMappings.arrayColumnMapping(new ArrayType(elementType),
                        (resultSet, columnIndex) -> {
                            Array jdbcArray = resultSet.getArray(columnIndex);
                            Object[] array = toBoxedArray(jdbcArray.getArray());
                            if (array != null) {
                                BlockBuilder blockBuilder = elementType.createBlockBuilder(null, array.length);
                                for (int i = 0; i < array.length; i++) {
                                    blockBuilder.writeInt((int) array[i]);
                                }
                                return blockBuilder.build();
                            }
                            return null;
                            
//                            switch(baseJdbcType)
//                            {
//                            case Types.INTEGER:
//                                
//                            }
                        },
                        arrayWriteFunction(baseJdbcTypeName)));
                } catch (SQLException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
        }
        // TODO support PostgreSQL's TIMESTAMP WITH TIME ZONE and TIME WITH TIME ZONE explicitly, otherwise predicate pushdown for these types may be incorrect
        return super.toPrestoType(session, typeHandle);
    }
    
    private Object[] toBoxedArray(Object jdbcArray)
    {
        if (!jdbcArray.getClass().isArray()) {
            return null;
        }

        if (!jdbcArray.getClass().getComponentType().isPrimitive()) {
            return (Object[]) jdbcArray;
        }

        int elementCount = getLength(jdbcArray);
        Object[] elements = new Object[elementCount];

        for (int i = 0; i < elementCount; i++) {
            elements[i] = get(jdbcArray, i);
        }

        return elements;
    }

    private BlockWriteFunction arrayWriteFunction(String baseJdbcTypeName) {
        return (connection, statement, index, block) -> {
            System.out.println("In Array Write");
            if (block instanceof IntArrayBlock) {
                int positionCount = block.getPositionCount();
                Object[] valuesArray = new Object[positionCount];
                for (int i = 0; i < positionCount; i++) {
                    int val = block.getInt(i, 0);
                    valuesArray[i] = val;
                }
                Array jdbcArray = connection.createArrayOf(baseJdbcTypeName, valuesArray);
                statement.setArray(index, jdbcArray);
            }
//                            statement.setArray(parameterIndex, x);
        };
    }

    @Override
    public WriteMapping toWriteMapping(Type type)
    {
        if (type instanceof ArrayType) {
            WriteMapping elementMapping = toWriteMapping(((ArrayType) type).getElementType());
            
            return WriteMapping.blockMapping(elementMapping.getDataType() + "[]", arrayWriteFunction(elementMapping.getDataType()));
        }
        if (VARBINARY.equals(type)) {
            return WriteMapping.sliceMapping("bytea", varbinaryWriteFunction());
        }

        return super.toWriteMapping(type);
    }
}
