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
package io.prestosql.plugin.phoenix;

import io.prestosql.plugin.jdbc.BaseJdbcClient;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ColumnMapping;
import io.prestosql.plugin.jdbc.DriverConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcColumnHandle;
import io.prestosql.plugin.jdbc.JdbcConnectorId;
import io.prestosql.plugin.jdbc.JdbcTableHandle;
import io.prestosql.plugin.jdbc.JdbcTypeHandle;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.predicate.TupleDomain;
import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixEmbeddedDriver;
import org.apache.phoenix.jdbc.PhoenixEmbeddedDriver.ConnectionInfo;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.schema.types.PDataType;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static io.prestosql.plugin.jdbc.StandardColumnMappings.arrayColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varcharColumnMapping;
import static io.prestosql.plugin.phoenix.MetadataUtil.toPhoenixSchemaName;
import static io.prestosql.plugin.phoenix.MetadataUtil.toPrestoSchemaName;
import static io.prestosql.plugin.phoenix.PhoenixErrorCode.PHOENIX_ERROR;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Objects.requireNonNull;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_SCHEM;

public class PhoenixClient
        extends BaseJdbcClient
{
    private final PhoenixConfig config;

    @Inject
    public PhoenixClient(JdbcConnectorId connectorId, PhoenixConfig config)
            throws SQLException
    {
        super(connectorId, new BaseJdbcConfig(), "\"", new DriverConnectionFactory(DriverManager.getDriver(config.getConnectionUrl()), config.getConnectionUrl(), config.getConnectionProperties()));
        this.config = requireNonNull(config, "config is null");
    }

    public PhoenixConnection getConnection()
            throws SQLException
    {
        Connection connection = connectionFactory.openConnection();
        try {
            return connection.unwrap(PhoenixConnection.class);
        }
        catch (Exception e) {
            connection.close();
            throw e;
        }
    }

    protected void execute(String query)
    {
        try (Connection connection = connectionFactory.openConnection()) {
            execute(connection, query);
        }
        catch (SQLException e) {
            throw new PrestoException(PHOENIX_ERROR, e);
        }
    }

    @Override
    protected SchemaTableName getSchemaTableName(ResultSet resultSet)
            throws SQLException
    {
        return new SchemaTableName(
                toPrestoSchemaName(resultSet.getString(TABLE_SCHEM)),
                resultSet.getString(TABLE_NAME));
    }

    @Override
    protected ResultSet getTables(Connection connection, String schemaName, String tableName)
            throws SQLException
    {
        return super.getTables(connection, toPhoenixSchemaName(schemaName), tableName);
    }

    @Override
    public Optional<ColumnMapping> toPrestoType(ConnectorSession session, JdbcTypeHandle typeHandle)
    {
        switch (typeHandle.getJdbcType()) {
            case Types.ARRAY:
                int elementTypeId = PDataType.fromSqlTypeName(typeHandle.getJdbcTypeName()).getSqlType() - PDataType.ARRAY_TYPE_BASE;
                String elementTypeName = PDataType.fromTypeId(elementTypeId).getSqlTypeName();
                JdbcTypeHandle elementTypeHandle = new JdbcTypeHandle(elementTypeId, elementTypeName, typeHandle.getColumnSize(), typeHandle.getDecimalDigits());
                Optional<ColumnMapping> elementMapping = super.toPrestoType(session, elementTypeHandle);
                return elementMapping.map(elementMap -> arrayColumnMapping(elementMap.getType()));
            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR:
                if (typeHandle.getColumnSize() == 0) {
                    return Optional.of(varcharColumnMapping(createUnboundedVarcharType()));
                }
                else {
                    return super.toPrestoType(session, typeHandle);
                }
        }
        return super.toPrestoType(session, typeHandle);
    }

    public List<JdbcColumnHandle> getNonRowkeyColumns(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        return super.getColumns(session, tableHandle).stream().filter(handle -> !PhoenixMetadata.ROWKEY.equals(handle.getColumnName())).collect(Collectors.toList());
    }

    public String buildSql(
            PhoenixConnection connection,
            String schemaName,
            String tableName,
            Optional<Set<ColumnHandle>> desiredColumns,
            TupleDomain<ColumnHandle> tupleDomain,
            List<JdbcColumnHandle> columnHandles)
            throws SQLException
    {
        return QueryBuilder.buildSql(
                connection,
                schemaName,
                tableName,
                desiredColumns,
                columnHandles,
                tupleDomain);
    }

    public String getConnectorId()
    {
        return connectorId;
    }

    public String getCatalogName()
    {
        // catalogName in Phoenix is used for tenantId
        // TODO tenant-specific connections not currently supported)
        return "";
    }

    public void setJobQueryConfig(String inputQuery, Configuration conf)
            throws SQLException
    {
        ConnectionInfo connectionInfo = PhoenixEmbeddedDriver.ConnectionInfo.create(config.getConnectionUrl());
        connectionInfo.asProps().forEach(prop -> conf.set(prop.getKey(), prop.getValue()));
        config.getConnectionProperties().forEach((k, v) -> conf.set((String) k, (String) v));
        PhoenixConfigurationUtil.setInputQuery(conf, inputQuery);
    }
}
