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

import io.airlift.log.Logger;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.predicate.TupleDomain;
import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixEmbeddedDriver;
import org.apache.phoenix.jdbc.PhoenixEmbeddedDriver.ConnectionInfo;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static io.prestosql.plugin.phoenix.PhoenixErrorCode.PHOENIX_ERROR;
import static java.util.Objects.requireNonNull;

public class PhoenixClient
{
    private static final Logger log = Logger.get(PhoenixClient.class);

    private final Driver driver;
    private final String connectionUrl;
    private final Properties connectionProperties;

    @Inject
    public PhoenixClient(PhoenixConfig config)
            throws SQLException
    {
        requireNonNull(config, "config is null");
        connectionUrl = config.getConnectionUrl();
        driver = DriverManager.getDriver(connectionUrl);
        connectionProperties = new Properties();
        connectionProperties.putAll(config.getConnectionProperties());
    }

    public PhoenixConnection getConnection()
            throws SQLException
    {
        Connection connection = driver.connect(connectionUrl, connectionProperties);
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
        try (PhoenixConnection connection = getConnection()) {
            execute(connection, query);
        }
        catch (SQLException e) {
            throw new PrestoException(PHOENIX_ERROR, e);
        }
    }

    protected void execute(PhoenixConnection connection, String query)
            throws SQLException
    {
        try (Statement statement = connection.createStatement()) {
            log.debug("Execute: %s", query);
            statement.execute(query);
        }
    }

    public String buildSql(
            PhoenixConnection connection,
            String schemaName,
            String tableName,
            Optional<Set<ColumnHandle>> desiredColumns,
            TupleDomain<ColumnHandle> tupleDomain,
            List<PhoenixColumnHandle> columnHandles)
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

    public void setJobQueryConfig(String inputQuery, Configuration conf)
            throws SQLException
    {
        ConnectionInfo connectionInfo = PhoenixEmbeddedDriver.ConnectionInfo.create(connectionUrl);
        connectionInfo.asProps().forEach(prop -> conf.set(prop.getKey(), prop.getValue()));
        connectionProperties.forEach((k, v) -> conf.set((String) k, (String) v));
        PhoenixConfigurationUtil.setInputQuery(conf, inputQuery);
    }
}
