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

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.tpch.TpchTable;
import io.prestosql.Session;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.plugin.tpch.TpchPlugin;
import io.prestosql.testing.QueryRunner;
import io.prestosql.tests.DistributedQueryRunner;
import org.intellij.lang.annotations.Language;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import static io.prestosql.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;

public final class PhoenixQueryRunner
{
    private static final Logger LOG = Logger.get(PhoenixQueryRunner.class);
    private static final String TPCH_SCHEMA = "tpch";

    private static boolean tpchLoaded;
    private static TestingPhoenixServer server = new TestingPhoenixServer();

    private PhoenixQueryRunner()
    {
    }

    public static QueryRunner createPhoenixQueryRunner(Map<String, String> extraProperties, List<TpchTable<?>> tables)
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(createSession())
                .setNodeCount(4)
                .setExtraProperties(extraProperties).build();

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("connection-url", server.getJdbcUrl())
                .build();

        queryRunner.installPlugin(new PhoenixPlugin());
        queryRunner.createCatalog("phoenix", "phoenix", properties);

        if (!tpchLoaded) {
            createSchema(server, TPCH_SCHEMA);
            copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, createSession(), tables);
            tpchLoaded = true;
        }

        return queryRunner;
    }

    public static void createSchema(TestingPhoenixServer phoenixServer, String schema)
            throws SQLException
    {
        try (Connection connection = DriverManager.getConnection(phoenixServer.getJdbcUrl());
                Statement statement = connection.createStatement()) {
            statement.execute(format("CREATE SCHEMA %s", schema));
        }
        catch (Exception e) {
            throw new IllegalStateException("Can't create schema: " + schema, e);
        }
    }

    private static void copyTpchTables(
            QueryRunner queryRunner,
            String sourceCatalog,
            String sourceSchema,
            Session session,
            Iterable<TpchTable<?>> tables)
    {
        LOG.debug("Loading data from %s.%s...", sourceCatalog, sourceSchema);
        for (TpchTable<?> table : tables) {
            copyTable(queryRunner, sourceCatalog, session, sourceSchema, table);
        }
    }

    private static void copyTable(
            QueryRunner queryRunner,
            String catalog,
            Session session,
            String schema,
            TpchTable<?> table)
    {
        QualifiedObjectName source = new QualifiedObjectName(catalog, schema, table.getTableName());
        String target = table.getTableName();

        @Language("SQL")
        String sql;
        switch (target) {
            case "customer":
                sql = format("CREATE TABLE %s AS SELECT * FROM %s", target, source);
                break;
            case "lineitem":
                sql = format("CREATE TABLE %s WITH (ROWKEYS = 'ORDERKEY,LINENUMBER', SALT_BUCKETS=10) AS SELECT * FROM %s", target, source);
                break;
            case "orders":
                sql = format("CREATE TABLE %s WITH (SALT_BUCKETS=10) AS SELECT * FROM %s", target, source);
                break;
            case "part":
                sql = format("CREATE TABLE %s AS SELECT * FROM %s", target, source);
                break;
            case "partsupp":
                sql = format("CREATE TABLE %s WITH (ROWKEYS = 'PARTKEY,SUPPKEY') AS SELECT * FROM %s", target, source);
                break;
            case "supplier":
                sql = format("CREATE TABLE %s AS SELECT * FROM %s", target, source);
                break;
            default:
                sql = format("CREATE TABLE %s AS SELECT * FROM %s", target, source);
                break;
        }

        LOG.debug("Running import for %s %s", target, sql);
        long rows = queryRunner.execute(session, sql).getUpdateCount().getAsLong();
        LOG.debug("%s rows loaded into %s", rows, target);
    }

    public static Session createSession()
    {
        return testSessionBuilder()
                .setCatalog("phoenix")
                .setSchema(TPCH_SCHEMA)
                .build();
    }
}
