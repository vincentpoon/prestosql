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

import com.google.common.collect.ImmutableSet;
import io.prestosql.plugin.jdbc.BaseJdbcClient;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ColumnMapping;
import io.prestosql.plugin.jdbc.DriverConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcColumnHandle;
import io.prestosql.plugin.jdbc.JdbcIdentity;
import io.prestosql.plugin.jdbc.JdbcOutputTableHandle;
import io.prestosql.plugin.jdbc.JdbcSplit;
import io.prestosql.plugin.jdbc.JdbcTableHandle;
import io.prestosql.plugin.jdbc.JdbcTypeHandle;
import io.prestosql.plugin.jdbc.WriteMapping;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.type.Type;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.iterate.MapReduceParallelScanGrouper;
import org.apache.phoenix.jdbc.DelegatePreparedStatement;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixEmbeddedDriver;
import org.apache.phoenix.jdbc.PhoenixEmbeddedDriver.ConnectionInfo;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.mapreduce.PhoenixRecordReader;
import org.apache.phoenix.mapreduce.PhoenixRecordWritable;
import org.apache.phoenix.query.HBaseFactoryProvider;

import javax.inject.Inject;

import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static io.prestosql.plugin.jdbc.StandardColumnMappings.doubleWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.realWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.timeWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.timestampWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varcharColumnMapping;
import static io.prestosql.plugin.phoenix.MetadataUtil.toPhoenixSchemaName;
import static io.prestosql.plugin.phoenix.MetadataUtil.toPrestoSchemaName;
import static io.prestosql.plugin.phoenix.PhoenixErrorCode.PHOENIX_INTERNAL_ERROR;
import static io.prestosql.plugin.phoenix.PhoenixErrorCode.PHOENIX_QUERY_ERROR;
import static io.prestosql.plugin.phoenix.PhoenixMetadata.DEFAULT_SCHEMA;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.sql.Types.LONGNVARCHAR;
import static java.sql.Types.LONGVARCHAR;
import static java.sql.Types.NVARCHAR;
import static java.sql.Types.VARCHAR;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_SCHEM;
import static org.apache.phoenix.util.SchemaUtil.ESCAPE_CHARACTER;

public class PhoenixClient
        extends BaseJdbcClient
{
    private final Field resultSetField;
    private final Configuration hbaseConfig;

    @Inject
    public PhoenixClient(PhoenixConfig config)
            throws SQLException
    {
        super(
                new BaseJdbcConfig(),
                ESCAPE_CHARACTER,
                new DriverConnectionFactory(DriverManager.getDriver(config.getConnectionUrl()), config.getConnectionUrl(), config.getConnectionProperties()));
        requireNonNull(config, "config is null");
        try {
            Field field = PhoenixRecordReader.class.getDeclaredField("resultSet");
            field.setAccessible(true);
            this.resultSetField = field;
        }
        catch (NoSuchFieldException e) {
            throw new PrestoException(PHOENIX_INTERNAL_ERROR, e);
        }
        hbaseConfig = new Configuration();
        ConnectionInfo connectionInfo = PhoenixEmbeddedDriver.ConnectionInfo.create(config.getConnectionUrl());
        connectionInfo.asProps().forEach(prop -> hbaseConfig.set(prop.getKey(), prop.getValue()));
        config.getConnectionProperties().forEach((k, v) -> hbaseConfig.set((String) k, (String) v));
    }

    public PhoenixConnection getConnection(JdbcIdentity identity)
            throws SQLException
    {
        Connection connection = connectionFactory.openConnection(identity);
        try {
            return connection.unwrap(PhoenixConnection.class);
        }
        catch (Exception e) {
            try (Connection closingConnection = connection) {
                // use try with resources to close properly
            }
            throw new PrestoException(PHOENIX_QUERY_ERROR, "Couldn't open Phoenix connection", e);
        }
    }

    public org.apache.hadoop.hbase.client.Connection getHConnection()
            throws IOException
    {
        return HBaseFactoryProvider.getHConnectionFactory().createConnection(hbaseConfig);
    }

    public void execute(ConnectorSession session, String statement)
    {
        try (Connection connection = connectionFactory.openConnection(JdbcIdentity.from(session))) {
            execute(connection, statement);
        }
        catch (SQLException e) {
            throw new PrestoException(PHOENIX_QUERY_ERROR, "Error while executing statement", e);
        }
    }

    @Override
    public Set<String> getSchemaNames(JdbcIdentity identity)
    {
        ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
        schemaNames.add(DEFAULT_SCHEMA);
        schemaNames.addAll(super.getSchemaNames(identity));
        return schemaNames.build();
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
    public PreparedStatement buildSql(ConnectorSession session, Connection connection, JdbcSplit split, List<JdbcColumnHandle> columnHandles)
            throws SQLException
    {
        PreparedStatement query = super.buildSql(session, connection, split, columnHandles);
        QueryPlan queryPlan = getQueryPlan((PhoenixPreparedStatement) query);
        try {
            // PhoenixRecordReader constructs the ResultSet internally
            RecordReader<NullWritable, PhoenixRecordWritable> reader = new PhoenixRecordReader<>(PhoenixRecordWritable.class, new JobConf(), queryPlan);
            reader.initialize(((PhoenixSplit) split).getPhoenixInputSplit(), null);
            return new DelegatePreparedStatement(query)
            {
                @Override
                public ResultSet executeQuery()
                        throws SQLException
                {
                    try {
                        return (ResultSet) resultSetField.get(reader);
                    }
                    catch (IllegalAccessException e) {
                        throw new PrestoException(PHOENIX_INTERNAL_ERROR, e);
                    }
                }
            };
        }
        catch (IOException | InterruptedException e) {
            throw new PrestoException(PHOENIX_QUERY_ERROR, "Error while setting up Phoenix ResultSet", e);
        }
    }

    @Override
    public String buildInsertSql(JdbcOutputTableHandle handle)
    {
        PhoenixOutputTableHandle outputHandle = (PhoenixOutputTableHandle) handle;
        String params = join(",", nCopies(handle.getColumnNames().size(), "?"));
        if (outputHandle.hasUUIDRowkey()) {
            String nextId = format(
                    "NEXT VALUE FOR %s, ",
                    quoted(null, handle.getSchemaName(), handle.getTableName() + "_sequence"));
            params = nextId + params;
        }
        return format(
                "UPSERT INTO %s VALUES (%s)",
                quoted(null, handle.getSchemaName(), handle.getTableName()),
                params);
    }

    @Override
    protected ResultSet getTables(Connection connection, Optional<String> schemaName, Optional<String> tableName)
            throws SQLException
    {
        return super.getTables(connection, Optional.of(toPhoenixSchemaName(schemaName.orElse(null))), tableName);
    }

    @Override
    public Optional<ColumnMapping> toPrestoType(ConnectorSession session, JdbcTypeHandle typeHandle)
    {
        switch (typeHandle.getJdbcType()) {
            case VARCHAR:
            case NVARCHAR:
            case LONGVARCHAR:
            case LONGNVARCHAR:
                if (typeHandle.getColumnSize() == 0) {
                    return Optional.of(varcharColumnMapping(createUnboundedVarcharType()));
                }
                else {
                    return super.toPrestoType(session, typeHandle);
                }
        }
        return super.toPrestoType(session, typeHandle);
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (DOUBLE.equals(type)) {
            return WriteMapping.doubleMapping("double", doubleWriteFunction());
        }
        if (REAL.equals(type)) {
            return WriteMapping.longMapping("float", realWriteFunction());
        }
        if (TIME.equals(type) || TIME_WITH_TIME_ZONE.equals(type)) {
            return WriteMapping.longMapping("time", timeWriteFunction());
        }
        if (TIMESTAMP.equals(type) || TIMESTAMP_WITH_TIME_ZONE.equals(type)) {
            return WriteMapping.longMapping("timestamp", timestampWriteFunction());
        }
        return super.toWriteMapping(session, type);
    }

    public List<JdbcColumnHandle> getNonRowkeyColumns(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        return super.getColumns(session, tableHandle).stream().filter(handle -> !PhoenixMetadata.ROWKEY.equalsIgnoreCase(handle.getColumnName())).collect(Collectors.toList());
    }

    public String getCatalogName()
    {
        // catalogName in Phoenix is used for tenantId, but
        // tenant-specific connections not currently supported)
        return "";
    }

    public QueryPlan getQueryPlan(PhoenixPreparedStatement inputQuery)
    {
        try {
            // Optimize the query plan so that we potentially use secondary indexes
            QueryPlan queryPlan = inputQuery.optimizeQuery();
            // Initialize the query plan so it sets up the parallel scans
            queryPlan.iterator(MapReduceParallelScanGrouper.getInstance());
            return queryPlan;
        }
        catch (SQLException e) {
            throw new PrestoException(PHOENIX_QUERY_ERROR, "Failed to get the Phoenix query plan", e);
        }
    }
}
