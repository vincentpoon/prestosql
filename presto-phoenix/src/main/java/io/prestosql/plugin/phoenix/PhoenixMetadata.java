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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import io.airlift.slice.Slice;
import io.prestosql.plugin.jdbc.JdbcColumnHandle;
import io.prestosql.plugin.jdbc.JdbcMetadata;
import io.prestosql.plugin.jdbc.JdbcTableHandle;
import io.prestosql.plugin.jdbc.JdbcTypeHandle;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorInsertTableHandle;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorNewTableLayout;
import io.prestosql.spi.connector.ConnectorOutputMetadata;
import io.prestosql.spi.connector.ConnectorOutputTableHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableLayout;
import io.prestosql.spi.connector.ConnectorTableLayoutHandle;
import io.prestosql.spi.connector.ConnectorTableLayoutResult;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.statistics.ComputedStatistics;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.RowType.Field;
import io.prestosql.spi.type.Type;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.TableProperty;
import org.apache.phoenix.util.SchemaUtil;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.plugin.phoenix.MetadataUtil.getEscapedTableName;
import static io.prestosql.plugin.phoenix.MetadataUtil.toPhoenixSchemaName;
import static io.prestosql.plugin.phoenix.MetadataUtil.toPrestoSchemaName;
import static io.prestosql.plugin.phoenix.PhoenixErrorCode.PHOENIX_ERROR;
import static io.prestosql.plugin.phoenix.TypeUtils.toSqlType;
import static io.prestosql.spi.StandardErrorCode.NOT_FOUND;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.hbase.HConstants.FOREVER;
import static org.apache.phoenix.util.PhoenixRuntime.getTable;

public class PhoenixMetadata
        extends JdbcMetadata
        implements ConnectorMetadata
{
    // col name used for PK if none provided in DDL
    public static final String ROWKEY = "ROWKEY";
    // only used within Presto for DELETE
    public static final String UPDATE_ROW_ID = "PHOENIX_UPDATE_ROW_ID";
    // Maps to Phoenix's default empty schema
    public static final String DEFAULT_SCHEMA = "default";
    private final PhoenixClient phoenixClient;
    private final AtomicReference<Runnable> rollbackAction = new AtomicReference<>();

    public PhoenixMetadata(PhoenixClient phoenixClient)
    {
        super(phoenixClient, true);
        this.phoenixClient = requireNonNull(phoenixClient, "client is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
        schemaNames.add(DEFAULT_SCHEMA);
        schemaNames.addAll(phoenixClient.getSchemaNames());
        return ImmutableList.copyOf(schemaNames.build());
    }

    @Override
    public JdbcTableHandle getTableHandle(ConnectorSession session, SchemaTableName schemaTableName)
    {
        JdbcTableHandle tableHandle = phoenixClient.getTableHandle(schemaTableName);
        return tableHandle == null ? tableHandle : new JdbcTableHandle(
                tableHandle.getConnectorId(),
                schemaTableName,
                tableHandle.getCatalogName(),
                toPrestoSchemaName(tableHandle.getSchemaName()),
                tableHandle.getTableName());
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        JdbcTableHandle tableHandle = (JdbcTableHandle) table;
        ConnectorTableLayout layout = new ConnectorTableLayout(new PhoenixTableLayoutHandle(tableHandle, constraint.getSummary(), desiredColumns));
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        return getTableMetadata(session, table, false);
    }

    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table, boolean rowkeyRequired)
    {
        JdbcTableHandle handle = (JdbcTableHandle) table;

        ImmutableList.Builder<ColumnMetadata> columnMetadata = ImmutableList.builder();
        List<JdbcColumnHandle> columns = rowkeyRequired ? phoenixClient.getColumns(session, handle) : phoenixClient.getNonRowkeyColumns(session, handle);
        for (JdbcColumnHandle column : columns) {
            columnMetadata.add(column.getColumnMetadata());
        }
        return new ConnectorTableMetadata(handle.getSchemaTableName(), columnMetadata.build());
    }

    public Map<String, Object> getTableProperties(JdbcTableHandle handle)
    {
        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();

        try (PhoenixConnection pconn = phoenixClient.getConnection(); HBaseAdmin admin = pconn.getQueryServices().getAdmin()) {
            PTable table = getTable(pconn, SchemaUtil.getTableName(toPhoenixSchemaName(handle.getSchemaName()), handle.getTableName()));

            if (table.getBucketNum() != null) {
                properties.put(PhoenixTableProperties.SALT_BUCKETS, table.getBucketNum());
            }
            if (table.isWALDisabled()) {
                properties.put(PhoenixTableProperties.DISABLE_WAL, table.isWALDisabled());
            }
            if (table.isImmutableRows()) {
                properties.put(PhoenixTableProperties.IMMUTABLE_ROWS, table.isImmutableRows());
            }

            String defaultFamilyName = table.getDefaultFamilyName() == null ? QueryConstants.DEFAULT_COLUMN_FAMILY : table.getDefaultFamilyName().getString();
            if (table.getDefaultFamilyName() != null) {
                properties.put(PhoenixTableProperties.DEFAULT_COLUMN_FAMILY, defaultFamilyName);
            }

            HTableDescriptor tableDesc = admin.getTableDescriptor(table.getPhysicalName().getBytes());

            HColumnDescriptor[] columnFamilies = tableDesc.getColumnFamilies();
            for (HColumnDescriptor columnFamily : columnFamilies) {
                if (columnFamily.getNameAsString().equals(defaultFamilyName)) {
                    if (!columnFamily.getBloomFilterType().toString().equals("NONE")) {
                        properties.put(PhoenixTableProperties.BLOOMFILTER, columnFamily.getBloomFilterType().toString());
                    }
                    if (columnFamily.getMaxVersions() != 1) {
                        properties.put(PhoenixTableProperties.VERSIONS, columnFamily.getMaxVersions());
                    }
                    if (columnFamily.getMinVersions() > 0) {
                        properties.put(PhoenixTableProperties.MIN_VERSIONS, columnFamily.getMinVersions());
                    }
                    if (!columnFamily.getCompression().toString().equals("NONE")) {
                        properties.put(PhoenixTableProperties.COMPRESSION, columnFamily.getCompression().toString());
                    }
                    if (columnFamily.getTimeToLive() < FOREVER) {
                        properties.put(PhoenixTableProperties.TTL, columnFamily.getTimeToLive());
                    }
                    break;
                }
            }
        }
        catch (IOException | SQLException e) {
            throw new PrestoException(PHOENIX_ERROR, e);
        }
        return properties.build();
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties)
    {
        Preconditions.checkArgument(!DEFAULT_SCHEMA.equalsIgnoreCase(schemaName), "Can't create 'default' schema which maps to Phoenix empty schema");
        StringBuilder sql = new StringBuilder()
                .append("CREATE SCHEMA ")
                .append(schemaName);

        phoenixClient.execute(sql.toString());
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName)
    {
        Preconditions.checkArgument(!DEFAULT_SCHEMA.equalsIgnoreCase(schemaName), "Can't delete 'default' schema which maps to Phoenix empty schema");
        StringBuilder sql = new StringBuilder()
                .append("DROP SCHEMA ")
                .append(schemaName);

        phoenixClient.execute(sql.toString());
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        JdbcTableHandle existingTable = getTableHandle(session, tableMetadata.getTable());
        if (existingTable != null && !ignoreExisting) {
            throw new IllegalArgumentException("Target table already exists: " + tableMetadata.getTable());
        }
        createTable(tableMetadata);
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        PhoenixOutputTableHandle handle = createTable(tableMetadata);
        setRollback(() -> dropTable(
                session,
                new JdbcTableHandle(
                        phoenixClient.getConnectorId(),
                        new SchemaTableName(handle.getSchemaName(), handle.getTableName()),
                        phoenixClient.getCatalogName(),
                        handle.getSchemaName(),
                        handle.getTableName())));
        return handle;
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        clearRollback();
        return Optional.empty();
    }

    private void clearRollback()
    {
        rollbackAction.set(null);
    }

    private void setRollback(Runnable action)
    {
        checkState(rollbackAction.compareAndSet(null, action), "rollback action is already set");
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        JdbcTableHandle handle = (JdbcTableHandle) tableHandle;
        ConnectorTableMetadata tableMetadata = getTableMetadata(session, tableHandle, true);
        return new PhoenixOutputTableHandle(
                handle.getSchemaName(),
                handle.getTableName(),
                tableMetadata.getColumns().stream().map(ColumnMetadata::getName).collect(Collectors.toList()),
                tableMetadata.getColumns().stream().map(ColumnMetadata::getType).collect(Collectors.toList()));
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return Optional.empty();
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column)
    {
        JdbcTableHandle handle = (JdbcTableHandle) tableHandle;
        StringBuilder sql = new StringBuilder()
                .append("ALTER TABLE ")
                .append(getEscapedTableName(handle.getSchemaName(), handle.getTableName()))
                .append(" ADD ").append(column.getName()).append(" ").append(toSqlType(column.getType()));

        phoenixClient.execute(sql.toString());
    }

    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        JdbcTableHandle handle = (JdbcTableHandle) tableHandle;
        JdbcColumnHandle columnHandle = (JdbcColumnHandle) column;
        StringBuilder sql = new StringBuilder()
                .append("ALTER TABLE ")
                .append(getEscapedTableName(handle.getSchemaName(), handle.getTableName()))
                .append(" DROP COLUMN ").append(columnHandle.getColumnName());

        phoenixClient.execute(sql.toString());
    }

    @Override
    public ColumnHandle getUpdateRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        JdbcTableHandle handle = (JdbcTableHandle) tableHandle;
        Map<String, JdbcColumnHandle> namesToHandles =
                Maps.uniqueIndex(phoenixClient.getColumns(session, handle), mapHandle -> mapHandle.getColumnName());
        try (PhoenixConnection connection = phoenixClient.getConnection();
                ResultSet resultSet =
                        connection.getMetaData().getPrimaryKeys("", toPhoenixSchemaName(handle.getSchemaName()), handle.getTableName())) {
            List<Field> fields = new ArrayList<>();
            while (resultSet.next()) {
                String columnName = resultSet.getString("COLUMN_NAME");
                JdbcColumnHandle columnHandle = namesToHandles.get(columnName);
                if (columnHandle == null) {
                    throw new PrestoException(NOT_FOUND, "Couldn't find metadata for column: " + columnName);
                }
                fields.add(RowType.field(columnName, columnHandle.getColumnType()));
            }
            if (fields.isEmpty()) {
                throw new PrestoException(NOT_FOUND, "No PK fields found for table " + handle.getTableName());
            }
            RowType rowType = RowType.from(fields);
            return new JdbcColumnHandle(
                    phoenixClient.getConnectorId(),
                    UPDATE_ROW_ID,
                    new JdbcTypeHandle(Types.ROWID, "ROWID", -1, 0),
                    rowType);
        }
        catch (SQLException e) {
            throw new PrestoException(PHOENIX_ERROR, e);
        }
    }

    @Override
    public ConnectorTableHandle beginDelete(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return tableHandle;
    }

    @Override
    public void finishDelete(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<Slice> fragments)
    {
    }

    @Override
    public boolean supportsMetadataDelete(ConnectorSession session, ConnectorTableHandle tableHandle, ConnectorTableLayoutHandle tableLayoutHandle)
    {
        //TODO add when implemented in Phoenix
        return false;
    }

    public PhoenixOutputTableHandle createTable(ConnectorTableMetadata tableMetadata)
    {
        SchemaTableName schemaTableName = tableMetadata.getTable();
        LinkedList<ColumnMetadata> tableColumns = new LinkedList<>(tableMetadata.getColumns());
        String schema = schemaTableName.getSchemaName().toUpperCase(ENGLISH);
        String table = schemaTableName.getTableName().toUpperCase(ENGLISH);

        Map<String, Object> tableProperties = tableMetadata.getProperties();

        StringBuilder sql = new StringBuilder();
        Optional<Boolean> immutableRows = PhoenixTableProperties.getImmutableRows(tableProperties);
        if (immutableRows.isPresent() && immutableRows.get()) {
            sql.append("CREATE IMMUTABLE TABLE ");
        }
        else {
            sql.append("CREATE TABLE ");
        }
        sql.append(getEscapedTableName(schema, table)).append(" (\n ");
        ImmutableList.Builder<String> columnNames = ImmutableList.builder();
        ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();
        ImmutableList.Builder<String> columnList = ImmutableList.builder();
        Set<ColumnMetadata> pkCols = tableColumns.stream().filter(col -> PhoenixTableProperties.isPrimaryKey(col, tableProperties)).collect(Collectors.toSet());
        if (pkCols.isEmpty()) {
            // Add a rowkey when not specified in DDL
            ColumnMetadata rowKeyCol = new ColumnMetadata(ROWKEY, VARCHAR);
            tableColumns.addFirst(rowKeyCol);
            pkCols.add(rowKeyCol);
        }
        List<String> pkNames = new ArrayList<>();

        for (ColumnMetadata column : tableColumns) {
            String columnName = column.getName().toUpperCase(ENGLISH);
            columnNames.add(columnName);
            columnTypes.add(column.getType());
            String typeStatement = toSqlType(column.getType());
            if (pkCols.contains(column)) {
                typeStatement += " not null";
                pkNames.add(columnName);
            }
            columnList.add(new StringBuilder()
                    .append(columnName)
                    .append(" ")
                    .append(typeStatement)
                    .toString());
        }

        List<String> columns = columnList.build();
        Joiner.on(", \n ").appendTo(sql, columns);
        sql.append("\n CONSTRAINT PK PRIMARY KEY(");
        Joiner.on(", ").appendTo(sql, pkNames);
        sql.append(")\n)\n");

        ImmutableList.Builder<String> tableOptions = ImmutableList.builder();
        PhoenixTableProperties.getSaltBuckets(tableProperties).ifPresent(value -> tableOptions.add(TableProperty.SALT_BUCKETS + "=" + value));
        PhoenixTableProperties.getSplitOn(tableProperties).ifPresent(value -> tableOptions.add("SPLIT ON (" + value.replace('"', '\'') + ")"));
        PhoenixTableProperties.getDisableWal(tableProperties).ifPresent(value -> tableOptions.add(TableProperty.DISABLE_WAL + "=" + value));
        PhoenixTableProperties.getDefaultColumnFamily(tableProperties).ifPresent(value -> tableOptions.add(TableProperty.DEFAULT_COLUMN_FAMILY + "=" + value));
        PhoenixTableProperties.getBloomfilter(tableProperties).ifPresent(value -> tableOptions.add(HColumnDescriptor.BLOOMFILTER + "='" + value + "'"));
        PhoenixTableProperties.getVersions(tableProperties).ifPresent(value -> tableOptions.add(HConstants.VERSIONS + "=" + value));
        PhoenixTableProperties.getMinVersions(tableProperties).ifPresent(value -> tableOptions.add(HColumnDescriptor.MIN_VERSIONS + "=" + value));
        PhoenixTableProperties.getCompression(tableProperties).ifPresent(value -> tableOptions.add(HColumnDescriptor.COMPRESSION + "='" + value + "'"));
        PhoenixTableProperties.getTimeToLive(tableProperties).ifPresent(value -> tableOptions.add(HColumnDescriptor.TTL + "=" + value));
        Joiner.on(", \n ").appendTo(sql, tableOptions.build());

        phoenixClient.execute(sql.toString());

        return new PhoenixOutputTableHandle(
                schema,
                table,
                columnNames.build(),
                columnTypes.build());
    }
}
