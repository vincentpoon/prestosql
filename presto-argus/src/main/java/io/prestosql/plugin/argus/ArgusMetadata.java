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
package io.prestosql.plugin.argus;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorTableProperties;
import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.connector.ConstraintApplicationResult;
import io.prestosql.spi.connector.LimitApplicationResult;
import io.prestosql.spi.connector.ProjectionApplicationResult;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.SchemaTablePrefix;
import io.prestosql.spi.expression.ConnectorExpression;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.MapType;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignatureParameter;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.plugin.argus.MetadataUtil.DISCOVERY_TABLE_NAME;
import static io.prestosql.plugin.argus.MetadataUtil.METRIC_COLUMN_HANDLE;
import static io.prestosql.plugin.argus.MetadataUtil.METRIC_REGEX;
import static io.prestosql.plugin.argus.MetadataUtil.SCOPE_COLUMN_HANDLE;
import static io.prestosql.plugin.argus.MetadataUtil.SCOPE_REGEX;
import static io.prestosql.plugin.argus.MetadataUtil.SYSTEM_SCHEMA_NAME;
import static io.prestosql.plugin.argus.MetadataUtil.TAG_KEY;
import static io.prestosql.plugin.argus.MetadataUtil.TAG_KEY_REGEX;
import static io.prestosql.plugin.argus.MetadataUtil.TAG_VALUE;
import static io.prestosql.plugin.argus.MetadataUtil.TAG_VALUE_REGEX;
import static io.prestosql.plugin.argus.MetadataUtil.isSystemSchema;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

public abstract class ArgusMetadata
        implements ConnectorMetadata
{
    public static ArgusSystemTable discoveryTable = new ArgusSystemTable(
            DISCOVERY_TABLE_NAME,
            "Discover schema records",
            ImmutableList.of(
                    SCOPE_COLUMN_HANDLE,
                    METRIC_COLUMN_HANDLE,
                    new ArgusColumnHandle(TAG_KEY, createUnboundedVarcharType()),
                    new ArgusColumnHandle(TAG_VALUE, createUnboundedVarcharType()),
                    new ArgusColumnHandle(SCOPE_REGEX, createUnboundedVarcharType()),
                    new ArgusColumnHandle(METRIC_REGEX, createUnboundedVarcharType()),
                    new ArgusColumnHandle(TAG_KEY_REGEX, createUnboundedVarcharType()),
                    new ArgusColumnHandle(TAG_VALUE_REGEX, createUnboundedVarcharType())));
    protected final TypeManager typeManager;
    protected final Set<ArgusSystemTable> systemTables;
    protected final ArgusSystemTable mappedMetricsTable;

    public ArgusMetadata(TypeManager typeManager, Set<ArgusSystemTable> systemTables, ArgusSystemTable mappedMetricsTable)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.mappedMetricsTable = requireNonNull(mappedMetricsTable, "mappedMetricsTable is null");
        this.systemTables = requireNonNull(systemTables, "systemTables is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.of(SYSTEM_SCHEMA_NAME);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        if (schemaName.isPresent() && isSystemSchema(schemaName.get())) {
            ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
            for (ArgusSystemTable systemTable : systemTables) {
                builder.add(new SchemaTableName(schemaName.get(), systemTable.toString()));
            }
            return builder.build();
        }
        return emptyList();
    }

    @Override
    public ArgusTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        return new ArgusTableHandle(tableName);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        ArgusTableHandle argusTableHandle = (ArgusTableHandle) table;
        SchemaTableName schemaTableName = argusTableHandle.getSchemaTableName();
        if (isSystemSchema(schemaTableName.getSchemaName())) {
            List<ColumnMetadata> columnMetadata = getSystemTableColumns(argusTableHandle).stream()
                    .map(ArgusColumnHandle::getColumnMetadata)
                    .collect(toImmutableList());
            return new ConnectorTableMetadata(
                    schemaTableName,
                    columnMetadata,
                    ImmutableMap.of(),
                    Optional.of(getArgusSystemTable(schemaTableName.getTableName()).getComment()));
        }
        //TODO
        throw new IllegalArgumentException(table.toString());
    }

    private List<ArgusColumnHandle> getSystemTableColumns(ArgusTableHandle argusTableHandle)
    {
        String tableName = argusTableHandle.getSchemaTableName().getTableName();
        return getArgusSystemTable(tableName).getColumns();
    }

    public ArgusSystemTable getArgusSystemTable(String tableName)
    {
        return systemTables.stream()
                .filter(systemTable -> systemTable.getTableName().equals(tableName))
                .findFirst()
                .orElseThrow(() -> new PrestoException(ArgusErrorCode.ARGUS_INTERNAL_ERROR, "Didn't find system table for: " + tableName));
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ArgusTableHandle argusTableHandle = (ArgusTableHandle) tableHandle;
        String schemaName = argusTableHandle.getSchemaTableName().getSchemaName();
        if (isSystemSchema(schemaName)) {
            ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
            for (ArgusColumnHandle column : getSystemTableColumns(argusTableHandle)) {
                columnHandles.put(column.getColumnName(), column);
            }
            return columnHandles.build();
        }
        return null;
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((ArgusColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        List<SchemaTableName> tables = prefix.toOptionalSchemaTableName()
                .<List<SchemaTableName>>map(ImmutableList::of)
                .orElseGet(() -> listTables(session, prefix.getSchema()));
        for (SchemaTableName tableName : tables) {
            ArgusTableHandle tableHandle = getTableHandle(session, tableName);
            columns.put(tableName, getTableMetadata(session, tableHandle).getColumns());
        }
        return columns.build();
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        return new ConnectorTableProperties();
    }

    @Override
    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(
            ConnectorSession session, ConnectorTableHandle handle,
            List<ConnectorExpression> projections, Map<String, ColumnHandle> assignments)
    {
        // TODO ideally, use this to filter datapoint values once expression pushdown is supported
        // e.g. map_filter(datapoints, (k,v) -> v > 99)
        return ConnectorMetadata.super.applyProjection(session, handle, projections, assignments);
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle table, Constraint constraint)
    {
        ArgusTableHandle handle = (ArgusTableHandle) table;
        TupleDomain<ColumnHandle> oldDomain = handle.getConstraint();
        TupleDomain<ColumnHandle> newDomain = oldDomain.intersect(constraint.getSummary());
        if (oldDomain.equals(newDomain)) {
            return Optional.empty();
        }

        handle = new ArgusTableHandle(
                handle.getSchemaTableName(),
                newDomain,
                handle.getLimit());

        return Optional.of(new ConstraintApplicationResult<>(handle, TupleDomain.all()));
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(ConnectorSession session, ConnectorTableHandle table, long limit)
    {
        ArgusTableHandle handle = (ArgusTableHandle) table;

        if (handle.getLimit().isPresent() && handle.getLimit().getAsLong() <= limit) {
            return Optional.empty();
        }

        handle = new ArgusTableHandle(
                handle.getSchemaTableName(),
                handle.getConstraint(),
                OptionalLong.of(limit));

        boolean limitGuaranteed = false;
        if (isSystemSchema(handle.getSchemaTableName().getSchemaName())) {
            if (MetadataUtil.DISCOVERY_TABLE_NAME.equals(handle.getSchemaTableName().getTableName())) {
                limitGuaranteed = true;
            }
        }
        return Optional.of(new LimitApplicationResult<>(handle, limitGuaranteed));
    }

    public static MapType mapType(TypeManager typeManager, Type keyType, Type valueType)
    {
        return (MapType) typeManager.getParameterizedType(StandardTypes.MAP, ImmutableList.of(
                TypeSignatureParameter.of(keyType.getTypeSignature()),
                TypeSignatureParameter.of(valueType.getTypeSignature())));
    }

    @Override
    public boolean usesLegacyTableLayouts()
    {
        return false;
    }

    public ArgusSystemTable getMappedMetricsTable()
    {
        return mappedMetricsTable;
    }
}
