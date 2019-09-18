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
import com.google.common.collect.ImmutableList.Builder;
import com.salesforce.dva.argus.entity.Metric;
import com.salesforce.dva.argus.sdk.entity.MetricDiscoveryQuery;
import com.salesforce.dva.argus.sdk.entity.MetricSchemaRecord;
import io.airlift.slice.Slice;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorRecordSetProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.RecordSet;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.plugin.argus.MetadataUtil.AGGREGATOR;
import static io.prestosql.plugin.argus.MetadataUtil.DATAPOINTS;
import static io.prestosql.plugin.argus.MetadataUtil.DOWNSAMPLER;
import static io.prestosql.plugin.argus.MetadataUtil.END;
import static io.prestosql.plugin.argus.MetadataUtil.EXPRESSION;
import static io.prestosql.plugin.argus.MetadataUtil.MAPPED_METRICS_TABLE_NAME;
import static io.prestosql.plugin.argus.MetadataUtil.METRIC;
import static io.prestosql.plugin.argus.MetadataUtil.METRIC_REGEX;
import static io.prestosql.plugin.argus.MetadataUtil.SCOPE;
import static io.prestosql.plugin.argus.MetadataUtil.SCOPE_COLUMN_HANDLE;
import static io.prestosql.plugin.argus.MetadataUtil.SCOPE_REGEX;
import static io.prestosql.plugin.argus.MetadataUtil.START;
import static io.prestosql.plugin.argus.MetadataUtil.TAGS;
import static io.prestosql.plugin.argus.MetadataUtil.TAG_KEY;
import static io.prestosql.plugin.argus.MetadataUtil.TAG_KEY_REGEX;
import static io.prestosql.plugin.argus.MetadataUtil.TAG_VALUE;
import static io.prestosql.plugin.argus.MetadataUtil.TAG_VALUE_REGEX;
import static io.prestosql.plugin.argus.MetadataUtil.VALUE_FILTER;
import static io.prestosql.plugin.argus.MetadataUtil.isSystemSchema;
import static io.prestosql.plugin.argus.MetadataUtil.urlEncoded;
import static io.prestosql.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static java.lang.Math.toIntExact;

public class ArgusRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final ArgusClient argusClient;
    private final ArgusMetadata argusMetadata;

    @Inject
    public ArgusRecordSetProvider(ArgusClient argusClient, ArgusMetadata argusMetadata)
    {
        this.argusClient = argusClient;
        this.argusMetadata = argusMetadata;
    }

    @Override
    public RecordSet getRecordSet(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<? extends ColumnHandle> columns)
    {
        List<ArgusColumnHandle> columnHandles = toArgusColumnHandles(columns);
        ArgusTableHandle tableHandle = (ArgusTableHandle) table;
        String schemaName = tableHandle.getSchemaTableName().getSchemaName();
        String tableName = tableHandle.getSchemaTableName().getTableName();
        if (isSystemSchema(schemaName)) {
            ArgusSystemTable systemTable = argusMetadata.getArgusSystemTable(tableName);
            if (systemTable.equals(ArgusMetadata.discoveryTable)) {
                return getDiscoveryRecordSet(columnHandles, tableHandle);
            }
            else if (MAPPED_METRICS_TABLE_NAME.equals(tableName)) {
                return getMetricsRecordSet(columnHandles, tableHandle, (ArgusSplit) split);
            }
            throw new PrestoException(ArgusErrorCode.ARGUS_QUERY_ERROR, "Unrecognized table: " + tableName);
        }
        return null;
    }

    private List<ArgusColumnHandle> toArgusColumnHandles(List<? extends ColumnHandle> columns)
    {
        if (columns.isEmpty()) {
            // for cases like SELECT count(*) with no projection, we still need a column to hold results
            return ImmutableList.of(SCOPE_COLUMN_HANDLE);
        }
        else {
            return columns.stream().map(ArgusColumnHandle.class::cast).collect(toImmutableList());
        }
    }

    private RecordSet getDiscoveryRecordSet(List<ArgusColumnHandle> handles, ArgusTableHandle discoveryTable)
    {
        MetricDiscoveryQuery discoveryQuery = buildDiscoveryQuery(discoveryTable);
        List<MetricSchemaRecord> metricSchemaRecords = argusClient.listMetricSchemaRecords(discoveryQuery);
        Collection<Type> columnTypes = handles.stream().map(ArgusColumnHandle::getColumnType).collect(toImmutableList());
        ArgusInMemoryRecordSet.Builder recordSetBuilder = ArgusInMemoryRecordSet.builder(columnTypes);
        for (MetricSchemaRecord record : metricSchemaRecords) {
            Builder<Object> values = ImmutableList.builder();
            for (ArgusColumnHandle columnHandle : handles) {
                switch (columnHandle.getColumnName()) {
                    case METRIC:
                        values.add(record.getMetric());
                        break;
                    case SCOPE:
                        values.add(record.getScope());
                        break;
                    case TAG_KEY:
                        values.add(record.getTagKey());
                        break;
                    case TAG_VALUE:
                        values.add(record.getTagValue());
                        break;
                    case METRIC_REGEX:
                        values.add(firstNonNull(discoveryQuery.getMetric(), "*"));
                        break;
                    case SCOPE_REGEX:
                        values.add(firstNonNull(discoveryQuery.getScope(), "*"));
                        break;
                    case TAG_KEY_REGEX:
                        values.add(discoveryQuery.getTagk());
                        break;
                    case TAG_VALUE_REGEX:
                        values.add(discoveryQuery.getTagv());
                        break;
                    default:
                        throw new PrestoException(ArgusErrorCode.ARGUS_INTERNAL_ERROR, "Unexpected column: " + columnHandle);
                }
            }
            recordSetBuilder.addRow(values.build().toArray());
        }
        return recordSetBuilder.build();
    }

    private MetricDiscoveryQuery buildDiscoveryQuery(ArgusTableHandle tableHandle)
    {
        MetricDiscoveryQuery query = new MetricDiscoveryQuery();
        for (ArgusColumnHandle column : ArgusMetadata.discoveryTable.getColumns()) {
            Domain domain = tableHandle.getConstraint().getDomains().get().get(column);
            if (domain != null) {
                Object singleValue = domain.getSingleValue();
                if (!(singleValue instanceof Slice)) {
                    throw new PrestoException(FUNCTION_IMPLEMENTATION_ERROR, "Object is not a Slice, but " + singleValue.getClass());
                }
                String value = urlEncoded(((Slice) singleValue).toStringUtf8());
                switch (column.getColumnName()) {
                    case METRIC_REGEX:
                        query.setMetric(value);
                        break;
                    case SCOPE_REGEX:
                        query.setScope(value);
                        break;
                    case TAG_KEY_REGEX:
                        query.setTagk(value);
                        break;
                    case TAG_VALUE_REGEX:
                        query.setTagv(value);
                        break;
                }
            }
        }
        tableHandle.getLimit().ifPresent(limit -> query.setLimit(toIntExact(limit)));
        return query;
    }

    private RecordSet getMetricsRecordSet(List<ArgusColumnHandle> handles, ArgusTableHandle tableHandle, ArgusSplit split)
    {
        List<Metric> metrics = argusClient.getMetrics(tableHandle, split);
        Collection<Type> columnTypes = handles.stream().map(ArgusColumnHandle::getColumnType).collect(toImmutableList());
        ArgusInMemoryRecordSet.Builder recordSetBuilder = ArgusInMemoryRecordSet.builder(columnTypes);
        for (Metric metric : metrics) {
            List<Object> values = new ArrayList<>();
            for (ArgusColumnHandle columnHandle : handles) {
                switch (columnHandle.getColumnName()) {
                    case START:
                        values.add(split.getStart().map(instant -> instant.toEpochMilli()).orElse(null));
                        break;
                    case END:
                        values.add(split.getEnd().map(instant -> instant.toEpochMilli()).orElse(null));
                        break;
                    case METRIC:
                        values.add(metric.getMetric());
                        break;
                    case SCOPE:
                        values.add(metric.getScope());
                        break;
                    case TAGS:
                        BlockBuilder tagsBlock = columnHandle.getColumnType().createBlockBuilder(null, 1);
                        BlockBuilder tagsBlockEntry = tagsBlock.beginBlockEntry();
                        for (Entry<String, String> tag : metric.getTags().entrySet()) {
                            VarcharType.VARCHAR.writeString(tagsBlockEntry, tag.getKey());
                            VarcharType.VARCHAR.writeString(tagsBlockEntry, tag.getValue());
                        }
                        tagsBlock.closeEntry();
                        values.add(tagsBlock.getObject(0, Block.class));
                        break;
                    case AGGREGATOR:
                    case DOWNSAMPLER:
                    case EXPRESSION:
                        Domain domain = tableHandle.getConstraint().getDomains().get().get(columnHandle);
                        if (domain != null) {
                            values.add(((Slice) domain.getSingleValue()).toStringUtf8());
                        }
                        else {
                            values.add(null);
                        }
                        break;
                    case DATAPOINTS:
                        BlockBuilder datapointsBlock = columnHandle.getColumnType().createBlockBuilder(null, 1);
                        BlockBuilder datapointsBlockEntry = datapointsBlock.beginBlockEntry();
                        for (Entry<Long, Double> datapoint : metric.getDatapoints().entrySet()) {
                            BIGINT.writeLong(datapointsBlockEntry, datapoint.getKey());
                            DOUBLE.writeDouble(datapointsBlockEntry, datapoint.getValue());
                        }
                        datapointsBlock.closeEntry();
                        values.add(datapointsBlock.getObject(0, Block.class));
                        break;
                    case VALUE_FILTER:
                        values.add(null);
                        break;
                    default:
                        throw new PrestoException(ArgusErrorCode.ARGUS_INTERNAL_ERROR, "Unexpected column: " + columnHandle);
                }
            }
            recordSetBuilder.addRow(values.toArray());
        }
        return recordSetBuilder.build();
    }
}
