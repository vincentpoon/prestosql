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
package io.prestosql.plugin.argus.columnar;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.salesforce.dva.argus.entity.Metric;
import com.salesforce.dva.argus.sdk.entity.MetricDiscoveryQuery;
import com.salesforce.dva.argus.sdk.entity.MetricSchemaRecord;
import com.salesforce.dva.argus.service.TSDBService;
import com.salesforce.dva.argus.service.metric.MetricReader.TimeUnit;
import com.salesforce.dva.argus.service.tsdb.MetricQuery;
import com.salesforce.dva.argus.service.tsdb.MetricQuery.Aggregator;
import io.airlift.slice.Slice;
import io.prestosql.plugin.argus.ArgusClient;
import io.prestosql.plugin.argus.ArgusColumnHandle;
import io.prestosql.plugin.argus.ArgusErrorCode;
import io.prestosql.plugin.argus.ArgusMetadata;
import io.prestosql.plugin.argus.ArgusSplit;
import io.prestosql.plugin.argus.ArgusTableHandle;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.SingleMapBlock;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.Range;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.prestosql.plugin.argus.ArgusSplitUtil.getEndTime;
import static io.prestosql.plugin.argus.ArgusSplitUtil.getMetricRanges;
import static io.prestosql.plugin.argus.ArgusSplitUtil.getScopeRanges;
import static io.prestosql.plugin.argus.ArgusSplitUtil.getStartTime;
import static io.prestosql.plugin.argus.MetadataUtil.AGGREGATOR;
import static io.prestosql.plugin.argus.MetadataUtil.DOWNSAMPLER;
import static io.prestosql.plugin.argus.MetadataUtil.EXPRESSION;
import static io.prestosql.plugin.argus.MetadataUtil.METRIC_COLUMN_HANDLE;
import static io.prestosql.plugin.argus.MetadataUtil.SCOPE_COLUMN_HANDLE;
import static io.prestosql.plugin.argus.MetadataUtil.TAGS;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class ArgusPrestoClient
        implements ArgusClient
{
    private final TSDBService tsdbService;
    private final ArgusMetadata metadata;

    @Inject
    public ArgusPrestoClient(TSDBService tsdbService, ArgusMetadata metadata)
    {
        this.tsdbService = requireNonNull(tsdbService, "tsdbService is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public List<MetricSchemaRecord> listMetricSchemaRecords(MetricDiscoveryQuery discoveryQuery)
    {
        throw new UnsupportedOperationException("listMetricSchemaRecords not implemented");
    }

    @Override
    public List<Metric> getMetrics(ArgusTableHandle tableHandle, ArgusSplit split)
    {
        MetricQuery metricsQuery = buildMetricsQuery(tableHandle);
        return tsdbService.getMetrics(ImmutableList.of(metricsQuery)).get(metricsQuery);
    }

    private MetricQuery buildMetricsQuery(ArgusTableHandle tableHandle)
    {
        Map<ColumnHandle, Domain> domains = tableHandle.getConstraint().getDomains().get();
        long startTimestamp = getStartTime(domains).toEpochMilli();
        Long endTimestamp = getEndTime(domains).map(endInstant -> endInstant.toEpochMilli()).orElse(null);
        String scope = null;
        if (domains.containsKey(SCOPE_COLUMN_HANDLE)) {
            scope = toDelimitedString(getScopeRanges(domains));
        }
        String metric = null;
        if (domains.containsKey(METRIC_COLUMN_HANDLE)) {
            metric = toDelimitedString(getMetricRanges(domains));
        }
        ArgusColumnarMetricQuery query = new ArgusColumnarMetricQuery(scope, metric, null, startTimestamp, endTimestamp);

        for (ArgusColumnHandle column : metadata.getMappedMetricsTable().getColumns()) {
            Domain domain = tableHandle.getConstraint().getDomains().get().get(column);
            if (domain == null) {
                continue;
            }
            switch (column.getColumnName()) {
                case TAGS:
                    SingleMapBlock mapBlock = (SingleMapBlock) domain.getSingleValue();
                    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
                    for (int i = 0; i < mapBlock.getPositionCount(); i += 2) {
                        builder.put(getString(mapBlock, i), getString(mapBlock, i + 1));
                    }
                    query.setTags(builder.build());
                    break;
                case AGGREGATOR:
                    query.setAggregator(Aggregator.fromString(getSingleValueString(domain)));
                    break;
                case DOWNSAMPLER:
                    String expressionString = getSingleValueString(domain);
                    query.setDownsampler(getDownsampler(expressionString));
                    query.setDownsamplingPeriod(getDownsamplingPeriod(expressionString));
                    break;
                case EXPRESSION:
                    throw new PrestoException(ArgusErrorCode.ARGUS_QUERY_ERROR, "expression not supported with argus-presto connector");
            }
        }
        return query;
    }

    private String toDelimitedString(List<Range> scopeRanges)
    {
        return scopeRanges.stream()
                .map(range -> (Slice) range.getSingleValue())
                .map(slice -> slice.toStringUtf8())
                .collect(Collectors.joining("|"));
    }

    private Aggregator getDownsampler(String token)
    {
        return Aggregator.fromString(token.split("-")[1]);
    }

    private Long getDownsamplingPeriod(String token)
    {
        String[] parts = token.split("-");
        String timeDigits = parts[0].substring(0, parts[0].length() - 1);
        String timeUnit = parts[0].substring(parts[0].length() - 1);
        Long time = Long.parseLong(timeDigits);
        TimeUnit unit = TimeUnit.fromString(timeUnit);
        return time * unit.getValue();
    }

    private String getSingleValueString(Domain domain)
    {
        return ((Slice) domain.getSingleValue()).toStringUtf8();
    }

    private String getString(SingleMapBlock mapBlock, int i)
    {
        return VARCHAR.getSlice(mapBlock, i).toStringUtf8();
    }
}
