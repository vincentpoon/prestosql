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
package io.prestosql.plugin.argus.webservices;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.salesforce.dva.argus.entity.Metric;
import com.salesforce.dva.argus.sdk.ArgusService;
import com.salesforce.dva.argus.sdk.MetricService;
import com.salesforce.dva.argus.sdk.entity.MetricDiscoveryQuery;
import com.salesforce.dva.argus.sdk.entity.MetricSchemaRecord;
import com.salesforce.dva.argus.sdk.exceptions.TokenExpiredException;
import io.airlift.slice.Slice;
import io.prestosql.plugin.argus.ArgusClient;
import io.prestosql.plugin.argus.ArgusColumnHandle;
import io.prestosql.plugin.argus.ArgusErrorCode;
import io.prestosql.plugin.argus.ArgusMetadata;
import io.prestosql.plugin.argus.ArgusSplit;
import io.prestosql.plugin.argus.ArgusTableHandle;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.SingleMapBlock;
import io.prestosql.spi.predicate.Domain;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

import static com.google.common.base.MoreObjects.firstNonNull;
import static io.prestosql.plugin.argus.ArgusErrorCode.ARGUS_IO_ERROR;
import static io.prestosql.plugin.argus.ArgusErrorCode.ARGUS_QUERY_ERROR;
import static io.prestosql.plugin.argus.MetadataUtil.AGGREGATOR;
import static io.prestosql.plugin.argus.MetadataUtil.DOWNSAMPLER;
import static io.prestosql.plugin.argus.MetadataUtil.EXPRESSION;
import static io.prestosql.plugin.argus.MetadataUtil.TAGS;
import static io.prestosql.plugin.argus.MetadataUtil.urlEncoded;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.lang.Math.toIntExact;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

public class ArgusWebServicesClient
        implements ArgusClient
{
    private final ArgusService argusService;

    private final ArgusMetadata metadata;

    @Inject
    public ArgusWebServicesClient(ArgusWebServicesConfig config, ArgusMetadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        try {
            argusService = ArgusService.getInstance(
                    config.getEndpoint(),
                    config.getMaxConnections(),
                    toIntExact(config.getConnectionTimeout().toMillis()),
                    toIntExact(config.getRequestTimeout().toMillis()));
            argusService.getAuthService().login(config.getUsername(), config.getPassword());
        }
        catch (IOException e) {
            throw new PrestoException(ARGUS_IO_ERROR, e);
        }
    }

    public List<MetricSchemaRecord> listMetricSchemaRecords(MetricDiscoveryQuery query)
    {
        return executeWithAuth(() -> argusService.getDiscoveryService().getMatchingRecords(
                "*",
                firstNonNull(query.getScope(), "*"),
                firstNonNull(query.getMetric(), "*"),
                query.getTagk(),
                query.getTagv(),
                query.getLimit()));
    }

    private <T> T executeWithAuth(ArgusResultSupplier<T> argusResultSupplier)
    {
        try {
            return argusResultSupplier.get();
        }
        catch (IOException e) {
            throw new PrestoException(ARGUS_IO_ERROR, "Error while querying Argus", e);
        }
        catch (TokenExpiredException e) {
            try {
                argusService.getAuthService().obtainNewAccessToken();
                return argusResultSupplier.get();
            }
            catch (TokenExpiredException tee) {
                throw new PrestoException(ArgusErrorCode.AUTH_ERROR, "Error while querying Argus", tee);
            }
            catch (IOException ioe) {
                throw new PrestoException(ARGUS_IO_ERROR, "Error while querying Argus", ioe);
            }
        }
    }

    public List<Metric> getMetrics(ArgusTableHandle tableHandle, ArgusSplit split)
    {
        MetricService metricService = argusService.getMetricService();
        List<com.salesforce.dva.argus.sdk.entity.Metric> sdkMetrics = executeWithAuth(() -> metricService.getMetrics(singletonList(buildMetricsQuery(tableHandle, split))));
        return toCoreMetrics(sdkMetrics);
    }

    private List<Metric> toCoreMetrics(List<com.salesforce.dva.argus.sdk.entity.Metric> sdkMetrics)
    {
        return sdkMetrics.stream()
                .map(sdkMetric -> {
                    Metric coreMetric = new Metric(sdkMetric.getScope(), sdkMetric.getMetric());
                    coreMetric.setDatapoints(sdkMetric.getDatapoints());
                    coreMetric.setTags(sdkMetric.getTags());
                    return coreMetric;
                })
                .collect(ImmutableList.toImmutableList());
    }

    private String buildMetricsQuery(ArgusTableHandle tableHandle, ArgusSplit split)
    {
        ArgusMetricQuery.Builder queryBuilder = new ArgusMetricQuery.Builder();

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
                    queryBuilder.setTags(builder.build());
                    break;
                case AGGREGATOR:
                    queryBuilder.setAggregator(getSingleValueString(domain));
                    break;
                case DOWNSAMPLER:
                    queryBuilder.setDownsampler(getSingleValueString(domain));
                    break;
                case EXPRESSION:
                    return urlEncoded(getSingleValueString(domain));
            }
        }
        return queryBuilder.setScope(split.getScope())
                .setMetric(split.getMetric())
                .setStart(split.getStart().orElseThrow(() -> new PrestoException(ARGUS_QUERY_ERROR, "Start time of split not found:" + tableHandle)))
                .setEnd(split.getEnd())
                .build().toString();
    }

    private String getSingleValueString(Domain domain)
    {
        return ((Slice) domain.getSingleValue()).toStringUtf8();
    }

    private String getString(SingleMapBlock mapBlock, int i)
    {
        return VARCHAR.getSlice(mapBlock, i).toStringUtf8();
    }

    @PreDestroy
    public void shutDown()
    {
        try {
            argusService.getAuthService().logout();
            argusService.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private interface ArgusResultSupplier<T>
    {
        T get()
                throws IOException, TokenExpiredException;
    }
}
