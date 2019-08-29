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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import io.airlift.slice.Slice;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.FixedSplitSource;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.Range;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.predicate.TupleDomain.ColumnDomain;

import javax.inject.Inject;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;

import static io.prestosql.plugin.argus.ArgusErrorCode.ARGUS_QUERY_ERROR;
import static io.prestosql.plugin.argus.MetadataUtil.AGGREGATOR;
import static io.prestosql.plugin.argus.MetadataUtil.DISCOVERY_TABLE_NAME;
import static io.prestosql.plugin.argus.MetadataUtil.DOWNSAMPLER;
import static io.prestosql.plugin.argus.MetadataUtil.END;
import static io.prestosql.plugin.argus.MetadataUtil.END_COLUMN_HANDLE;
import static io.prestosql.plugin.argus.MetadataUtil.EXPRESSION;
import static io.prestosql.plugin.argus.MetadataUtil.EXPRESSION_HANDLE;
import static io.prestosql.plugin.argus.MetadataUtil.MAPPED_METRICS_TABLE_NAME;
import static io.prestosql.plugin.argus.MetadataUtil.METRIC;
import static io.prestosql.plugin.argus.MetadataUtil.METRIC_COLUMN_HANDLE;
import static io.prestosql.plugin.argus.MetadataUtil.SCOPE;
import static io.prestosql.plugin.argus.MetadataUtil.SCOPE_COLUMN_HANDLE;
import static io.prestosql.plugin.argus.MetadataUtil.START;
import static io.prestosql.plugin.argus.MetadataUtil.START_COLUMN_HANDLE;
import static io.prestosql.plugin.argus.MetadataUtil.isSystemSchema;
import static java.lang.String.format;
import static java.time.temporal.ChronoUnit.HOURS;

public class ArgusSplitManager
        implements ConnectorSplitManager
{
    @Inject
    public ArgusSplitManager() {}

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            SplitSchedulingStrategy splitSchedulingStrategy)
    {
        ArgusTableHandle handle = (ArgusTableHandle) table;
        SchemaTableName schemaTableName = handle.getSchemaTableName();
        if (isSystemSchema(schemaTableName.getSchemaName())) {
            if (DISCOVERY_TABLE_NAME.equals(schemaTableName.getTableName())) {
                return new FixedSplitSource(ImmutableList.of(new ArgusSplit()));
            }
            else if (MAPPED_METRICS_TABLE_NAME.equals(schemaTableName.getTableName())) {
                validateMetricConstraint(handle.getConstraint());
                ImmutableList<ArgusSplit> splitsList = getMetricQuerySplits(session, handle);
                return new FixedSplitSource(splitsList);
            }
        }
        throw new PrestoException(ARGUS_QUERY_ERROR, "Unsupported table: " + handle);
    }

    @VisibleForTesting
    protected ImmutableList<ArgusSplit> getMetricQuerySplits(ConnectorSession session, ArgusTableHandle handle)
    {
        Builder<ArgusSplit> splitsList = ImmutableList.builder();
        Map<ColumnHandle, Domain> domains = handle.getConstraint().getDomains().get();

        // don't want to parse expression string, so just have single split for expression queries
        if (domains.get(EXPRESSION_HANDLE) != null) {
            return ImmutableList.of(new ArgusSplit());
        }

        int numSplits = ArgusSessionProperties.getTimeRangeSplits(session);
        Instant start = getDomainInstant(domains, START_COLUMN_HANDLE).orElse(Instant.now().minus(24, HOURS));
        Optional<Instant> end = getDomainInstant(domains, END_COLUMN_HANDLE);

        Duration windowDuration = Duration.between(start, end.orElse(Instant.now()))
                .dividedBy(numSplits);
        Instant currentWindowStart = start;
        while (numSplits-- > 0) {
            Optional<Instant> currentWindowEnd = numSplits == 0 ? end : Optional.of(currentWindowStart.plus(windowDuration));
            // for list of values e.g. WHERE metric IN (...), build one split per scope/metric combination
            for (Range scopeRange : domains.get(SCOPE_COLUMN_HANDLE).getValues().getRanges().getOrderedRanges()) {
                for (Range metricRange : domains.get(METRIC_COLUMN_HANDLE).getValues().getRanges().getOrderedRanges()) {
                    splitsList.add(new ArgusSplit(Optional.of(currentWindowStart), currentWindowEnd, ((Slice) scopeRange.getSingleValue()).toStringUtf8(), ((Slice) metricRange.getSingleValue()).toStringUtf8()));
                }
            }
            currentWindowStart = currentWindowEnd.orElse(Instant.now()).plusMillis(1);
        }
        return splitsList.build();
    }

    private Optional<Instant> getDomainInstant(Map<ColumnHandle, Domain> domains, ArgusColumnHandle handle)
    {
        Optional<Instant> instant = Optional.empty();
        Domain handleDomain = domains.get(handle);
        if (handleDomain != null) {
            instant = Optional.of(Instant.ofEpochMilli((long) handleDomain.getSingleValue()));
        }
        return instant;
    }

    private void validateMetricConstraint(TupleDomain<ColumnHandle> constraint)
    {
        Map<ColumnHandle, Domain> domains = constraint.getDomains().get();
        if (domains.get(EXPRESSION_HANDLE) != null) {
            return;
        }
        for (ColumnDomain<ColumnHandle> columnDomain : constraint.getColumnDomains().get()) {
            ArgusColumnHandle handle = (ArgusColumnHandle) columnDomain.getColumn();
            Domain domain = columnDomain.getDomain();
            switch (handle.getColumnName()) {
                case START:
                case END:
                    if (!domain.isSingleValue()) {
                        throw new PrestoException(ARGUS_QUERY_ERROR, "Start/end must be a single value");
                    }
                    break;
                case SCOPE:
                case METRIC:
                case AGGREGATOR:
                case DOWNSAMPLER:
                case EXPRESSION:
                    for (Range range : domain.getValues().getRanges().getOrderedRanges()) {
                        if (!range.isSingleValue()) {
                            throw new PrestoException(ARGUS_QUERY_ERROR, format("Only equality allowed for %s, but got: %s", handle.getColumnName(), range));
                        }
                    }
                    break;
            }
        }
    }
}
