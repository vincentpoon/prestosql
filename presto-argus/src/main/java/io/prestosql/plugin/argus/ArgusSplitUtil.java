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
import io.airlift.slice.Slice;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.Range;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.prestosql.plugin.argus.MetadataUtil.END_COLUMN_HANDLE;
import static io.prestosql.plugin.argus.MetadataUtil.EXPRESSION_HANDLE;
import static io.prestosql.plugin.argus.MetadataUtil.METRIC_COLUMN_HANDLE;
import static io.prestosql.plugin.argus.MetadataUtil.SCOPE_COLUMN_HANDLE;
import static io.prestosql.plugin.argus.MetadataUtil.START_COLUMN_HANDLE;
import static java.time.temporal.ChronoUnit.HOURS;

public class ArgusSplitUtil
{
    private ArgusSplitUtil() {}

    public static ImmutableList<ArgusSplit> getMetricQuerySplits(int numSplits, ArgusTableHandle handle)
    {
        Builder<ArgusSplit> splitsList = ImmutableList.builder();
        Map<ColumnHandle, Domain> domains = handle.getConstraint().getDomains().get();

        // don't want to parse expression string, so just have single split for expression queries
        if (domains.get(EXPRESSION_HANDLE) != null) {
            return ImmutableList.of(new ArgusSplit());
        }

        Instant start = getStartTime(domains);
        Optional<Instant> end = getEndTime(domains);

        Duration windowDuration = Duration.between(start, end.orElse(Instant.now()))
                .dividedBy(numSplits);
        Instant currentWindowStart = start;
        while (numSplits-- > 0) {
            Optional<Instant> currentWindowEnd = numSplits == 0 ? end : Optional.of(currentWindowStart.plus(windowDuration));
            // for list of values e.g. WHERE metric IN (...), build one split per scope/metric combination
            for (Range scopeRange : getScopeRanges(domains)) {
                for (Range metricRange : getMetricRanges(domains)) {
                    splitsList.add(new ArgusSplit(Optional.of(currentWindowStart), currentWindowEnd, ((Slice) scopeRange.getSingleValue()).toStringUtf8(), ((Slice) metricRange.getSingleValue()).toStringUtf8()));
                }
            }
            currentWindowStart = currentWindowEnd.orElse(Instant.now()).plusMillis(1);
        }
        return splitsList.build();
    }

    public static List<Range> getMetricRanges(Map<ColumnHandle, Domain> domains)
    {
        return domains.get(METRIC_COLUMN_HANDLE).getValues().getRanges().getOrderedRanges();
    }

    public static List<Range> getScopeRanges(Map<ColumnHandle, Domain> domains)
    {
        return domains.get(SCOPE_COLUMN_HANDLE).getValues().getRanges().getOrderedRanges();
    }

    public static Optional<Instant> getEndTime(Map<ColumnHandle, Domain> domains)
    {
        return getDomainInstant(domains, END_COLUMN_HANDLE);
    }

    public static Instant getStartTime(Map<ColumnHandle, Domain> domains)
    {
        return getDomainInstant(domains, START_COLUMN_HANDLE).orElse(Instant.now().minus(24, HOURS));
    }

    private static Optional<Instant> getDomainInstant(Map<ColumnHandle, Domain> domains, ArgusColumnHandle handle)
    {
        Optional<Instant> instant = Optional.empty();
        Domain handleDomain = domains.get(handle);
        if (handleDomain != null) {
            instant = Optional.of(Instant.ofEpochMilli((long) handleDomain.getSingleValue()));
        }
        return instant;
    }
}
