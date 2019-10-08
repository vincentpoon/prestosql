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
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.Range;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.predicate.ValueSet;
import org.testng.annotations.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.plugin.argus.ArgusSplitUtil.getMetricQuerySplits;
import static io.prestosql.plugin.argus.MetadataUtil.END_COLUMN_HANDLE;
import static io.prestosql.plugin.argus.MetadataUtil.MAPPED_METRICS_TABLE_NAME;
import static io.prestosql.plugin.argus.MetadataUtil.METRIC_COLUMN_HANDLE;
import static io.prestosql.plugin.argus.MetadataUtil.SCOPE_COLUMN_HANDLE;
import static io.prestosql.plugin.argus.MetadataUtil.START_COLUMN_HANDLE;
import static io.prestosql.plugin.argus.MetadataUtil.SYSTEM_SCHEMA_NAME;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestArgusSplitUtil
{
    @Test
    public void testGetMetricQuerySplits()
    {
        int numTimeRangeSplits = 1;
        Instant start = LocalDateTime.of(2019, 6, 27, 13, 5).toInstant(UTC);
        Map<ColumnHandle, Domain> domains = new HashMap<>(ImmutableMap.of(
                START_COLUMN_HANDLE,
                Domain.create(ValueSet.ofRanges(
                        Range.equal(TIMESTAMP, start.toEpochMilli())),
                        false),
                SCOPE_COLUMN_HANDLE,
                Domain.create(ValueSet.ofRanges(
                        Range.equal(VARCHAR, utf8Slice("aScope"))),
                        false),
                METRIC_COLUMN_HANDLE,
                Domain.create(ValueSet.ofRanges(
                        Range.equal(VARCHAR, utf8Slice("aMetric"))),
                        false)));
        TupleDomain<ColumnHandle> constraint = TupleDomain.withColumnDomains(domains);
        ArgusTableHandle handle = new ArgusTableHandle(new SchemaTableName(SYSTEM_SCHEMA_NAME, MAPPED_METRICS_TABLE_NAME), constraint, OptionalLong.empty());

        ImmutableList<ArgusSplit> splits = getMetricQuerySplits(numTimeRangeSplits, handle);
        assertEquals(splits.size(), numTimeRangeSplits);
        ArgusSplit split = splits.get(0);
        assertEquals(split.getStart().get(), start);
        assertEquals(split.getEnd(), Optional.empty());

        numTimeRangeSplits = 2;
        splits = getMetricQuerySplits(numTimeRangeSplits, handle);
        assertEquals(splits.size(), numTimeRangeSplits);
        ArgusSplit split0 = splits.get(0);
        assertEquals(split0.getStart().get(), start);
        assertTrue(split0.getEnd().get().compareTo(start) > 0);
        ArgusSplit split1 = splits.get(1);
        assertEquals(split1.getStart().get(), split0.getEnd().get().plusSeconds(1).truncatedTo(SECONDS));
        assertEquals(split1.getEnd(), Optional.empty());

        Instant end = LocalDateTime.of(2019, 6, 30, 13, 5).toInstant(UTC);
        domains.put(
                END_COLUMN_HANDLE,
                Domain.create(ValueSet.ofRanges(
                        Range.equal(TIMESTAMP, end.toEpochMilli())),
                        false));
        constraint = TupleDomain.withColumnDomains(domains);
        handle = new ArgusTableHandle(new SchemaTableName(SYSTEM_SCHEMA_NAME, MAPPED_METRICS_TABLE_NAME), constraint, OptionalLong.empty());
        numTimeRangeSplits = 3;
        splits = getMetricQuerySplits(numTimeRangeSplits, handle);
        assertEquals(splits.size(), 3);
        split0 = splits.get(0);
        assertEquals(split0.getStart().get(), start);
        assertEquals(LocalDateTime.ofInstant(split0.getEnd().get(), UTC).getDayOfMonth(), 28);
        split1 = splits.get(1);
        assertEquals(LocalDateTime.ofInstant(split1.getEnd().get(), UTC).getDayOfMonth(), 29);
        ArgusSplit split2 = splits.get(2);
        assertEquals(split2.getEnd().get(), end);
    }
}
