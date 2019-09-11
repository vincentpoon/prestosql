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
import com.google.common.collect.ImmutableSet;
import io.prestosql.plugin.argus.ArgusColumnHandle;
import io.prestosql.plugin.argus.ArgusMetadata;
import io.prestosql.plugin.argus.ArgusSystemTable;
import io.prestosql.spi.type.TypeManager;

import javax.inject.Inject;

import static io.prestosql.plugin.argus.MetadataUtil.AGGREGATOR;
import static io.prestosql.plugin.argus.MetadataUtil.DATAPOINTS;
import static io.prestosql.plugin.argus.MetadataUtil.DOWNSAMPLER;
import static io.prestosql.plugin.argus.MetadataUtil.END_COLUMN_HANDLE;
import static io.prestosql.plugin.argus.MetadataUtil.EXPRESSION_HANDLE;
import static io.prestosql.plugin.argus.MetadataUtil.MAPPED_METRICS_TABLE_NAME;
import static io.prestosql.plugin.argus.MetadataUtil.METRIC_COLUMN_HANDLE;
import static io.prestosql.plugin.argus.MetadataUtil.SCOPE_COLUMN_HANDLE;
import static io.prestosql.plugin.argus.MetadataUtil.START_COLUMN_HANDLE;
import static io.prestosql.plugin.argus.MetadataUtil.TAGS;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.VarcharType.VARCHAR;

public class ArgusWebServicesMetadata
        extends ArgusMetadata
{
    @Inject
    public ArgusWebServicesMetadata(TypeManager typeManager)
    {
        this(typeManager, mappedMetricsTable(typeManager));
    }

    public ArgusWebServicesMetadata(TypeManager typeManager, ArgusSystemTable mappedMetricsTable)
    {
        super(typeManager, ImmutableSet.of(discoveryTable, mappedMetricsTable), mappedMetricsTable);
    }

    private static ArgusSystemTable mappedMetricsTable(TypeManager typeManager)
    {
        return new ArgusSystemTable(MAPPED_METRICS_TABLE_NAME, "Query for mapped metrics", ImmutableList.of(
                START_COLUMN_HANDLE,
                END_COLUMN_HANDLE,
                SCOPE_COLUMN_HANDLE,
                METRIC_COLUMN_HANDLE,
                new ArgusColumnHandle(TAGS, mapType(typeManager, VARCHAR, VARCHAR)),
                new ArgusColumnHandle(AGGREGATOR, VARCHAR),
                new ArgusColumnHandle(DOWNSAMPLER, VARCHAR),
                new ArgusColumnHandle(DATAPOINTS, mapType(typeManager, TIMESTAMP, DOUBLE)),
                EXPRESSION_HANDLE));
    }
}
