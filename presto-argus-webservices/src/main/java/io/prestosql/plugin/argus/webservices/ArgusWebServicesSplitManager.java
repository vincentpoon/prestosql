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
import io.prestosql.plugin.argus.ArgusColumnHandle;
import io.prestosql.plugin.argus.ArgusSplit;
import io.prestosql.plugin.argus.ArgusTableHandle;
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

import java.util.Map;

import static io.prestosql.plugin.argus.ArgusErrorCode.ARGUS_QUERY_ERROR;
import static io.prestosql.plugin.argus.ArgusSplitUtil.getMetricQuerySplits;
import static io.prestosql.plugin.argus.MetadataUtil.AGGREGATOR;
import static io.prestosql.plugin.argus.MetadataUtil.DISCOVERY_TABLE_NAME;
import static io.prestosql.plugin.argus.MetadataUtil.DOWNSAMPLER;
import static io.prestosql.plugin.argus.MetadataUtil.END;
import static io.prestosql.plugin.argus.MetadataUtil.EXPRESSION;
import static io.prestosql.plugin.argus.MetadataUtil.EXPRESSION_HANDLE;
import static io.prestosql.plugin.argus.MetadataUtil.MAPPED_METRICS_TABLE_NAME;
import static io.prestosql.plugin.argus.MetadataUtil.METRIC;
import static io.prestosql.plugin.argus.MetadataUtil.METRIC_COLUMN_HANDLE;
import static io.prestosql.plugin.argus.MetadataUtil.SCOPE;
import static io.prestosql.plugin.argus.MetadataUtil.START;
import static io.prestosql.plugin.argus.MetadataUtil.isSystemSchema;
import static java.lang.String.format;

public class ArgusWebServicesSplitManager
        implements ConnectorSplitManager
{
    @Inject
    public ArgusWebServicesSplitManager() {}

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
                int numSplits = ArgusWebServicesSessionProperties.getTimeRangeSplits(session);
                ImmutableList<ArgusSplit> splitsList = getMetricQuerySplits(numSplits, handle);
                return new FixedSplitSource(splitsList);
            }
        }
        throw new PrestoException(ARGUS_QUERY_ERROR, "Unsupported table: " + handle);
    }

    private static void validateMetricConstraint(TupleDomain<ColumnHandle> constraint)
    {
        Map<ColumnHandle, Domain> domains = constraint.getDomains().get();
        if (domains.get(EXPRESSION_HANDLE) != null) {
            return;
        }
        // required by com.salesforce.dva.argus.service.tsdb.MetricQuery
        if (domains.get(METRIC_COLUMN_HANDLE) == null) {
            throw new PrestoException(ARGUS_QUERY_ERROR, "metric filter is required");
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
