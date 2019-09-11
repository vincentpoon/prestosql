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

import com.salesforce.dva.argus.service.tsdb.MetricQuery;
import io.prestosql.spi.predicate.Domain;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;

import static io.prestosql.plugin.argus.columnar.ArgusColumnarMetadata.VALUE;
import static io.prestosql.plugin.argus.columnar.PrestoTSDBQueryBuilder.toPredicate;
import static io.prestosql.spi.type.DoubleType.DOUBLE;

/**
 * Wrapper around PrestoTSDBService which adds LIMIT
 * since Argus metric queries don't support LIMIT
 */
public class ArgusColumnarTSDBService
        extends PrestoTSDBService
{
    @Inject
    public ArgusColumnarTSDBService(ArgusColumnarConfig config)
    {
        super(config);
    }

    @Override
    protected String getPrestoQuery(MetricQuery query, List<TypeAndValue> bindings)
    {
        return super.getPrestoQuery(query, bindings);
        //TODO disable LIMIT because it's currently not pushed down in all cases
        // pushed down: SELECT col_A FROM table WHERE col_A = 'val' LIMIT 10
        // not pushed down: SELECT col_B FROM table WHERE col_A = 'val' LIMIT 10
//        ArgusColumnarMetricQuery columnarQuery = (ArgusColumnarMetricQuery) query;
//        String prestoQuery = super.getPrestoQuery(query);
//        OptionalLong limit = columnarQuery.getLimit();
//        if (limit.isPresent()) {
//            return prestoQuery + " LIMIT " + limit.getAsLong();
//        }
//        return prestoQuery;
    }

    @Override
    protected List<TSDBColumn> getProjectedColumns(MetricQuery query)
    {
        ArgusColumnarMetricQuery columnarQuery = (ArgusColumnarMetricQuery) query;
        return columnarQuery.getProjection().orElse(ALL_COLUMNS);
    }

    @Override
    protected Optional<String> additionalFilter(MetricQuery query, List<TypeAndValue> bindings)
    {
        ArgusColumnarMetricQuery columnarQuery = (ArgusColumnarMetricQuery) query;
        Optional<Domain> valueDomain = columnarQuery.getValueDomain();
        if (valueDomain.isPresent()) {
            return Optional.of(toPredicate(DOUBLE, VALUE, valueDomain.get(), bindings));
        }
        return super.additionalFilter(query, bindings);
    }
}
