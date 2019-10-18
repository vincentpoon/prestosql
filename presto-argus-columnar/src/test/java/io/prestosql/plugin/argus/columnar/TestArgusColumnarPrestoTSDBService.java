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

import io.prestosql.plugin.argus.columnar.PrestoTSDBService.TypeAndValue;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.Range;
import io.prestosql.spi.predicate.ValueSet;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static java.util.stream.Collectors.joining;
import static org.testng.Assert.assertEquals;

public class TestArgusColumnarPrestoTSDBService
{
    @Test
    public void testValueFilter()
    {
        ArgusColumnarTSDBService tsdbService = new ArgusColumnarTSDBService(defaultConfig());
        List<TypeAndValue> bindings = new ArrayList<>();
        ArgusColumnarMetricQuery query = new ArgusColumnarMetricQuery("aScope", "aMetric", null, 0L, 100L, OptionalLong.of(10L));
        Domain valueDomain = Domain.create(
                ValueSet.ofRanges(
                        Range.greaterThanOrEqual(DOUBLE, 13.5),
                        Range.lessThan(DOUBLE, 10.1)),
                false);
        query.setValueDomain(Optional.of(valueDomain));
        String filters = tsdbService.buildFilters(query, bindings).stream().collect(joining(" AND "));
        assertEquals(filters, "scope IN (?) AND metric IN (?) AND ((\"value\" < ?) OR (\"value\" >= ?))");

        TypeAndValue lessThanBinding = bindings.get(2);
        assertEquals(DOUBLE, lessThanBinding.getType());
        assertEquals(10.1, lessThanBinding.getValue());
        TypeAndValue greaterThanOrEqualBinding = bindings.get(3);
        assertEquals(DOUBLE, greaterThanOrEqualBinding.getType());
        assertEquals(13.5, greaterThanOrEqualBinding.getValue());
    }

    private ArgusColumnarConfig defaultConfig()
    {
        ArgusColumnarConfig config = new ArgusColumnarConfig();
        config.setPrestoJdbcUrl("testPrestoJdbcUrl");
        config.setCatalogName("testCatalogName");
        config.setSchemaName("testSchemaName");
        config.setTableName("testTableName");
        config.setUserName("testUserName");
        return config;
    }
}
