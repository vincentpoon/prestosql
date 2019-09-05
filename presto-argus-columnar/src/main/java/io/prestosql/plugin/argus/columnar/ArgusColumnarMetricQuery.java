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

import java.util.Map;

// This differs from the parent class in that it allows for null scope/metric values
public class ArgusColumnarMetricQuery
        extends MetricQuery
{
    public ArgusColumnarMetricQuery(String scope, String metric, Map<String, String> tags, Long startTimestamp, Long endTimestamp)
    {
        _startTimestamp = startTimestamp;
        _endTimestamp = endTimestamp;
        _scope = scope;
        _metric = metric;
        if (tags != null) {
            setTags(tags);
        }
    }
}
