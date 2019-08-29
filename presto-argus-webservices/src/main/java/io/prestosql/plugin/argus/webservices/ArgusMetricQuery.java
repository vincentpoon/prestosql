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

import java.time.Instant;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.StringJoiner;

import static io.prestosql.plugin.argus.MetadataUtil.urlEncoded;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ArgusMetricQuery
{
    private Instant start;
    private Optional<Instant> end;
    private String scope;
    private String metric;
    private Optional<Map<String, String>> tags;
    private String aggregator;
    private Optional<String> downsampler;

    public static class Builder
    {
        private Instant start;
        private Optional<Instant> end;
        private String scope;
        private String metric;
        private Map<String, String> tags;
        private String aggregator = "none";
        private String downsampler;

        public Builder setStart(Instant start)
        {
            this.start = start;
            return this;
        }

        public Builder setEnd(Optional<Instant> end)
        {
            this.end = end;
            return this;
        }

        public Builder setScope(String scope)
        {
            this.scope = scope;
            return this;
        }

        public Builder setMetric(String metric)
        {
            this.metric = metric;
            return this;
        }

        public Builder setTags(Map<String, String> tags)
        {
            this.tags = tags;
            return this;
        }

        public Builder setAggregator(String aggregator)
        {
            this.aggregator = aggregator;
            return this;
        }

        public Builder setDownsampler(String downsampler)
        {
            this.downsampler = downsampler;
            return this;
        }

        public ArgusMetricQuery build()
        {
            return new ArgusMetricQuery(this);
        }
    }

    private ArgusMetricQuery(Builder builder)
    {
        this.start = builder.start;
        this.end = builder.end;
        this.scope = requireNonNull(builder.scope, "scope is null");
        this.metric = requireNonNull(builder.metric, "metric is null");
        this.tags = Optional.ofNullable(builder.tags);
        this.aggregator = requireNonNull(builder.aggregator, "aggregator is null");
        this.downsampler = Optional.ofNullable(builder.downsampler);
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append(start.toEpochMilli()).append(":");
        end.ifPresent(endTime -> builder.append(endTime.toEpochMilli()).append(":"));
        builder.append(scope).append(":").append(metric);
        tags.ifPresent(tagMap -> {
            StringJoiner tagJoiner = new StringJoiner(",", "{", "}");
            for (Entry<String, String> entry : tagMap.entrySet()) {
                tagJoiner.add(format("%s=%s", entry.getKey(), entry.getValue()));
            }
            builder.append(tagJoiner.toString());
        });
        builder.append(":").append(aggregator);
        downsampler.ifPresent(sampler -> builder.append(":").append(sampler));
        return urlEncoded(builder.toString());
    }
}
