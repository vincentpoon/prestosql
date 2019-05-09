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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.connector.ConnectorSplit;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

public class ArgusSplit
        implements ConnectorSplit
{
    private final Optional<Instant> start;
    private final Optional<Instant> end;
    private final String scope;
    private final String metric;

    @JsonCreator
    public ArgusSplit(
            @JsonProperty("start") Optional<Instant> start,
            @JsonProperty("end") Optional<Instant> end,
            @JsonProperty("scope") String scope,
            @JsonProperty("metric") String metric)
    {
        this.start = start;
        this.end = end;
        this.scope = scope;
        this.metric = metric;
    }

    public ArgusSplit()
    {
        this(Optional.empty(), Optional.empty(), null, null);
    }

    @JsonProperty
    public Optional<Instant> getStart()
    {
        return start;
    }

    @JsonProperty
    public Optional<Instant> getEnd()
    {
        return end;
    }

    @JsonProperty
    public String getScope()
    {
        return scope;
    }

    @JsonProperty
    public String getMetric()
    {
        return metric;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return ImmutableList.of();
    }

    @Override
    public Object getInfo()
    {
        return this;
    }
}
