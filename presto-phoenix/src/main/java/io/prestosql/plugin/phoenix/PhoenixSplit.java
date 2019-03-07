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
package io.prestosql.plugin.phoenix;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects.ToStringHelper;
import io.prestosql.plugin.jdbc.JdbcSplit;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.predicate.TupleDomain;
import org.apache.phoenix.mapreduce.PhoenixInputSplit;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class PhoenixSplit
        extends JdbcSplit
{
    private final List<HostAddress> addresses;
    private final WrappedPhoenixInputSplit phoenixInputSplit;

    @JsonCreator
    public PhoenixSplit(
            @JsonProperty("catalogName") @Nullable String catalogName,
            @JsonProperty("schemaName") @Nullable String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("tupleDomain") TupleDomain<ColumnHandle> tupleDomain,
            @JsonProperty("addresses") List<HostAddress> addresses,
            @JsonProperty("phoenixInputSplit") WrappedPhoenixInputSplit wrappedPhoenixInputSplit)
    {
        super(catalogName, schemaName, tableName, tupleDomain, Optional.empty());
        requireNonNull(wrappedPhoenixInputSplit, "wrappedPhoenixInputSplit is null");
        requireNonNull(addresses, "addresses is null");
        this.addresses = addresses;
        this.phoenixInputSplit = wrappedPhoenixInputSplit;
    }

    @JsonProperty("phoenixInputSplit")
    public WrappedPhoenixInputSplit getWrappedPhoenixInputSplit()
    {
        return phoenixInputSplit;
    }

    @JsonIgnore
    public PhoenixInputSplit getPhoenixInputSplit()
    {
        return phoenixInputSplit.getPhoenixInputSplit();
    }

    @JsonProperty
    @Override
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PhoenixSplit that = (PhoenixSplit) o;
        return Objects.equals(getCatalogName(), that.getCatalogName()) &&
                Objects.equals(getSchemaName(), that.getSchemaName()) &&
                Objects.equals(getTableName(), that.getTableName()) &&
                Objects.equals(getTupleDomain(), that.getTupleDomain()) &&
                Objects.equals(addresses, that.addresses) &&
                Objects.equals(phoenixInputSplit, that.phoenixInputSplit);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(super.hashCode(), addresses, phoenixInputSplit);
    }

    @Override
    public String toString()
    {
        ToStringHelper helper = toStringHelper(this)
                .add("catalogName", getCatalogName())
                .add("schemaName", getSchemaName())
                .add("tableName", getTableName())
                .add("tupleDomain", getTupleDomain())
                .add("addresses", addresses)
                .add("phoenixInputSplit", getPhoenixInputSplit().getKeyRange());

        return helper.omitNullValues().toString();
    }
}
