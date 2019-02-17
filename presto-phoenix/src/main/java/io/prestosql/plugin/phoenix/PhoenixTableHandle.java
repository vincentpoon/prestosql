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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.SchemaTableName;

import javax.annotation.Nullable;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class PhoenixTableHandle
        implements ConnectorTableHandle
{
    private final SchemaTableName schemaTableName;
    private final String schemaName;
    private final String tableName;

    @JsonCreator
    public PhoenixTableHandle(
            @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
            @JsonProperty("schemaName") @Nullable String schemaName,
            @JsonProperty("tableName") String tableName)
    {
        this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        this.schemaName = schemaName;
        this.tableName = requireNonNull(tableName, "tableName is null");
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    @JsonProperty
    @Nullable
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        PhoenixTableHandle o = (PhoenixTableHandle) obj;
        return Objects.equals(this.schemaTableName, o.schemaTableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaTableName);
    }

    @Override
    public String toString()
    {
        return Joiner.on(":").useForNull("null").join(schemaTableName, schemaName, tableName);
    }
}
