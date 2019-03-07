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
import io.prestosql.plugin.jdbc.JdbcOutputTableHandle;
import io.prestosql.spi.type.Type;

import javax.annotation.Nullable;

import java.util.List;

public class PhoenixOutputTableHandle
        extends JdbcOutputTableHandle
{
    private final boolean hasUUIDRowkey;

    @JsonCreator
    public PhoenixOutputTableHandle(
            @JsonProperty("catalogName") @Nullable String catalogName,
            @JsonProperty("schemaName") @Nullable String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("columnNames") List<String> columnNames,
            @JsonProperty("columnTypes") List<Type> columnTypes,
            @JsonProperty("temporaryTableName") String temporaryTableName,
            @JsonProperty("hadUUIDRowkey") boolean hasUUIDRowkey)
    {
        super(catalogName, schemaName, tableName, columnNames, columnTypes, temporaryTableName);
        this.hasUUIDRowkey = hasUUIDRowkey;
    }

    @JsonProperty
    public boolean hasUUIDRowkey()
    {
        return hasUUIDRowkey;
    }
}
