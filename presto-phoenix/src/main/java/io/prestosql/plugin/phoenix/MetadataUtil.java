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

import org.apache.phoenix.util.SchemaUtil;

import javax.annotation.Nullable;

import static com.google.common.base.Strings.isNullOrEmpty;
import static io.prestosql.plugin.phoenix.PhoenixMetadata.DEFAULT_SCHEMA;

public class MetadataUtil
{
    private MetadataUtil()
    {
    }

    public static String getEscapedTableName(@Nullable String schema, String table)
    {
        return SchemaUtil.getEscapedTableName(toPhoenixSchemaName(schema), table);
    }

    public static String toPhoenixSchemaName(@Nullable String prestoSchemaName)
    {
        return DEFAULT_SCHEMA.equalsIgnoreCase(prestoSchemaName) ? "" : prestoSchemaName;
    }

    public static String toPrestoSchemaName(@Nullable String phoenixSchemaName)
    {
        return isNullOrEmpty(phoenixSchemaName) ? DEFAULT_SCHEMA : phoenixSchemaName;
    }
}
