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

import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.Type;
import org.apache.phoenix.util.SchemaUtil;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.prestosql.plugin.phoenix.PhoenixMetadata.DEFAULT_SCHEMA;
import static io.prestosql.plugin.phoenix.PhoenixMetadata.UPDATE_ROW_ID;
import static java.util.Objects.requireNonNull;

public class MetadataUtil
{
    private MetadataUtil()
    {
    }

    // decompose the compound PK from getUpdateRowIdColumnHandle
    public static List<PhoenixColumnHandle> decomposePrimaryKeyColumn(List<PhoenixColumnHandle> columns)
    {
        return columns.stream().flatMap(MetadataUtil::decomposePrimaryKey).collect(Collectors.toList());
    }

    private static Stream<PhoenixColumnHandle> decomposePrimaryKey(PhoenixColumnHandle handle)
    {
        if (isUpdateRowId(handle)) {
            RowType row = (RowType) handle.getColumnType();
            return row.getFields().stream().map(field -> new PhoenixColumnHandle(field.getName().get(), field.getType()));
        }
        return Stream.of(handle);
    }

    public static String getEscapedTableName(String schema, String table)
    {
        return SchemaUtil.getEscapedTableName(toPhoenixSchemaName(schema), table);
    }

    public static String toPhoenixSchemaName(String prestoSchemaName)
    {
        return DEFAULT_SCHEMA.equalsIgnoreCase(prestoSchemaName) ? "" : prestoSchemaName;
    }

    public static String toPrestoSchemaName(String phoenixSchemaName)
    {
        return isNullOrEmpty(phoenixSchemaName) ? DEFAULT_SCHEMA : phoenixSchemaName;
    }

    public static Optional<PhoenixColumnHandle> getPrimaryKeyHandle(List<PhoenixColumnHandle> columns)
    {
        Optional<PhoenixColumnHandle> columnHandle = columns.stream().filter(MetadataUtil::isUpdateRowId).findFirst();
        return columnHandle;
    }

    private static boolean isUpdateRowId(PhoenixColumnHandle columnHandle)
    {
        return isPrimaryKeyColumn(columnHandle.getColumnName(), columnHandle.getColumnType());
    }

    public static boolean isPrimaryKeyColumn(String columnName, Type columnType)
    {
        return UPDATE_ROW_ID.equals(columnName) && columnType instanceof RowType;
    }

    public static String escapeNamePattern(@Nullable String name, String escape)
    {
        requireNonNull(escape, "escape is null");
        if ((name == null)) {
            return name;
        }
        checkArgument(!escape.equals("_"), "Escape string must not be '_'");
        checkArgument(!escape.equals("%"), "Escape string must not be '%'");
        name = name.replace(escape, escape + escape);
        name = name.replace("_", escape + "_");
        name = name.replace("%", escape + "%");
        return name;
    }
}
