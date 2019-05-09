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

import io.airlift.configuration.Config;

import javax.validation.constraints.NotNull;

public class ArgusColumnarConfig
{
    private static final String PRESTO_JDBC_URL = "argus_columnar.presto.jdbc.url";
    private static final String PRESTO_CATALOG_NAME = "argus_columnar.presto.catalog.name";
    private static final String PRESTO_SCHEMA_NAME = "argus_columnar.presto.schema.name";
    private static final String PRESTO_TABLE_NAME = "argus_columnar.presto.table.name";
    private static final String PRESTO_USER_NAME = "argus_columnar.presto.user.name";

    private String prestoJdbcUrl;
    private String catalogName;
    private String schemaName;
    private String tableName;
    private String userName;

    @NotNull
    public String getPrestoJdbcUrl()
    {
        return prestoJdbcUrl;
    }

    @Config(PRESTO_JDBC_URL)
    public ArgusColumnarConfig setPrestoJdbcUrl(String prestoJdbcUrl)
    {
        this.prestoJdbcUrl = prestoJdbcUrl;
        return this;
    }

    @NotNull
    public String getCatalogName()
    {
        return catalogName;
    }

    @Config(PRESTO_CATALOG_NAME)
    public ArgusColumnarConfig setCatalogName(String catalogName)
    {
        this.catalogName = catalogName;
        return this;
    }

    @NotNull
    public String getSchemaName()
    {
        return schemaName;
    }

    @Config(PRESTO_SCHEMA_NAME)
    public ArgusColumnarConfig setSchemaName(String schemaName)
    {
        this.schemaName = schemaName;
        return this;
    }

    @NotNull
    public String getTableName()
    {
        return tableName;
    }

    @Config(PRESTO_TABLE_NAME)
    public ArgusColumnarConfig setTableName(String tableName)
    {
        this.tableName = tableName;
        return this;
    }

    @NotNull
    public String getUserName()
    {
        return userName;
    }

    @Config(PRESTO_USER_NAME)
    public ArgusColumnarConfig setUserName(String username)
    {
        this.userName = username;
        return this;
    }
}
