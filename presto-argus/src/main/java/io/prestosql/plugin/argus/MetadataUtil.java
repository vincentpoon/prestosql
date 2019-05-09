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

import io.prestosql.spi.PrestoException;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;

public class MetadataUtil
{
    private MetadataUtil() {}

    public static final String SYSTEM_SCHEMA_NAME = "system";
    public static final String METRIC_SCHEMA_NAME = "metrics";
    public static final String DISCOVERY_TABLE_NAME = "discovery";
    public static final String MAPPED_METRICS_TABLE_NAME = "mapped_metrics";

    // Shared column names
    public static final String SCOPE = "scope";
    public static final String METRIC = "metric";
    public static final String TAG_KEY = "tag_key";
    public static final String TAG_VALUE = "tag_value";

    // Discovery table columns
    public static final String SCOPE_REGEX = "scope_regex";
    public static final String METRIC_REGEX = "metric_regex";
    public static final String TAG_KEY_REGEX = "tag_key_regex";
    public static final String TAG_VALUE_REGEX = "tag_value_regex";

    // Metrics table columns
    public static final String START = "start_time";
    public static final String END = "end_time";
    public static final String TAGS = "tags";
    public static final String AGGREGATOR = "aggregator";
    public static final String DOWNSAMPLER = "downsampler";
    public static final String DATAPOINTS = "datapoints";
    public static final String EXPRESSION = "expression";

    public static final ArgusColumnHandle SCOPE_COLUMN_HANDLE = new ArgusColumnHandle(SCOPE, createUnboundedVarcharType());
    public static final ArgusColumnHandle METRIC_COLUMN_HANDLE = new ArgusColumnHandle(METRIC, createUnboundedVarcharType());
    public static final ArgusColumnHandle START_COLUMN_HANDLE = new ArgusColumnHandle(START, TIMESTAMP);
    public static final ArgusColumnHandle END_COLUMN_HANDLE = new ArgusColumnHandle(END, TIMESTAMP);
    public static final ArgusColumnHandle EXPRESSION_HANDLE = new ArgusColumnHandle(EXPRESSION, VARCHAR);

    public static boolean isSystemSchema(String schemaName)
    {
        return SYSTEM_SCHEMA_NAME.equals(schemaName) || METRIC_SCHEMA_NAME.equals(schemaName);
    }

    public static String urlEncoded(String param)
    {
        try {
            return URLEncoder.encode(param, "UTF-8");
        }
        catch (UnsupportedEncodingException e) {
            throw new PrestoException(ArgusErrorCode.ARGUS_INTERNAL_ERROR, e);
        }
    }
}
