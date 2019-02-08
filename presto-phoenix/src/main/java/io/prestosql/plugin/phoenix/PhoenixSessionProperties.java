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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.session.PropertyMetadata;

import javax.inject.Inject;

import java.util.List;

import static io.prestosql.spi.session.PropertyMetadata.longProperty;
import static io.prestosql.spi.session.PropertyMetadata.stringProperty;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

/**
 * Class contains all session-based properties for the Phoenix connector.
 * Use SHOW SESSION to view all available properties in the Presto CLI.
 * <p>
 * Can set the property using:
 * <p>
 * SET SESSION &lt;property&gt; = &lt;value&gt;;
 */
public final class PhoenixSessionProperties
{
    private static final String DUPLICATE_KEY_UPDATE_COLUMNS = "duplicate_key_update_columns";
    private static final Splitter DUPLICATE_KEY_UPDATE_COLUMNS_SPLITTER = Splitter.on(" and ").trimResults();
    private static final String WRITE_BATCH_SIZE = "write_batch_size";
    private static final String READ_PAGE_SIZE = "read_page_size";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public PhoenixSessionProperties()
    {
        sessionProperties = ImmutableList.of(
                stringProperty(
                        DUPLICATE_KEY_UPDATE_COLUMNS,
                        "Comma separated list of columns for 'ON DUPLICATE KEY UPDATE' clause",
                        null,
                        false),
                longProperty(
                        WRITE_BATCH_SIZE,
                        "Number of rows to write for each batch",
                        1024L,
                        false),
                longProperty(
                        READ_PAGE_SIZE,
                        "Number of rows to read from Phoenix for each Presto page",
                        4096L,
                        false));
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static List<String> getDuplicateKeyUpdateColumns(ConnectorSession session)
    {
        requireNonNull(session);

        String value = session.getProperty(DUPLICATE_KEY_UPDATE_COLUMNS, String.class);
        if (value == null) {
            return ImmutableList.of();
        }
        return ImmutableList.copyOf(DUPLICATE_KEY_UPDATE_COLUMNS_SPLITTER.split(value.toLowerCase(ENGLISH)));
    }

    public static long getWriteBatchSize(ConnectorSession session)
    {
        return session.getProperty(WRITE_BATCH_SIZE, Long.class);
    }

    public static long getReadPageSize(ConnectorSession session)
    {
        return session.getProperty(READ_PAGE_SIZE, Long.class);
    }
}
