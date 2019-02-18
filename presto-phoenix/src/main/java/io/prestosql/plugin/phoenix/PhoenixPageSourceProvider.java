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

import io.prestosql.plugin.jdbc.JdbcColumnHandle;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;

import java.util.List;
import java.util.stream.Collectors;

import static io.prestosql.plugin.phoenix.MetadataUtil.getPrimaryKeyHandle;
import static java.util.Objects.requireNonNull;

public class PhoenixPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final PhoenixClient phoenixClient;

    @Inject
    public PhoenixPageSourceProvider(PhoenixClient phoenixClient)
    {
        this.phoenixClient = requireNonNull(phoenixClient, "phoenixClient is null");
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorSplit split, List<ColumnHandle> columns)
    {
        List<JdbcColumnHandle> phoenixCols = columns.stream().map(column -> (JdbcColumnHandle) column).collect(Collectors.toList());
        if (getPrimaryKeyHandle(phoenixCols).isPresent()) {
            return new PhoenixUpdatablePageSource(
                    session,
                    phoenixClient,
                    (PhoenixSplit) split,
                    phoenixCols);
        }
        return new PhoenixPageSource(
                session,
                phoenixClient,
                (PhoenixSplit) split,
                phoenixCols);
    }
}
