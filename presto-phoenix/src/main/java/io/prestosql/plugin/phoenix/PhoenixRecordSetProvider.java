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

import io.prestosql.plugin.jdbc.JdbcRecordSetProvider;
import io.prestosql.plugin.jdbc.JdbcTableHandle;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.RecordSet;

import javax.inject.Inject;

import java.util.List;

public class PhoenixRecordSetProvider
        extends JdbcRecordSetProvider
{
    @Inject
    public PhoenixRecordSetProvider(PhoenixClient phoenixClient)
    {
        super(phoenixClient);
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit split, ConnectorTableHandle table, List<? extends ColumnHandle> columns)
    {
        JdbcTableHandle jdbcTable = (JdbcTableHandle) table;
        if (jdbcTable.getProjectedColumns().isPresent()) {
            columns = jdbcTable.getProjectedColumns().get();
        }
        return super.getRecordSet(transaction, session, split, table, columns);
    }
}
