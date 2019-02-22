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
package io.prestosql.plugin.jdbc;

import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.statistics.TableStatistics;
import io.prestosql.spi.type.Type;

import javax.annotation.Nullable;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public interface JdbcClient
{
    default boolean schemaExists(String schema)
    {
        return getSchemaNames().contains(schema);
    }

    Set<String> getSchemaNames();

    List<SchemaTableName> getTableNames(@Nullable String schema);

    @Nullable
    JdbcTableHandle getTableHandle(SchemaTableName schemaTableName);

    List<JdbcColumnHandle> getColumns(ConnectorSession session, JdbcTableHandle tableHandle);

    Optional<ColumnMapping> toPrestoType(ConnectorSession session, JdbcTypeHandle typeHandle);

    WriteMapping toWriteMapping(Type type);

    ConnectorSplitSource getSplits(JdbcTableLayoutHandle layoutHandle);

    Connection getConnection(JdbcSplit split)
            throws SQLException;

    default void abortReadConnection(Connection connection)
            throws SQLException
    {
        // most drivers do not need this
    }

    PreparedStatement buildSql(ConnectorSession session, Connection connection, JdbcSplit split, List<JdbcColumnHandle> columnHandles)
            throws SQLException;

    JdbcOutputTableHandle beginCreateTable(ConnectorTableMetadata tableMetadata);

    void commitCreateTable(JdbcOutputTableHandle handle);

    JdbcOutputTableHandle beginInsertTable(ConnectorTableMetadata tableMetadata);

    void finishInsertTable(JdbcOutputTableHandle handle);

    void dropTable(JdbcTableHandle jdbcTableHandle);

    void rollbackCreateTable(JdbcOutputTableHandle handle);

    String buildInsertSql(JdbcOutputTableHandle handle);

    Connection getConnection(JdbcOutputTableHandle handle)
            throws SQLException;

    PreparedStatement getPreparedStatement(Connection connection, String sql)
            throws SQLException;

    TableStatistics getTableStatistics(ConnectorSession session, JdbcTableHandle handle, TupleDomain<ColumnHandle> tupleDomain);
    
    public Array getArray(Connection connection, Type elementType, Block arrayBlock)
            throws SQLException; 
}
