/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.registry.storage.core.impl.jdbc.provider.postgresql.factory;

/**
 *
 */

import com.google.common.cache.CacheBuilder;
import org.apache.hadoop.hive.registry.common.Schema;
import org.apache.hadoop.hive.registry.storage.core.OrderByField;
import org.apache.hadoop.hive.registry.storage.core.impl.jdbc.provider.postgresql.query.PostgresqlInsertUpdateDuplicate;
import org.apache.hadoop.hive.registry.storage.core.impl.jdbc.provider.postgresql.query.PostgresqlSelectQuery;
import org.apache.hadoop.hive.registry.storage.core.impl.jdbc.provider.postgresql.query.PostgresqlUpdateQuery;
import org.apache.hadoop.hive.registry.storage.core.impl.jdbc.provider.sql.query.SqlQuery;
import org.apache.hadoop.hive.registry.storage.core.Storable;
import org.apache.hadoop.hive.registry.storage.core.StorableKey;
import org.apache.hadoop.hive.registry.storage.core.impl.jdbc.config.ExecutionConfig;
import org.apache.hadoop.hive.registry.storage.core.impl.jdbc.connection.ConnectionBuilder;
import org.apache.hadoop.hive.registry.storage.core.impl.jdbc.provider.postgresql.query.PostgresqlDeleteQuery;
import org.apache.hadoop.hive.registry.storage.core.impl.jdbc.provider.postgresql.query.PostgresqlInsertQuery;
import org.apache.hadoop.hive.registry.storage.core.impl.jdbc.provider.sql.factory.AbstractQueryExecutor;
import org.apache.hadoop.hive.registry.storage.core.impl.jdbc.provider.sql.statement.PreparedStatementBuilder;
import org.apache.hadoop.hive.registry.storage.core.search.SearchQuery;

import java.sql.ResultSet;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * SQL query executor for PostgreSQL
 * <p>
 * To issue the new ID to insert and get auto issued key in concurrent manner, PostgresExecutor utilizes Postgres's
 * SERIAL feature
 * <p>
 * If the value of id is null, we let Postgres issue new ID and get the new ID. If the value of id is not null, we just use that value.
 */
public class PostgresqlExecutor extends AbstractQueryExecutor {

    /**
     * @param config            Object that contains arbitrary configuration that may be needed for any of the steps of the query execution process
     * @param connectionBuilder Object that establishes the connection to the database
     */
    public PostgresqlExecutor(ExecutionConfig config, ConnectionBuilder connectionBuilder) {
        super(config, connectionBuilder);
    }

    /**
     * @param config            Object that contains arbitrary configuration that may be needed for any of the steps of the query execution process
     * @param connectionBuilder Object that establishes the connection to the database
     * @param cacheBuilder      Guava cache configuration. The maximum number of entries in cache (open connections)
     *                          must not exceed the maximum number of open database connections allowed
     */
    public PostgresqlExecutor(ExecutionConfig config, ConnectionBuilder connectionBuilder, CacheBuilder<SqlQuery, PreparedStatementBuilder> cacheBuilder) {
        super(config, connectionBuilder, cacheBuilder);
    }

    // ============= Public API methods =============

    @Override
    public void insert(Storable storable) {
        insertOrUpdateWithUniqueId(storable, new PostgresqlInsertQuery(storable));
    }

    @Override
    public void insertOrUpdate(final Storable storable) {
        insertOrUpdateWithUniqueId(storable, new PostgresqlInsertUpdateDuplicate(storable));
    }

    @Override
    public int update(Storable storable) {
        return executeUpdate(new PostgresqlUpdateQuery(storable));
    }

    @Override
    public <T extends Storable> Collection<T> select(String namespace) {
        return executeQuery(namespace, new PostgresqlSelectQuery(namespace));
    }

    @Override
    public <T extends Storable> Collection<T> select(String namespace, List<OrderByField> orderByFields) {
        return executeQuery(namespace, new PostgresqlSelectQuery(namespace, orderByFields));
    }

    @Override
    public <T extends Storable> Collection<T> select(StorableKey storableKey) {
        return executeQuery(storableKey.getNameSpace(), new PostgresqlSelectQuery(storableKey));
    }

    @Override
    public <T extends Storable> Collection<T> select(StorableKey storableKey, List<OrderByField> orderByFields) {
        return executeQuery(storableKey.getNameSpace(), new PostgresqlSelectQuery(storableKey, orderByFields));
    }

    @Override
    public void delete(StorableKey storableKey) {
        executeUpdate(new PostgresqlDeleteQuery(storableKey));
    }

    @Override
    public Long nextId(String namespace) {
        // We intentionally return null. Please refer the class javadoc for more details.
        return null;
    }

    @Override
    public <T extends Storable> Collection<T> select(SearchQuery searchQuery) {
        Schema schema = storableFactory.create(searchQuery.getNameSpace()).getSchema();
        return executeQuery(searchQuery.getNameSpace(), new PostgresqlSelectQuery(searchQuery, schema));
    }

    // this is required since the Id type in Storable is long and Postgres supports Int type for SERIAL (auto increment) field
    @Override
    protected QueryExecution getQueryExecution(SqlQuery sqlQuery) {
        return new QueryExecution(sqlQuery) {
            @Override
            protected List<Map<String, Object>> getMapsFromResultSet(ResultSet resultSet) {
                List<Map<String, Object>> res = super.getMapsFromResultSet(resultSet);
                if (res != null) {
                    res.forEach(m -> {
                        Object id = m.get("id");
                        if (id != null && id instanceof Integer) {
                            m.put("id", Long.valueOf((Integer) id));
                        }
                    });
                }
                return res;
            }
        };
    }

    private void insertOrUpdateWithUniqueId(final Storable storable, final SqlQuery sqlQuery) {
        try {
            Long id = storable.getId();
            if (id == null) {
                id = executeUpdateWithReturningGeneratedKey(sqlQuery);
                log.debug("after executeUpdate, generated id {}", id);
                storable.setId(id);
            } else {
                executeUpdate(sqlQuery);
            }
        } catch (Exception e) {
            executeUpdate(sqlQuery);
        }
    }


}