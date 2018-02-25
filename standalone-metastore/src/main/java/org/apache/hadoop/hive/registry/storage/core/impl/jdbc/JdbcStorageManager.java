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
package org.apache.hadoop.hive.registry.storage.core.impl.jdbc;


import org.apache.hadoop.hive.registry.common.QueryParam;
import org.apache.hadoop.hive.registry.common.Schema;
import org.apache.hadoop.hive.registry.common.transaction.TransactionIsolation;
import org.apache.hadoop.hive.registry.storage.core.OrderByField;
import org.apache.hadoop.hive.registry.storage.core.exception.StorageException;
import org.apache.hadoop.hive.registry.storage.core.PrimaryKey;
import org.apache.hadoop.hive.registry.storage.core.Storable;
import org.apache.hadoop.hive.registry.storage.core.StorableFactory;
import org.apache.hadoop.hive.registry.storage.core.StorableKey;
import org.apache.hadoop.hive.registry.storage.core.StorageManager;
import org.apache.hadoop.hive.registry.storage.core.TransactionManager;
import org.apache.hadoop.hive.registry.storage.core.exception.AlreadyExistsException;
import org.apache.hadoop.hive.registry.storage.core.exception.IllegalQueryParameterException;
import org.apache.hadoop.hive.registry.storage.core.impl.jdbc.provider.QueryExecutorFactory;
import org.apache.hadoop.hive.registry.storage.core.impl.jdbc.provider.sql.factory.QueryExecutor;
import org.apache.hadoop.hive.registry.storage.core.impl.jdbc.provider.sql.query.SqlSelectQuery;
import org.apache.hadoop.hive.registry.storage.core.impl.jdbc.util.CaseAgnosticStringSet;
import org.apache.hadoop.hive.registry.storage.core.search.SearchQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//Use unique constraints on respective columns of a table for handling concurrent inserts etc.
public class JdbcStorageManager implements TransactionManager, StorageManager {
    private static final Logger log = LoggerFactory.getLogger(StorageManager.class);
    public static final String DB_TYPE = "db.type";

    private final StorableFactory storableFactory = new StorableFactory();
    private QueryExecutor queryExecutor;

    public JdbcStorageManager() {
    }

    public JdbcStorageManager(QueryExecutor queryExecutor) {
        this.queryExecutor = queryExecutor;
        queryExecutor.setStorableFactory(storableFactory);
    }

    @Override
    public void add(Storable storable) throws AlreadyExistsException {
        log.debug("Adding storable [{}]", storable);
        queryExecutor.insert(storable);
    }

    @Override
    public <T extends Storable> T remove(StorableKey key) throws StorageException {
        T oldVal = get(key);
        if (key != null) {
            log.debug("Removing storable key [{}]", key);
            queryExecutor.delete(key);
        }
        return oldVal;
    }

    @Override
    public void addOrUpdate(Storable storable) throws StorageException {
        log.debug("Adding or updating storable [{}]", storable);
        queryExecutor.insertOrUpdate(storable);
    }

    @Override
    public void update(Storable storable) {
        queryExecutor.update(storable);
    }

    @Override
    public <T extends Storable> T get(StorableKey key) throws StorageException {
        log.debug("Searching entry for storable key [{}]", key);

        final Collection<T> entries = queryExecutor.select(key);
        T entry = null;
        if (entries.size() > 0) {
            if (entries.size() > 1) {
                log.debug("More than one entry found for storable key [{}]", key);
            }
            entry = entries.iterator().next();
        }
        log.debug("Querying key = [{}]\n\t returned [{}]", key, entry);
        return entry;
    }

    @Override
    public <T extends Storable> Collection<T> find(String namespace, List<QueryParam> queryParams)
            throws StorageException {
        log.debug("Searching for entries in table [{}] that match queryParams [{}]", namespace, queryParams);

        return find(namespace, queryParams, Collections.emptyList());
    }

    @Override
    public <T extends Storable> Collection<T> find(String namespace,
                                                   List<QueryParam> queryParams,
                                                   List<OrderByField> orderByFields) throws StorageException {

        log.debug("Searching for entries in table [{}] that match queryParams [{}] and order by [{}]", namespace, queryParams, orderByFields);

        if (queryParams == null || queryParams.isEmpty()) {
            return list(namespace, orderByFields);
        }

        Collection<T> entries = Collections.emptyList();
        try {
            StorableKey storableKey = buildStorableKey(namespace, queryParams);
            if (storableKey != null) {
                entries = queryExecutor.select(storableKey, orderByFields);
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }

        log.debug("Querying table = [{}]\n\t filter = [{}]\n\t returned [{}]", namespace, queryParams, entries);

        return entries;
    }

    @Override
    public <T extends Storable> Collection<T> search(SearchQuery searchQuery) {
        return queryExecutor.select(searchQuery);
    }

    private <T extends Storable> Collection<T> list(String namespace, List<OrderByField> orderByFields) {
        log.debug("Listing entries for table [{}]", namespace);
        final Collection<T> entries = queryExecutor.select(namespace, orderByFields);
        log.debug("Querying table = [{}]\n\t returned [{}]", namespace, entries);
        return entries;
    }

    @Override
    public <T extends Storable> Collection<T> list(String namespace) throws StorageException {
        return list(namespace, Collections.emptyList());
    }

    @Override
    public void cleanup() throws StorageException {
        queryExecutor.cleanup();
    }

    @Override
    public Long nextId(String namespace) {
        log.debug("Finding nextId for table [{}]", namespace);
        // This only works if the table has auto-increment. The TABLE_SCHEMA part is implicitly specified in the Connection object
        // SELECT AUTO_INCREMENT FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'temp' AND TABLE_SCHEMA = 'test'
        return queryExecutor.nextId(namespace);
    }

    @Override
    public void registerStorables(Collection<Class<? extends Storable>> classes) throws StorageException {
        storableFactory.addStorableClasses(classes);
    }

    // private helper methods

    /**
     * Query parameters are typically specified for a column or key in a database table or storage namespace. Therefore, we build
     * the {@link StorableKey} from the list of query parameters, and then can use {@link SqlSelectQuery} builder to generate the query using
     * the query parameters in the where clause
     *
     * @return {@link StorableKey} with all query parameters that match database columns <br/>
     * null if none of the query parameters specified matches a column in the DB
     */
    private StorableKey buildStorableKey(String namespace, List<QueryParam> queryParams) throws Exception {
        final Map<Schema.Field, Object> fieldsToVal = new HashMap<>();
        StorableKey storableKey = null;

        try {
            CaseAgnosticStringSet columnNames = queryExecutor.getColumnNames(namespace);
            for (QueryParam qp : queryParams) {
                if (!columnNames.contains(qp.getName())) {
                    log.warn("Query parameter [{}] does not exist for namespace [{}]. Query parameter ignored.", qp.getName(), namespace);
                } else {
                    final String val = qp.getValue();
                    final Schema.Type typeOfVal = Schema.Type.getTypeOfVal(val);
                    fieldsToVal.put(new Schema.Field(qp.getName(), typeOfVal),
                            typeOfVal.getJavaType().getConstructor(String.class).newInstance(val)); // instantiates object of the appropriate type
                }
            }

            // it is empty when none of the query parameters specified matches a column in the DB
            if (!fieldsToVal.isEmpty()) {
                final PrimaryKey primaryKey = new PrimaryKey(fieldsToVal);
                storableKey = new StorableKey(namespace, primaryKey);
            }

            log.debug("Building StorableKey from QueryParam: \n\tnamespace = [{}]\n\t queryParams = [{}]\n\t StorableKey = [{}]",
                    namespace, queryParams, storableKey);
        } catch (Exception e) {
            log.debug("Exception occurred when attempting to generate StorableKey from QueryParam", e);
            throw new IllegalQueryParameterException(e);
        }

        return storableKey;
    }

    /**
     * Initializes this instance with {@link QueryExecutor} created from the given {@code properties}.
     * Some of these properties are jdbcDriverClass, jdbcUrl, queryTimeoutInSecs.
     *
     * @param properties properties with name/value pairs
     */
    @Override
    public void init(Map<String, Object> properties) {

        if(!properties.containsKey(DB_TYPE)) {
            throw new IllegalArgumentException("db.type should be set on jdbc properties");
        }

        String type = (String) properties.get(DB_TYPE);

        // When we have more providers we can add a layer to have a factory to create respective jdbc storage managers.
        // For now, keeping it simple as there are only 2.
        if(!"mysql".equals(type) && !"postgresql".equals(type) && !"oracle".equals(type)) {
            throw new IllegalArgumentException("Unknown jdbc storage provider type: "+type);
        }
        log.info("jdbc provider type: [{}]", type);
        Map<String, Object> dbProperties = (Map<String, Object>) properties.get("db.properties");

        QueryExecutor queryExecutor = QueryExecutorFactory.get(type, dbProperties);

        this.queryExecutor = queryExecutor;
        this.queryExecutor.setStorableFactory(storableFactory);
    }

    @Override
    public void beginTransaction(TransactionIsolation transactionIsolationLevel) {
        queryExecutor.beginTransaction(transactionIsolationLevel);
    }

    @Override
    public void rollbackTransaction() {
        // The guarantee in interface is accomplished once the instance has an implementation of
        // QueryExecutor which extends AbstractQueryExecutor,
        // given that AbstractQueryExecutor.rollbackTransaction() guarantees the behavior.

        // Another implementations of QueryExecutor should provide a way of guaranteeing the
        // behavior, like call closeConnection() when rollbackTransaction() is failing.
        queryExecutor.rollbackTransaction();
    }

    @Override
    public void commitTransaction() {
        queryExecutor.commitTransaction();
    }
}
