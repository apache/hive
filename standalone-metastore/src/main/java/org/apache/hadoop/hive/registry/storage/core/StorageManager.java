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
package org.apache.hadoop.hive.registry.storage.core;

import org.apache.hadoop.hive.registry.common.QueryParam;
import org.apache.hadoop.hive.registry.storage.core.exception.StorageException;
import org.apache.hadoop.hive.registry.storage.core.search.SearchQuery;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * TODO: All the methods are very restrictive and needs heavy synchronization to get right but my assumption is that
 * Writes are infrequent and the best place to achieve these guarantees, if we ever need it, is going to be at storage
 * layer implementation it self.
 *
 */
public interface StorageManager {

    /**
     * Initialize respective {@link StorageManager}  with the given properties
     *
     * @param properties the properties
     */
    void init(Map<String, Object> properties);
    
    /**
     * TODO: update this javadoc
     * Adds this storable to storage layer, if the storable with same {@code Storable.getPrimaryKey} exists,
     * it will do a get and ensure new and old storables instances are equal in which case it will return false.
     * If the old and new instances are not the same it will throw {@code AlreadyExistsException}.
     * Will return true if no existing storable entity was found and the supplied entity gets added successfully.
     *
     * @param storable the storable
     * @throws StorageException
     */
    void add(Storable storable) throws StorageException;

    /**
     * Removes a {@link Storable} object identified by a {@link StorableKey}.
     * If the key does not exist a null value is returned, no exception is thrown.
     *
     * @param key of the {@link Storable} object to remove
     * @return object that got removed, null if no object was removed.
     * @throws StorageException
     */
    <T extends Storable> T remove(StorableKey key) throws StorageException;

    /**
     * Unlike add, if the storage entity already exists, it will be updated. If it does not exist, it will be created.
     *
     * @param storable the storable
     * @throws StorageException
     */
    void addOrUpdate(Storable storable) throws StorageException;

    /**
     * Updates an existing {@link Storable}. Throws {@link StorageException} if the
     * storable could not be updated.
     *
     * @param storable the storable
     */
    void update(Storable storable);

    /**
     * Gets the storable entity by using {@code Storable.getPrimaryKey()} as lookup key, return null if no storable entity with
     * the supplied key is found.
     *
     * @param key the key
     * @return the storable
     * @throws StorageException
     */
    <T extends Storable> T get(StorableKey key) throws StorageException;

    /**
     * Returns if an entity with the given key exists or not.
     *
     * @param key the Storable key
     * @return true if the entity with given key exists, false otherwise
     */
    default boolean exists(StorableKey key) {
        return get(key) != null;
    }

    /**
     * Get the list of storable entities in the namespace, matching the query params.
     * <pre>
     * E.g get a list of all devices with deviceId="nest" and version=1
     *
     * List&lt;QueryParam&gt; params = Arrays.asList(new QueryParam("deviceId", "nest"), new QueryParam("version", "1");
     *
     * List&lt;Device&gt; devices = find(DEVICE_NAMESPACE, params);
     * </pre>
     *
     * @param namespace the namespace
     * @param queryParams the query params
     * @return the storables
     * @throws StorageException
     */
    <T extends Storable> Collection<T> find(String namespace, List<QueryParam> queryParams) throws StorageException;

    /**
     * Returns the collection of storable entities in the given {@code namespace}, matching given {@code queryParams} and
     * order by the given list of {@code orderByFields}
     *
     * @param namespace the namespace
     * @param queryParams the query params
     * @param orderByFields the order by fields
     * @param <T> the storable type
     * @return the storables
     * @throws StorageException when any storage error occurs
     */
    <T extends Storable> Collection<T> find(String namespace, List<QueryParam> queryParams, List<OrderByField> orderByFields) throws StorageException;

    /**
     *
     * @param searchQuery the search query
     * @param <T> the type
     * @return the storables
     */
    <T extends Storable> Collection<T> search(SearchQuery searchQuery);

    /**
     * Lists all {@link Storable} objects existing in the given namespace. If no entity is found, and empty list will be returned.
     * @param namespace the namespace
     * @return the storables
     * @throws StorageException
     */
    <T extends Storable> Collection<T> list(String namespace) throws StorageException;

    /**
     * This can be used to cleanup resources held by this instance.
     *
     * @throws StorageException
     */
    void cleanup() throws StorageException;

    Long nextId(String namespace) throws StorageException;

    /**
     * Registers a Collection of {@link Storable}} classes to be used in {@link StorableFactory} for creating instances
     * of a given namespace.
     *
     * @param classes the storable classes to register
     * @throws StorageException
     */
    void registerStorables(Collection<Class<? extends Storable>> classes) throws StorageException;

}
