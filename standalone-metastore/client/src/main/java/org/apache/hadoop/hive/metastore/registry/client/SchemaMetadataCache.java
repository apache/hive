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
package org.apache.hadoop.hive.metastore.registry.client;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.hadoop.hive.registry.SchemaMetadataInfo;
import org.apache.hadoop.hive.registry.common.errors.SchemaNotFoundException;
import org.apache.hadoop.hive.metastore.registry.exceptions.RegistryException;
import org.apache.hadoop.hive.metastore.registry.exceptions.RegistryRetryableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class SchemaMetadataCache {
    private static final Logger LOG = LoggerFactory.getLogger(SchemaMetadataCache.class);

    private final LoadingCache<Key, SchemaMetadataInfo> loadingCache;

    public SchemaMetadataCache(Long size, Long expiryInSecs, final SchemaMetadataFetcher schemaMetadataFetcher) {
        loadingCache = CacheBuilder.newBuilder()
                .maximumSize(size)
                .expireAfterAccess(expiryInSecs, TimeUnit.SECONDS)
                .build(new CacheLoader<Key, SchemaMetadataInfo>() {
                    @Override
                    public SchemaMetadataInfo load(Key key) throws Exception {
                        if (key.getName() != null) {
                            return schemaMetadataFetcher.fetch(key.getName());
                        } else if (key.getId() != null) {
                            return schemaMetadataFetcher.fetch(key.getId());
                        } else {
                            throw new RegistryException("Key should have name or id as non null");
                        }
                    }
                });
    }

    public SchemaMetadataInfo get(Key key) {
        SchemaMetadataInfo schemaMetadataInfo;
        try {
            schemaMetadataInfo = loadingCache.get(key);
        } catch (ExecutionException e) {
            LOG.error("Error occurred while retrieving schema metadata for [{}]", key, e);
            Throwable cause = e.getCause();
            if (cause instanceof IOException) {
                throw new RegistryRetryableException(cause.getMessage(), cause);
            } else if (cause instanceof RuntimeException) {
                if (cause.getCause() instanceof IOException) {
                    throw new RegistryRetryableException(cause.getMessage(), cause);
                } else {
                    throw new RegistryException(cause.getMessage(), cause);
                }
            } else if (!(cause instanceof SchemaNotFoundException)) {
                throw new RegistryException(cause.getMessage(), cause);
            }
            schemaMetadataInfo = null;
        }

        return schemaMetadataInfo;
    }
    
    public void put(Key key, SchemaMetadataInfo schemaMetadataInfo) {
        loadingCache.put(key, schemaMetadataInfo);
    }

    public SchemaMetadataInfo getIfPresent(Key key) {
        return loadingCache.getIfPresent(key);
    }

    public interface SchemaMetadataFetcher {
        SchemaMetadataInfo fetch(String name) throws SchemaNotFoundException;

        SchemaMetadataInfo fetch(Long id) throws SchemaNotFoundException;
    }

    public static class Key {
        private String name;
        private Long id;

        private Key(String name) {
            Preconditions.checkNotNull("name can not be null", name);
            this.name = name;
        }

        private Key(Long id) {
            Preconditions.checkNotNull("id can not be null", id);
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public Long getId() {
            return id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Key key = (Key) o;

            if (name != null ? !name.equals(key.name) : key.name != null) return false;
            return id != null ? id.equals(key.id) : key.id == null;

        }

        @Override
        public int hashCode() {
            int result = name != null ? name.hashCode() : 0;
            result = 31 * result + (id != null ? id.hashCode() : 0);
            return result;
        }

        public static Key of(String name) {
            return new Key(name);
        }

        public static Key of(Long id) {
            return new Key(id);
        }
    }
}
