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
package org.apache.hadoop.hive.registry.cache;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.apache.hadoop.hive.registry.SchemaBranch;
import org.apache.hadoop.hive.registry.SchemaBranchKey;
import org.apache.hadoop.hive.registry.errors.SchemaBranchNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class SchemaBranchCache implements AbstractCache {
  private static final Logger LOG = LoggerFactory.getLogger(SchemaBranchCache.class);

  private final LoadingCache<Key, SchemaBranch> loadingCache;
  private final BiMap <SchemaBranchKey, Long> schemaBranchNameToIdMap;

  public SchemaBranchCache(Integer size, Long expiryInSecs, final SchemaBranchFetcher schemaBranchFetcher) {
    schemaBranchNameToIdMap = Maps.synchronizedBiMap(HashBiMap.create());
    loadingCache = CacheBuilder.newBuilder()
            .maximumSize(size)
            .expireAfterAccess(expiryInSecs, TimeUnit.SECONDS)
            .build(new CacheLoader<Key, SchemaBranch>() {
              @Override
              public SchemaBranch load(Key key) throws Exception {
                SchemaBranch schemaBranch;
                Key otherKey;
                if (key.getSchemaBranchKey() != null) {
                  schemaBranch = schemaBranchFetcher.getSchemaBranch(key.getSchemaBranchKey());
                  otherKey = Key.of(schemaBranch.getId());
                  schemaBranchNameToIdMap.put(key.getSchemaBranchKey(), schemaBranch.getId());
                } else if (key.getId() != null) {
                  schemaBranch = schemaBranchFetcher.getSchemaBranch(key.getId());
                  otherKey = Key.of(new SchemaBranchKey(schemaBranch.getName(), schemaBranch.getSchemaMetadataName()));
                  schemaBranchNameToIdMap.put(otherKey.schemaBranchKey, schemaBranch.getId());
                } else {
                  throw new IllegalArgumentException("Given argument is not valid: " + key);
                }
                loadingCache.put(otherKey, schemaBranch);
                return schemaBranch;
              }
            });
  }

  public SchemaBranch get(Key key) throws SchemaBranchNotFoundException {
    SchemaBranch schemaBranch;
    try {
      schemaBranch = loadingCache.get(key);
    } catch (UncheckedExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof SchemaBranchNotFoundException) {
        throw (SchemaBranchNotFoundException) cause;
      } else {
        throw new RuntimeException(e);
      }
    } catch (ExecutionException e) {
      LOG.error("Error occurred while retrieving schema branch for [{}]", key, e);
      throw new RuntimeException(e);
    }

    return schemaBranch;
  }

  public void put(Key key, SchemaBranch schemaBranch) {
    loadingCache.put(key, schemaBranch);
  }

  public SchemaBranch getIfPresent(Key key) {
    return loadingCache.getIfPresent(key);
  }


  public void invalidateSchemaBranch(SchemaBranchCache.Key key) {
    LOG.info("Invalidating cache entry for key [{}]", key);

    // If the cache doesn't have entry for the key, then no need to invalidate the cache
    if(loadingCache.getIfPresent(key) == null)
      return;

    loadingCache.invalidate(key);

    Key otherKey = key.id == null ? Key.of(schemaBranchNameToIdMap.get(key.getSchemaBranchKey())) : Key.of(schemaBranchNameToIdMap.inverse().get(key.id));
    loadingCache.invalidate(otherKey);
  }

  public void invalidateAll() {
    LOG.info("Invalidating all the cache entries");

    loadingCache.invalidateAll();
  }

  @Override
  public SchemaRegistryCacheType getCacheType() {
    return SchemaRegistryCacheType.SCHEMA_BRANCH_CACHE;
  }

  public interface SchemaBranchFetcher {
    SchemaBranch getSchemaBranch(SchemaBranchKey schemaBranchKey) throws SchemaBranchNotFoundException;

    SchemaBranch getSchemaBranch(Long id) throws SchemaBranchNotFoundException;
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Key {

    @JsonProperty
    private SchemaBranchKey schemaBranchKey;

    @JsonProperty
    private Long id;

    private Key(SchemaBranchKey schemaBranchKey) {
      Preconditions.checkNotNull(schemaBranchKey, "schemaBranchKey can not be null");
      this.schemaBranchKey = schemaBranchKey;
    }

    private Key(Long id) {
      Preconditions.checkNotNull(id, "id can not be null");
      this.id = id;
    }

    // For JSON serialization/deserialization
    private Key() {

    }

    public SchemaBranchKey getSchemaBranchKey() {
      return schemaBranchKey;
    }

    public Long getId() {
      return id;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      Key key = (Key) o;

      if (schemaBranchKey != null ? !schemaBranchKey.equals(key.schemaBranchKey) : key.schemaBranchKey != null) return false;
      return id != null ? id.equals(key.id) : key.id == null;

    }

    @Override
    public int hashCode() {
      int result = schemaBranchKey != null ? schemaBranchKey.hashCode() : 0;
      result = 31 * result + (id != null ? id.hashCode() : 0);
      return result;
    }

    public static Key of(SchemaBranchKey schemaBranchKey) {
      return new Key(schemaBranchKey);
    }

    public static Key of(Long id) {
      return new Key(id);
    }
  }
}

