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
package org.apache.hadoop.hive.metastore.properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;

import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A property map store using a pull-thru cache.
 * <p>
 * Before a map is returned, a check against the stored corresponding digest is performed to avoid
 * using stale data.
 * </p>
 */
public class CachingPropertyStore extends PropertyStore {
  protected final SoftCache<String, PropertyMap> maps;
  protected final PropertyStore store;
  public CachingPropertyStore(PropertyStore wrap) {
    this(wrap, new Configuration());
  }

  public CachingPropertyStore(PropertyStore wrap, Configuration conf) {
    store = wrap;
    int capacity = MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.PROPERTIES_CACHE_CAPACITY);
    float fillFactor = (float) MetastoreConf.getDoubleVar(conf, MetastoreConf.ConfVars.PROPERTIES_CACHE_LOADFACTOR);
    maps = new SoftCache<>(capacity, fillFactor, false);
  }
  public void clearCache() {
    maps.clear();
  }

  @Override public UUID fetchDigest(String mapKey) {
    return store.fetchDigest(mapKey);
  }

  @Override
  public Map<String, UUID> selectDigest(String keyPrefix, Predicate<String> keyFilter) {
    return store.selectDigest(keyPrefix, keyFilter);
  }

  @Override
  public PropertyMap fetchProperties(final String mapKey, final Function<String, PropertySchema> getSchema) {
    synchronized(this) {
      PropertyMap map = maps.compute(mapKey, mapsCompute(mapKey, getSchema));
      // we always return a copy of the properties in the cache
      return map != null? map.copy() : null;
    }
  }

  BiFunction<String, PropertyMap, PropertyMap> mapsCompute(String string, Function<String, PropertySchema> getSchema) {
    return (k, v) -> {
      PropertyMap map = v;
      if (map != null) {
        UUID digest = map.getDigest();
        UUID fetchedDigest = fetchDigest(string);
        if (fetchedDigest != null && !Objects.equals(digest, fetchedDigest)) {
          map = null;
        }
      }
      if (map == null) {
        map = store.fetchProperties(string, getSchema);
      }
      return map;
    };
  }

  @Override
  public Map<String, PropertyMap> selectProperties(final String keyPrefix, Predicate<String> keyFilter, Function<String, PropertySchema> getSchema) {
    final Map<String, PropertyMap> results = new TreeMap<>();
    // go select the digests for the maps we seek
    final Map<String, UUID> digests = store.selectDigest(keyPrefix, keyFilter);
    final Iterator<Map.Entry<String, UUID>> idigest = digests.entrySet().iterator();
    while (idigest.hasNext()) {
      Map.Entry<String, UUID> entry = idigest.next();
      String key = entry.getKey();
      PropertyMap map = maps.get(key);
      // remove from maps to select and add to results if in the cache and digest is valid
      if (map != null && Objects.equals(map.getDigest(), entry.getValue())) {
        results.put(key, map.copy());
        idigest.remove();
      }
    }
    // digests now contains the names of maps required that are not results
    Map<String, PropertyMap> selectedMaps = store.selectProperties(keyPrefix, digests::containsKey, getSchema);
    // we cache those new maps and for each add the copy to the result if we have not loaded and cached it concurrently
    selectedMaps.forEach((k, v) -> {
      PropertyMap m = maps.putIfAbsent(k, v);
      results.put(k, m != null && m.isDirty()? m : v.copy());
    });
    return results;
  }

  @Override
  public void saveProperties(String mapKey, PropertyMap map) {
    synchronized(this) {
      store.saveProperties(mapKey, map);
      maps.put(mapKey, map);
    }
  }

  @Override
  protected boolean dropProperties(String mapKey) {
    synchronized(this) {
      boolean b = store.dropProperties(mapKey);
      maps.clear();
      return b;
    }
  }

  @Override
  public boolean renameProperties(String mapKey, String newKey) {
    synchronized (this) {
      // target is unencumbered
      if (!maps.containsKey(newKey)) {
        PropertyMap map = maps.remove(mapKey);
        // we got a source
        if (map != null) {
          maps.put(newKey, map);
          return true;
        }
      }
      return false;
    }
  }

}
