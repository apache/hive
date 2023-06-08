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

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A non-persistent store, for tests mainly.
 */
public class TransientPropertyStore extends PropertyStore {
  /**
   * The property maps of this store.
   */
  private final Map<String, byte[]> properties;
  /**
   * The digests for the stored maps.
   */
  private final Map<String, UUID> digests;

  public TransientPropertyStore() {
    properties = new TreeMap<>();
    digests = new TreeMap<>();
  }

  @Override public synchronized PropertyMap fetchProperties(String mapKey, Function<String, PropertySchema> getSchema) {
    byte[] bytes = properties.get(mapKey);
    if (bytes != null) {
      PropertyMap map = deserialize(bytes, getSchema);
      // if read dirty, write back to make clean
      if (map.isDirty()) {
        saveProperties(mapKey, map);
      }
      return map;
    }
    return null;
  }

  @Override public synchronized Map<String, PropertyMap> selectProperties(String keyPrefix, Predicate<String> keyFilter, Function<String, PropertySchema> getSchema) {
    final Map<String, PropertyMap> result = new TreeMap<>();
    properties.forEach((key,bytes)-> {
      if ((keyPrefix == null || key.startsWith(keyPrefix))
          && (keyFilter == null || keyFilter.test(key))
          && bytes != null) {
        PropertyMap map = deserialize(bytes, getSchema);
        // if read dirty, write back to make clean
        if (map.isDirty()) {
          saveProperties(key, map);
        }
        result.put(key, map);
      }
    });
    return result.isEmpty()? Collections.emptyMap() : result;
  }

  @Override public synchronized UUID fetchDigest(String mapKey) {
    return digests.get(mapKey.toString());
  }

  @Override
  public synchronized Map<String, UUID> selectDigest(String keyPrefix, Predicate<String> keyFilter) {
    final Map<String, UUID> result = new TreeMap<>();
    digests.forEach((name, uuid) -> {
      if (name.startsWith(keyPrefix) && (keyFilter == null || keyFilter.test(name))) {
        result.put(name, uuid);
      }
    });
    return result.isEmpty() ? Collections.emptyMap() : result;
  }

  @Override public synchronized void saveProperties(String mapKey, PropertyMap map) {
    UUID digest = map.getDigest();
    byte[] data = serialize(map);
    digests.put(mapKey, digest);
    properties.put(mapKey, data);
    map.setClean();
  }

  @Override public synchronized boolean dropProperties(String mapKey) {
    boolean m = properties.remove(mapKey) != null;
    boolean d = digests.remove(mapKey) != null;
    return m & d;
  }

  @Override
  public synchronized boolean renameProperties(String mapKey, String newKey) {
    if (!properties.containsKey(newKey)) {
      byte[] map = properties.remove(mapKey);
      if (map != null) {
        properties.put(newKey, map);
        UUID digest = digests.remove(mapKey);
        if (digest != null) {
          digests.put(newKey, digest);
        }
        return true;
      }
    }
    return false;
  }
}

