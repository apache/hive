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
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * The PropertyStore is the persistent container of property maps.
 * Maps are addressed in the store by their key -  their name prepended by their manager&quot;s namespace.
 */

public abstract class PropertyStore {
  /**
   * Fetches a property map.
   * @param mapKey the map key
   * @param getSchema the method to retrieve a schema if the map needs to be created
   * @return the item property map
   */
  public abstract PropertyMap fetchProperties(String mapKey, Function<String, PropertySchema> getSchema);


  /**
   * Fetches a map of property maps.
   * @param keyPrefix the map key prefix
   * @param keyFilter a filter for map keys
   * @param getSchema the method to retrieve a schema if the map needs to be created
   * @return the map of property map
   */
  public abstract Map<String, PropertyMap> selectProperties(final String keyPrefix, Predicate<String> keyFilter, Function<String, PropertySchema> getSchema);
  /**
   * Fetches a property map digest.
   * @param mapKey the map key
   * @return the item property map
   */
  public abstract UUID fetchDigest(String mapKey);

  /**
   * Fetches a map of property maps digest keyed by their name.
   * @param keyPrefix the map key prefix
   * @param keyFilter a filter for map keys
   * @return the map of property map digests
   */
  public abstract Map<String, UUID> selectDigest(String keyPrefix, Predicate<String> keyFilter);

    /**
     * Persists a property map.
     * @param mapKey the map key
     * @param map the map instance
     */
  protected abstract void saveProperties(String mapKey, PropertyMap map);


  /**
   * Drops a property map.
   * @param mapKey the map key
   */
  protected abstract boolean dropProperties(String mapKey);

  /**
   * Renames a property map.
   * @param mapKey the map source key
   * @param newKey the new target key
   */
  public abstract boolean renameProperties(String mapKey, String newKey);

  /**
   * Persists an iterator property map.
   * <p>May be useful to override to use one transaction.</p>
   * @param save the iterator on pairs for map key, property map
   */
  public void saveProperties(Iterator<Map.Entry<String, PropertyMap>> save) {
    while(save.hasNext()) {
      Map.Entry<String, PropertyMap> pair = save.next();
      PropertyMap map = pair.getValue();
      if (map != null) {
        saveProperties(pair.getKey(), map);
      } else {
        dropProperties(pair.getKey());
      }
    }
  }

  /**
   * Serializes a map as a byte array.
   * @param map the (nonnull) map to write
   * @return the byte array
   */
  public byte[] serialize(PropertyMap map) {
    return SerializationProxy.toBytes(map);
  }

  /**
   * Deserializes a map from a byte array.
   * @param bytes the byte array
   * @return the (nonnull) oroperty map
   */
  public PropertyMap deserialize(byte[] bytes, Function<String, PropertySchema> getSchema) {
    return SerializationProxy.fromBytes(bytes, this, getSchema);
  }

  /**
   * Default ctor.
   */
  protected PropertyStore() {
  }

  /**
   * A non-persistent store, for tests mainly.
   */
  public static class Transient extends PropertyStore {
    /**
     * The property maps of this store.
     */
    private final Map<String, byte[]> properties;
    /**
     * The digests for the stored maps.
     */
    private final Map<String, UUID> digests;

    public Transient() {
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

}
