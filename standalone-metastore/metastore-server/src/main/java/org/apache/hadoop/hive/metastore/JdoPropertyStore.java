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
package org.apache.hadoop.hive.metastore;

import org.apache.commons.jexl3.JexlException;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.model.MMetastoreDBProperties;
import org.apache.hadoop.hive.metastore.properties.PropertyException;
import org.apache.hadoop.hive.metastore.properties.PropertyMap;
import org.apache.hadoop.hive.metastore.properties.PropertySchema;
import org.apache.hadoop.hive.metastore.properties.PropertyStore;

import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Implementation of the property store delegating persistence to a (jdo) raw store.
 */
public class JdoPropertyStore extends PropertyStore {
  /** The jdo objects store. */
  private final ObjectStore objectStore;

  /**
   * Basic ctor.
   * @param store the object store
   */
  public JdoPropertyStore(ObjectStore store) {
    this.objectStore = store;
  }

  @Override
  public PropertyMap fetchProperties(final String mapKey, Function<String, PropertySchema> getSchema) {
    try {
      return objectStore.getProperties(mapKey, getPropertyMapFunction(null, getSchema));
    } catch (MetaException | JexlException e) {
      throw new PropertyException(e);
    }
  }

  @Override
  public Map<String, PropertyMap> selectProperties(final String keyPrefix, Predicate<String> keyFilter, Function<String, PropertySchema> getSchema) {
    try {
      return objectStore.selectProperties(keyPrefix, getPropertyMapFunction(keyFilter, getSchema));
    } catch (MetaException | JexlException e) {
      throw new PropertyException(e);
    }
  }

  @Override
  public UUID fetchDigest(String mapKey) {
    try {
      return objectStore.getProperties(mapKey, (mm) -> UUID.fromString(mm.getPropertyValue()));
    } catch (MetaException | JexlException e) {
      throw new PropertyException(e);
    }
  }

  @Override
  public Map<String, UUID> selectDigest(String keyPrefix, Predicate<String> keyFilter) {
    try {
      return objectStore.selectProperties(keyPrefix, (mm) -> {
        if (keyFilter == null || keyFilter.test(mm.getPropertykey())) {
          return UUID.fromString(mm.getPropertyValue());
        }
        return null;
      });
    } catch (MetaException | JexlException e) {
      throw new PropertyException(e);
    }
  }

  @Override
  public void saveProperties(Iterator<Map.Entry<String, PropertyMap>> save) {
    // will run the super method in a transaction
    try {
      objectStore.runInTransaction(()-> super.saveProperties(save));
    } catch (MetaException e) {
      throw new PropertyException(e);
    }
  }

  @Override
  protected void saveProperties(String mapKey, PropertyMap map) {
    try {
      if (map.isDropped()) {
        objectStore.dropProperties(mapKey);
      } else {
        objectStore.putProperties(mapKey, map.getDigest().toString(), null, serialize(map));
      }
    } catch (MetaException e) {
      throw new PropertyException(e);
    }
  }

  @Override public boolean dropProperties(String mapKey) {
   try {
     return objectStore.dropProperties(mapKey);
   } catch (MetaException e) {
     throw new PropertyException(e);
   }
  }

  @Override public boolean renameProperties(String mapKey, String newKey) {
    try {
      return objectStore.renameProperties(mapKey, newKey);
    } catch (MetaException e) {
      throw new PropertyException(e);
    }
  }
  /**
   * Creates a function that transforms an MMetastoreDBProperties into a PropertyMap.
   * @param keyFilter a map key filtering predicate that will make the function return null if test fails
   * @param getSchema the function that solves a schema from a key
   * @return a function
   */
  Function<MMetastoreDBProperties, PropertyMap> getPropertyMapFunction(final Predicate<String> keyFilter, final Function<String, PropertySchema> getSchema) {
    return (mm) -> {
      final String key = mm.getPropertykey();
      if (keyFilter == null || keyFilter.test(key)) {
        final byte[] bytes = mm.getPropertyContent();
        if (bytes != null) {
          final PropertySchema schema = getSchema.apply(key);
          final PropertyMap map = deserialize(bytes, s->schema);
          // if read dirty - the schema version changed -, write back to make clean
          if (map.isDirty()) {
            saveProperties(key, map);
          }
          return map;
        }
      }
      return null;
    };
  }
}

