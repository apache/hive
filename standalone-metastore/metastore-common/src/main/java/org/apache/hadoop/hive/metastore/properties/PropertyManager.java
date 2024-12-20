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


import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlContext;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlException;
import org.apache.commons.jexl3.JexlExpression;
import org.apache.commons.jexl3.JexlFeatures;
import org.apache.commons.jexl3.JexlScript;
import org.apache.commons.jexl3.ObjectContext;
import org.apache.commons.jexl3.introspection.JexlPermissions;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.Function;

/**
 * A property manager.
 * <p>
 * This handles operations at the higher functional level; an instance is created per-session and
 * drives queries and updates in a transactional manner.
 * </p>
 * <p>
 * The manager ties the property schemas into one namespace; all property maps it handles must and will use
 * one of its known schema.
 * </p>
 * <p>The manager class needs to be registered with its namespace as key</p>
 * <p>
 *   Since a collection of properties are stored in a map, to avoid hitting the persistence store for each update
 *   - which would mean rewriting the map multiple times - the manager keeps track of dirty maps whilst
 *   serving as transaction manager. This way, when importing multiple properties targeting different elements (think
 *   setting properties for different tables), each impacted map is only rewritten
 *   once by the persistence layer during commit. This also allows multiple calls to participate to one transactions.
 * </p>
 */
public abstract class PropertyManager {
  /** The logger. */
  public static final Logger LOGGER = LoggerFactory.getLogger(PropertyManager.class);
  /** The set of dirty maps. */
  protected final Map<String, PropertyMap> dirtyMaps = new HashMap<>();
  /** This manager namespace. */
  protected final String namespace;
  /** The property map store. */
  protected final PropertyStore store;
  /** A Jexl engine for convenience. */
  static final JexlEngine JEXL;
  static {
    JexlFeatures features = new JexlFeatures()
        .sideEffect(false)
        .sideEffectGlobal(false);
    JexlPermissions p = JexlPermissions.RESTRICTED
        .compose("org.apache.hadoop.hive.metastore.properties.*");
    JEXL = new JexlBuilder()
        .features(features)
        .permissions(p)
        .create();
  }

  /**
   * The map of defined managers.
   */
  private static final Map<String, Constructor<? extends PropertyManager>> NSMANAGERS = new HashMap<>();

  /**
   * Declares a property manager class.
   * @param ns the namespace
   * @param pmClazz the property manager class
   */
  public static boolean declare(String ns, Class<? extends PropertyManager> pmClazz) {
    try {
      synchronized(NSMANAGERS) {
        Constructor<? extends PropertyManager> ctor = NSMANAGERS.get(ns);
        if (ctor == null) {
          ctor = pmClazz.getConstructor(String.class, PropertyStore.class);
          NSMANAGERS.put(ns, ctor);
          return true;
        } else {
          if (!Objects.equals(ctor.getDeclaringClass(), pmClazz)) {
            LOGGER.error("namespace {} is already declared for {}", ns, pmClazz.getCanonicalName());
          }
        }
      }
    } catch(NoSuchMethodException xnom ) {
      LOGGER.error("namespace declaration failed: " + ns + ", " + pmClazz.getCanonicalName(),
          xnom);
    }
    return false;
  }

  /**
   * Creates an instance of manager using its declared namespace.
   * @param namespace the manager&quot;s namespace
   * @param store the property store
   * @return a property manager instance
   * @throws MetaException if the manager creation fails
   * @throws NoSuchObjectException if the store is null or no constructor was declared
   */
  public static PropertyManager create(String namespace, PropertyStore store) throws MetaException, NoSuchObjectException {
    final Constructor<? extends PropertyManager> ctor;
    synchronized (NSMANAGERS) {
      ctor = NSMANAGERS.get(namespace);
    }
    if (ctor == null) {
      throw new NoSuchObjectException("no PropertyManager namespace is declared, namespace " + namespace);
    }
    if (store == null) {
      throw new NoSuchObjectException("no PropertyStore exists " + namespace);
    }
    try {
      return ctor.newInstance(namespace, store);
    } catch (Exception xany) {
      LOGGER.error("PropertyManager creation failed " + namespace, xany);
      throw new MetaException("PropertyManager creation failed, namespace " + namespace);
    }
  }

  /**
   * JEXL adapter.
   * <p>public for introspection.</p>
   */
  public static class MapWrapper implements JexlContext {
    PropertyMap map;
    MapWrapper(PropertyMap map) {
      this.map = map;
    }

    public Object get(String p) {
      return map.getPropertyValue(p);
    }

    @Override
    public void set(String name, Object value) {
      map.putProperty(name, value);
    }

    @Override
    public boolean has(String name) {
      return map.getTypeOf(name) != null;
    }
  }

  /**
   * Creates a manager instance.
   * @param store the store instance which must use an appropriate property map factory (probably use createMap).
   */
  protected PropertyManager(String ns, PropertyStore store) {
    this.namespace = ns;
    this.store = store;
  }

  /**
   * Saves all pending updates to store.
   */
  public void commit() {
    final Map<String, PropertyMap> dirtyMaps = this.dirtyMaps;
    synchronized(dirtyMaps) {
      if (!dirtyMaps.isEmpty()) {
        store.saveProperties(dirtyMaps.entrySet().iterator());
        dirtyMaps.clear();
      }
    }
  }

  /**
   * Forget all pending updates.
   */
  public void rollback() {
    final Map<String, PropertyMap> dirtyMaps = this.dirtyMaps;
    synchronized(dirtyMaps) {
      dirtyMaps.clear();
    }
  }

  /**
   * Imports a set of default values into this store&quot;s schema.
   * The properties should be of the form schema_name.property_name=value.
   * Note that this implies the manager has at least one known property map schema.
   * @param importsp the properties
   */
  public void importDefaultValues(Properties importsp) {
    importsp.forEach((k, v)->{
      String importName = k.toString();
      final int dotPosition = importName.indexOf(".");
      if (dotPosition > 0) {
        String schemaName = importName.substring(0, dotPosition);
        PropertySchema schema = getSchema(schemaName);
        if (schema != null) {
          String propertyName = importName.substring(dotPosition + 1);
          schema.setDefaultValue(propertyName, v);
        }
      }
    });
  }

  /**
   * Imports a set of property values.
   * <p>Transactional call that requires calling {@link #commit()} or {@link #rollback()}.</p>
   * @param map the properties key=value
   */
  public void setProperties(Properties map) {
    map.forEach((k, v)-> setProperty(k.toString(), v));
  }

  /**
   * Injects a set of properties.
   * If the value is null, the property is removed.
   * <p>Transactional call that requires calling {@link #commit()} or {@link #rollback()}.</p>
   * @param map the map of properties to inject.
   */
  public void setProperties(Map<String, ?> map) {
    map.forEach(this::setProperty);
  }

  /**
   * Sets a property value.
   * <p>Transactional call that requires calling {@link #commit()} or {@link #rollback()}.</p>
   * @param key the property key
   * @param value the property value or null to unset
   */
  public void setProperty(String key, Object value) {
    setProperty(splitKey(key), value);
  }

  /**
   * Runs a JEXL script using this manager as context.
   * @param src the script source
   * @return the script result
   * @throws PropertyException if any error occurs in JEXL
   */
  public Object runScript(String src) throws PropertyException {
    try {
      JexlScript script = JEXL.createScript(src);
      ObjectContext<PropertyManager> context = new ObjectContext<>(JEXL, this);
      return script.execute(context);
    } catch(JexlException je) {
      throw new PropertyException("script failed", je);
    }
  }

  /**
   * Gets a property value.
   * @param key the property key
   * @return property value or null if not assigned
   */
  public Object getProperty(String key) {
    return getProperty(splitKey(key));
  }

  /**
   * Fetches and formats a property value.
   * @param key the property key
   * @return the formatted property value (or the default value) or null if not assigned
   */
  public String exportPropertyValue(String key) {
    return Objects.toString(fetchPropertyValue(splitKey(key), true));
  }

  /**
   * Gets a property value.
   * @param key the property key
   * @return property value or the schema default value if not assigned
   */
  public Object getPropertyValue(String key) {
    return getPropertyValue(splitKey(key));
  }

  /**
   * Splits a property key into its fragments.
   * @param key the property key
   * @return the key fragments
   */
  protected String[] splitKey(String key) {
    String[] splits = key.split("(?<!\\\\)\\.");
    if (splits.length < 1) {
      splits = new String[]{key};
    }
    return splits;
  }

  /**
   * Gets a schema by name.
   * <p>Only used by {@link #importDefaultValues(Properties)}</p>
   * @param name schema name
   * @return the schema instance, null if no such schema is known
   */
  public PropertySchema getSchema(String name) {
    return null;
  }

  /**
   * Determines the schema from the property key fragments.
   * @param keys the key fragments
   * @return the schema, {@link PropertySchema#NONE} if no such schema is known
   */
  protected PropertySchema schemaOf(String[] keys) {
    return PropertySchema.NONE;
  }

  /**
   * @param keys property key fragments
   * @return number of fragments composing the map name in the fragments array
   */
  protected int getMapNameLength(String[] keys) {
    return keys.length - 1;
  }

  /**
   * Compose a property map key from a property map name.
   * @param name the property map name, may be null or empty
   * @return the property map key used by the store
   */
  protected String mapKey(String name) {
    StringBuilder strb = new StringBuilder(namespace);
    if (name != null && !name.isEmpty()){
      strb.append('.');
      strb.append(name);
    }
    return strb.toString();
  }

  /**
   * Extract a property map name from a property map key.
   * @param key property map key
   * @return the property map name
   */
  protected String mapName(String key) {
    int dot = key.indexOf('.');
    return dot > 0? key.substring(dot + 1) : key;
  }

  /**
   * Compose a property map key from property key fragments.
   * @param keys the key fragments
   * @return the property map key used by the store
   */
  protected String mapKey(String[] keys) {
    return mapKey(keys, getMapNameLength(keys));
  }

  /**
   * Compose a property map key from property key fragments.
   * @param keys the property key fragments
   * @param maxkl the maximum number of fragments in the map key
   * @return the property key used by the store
   */
  protected String mapKey(String[] keys, int maxkl) {
    if (keys.length < 1) {
      throw new IllegalArgumentException("at least 1 key fragments expected");
    }
    // shortest map key is namespace
    StringBuilder strb = new StringBuilder(namespace);
    for(int k = 0; k < Math.min(maxkl, keys.length - 1); ++k) {
      strb.append('.');
      strb.append(keys[k]);
    }
    return strb.toString();
  }

  /**
   * Compose a property name from property key fragments.
   * @param keys the key fragments
   * @return the property name
   */
  protected String propertyName(String[] keys) {
    return propertyName(keys, getMapNameLength(keys));
  }

  /**
   * Compose a property name from property key fragments.
   * @param keys the key fragments
   * @param maxkl the maximum number of fragments in the map name
   * @return the property name
   */
  protected String propertyName(String[] keys, int maxkl) {
    if (keys.length < 1) {
      throw new IllegalArgumentException("at least 1 key fragments expected");
    }
    if (keys.length <= maxkl) {
      return keys[keys.length - 1];
    }
    StringBuilder strb = new StringBuilder(keys[maxkl]);
    for(int k = maxkl + 1; k < keys.length; ++k) {
      strb.append('.');
      strb.append(keys[k]);
    }
    return strb.toString();
  }

  /**
   * Gets a property value.
   * @param keys the key fragments
   * @return the value or null if none was assigned
   */
  public Object getProperty(String[] keys) {
    final String mapKey = mapKey(keys);
    PropertyMap map;
    final Map<String, PropertyMap> dirtyMaps = this.dirtyMaps;
    synchronized(dirtyMaps) {
      map = dirtyMaps.get(mapKey);
    }
    if (map == null) {
      map = store.fetchProperties(mapKey, null);
    }
    if (map != null) {
      return map.getProperty(propertyName(keys));
    }
    return null;
  }

  /**
   * Gets a property value.
   * @param keys the key fragments
   * @return the value or the default schema value if not assigned
   */
  public Object getPropertyValue(String[] keys) {
    return fetchPropertyValue(keys, false);
  }
  private Object fetchPropertyValue(String[] keys, boolean format) {
    final String mapKey = mapKey(keys);
    PropertyMap map;
    final Map<String, PropertyMap> dirtyMaps = this.dirtyMaps;
    synchronized(dirtyMaps) {
      map = dirtyMaps.get(mapKey);
    }
    PropertySchema schema = schemaOf(keys);
    if (map == null) {
      map = store.fetchProperties(mapKey, s->schema);
    }
    String propertyName = propertyName(keys);
    Object value = null;
    if (map != null) {
      value = map.getPropertyValue(propertyName);
    } else if (schema != null) {
      value = schema.getDefaultValue(propertyName);
    }
    if (format && value != null && schema != null) {
      PropertyType<?> type = schema.getPropertyType(propertyName);
      if (type != null) {
        return type.format(value);
      }
    }
    return value;
  }

  /**
   * Drops a property map.
   * <p>Transactional call that requires calling {@link #commit()} or {@link #rollback()}.</p>
   * @param mapName the map name
   * @return true if the properties may exist, false if they did nots
   */
  public boolean dropProperties(String mapName) {
    final String mapKey = mapKey(mapName);
    PropertyMap dirtyMap;
    final Map<String, PropertyMap> dirtyMaps = this.dirtyMaps;
    synchronized (dirtyMaps) {
      dirtyMap = dirtyMaps.get(mapKey);
    }
    PropertyMap map;
    if (dirtyMap != null && Objects.equals(PropertyMap.DROPPED, dirtyMap.getDigest())) {
      map = dirtyMap;
    } else {
      // is it stored ?
      UUID digest = store.fetchDigest(mapKey);
      // not stored nor cached, nothing to do
      if (digest == null) {
        return false;
      }
      map = new PropertyMap(schemaOf(splitKey(mapName + ".*")), PropertyMap.DROPPED);
      synchronized (dirtyMaps) {
        dirtyMaps.put(mapName, map);
      }
    }
    // if this is the first update to the map
    if (map != dirtyMap) {
      dirtyMaps.put(mapName, map);
    }
    return false;
  }

  /**
   * Sets a property value.
   * @param keys the key fragments
   * @param value the new value or null if mapping should be removed
   */
  protected void setProperty(String[] keys, Object value) {
    // find schema from key (length)
    PropertySchema schema = schemaOf(keys);
    String mapKey = mapKey(keys);
    PropertyMap dirtyMap;
    final Map<String, PropertyMap> dirtyMaps = this.dirtyMaps;
    synchronized (dirtyMaps) {
      dirtyMap = dirtyMaps.get(mapKey);
    }
    PropertyMap map;
    if (dirtyMap != null) {
      map = dirtyMap;
    } else {
      // is is stored ?
      map = store.fetchProperties(mapKey, s->schema);
      if (map == null) {
        // remove a value from a non persisted map, noop
        if (value == null) {
          return;
        }
        map = new PropertyMap(schema);
      }
    }
    // map is not null
    String propertyName = propertyName(keys);
    if (value != null) {
      map.putProperty(propertyName, value);
    } else {
      map.removeProperty(propertyName);
    }
    // if this is the first update to the map
    if (map != dirtyMap) {
      dirtyMaps.put(mapKey, map);
    }
  }

  /**
   * Selects a set of properties.
   * @param namePrefix the map name prefix
   * @param predicateStr the condition selecting maps
   * @param projectStr the projection property names or script
   * @return the map of property maps keyed by their name
   */
  public Map<String, PropertyMap> selectProperties(String namePrefix, String predicateStr, String... projectStr) {
    return selectProperties(namePrefix, predicateStr,
        projectStr == null
            ? Collections.emptyList()
            : Arrays.asList(projectStr));
  }

  /**
   * Selects a set of properties.
   * @param namePrefix the map name prefix
   * @param selector the selector/transformer function
   * @return the map of property maps keyed by their name
   */
  public Map<String, PropertyMap> selectProperties(String namePrefix, Function<PropertyMap, PropertyMap> selector) {
    final String mapKey = mapKey(namePrefix);
    final Map<String, PropertyMap> selected = store.selectProperties(mapKey,null, k->schemaOf(splitKey(k)) );
    final Map<String, PropertyMap> maps = new TreeMap<>();
    final Function<PropertyMap, PropertyMap> transform = selector == null? Function.identity() : selector;
    selected.forEach((k, p) -> {
      final PropertyMap dirtyMap = dirtyMaps.get(k);
      final PropertyMap map = transform.apply(dirtyMap == null ? p.copy() : dirtyMap.copy());
      if (map != null && !map.isEmpty()) {
        maps.put(mapName(k), map);
      }
    });
    // apply to new (dirty) maps
    dirtyMaps.forEach((k, p) -> {
      // exclude selected (updated) maps that were handled above
      if (k.startsWith(mapKey) && !selected.containsKey(k)) {
        final PropertyMap map = transform.apply(p.copy());
        if (map != null && !map.isEmpty()) {
          maps.put(mapName(k), map);
        }
      }
    });
    return maps;
  }

  /**
   * Selects a set of properties.
   * @param namePrefix the map name prefix
   * @param predicateStr the condition selecting maps
   * @param projectStr the projection property names or script
   * @return the map of property maps keyed by their name
   */
  public Map<String, PropertyMap> selectProperties(String namePrefix, String predicateStr, List<String> projectStr) {
    final JexlExpression predicate;
    try {
      predicate = JEXL.createExpression(predicateStr);
    } catch (JexlException.Parsing xparse) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(predicateStr, xparse);
      }
      throw new PropertyException(xparse);
    }
    return selectProperties(namePrefix, predicate, projectStr);
  }


  /**
   * Selects a set of properties.
   * @param namePrefix the map name prefix
   * @param predicate the condition selecting maps
   * @param projectStr the projection property names or script
   * @return the map of property maps keyed by their name
   */
  public Map<String, PropertyMap> selectProperties(String namePrefix, JexlExpression predicate, String... projectStr) {
    return selectProperties(namePrefix, predicate, projectStr == null
          ? Collections.emptyList()
          : Arrays.asList(projectStr));
  }

    /**
     * Selects a set of properties.
     * @param namePrefix the map name prefix
     * @param predicate the condition selecting maps
     * @param projectStr the projection property names or script
     * @return the map of property maps keyed by their name
     */
    public Map<String, PropertyMap> selectProperties(String namePrefix, JexlExpression predicate, List<String> projectStr) {
    final Function<PropertyMap, PropertyMap> transform = (map)->{
      MapWrapper wrapped = new MapWrapper(map);
      Object result;
      try {
        result = predicate.evaluate(wrapped);
      } catch(JexlException xany) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug(predicate.getSourceText(), xany);
        }
        throw new PropertyException(xany);
      }
      if (Boolean.TRUE.equals(result)) {
        if (projectStr == null || projectStr.isEmpty()) {
          return map;
        }
        Map<String, Object> projected = new TreeMap<>();
        for(String projectName : projectStr) {
          // if this looks like the name of a property, use it
          if (map.getTypeOf(projectName)!= null) {
            Object value = map.getPropertyValue(projectName);
            if (value != null) {
              projected.put(projectName, value);
            }
          } else {
            // try to use it as the source of a JEXL expression
            try {
              JexlExpression projector = JEXL.createExpression(projectName);
              Object evaluated = projector.evaluate(wrapped);
              if (evaluated instanceof Map<?,?>) {
                @SuppressWarnings("unchecked") Map<String,?> cast = (Map<String,?>) evaluated;
                projected.putAll(cast);
              }
            } catch(JexlException xany) {
              LOGGER.warn(projectName, xany);
            }
          }
        }
        return new PropertyMap(map, projected);
      }
      return  null;
    };
    return selectProperties(namePrefix, transform);
  }
}
