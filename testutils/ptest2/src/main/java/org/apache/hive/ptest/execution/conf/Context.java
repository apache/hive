/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hive.ptest.execution.conf;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

/**
 * The context is a key-value store used to pass configuration information
 * throughout the system.
 */
public class Context {

  private Map<String, String> parameters;

  public Context() {
    parameters = Collections.synchronizedMap(new HashMap<String, String>());
  }
  public Context(Map<String, String> paramters) {
    this();
    this.putAll(paramters);
  }

  /**
   * Gets a copy of the backing map structure.
   * @return immutable copy of backing map structure
   */
  public ImmutableMap<String, String> getParameters() {
    synchronized (parameters) {
      return ImmutableMap.copyOf(parameters);
    }
  }
  /**
   * Removes all of the mappings from this map.
   */
  public void clear() {
    parameters.clear();
  }

  /**
   * Get properties which start with a prefix. When a property is returned,
   * the prefix is removed the from name. For example, if this method is
   * called with a parameter &quot;hdfs.&quot; and the context contains:
   * <code>
   * { hdfs.key = value, otherKey = otherValue }
   * </code>
   * this method will return a map containing:
   * <code>
   * { key = value}
   * </code>
   *
   * <b>Note:</b> The <tt>prefix</tt> must end with a period character. If not
   * this method will raise an IllegalArgumentException.
   *
   * @param prefix key prefix to find and remove from keys in resulting map
   * @return map with keys which matched prefix with prefix removed from
   *   keys in resulting map. If no keys are matched, the returned map is
   *   empty
   * @throws IllegalArguemntException if the given prefix does not end with
   *   a period character.
   */
  public ImmutableMap<String, String> getSubProperties(String prefix) {
    Preconditions.checkArgument(prefix.endsWith("."),
        "The given prefix does not end with a period (" + prefix + ")");
    Map<String, String> result = Maps.newHashMap();
    synchronized(parameters) {
      for (String key : parameters.keySet()) {
        if (key.startsWith(prefix)) {
          String name = key.substring(prefix.length());
          result.put(name, parameters.get(key));
        }
      }
    }
    return ImmutableMap.copyOf(result);
  }
  /**
   * Associates all of the given map's keys and values in the Context.
   */
  public void putAll(Map<String, String> map) {
    parameters.putAll(map);
  }
  /**
   * Associates the specified value with the specified key in this context.
   * If the context previously contained a mapping for the key, the old value
   * is replaced by the specified value.
   * @param key key with which the specified value is to be associated
   * @param value to be associated with the specified key
   */
  public void put(String key, String value) {
    parameters.put(key, value);
  }
  /**
   * Gets value mapped to key, returning defaultValue if unmapped.
   * @param key to be found
   * @param defaultValue returned if key is unmapped
   * @return value associated with key
   */
  public Boolean getBoolean(String key, Boolean defaultValue) {
    String value = get(key);
    if(value != null) {
      return Boolean.parseBoolean(value.trim());
    }
    return defaultValue;
  }
  /**
   * Gets value mapped to key, returning null if unmapped.
   * <p>
   * Note that this method returns an object as opposed to a
   * primitive. The configuration key requested may not be mapped
   * to a value and by returning the primitive object wrapper we can
   * return null. If the key does not exist the return value of
   * this method is assigned directly to a primitive, a
   * {@link NullPointerException} will be thrown.
   * </p>
   * @param key to be found
   * @return value associated with key or null if unmapped
   */
  public Boolean getBoolean(String key) {
    return getBoolean(key, null);
  }
  /**
   * Gets value mapped to key, returning defaultValue if unmapped.
   * @param key to be found
   * @param defaultValue returned if key is unmapped
   * @return value associated with key
   */
  public Integer getInteger(String key, Integer defaultValue) {
    String value = get(key);
    if(value != null) {
      return Integer.parseInt(value.trim());
    }
    return defaultValue;
  }
  /**
   * Gets value mapped to key, returning null if unmapped.
   * <p>
   * Note that this method returns an object as opposed to a
   * primitive. The configuration key requested may not be mapped
   * to a value and by returning the primitive object wrapper we can
   * return null. If the key does not exist the return value of
   * this method is assigned directly to a primitive, a
   * {@link NullPointerException} will be thrown.
   * </p>
   * @param key to be found
   * @return value associated with key or null if unmapped
   */
  public Integer getInteger(String key) {
    return getInteger(key, null);
  }
  /**
   * Gets value mapped to key, returning defaultValue if unmapped.
   * @param key to be found
   * @param defaultValue returned if key is unmapped
   * @return value associated with key
   */
  public Long getLong(String key, Long defaultValue) {
    String value = get(key);
    if(value != null) {
      return Long.parseLong(value.trim());
    }
    return defaultValue;
  }
  /**
   * Gets value mapped to key, returning null if unmapped.
   * <p>
   * Note that this method returns an object as opposed to a
   * primitive. The configuration key requested may not be mapped
   * to a value and by returning the primitive object wrapper we can
   * return null. If the key does not exist the return value of
   * this method is assigned directly to a primitive, a
   * {@link NullPointerException} will be thrown.
   * </p>
   * @param key to be found
   * @return value associated with key or null if unmapped
   */
  public Long getLong(String key) {
    return getLong(key, null);
  }
  /**
   * Gets value mapped to key, returning defaultValue if unmapped.
   * @param key to be found
   * @param defaultValue returned if key is unmapped
   * @return value associated with key
   */
  public Float getFloat(String key, Float defaultValue) {
    String value = get(key);
    if(value != null) {
      return Float.parseFloat(value.trim());
    }
    return defaultValue;
  }
  /**
   * Gets value mapped to key, returning null if unmapped.
   * <p>
   * Note that this method returns an object as opposed to a
   * primitive. The configuration key requested may not be mapped
   * to a value and by returning the primitive object wrapper we can
   * return null. If the key does not exist the return value of
   * this method is assigned directly to a primitive, a
   * {@link NullPointerException} will be thrown.
   * </p>
   * @param key to be found
   * @return value associated with key or null if unmapped
   */
  public Float getFloat(String key) {
    return getFloat(key, null);
  }
  /**
   * Gets value mapped to key, returning defaultValue if unmapped.
   * @param key to be found
   * @param defaultValue returned if key is unmapped
   * @return value associated with key
   */
  public String getString(String key, String defaultValue) {
    return get(key, defaultValue);
  }
  /**
   * Gets value mapped to key, returning null if unmapped.
   * @param key to be found
   * @return value associated with key or null if unmapped
   */
  public String getString(String key) {
    return get(key);
  }
  private String get(String key, String defaultValue) {
    String result = parameters.get(key);
    if(result != null) {
      return result;
    }
    return defaultValue;
  }
  private String get(String key) {
    return get(key, null);
  }
  @Override
  public String toString() {
    return "{ parameters:" + parameters + " }";
  }

  /**
   * Build a context with the properties read from an input stream.
   * @param inputStream
   * @return context
   * @throws IOException
   */
  public static Context fromInputStream(InputStream inputStream)
      throws IOException {
    Properties properties = new Properties();
    properties.load(inputStream);
    return new Context(Maps.fromProperties(properties));
  }

  /**
   * Build a context with the properties read from a file
   * @param file
   * @return
   * @throws IOException
   */
  public static Context fromFile(String file) throws IOException {
    return fromFile(new File(file));
  }

  /**
   * Build a context with the properties read from a file
   * @param file
   * @return
   * @throws IOException
   */
  public static Context fromFile(File file) throws IOException {
    try (InputStream in = new FileInputStream(file)){
      return fromInputStream(in);
    }
  }

  /**
   * Builder that can aggregate properties from several files
   * when building a context. If the same key is present in more
   * than one file, the last one will be used.
   */
  public static class ContextBuilder {

    private Context context = new Context();

    /**
     * Add properties from a file to the context
     * @param file
     * @return
     * @throws IOException
     */
    public ContextBuilder addPropertiesFile(File file) throws IOException {
      try(InputStream is = new FileInputStream(file)) {
        Properties properties = new Properties();
        properties.load(is);
        context.putAll(Maps.fromProperties(properties));
      }
      return this;
    }

    /**
     * Add properties from a file to the context
     * @param file
     * @return
     * @throws IOException
     */
    public ContextBuilder addPropertiesFile(String file) throws IOException {
      return  addPropertiesFile(new File(file));
    }

    /**
     * Build the context using the aggregated properties
     * @return
     * @throws IOException
     */
    public Context build() throws IOException {
      return context;
    }
  }
}