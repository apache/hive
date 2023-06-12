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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * A property map pertaining to a given object type (cluster, database, table).
 * <p>
 *   Maps follow a copy-on-write scheme gated by a dirty flag (avoid copy of a dirty map). This allows
 *   sharing their content (the inner properties map) with guaranteed isolation and thread safety.
 * </p>
 */
public class PropertyMap implements Serializable {
  // Vital immutable serialization information.
  static {
    SerializationProxy.registerType(0, PropertyMap.class);
  }

  /**
   * The logger.
   */
  public static final Logger LOGGER = LoggerFactory.getLogger(PropertyMap.class);
  /**
   * Serial version.
   */
  private static final long serialVersionUID = 202212291759L;
  /**
   * The schema for this map, describes allowed properties and their types.
   */
  protected final transient PropertySchema schema;
  /**
   * The uuid.
   */
  protected transient volatile UUID digest;
  /**
   * The properties and their values; the map is cow-once.
   */
  protected transient Map<String, Object> properties;
  /**
   * Whether this map is dirty which also reflects its copy-on-write state.
   */
  protected transient boolean dirty;

  /**
   * A digest for dropped maps.
   */
  static final UUID DROPPED = new Digester().digest(PropertyMap.class.getName()).digest("dropped").getUUID();

  /**
   * The main ctor.
   *
   * @param schema the schema this map adheres to
   */
  PropertyMap(PropertySchema schema) {
    this.schema = schema == null ? PropertySchema.NONE : schema;
    this.properties = Collections.emptyMap();
    this.dirty = false;
  }

  /**
   * The copy constructor.
   *
   * @param src the instance to copy
   */
  private PropertyMap(PropertyMap src) {
    this.schema = src.schema;
    this.properties = src.properties;
    this.digest = src.digest;
    this.dirty = false;
  }

  /**
   * The projection constructor.
   * <p>This copies an instance and sets key/value pairs at the same time.</p>
   * <p>Note that the resulting map is <em>not</em> dirty.</p>
   *
   * @param src   the instance to copy
   * @param input the pairs to set
   */
  public PropertyMap(PropertyMap src, Map<String, Object> input) {
    this.schema = src.schema;
    this.properties = Collections.emptyMap();
    input.forEach((k, v) -> {
      try {
        putProperty(k, v);
      } catch (IllegalArgumentException xill) {
        // ignore errors
      }
    });
    this.dirty = false;
  }

  /**
   * An empty alias of a map, used in drop.
   *
   * @param schema the schema
   * @param digest the initial digest
   */
  PropertyMap(PropertySchema schema, UUID digest) {
    this(schema);
    this.digest = digest;
  }

  /**
   * Deserialization ctor.
   * <p>Used through deserializtion through reflection by the serialization proxy.</p>
   * @param input the input stream
   * @throws IOException if IO fail
   */
  public PropertyMap(DataInput input, Function<String, PropertySchema> getSchema) throws IOException {
    // serial
    long serial = input.readLong();
    if (serial != serialVersionUID) {
      throw new InvalidObjectException("serial mismatch");
    }
    // schema as string
    String schemaName = input.readUTF();
    // schema version number
    int schemaVersion = input.readInt();
    this.schema = fetchSchema(schemaName, getSchema);
    // number of properties
    properties = new TreeMap<>();
    dirty = false;
    int size = input.readInt();
    for (int p = 0; p < size; ++p) {
      // key as string
      String name = input.readUTF();
      PropertyType<?> type = schema.getPropertyType(name);
      if (type == null) {
        LOGGER.warn(schema.getName() + ": unsolvable property type for " + name);
        type = PropertyType.STRING;
      }
      // value as string
      Object value = type.read(input);
      if (value != null) {
        properties.put(name, value);
      } else if (schema.getVersionNumber() > schemaVersion) {
        dirty = true;
      }
    }
  }

  /**
   * Fetches the schema.
   *
   * @param schemaName the schema name
   * @param getSchema  the schema function provider
   * @return a non null schema instance
   */
  private static PropertySchema fetchSchema(String schemaName, Function<String, PropertySchema> getSchema) {
    PropertySchema schema = getSchema != null ? getSchema.apply(schemaName) : null;
    if (schema == null) {
      LOGGER.warn("unsolvable schema " + schemaName);
      return PropertySchema.NONE;
    } else if (!schema.getName().equals(schemaName)) {
      LOGGER.warn("potential schema mismatch, expected " + schema.getName() + ", got " + schemaName);
    }
    return schema;
  }

  /**
   * The Serialization method.
   *
   * @param out the output stream
   * @throws IOException if IO fail
   */
  @SuppressWarnings("unchecked")
  public void write(DataOutput out) throws IOException {
    // serial
    out.writeLong(serialVersionUID);
    // schema as string
    out.writeUTF(schema.getName());
    // schema version number
    out.writeInt(schema.getVersionNumber());
    // need a schema serial as well
    int size = properties.size();
    // number of properties
    out.writeInt(size);
    for (Map.Entry<String, Object> entry : properties.entrySet()) {
      String name = entry.getKey();
      // key as string
      out.writeUTF(name);
      PropertyType type = schema.getPropertyType(name);
      // value
      type.write(out, entry.getValue());
    }
  }

  private Object writeReplace() throws ObjectStreamException {
    // writeReplace() should hint spotbugs that we are taking over serialization;
    // having to annotate all fields as transient is just to please it
    return new SerializationProxy<>(this);
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    throw new InvalidObjectException("proxy required");
  }

  public int size() {
    return properties.size();
  }

  /**
   * @return true if any property is valued, false otherwise
   */
  public boolean isEmpty() {
    return properties.isEmpty();
  }

  /**
   * @return true if some property value has changed, false otherwise
   */
  public boolean isDirty() {
    return dirty;
  }

  /**
   * Clears the inner dirty flag.
   * <p>This implies the inner map becomes shareable between instances (see #copy()).</p>
   * <p>This should only be called after a save()/write().</p>
   */
  public void setClean() {
    dirty = false;
  }

  /**
   * @return true if this map has been explicitly dropped, false otherwise
   */
  public boolean isDropped() {
    return Objects.equals(DROPPED, digest);
  }

  /**
   * A copy of this map.
   * <p>Called to avoid sharing instances from cache.</p>
   *
   * @return a shallow copy of this map
   */
  public PropertyMap copy() {
    return new PropertyMap(this);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    PropertyMap that = (PropertyMap) o;

    if (!Objects.equals(getDigest(), that.getDigest())) return false;
    return Objects.equals(schema, that.schema);
  }

  @Override
  public int hashCode() {
    int result = getDigest().hashCode();
    result = 31 * result + (schema != null ? schema.hashCode() : 0);
    return result;
  }

  protected PropertyType<?> getTypeOf(String name) {
    return schema.getPropertyType(name);
  }

  /**
   * Exports this map properties to java properties
   *
   * @param javap the java properties
   */
  public void exportToProperties(Properties javap) {
    this.forEach((k, v) -> javap.setProperty(k, getTypeOf(k).format(v)));
  }

  /**
   * Import key value pairs from java properties.
   *
   * @param javap the java properties
   */
  public void importFromProperties(Properties javap) {
    javap.forEach((k, v) -> this.putProperty(k.toString(), v));
  }

  /**
   * Gets this property set digest.
   *
   * @return the digest uuid
   */
  public UUID getDigest() {
    UUID uuid = digest;
    if (uuid == null) {
      final Map<String, Object> map = this.properties;
      synchronized (map) {
        uuid = digest;
        if (uuid == null) {
          final Digester digester = new Digester();
          map.forEach((k, v) -> {
            digester.digest(k);
            digester.digest(v);
          });
          uuid = digest = digester.getUUID();
        }
      }
    }
    return uuid;
  }

  /**
   * Sets the value of a property.
   *
   * @param name  the property name
   * @param value the value
   * @return the previous value or null
   * @throws IllegalArgumentException if the property is not declared or the value is of an incorrect type
   */
  public Object putProperty(String name, Object value) {
    PropertyType<?> type = getTypeOf(name);
    if (type == null) {
      throw new IllegalArgumentException("property " + name + " is not declared");
    }
    Object validated = type.cast(value);
    if (validated == null) {
      throw new IllegalArgumentException("property " + name
          + ", type " + type.getName() + ", " + value);
    }
    if (!dirty) {
      final Map<String, Object> map = this.properties;
      synchronized (map) {
        // copy on write
        this.properties = new TreeMap<>(map);
        dirty = true;
      }
    }
    final Map<String, Object> map = this.properties;
    synchronized (map) {
      digest = null;
      return map.put(name, validated);
    }
  }

  /**
   * Removes a property from this map.
   *
   * @param name the property name
   * @return the value that was set or null
   */
  public Object removeProperty(String name) {
    if (!dirty) {
      final Map<String, Object> map = this.properties;
      synchronized (map) {
        if (!map.containsKey(name)) {
          return null;
        }
        // copy on write
        this.properties = new TreeMap<>(map);
        dirty = true;
      }
    }
    final Map<String, Object> map = this.properties;
    synchronized (map) {
      Object value;
      if ((value = map.remove(name)) != null) {
        digest = null;
      }
      return value;
    }
  }

  /**
   * Gets the value of a property.
   *
   * @param name the property name
   * @return the property value or null if no property with that name is set
   */
  public Object getProperty(String name) {
    final Map<String, Object> map = this.properties;
    synchronized (map) {
      return map.get(name);
    }
  }

  /**
   * Gets the value of a property.
   *
   * @param name the property name
   * @return the property value or the schema default value if not set
   */
  public Object getPropertyValue(String name) {
    final Map<String, Object> map = this.properties;
    synchronized (map) {
      return map.getOrDefault(name, schema.getDefaultValue(name));
    }
  }

  /**
   * Call action on each property name/value.
   *
   * @param action the action to call
   */
  public void forEach(BiConsumer<? super String, Object> action) {
    final Map<String, Object> map = this.properties;
    synchronized (map) {
      map.forEach(action);
    }
  }

  /**
   * Exports this property map as a key/value as string map.
   * @return a string.string map
   */
  public Map<String,String> export() {
    return export(true);
  }

  /**
   * Exports this property map as a key/value as string map.
   * @param withDefaults whether default values should be exported as well
   * @return a string.string map
   */
  public Map<String,String> export(boolean withDefaults) {
    Map<String, String> map = new TreeMap<>();
    final Map<String, PropertyType<?>> schemaMap = schema.properties;
    final Map<String, Object> valueMap = this.properties;
    synchronized (valueMap) {
      for (Map.Entry<String, PropertyType<?>> entry : schemaMap.entrySet()) {
        String pname = entry.getKey();
        Object value = withDefaults
            ? valueMap.getOrDefault(pname, schema.getDefaultValue(pname))
            : valueMap.get(pname);
        if (value != null) {
          map.put(pname, entry.getValue().format(value));
        }
      }
    }
    return map.isEmpty()? Collections.emptyMap() : map;
  }
}
