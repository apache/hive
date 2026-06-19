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
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

/**
 * A property map schema pertaining to a given object type (cluster, database, table).
 * <p>A schema declares the properties that can be set, their type and their eventual default value.</p>
 */
public class PropertySchema implements Serializable {
  /**
   * Serial version.
   */
  private static final long serialVersionUID = 202212301459L;
  /**
   * The schema name.
   */
  private transient final String name;
  /**
   * The schema version number.
   */
  private transient final AtomicInteger versionNumber;
  /**
   * The uuid.
   */
  protected transient volatile UUID digest;
  /**
   * The properties and their types, may be empty, never null.
   */
  protected transient final Map<String, PropertyType<?>> properties;
  /**
   * The properties default value.
   */
  protected transient Map<String, Object> values;

  /**
   * A default schema that treats all properties as string.
   */
  public static final PropertySchema NONE = new PropertySchema("", 1, Collections.emptyMap()) {

    @Override
    public PropertyType<?> getPropertyType(String name) {
      return PropertyType.STRING;
    }

    @Override
    public boolean declareProperty(String name, PropertyType<?> type) {
      throw new UnsupportedOperationException("schema is readonly");
    }

    @Override
    public boolean removeProperty(String name) {
      throw new UnsupportedOperationException("schema is readonly");
    }
  };

  /**
   * Default ctor.
   *
   * @param schemaName the schema name
   */
  PropertySchema(String schemaName) {
    this(schemaName, 1, null);
  }

  /**
   * The deserializing constructor.
   * <p>Called by reflection through SerializationProxy</p>
   * @param input the input stream
   * @throws IOException if IO fail
   */
  public PropertySchema(DataInput input) throws IOException {
    // serial
    long serial = input.readLong();
    if (serial != serialVersionUID) {
      throw new InvalidObjectException("serial mismatch");
    }
    // name
    name = input.readUTF();
    // version number
    versionNumber = new AtomicInteger(input.readInt());
    // number of properties
    int size = input.readInt();
    properties = new TreeMap<>();
    // the properties
    for (int p = 0; p < size; ++p) {
      String name = input.readUTF();
      String typeName = input.readUTF();
      PropertyType<?> type = PropertyType.get(typeName);
      properties.put(name, Objects.requireNonNull(type));
    }
    // number of default values
    size = input.readInt();
    values = size == 0 ? null : new TreeMap<>();
    // the values
    for (int v = 0; v < size; ++v) {
      String name = input.readUTF();
      PropertyType<?> type = properties.get(name);
      String strValue = input.readUTF();
      Object value = type.parse(strValue);
      values.put(name, value);
    }
  }

  /**
   * The serializing write method.
   *
   * @param output the output stream
   * @throws IOException if IO fail
   */
  public void write(DataOutput output) throws IOException {
    // serial
    output.writeLong(serialVersionUID);
    // name
    output.writeUTF(name);
    // version number
    output.writeInt(versionNumber.get());
    if (properties != null) {
      // number of properties
      output.writeInt(properties.size());
      // the properties
      for (Map.Entry<String, PropertyType<?>> entry : properties.entrySet()) {
        output.writeUTF(entry.getKey());
        output.writeUTF(entry.getValue().getName());
      }
    } else {
      output.writeInt(0);
    }
    // the values
    if (values != null && properties != null) {
      output.writeInt(values.size());    // the properties
      for (Map.Entry<String, Object> entry : values.entrySet()) {
        String name = entry.getKey();
        PropertyType<?> type = properties.get(name);
        output.writeUTF(name);
        output.writeUTF(type.format(entry.getValue()));
      }
    } else {
      output.writeInt(0);
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

  /**
   * Default constructor.
   * <p>The version must only and always change when the schema adds or removes properties.</p>
   *
   * @param schemaName the schema name
   * @param version the schema version number
   * @param map        the map of properties and their types
   */
  PropertySchema(String schemaName, int version, Map<String, PropertyType<?>> map) {
    this.name = schemaName;
    this.versionNumber = new AtomicInteger(version);
    this.properties = map == null ? new TreeMap<>() : map;
    this.values = null;
  }

  /**
   * @return this schema name
   */
  public String getName() {
    return name;
  }

  /**
   * @return this schema version number
   */
  public int getVersionNumber() {
    return versionNumber.get();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    PropertySchema that = (PropertySchema) o;

    if (!Objects.equals(getDigest(), that.getDigest())) return false;
    return Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    int result = getDigest().hashCode();
    result = 31 * result + Objects.hashCode(name);
    return result;
  }

  @Override
  public String toString() {
    return "schema{" + name + "}";
  }

  /**
   * Gets this property schema digest.
   *
   * @return the digest uuid
   */
  public UUID getDigest() {
    UUID uuid = digest;
    if (uuid == null) {
      synchronized (this) {
        uuid = digest;
        if (uuid == null) {
          final Digester digester = new Digester();
          properties.forEach((k, v) -> {
            digester.digest(k);
            digester.digest(v.getName());
          });
          if (values != null) {
            values.forEach((k, v) -> {
              digester.digest(k);
              digester.digest(v);
            });
          }
          uuid = digest = digester.getUUID();
        }
      }
    }
    return uuid;
  }

  /**
   * Perform an action on all properties.
   *
   * @param action the action
   */
  public void forEach(BiConsumer<String, PropertyType<?>> action) {
    properties.forEach(action);
  }

  /**
   * Gets the type for a given property.
   *
   * @param name the property name
   * @return the property type or null of not set
   */
  public PropertyType<?> getPropertyType(String name) {
    return properties.get(name);
  }

  /**
   * Gets the default value for a property.
   *
   * @param name the property name
   * @return the default value or null if not set
   */
  public Object getDefaultValue(String name) {
    return values != null ? values.get(name) : null;
  }

  /**
   * Sets the default value for a property.
   *
   * @param name the property name
   * @param value the default value to set
   */
  public void setDefaultValue(String name, Object value) {
    PropertyType<?> type = getPropertyType(name);
    if (type != null) {
      Object tvalue = type.cast(value);
      if (tvalue != null) {
        values.put(name, tvalue);
      }
    }
  }

  /**
   * Declares a new property.
   *
   * @param name the property name
   * @param type the property type
   * @return true if property is successfully declared, false if it was already declared
   * @throws IllegalArgumentException if a property with the same name but a different type has already been declared
   */
  public boolean declareProperty(String name, PropertyType<?> type) {
    return declareProperty(name, type, null);
  }

  /**
   * Declares a new property.
   *
   * @param name         the property name
   * @param type         the property type
   * @param defaultValue the default property value
   * @return true if property is successfully declared, false if it was already declared
   * @throws IllegalArgumentException if a property with the same name but a different type has already been declared
   */
  public boolean declareProperty(String name, PropertyType<?> type, Object defaultValue) {
    if (name == null) {
      throw new IllegalArgumentException("null name");
    }
    if (type == null) {
      throw new IllegalArgumentException("null type");
    }
    PropertyType<?> ptype = properties.putIfAbsent(name, type);
    if (ptype != null) {
      if (ptype.equals(type)) {
        return false;
      }
      throw new IllegalArgumentException("property " + name + " is already declared");
    }
    Object value = type.cast(defaultValue);
    if (value != null) {
      if (values == null) {
        values = new TreeMap<>();
      }
      values.put(name, value);
    }
    versionNumber.incrementAndGet();
    digest = null;
    return true;
  }

  /**
   * For testing purpose, removes a property from this schema.
   *
   * @param name the property name
   * @return true if the property was successfully removed, false otherwise
   */
  public boolean removeProperty(String name) {
    if (properties.remove(name) != null) {
      if (values != null) {
        values.remove(name);
      }
      versionNumber.incrementAndGet();
      digest = null;
      return true;
    }
    return false;
  }

}
