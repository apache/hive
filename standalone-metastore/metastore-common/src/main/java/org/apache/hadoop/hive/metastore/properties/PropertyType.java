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

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;

import javax.annotation.Nullable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

/**
 * A simple type system for properties.
 */
public abstract class PropertyType<T> {
  /** The set of known types; can be extended by calling register. */
  private static final Map<String, PropertyType<?>> TYPES = new HashMap<>();
  /** The UTC time zone. */
  public static final TimeZone UTC = TimeZone.getTimeZone("UTC");

  /**
   * @return an ISO8601 format, TZ=UTC
   */
  private static SimpleDateFormat getDateFormat() {
    SimpleDateFormat sdtf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SS'Z'");
    sdtf.setTimeZone(UTC);
    return sdtf;
  }

  /** The type name. */
  private final String name;

  /**
   * @param name the type name
   * @return a type instance if it has been registered
   */
  public static PropertyType<?> get(String name) {
    synchronized(TYPES) {
      return TYPES.get(name);
    }
  }

  /**
   * Registers a property type.
   * @param type the type
   * @return true if type is registered, false if it was already registered
   * @throws IllegalArgumentException if a type with the same name has already been registered
   */
  public static boolean register(PropertyType<?> type) {
    String name = type.getName();
    synchronized(TYPES) {
      PropertyType<?> ptype = TYPES.putIfAbsent(name, type);
      if (ptype != null) {
        if (ptype == type) {
          return false;
        }
        throw new IllegalArgumentException("type " + name + " is already registered");
      }
    }
    return true;
  }

  /**
   * Basic constructor to derive from.
   * @param name the type name
   */
  protected PropertyType(String name) {
    this.name = name;
  }

  /**
   * @return this type name
   */
  public String getName() {
    return name;
  }

  @Override public String toString() {
    return getClass().getSimpleName() + ":" + getName();
  }

  /**
   * Casts a value according to this type.
   * @param value a value
   * @return an instance for the type or null
   */
  public abstract T cast(Object value);

  /**
   * Parses a string according to this type.
   * @param str a value
   * @return an instance for the type or null
   */
  public abstract T parse(String str);

  /**
   * Formats a value according to this type.
   * @param value a value
   * @return a string representing a value for the type or null
   */
  public String format(Object value) {
    return value == null? null : value.toString();
  }

  /**
   * Writes a value according to this type.
   * @param out a data output
   * @param value a value
   * @throws IOException if write fails
   */
  public void write(DataOutput out, T value) throws IOException {
    out.writeUTF(format(value));
  }
  /**
   * Reads a value according to this type.
   * @param in a data input
   * @return an instance of a value for the type or null
   */
  public T read(DataInput in) throws IOException {
    return parse(in.readUTF());
  }

  public static final PropertyType<String> STRING = new PropertyType<String>("string") {

    @Override
    public String cast(Object value) {
      return value == null? null : value.toString();
    }

    @Override
    public String parse(String str) {
      return str;
    }
  };

  public static final PropertyType<Boolean> BOOLEAN = new PropertyType<Boolean>("boolean") {
    @Nullable
    @Override public Boolean cast(Object value) {
      if (value instanceof Boolean) {
        return (Boolean) value;
      }
      if (value == null) {
        return null;
      }
      return parse(value.toString());
    }
    @Nullable
    @Override public Boolean parse(String str) {
      if ("true".equalsIgnoreCase(str)) {
        return true;
      }
      if ("false".equalsIgnoreCase(str)) {
        return false;
      }
      return null;
    }

    @Nullable
    @Override public String format(Object value) {
      if (value instanceof Boolean) {
        return ((Boolean) value) ? "true" : "false";
      }
      return null;
    }

    @Override
    public void write(DataOutput out, Boolean value) throws IOException {
      out.writeBoolean(value);
    }

    @Override
    public Boolean read(DataInput in) throws IOException {
      return in.readBoolean();
    }
  };

  public static final PropertyType<Integer> INTEGER = new PropertyType<Integer>("integer") {
    @Override public Integer cast(Object value) {
      if (value instanceof Number) {
        return ((Number) value).intValue();
      }
      if (value == null) {
        return null;
      }
      return parse(value.toString());
    }

    @Override public Integer parse(String str) {
      if (str == null) {
        return null;
      }
      try {
        return Integer.parseInt(str);
      } catch (NumberFormatException xformat) {
        return null;
      }
    }

    @Override public String format(Object value) {
      if (value instanceof Integer) {
        return String.valueOf(value);
      }
      return null;
    }

    @Override
    public void write(DataOutput out, Integer value) throws IOException {
      out.writeInt(value);
    }

    @Override
    public Integer read(DataInput in) throws IOException {
      return in.readInt();
    }
  };

  public static final PropertyType<Long> LONG = new PropertyType<Long>("long"){
    @Override public Long cast(Object value) {
      if (value instanceof Number) {
        return ((Number) value).longValue();
      }
      if (value == null) {
        return null;
      }
      return parse(value.toString());
    }

    @Override public Long parse(String str) {
      if (str == null) {
        return null;
      }
      try {
        return Long.parseLong(str);
      } catch (NumberFormatException xformat) {
        return null;
      }
    }

    @Override public String format(Object value) {
      if (value instanceof Long) {
        return String.valueOf(value);
      }
      return null;
    }

    @Override
    public void write(DataOutput out, Long value) throws IOException {
      out.writeLong(value);
    }

    @Override
    public Long read(DataInput in) throws IOException {
      return in.readLong();
    }
  };

  public static final PropertyType<Date> DATETIME = new PropertyType<Date>("date"){
    @Override public Date cast(Object value) {
      if (value instanceof Number) {
        return new Date(((Number) value).longValue());
      }
      if (value instanceof Date) {
        return (Date) value;
      }
      if (value == null) {
        return null;
      }
      return parse(value.toString());
    }

    @Override public Date parse(String str) {
      if (str == null) {
        return null;
      }
      try {
        return getDateFormat().parse(str);
      } catch (java.text.ParseException xparse) {
        return null;
      }
    }

    @Override public String format(Object value) {
      if (value instanceof Date) {
        return getDateFormat().format((Date) value);
      }
      return null;
    }

    @Override public void write(DataOutput out, Date value) throws IOException {
      out.writeLong(value.getTime());
    }

    @Override public Date read(DataInput in) throws IOException {
      return new Date(in.readLong());
    }
  };

  public static final PropertyType<Double> DOUBLE = new PropertyType<Double>("double"){
    @Override public Double cast(Object value) {
      if (value instanceof Number) {
        return ((Number) value).doubleValue();
      }
      if (value == null) {
        return null;
      }
      return parse(value.toString());
    }

    @Override public Double parse(String str) {
      if (str == null) {
        return null;
      }
      try {
        return Double.parseDouble(str);
      } catch(NumberFormatException xformat) {
        return null;
      }
    }

    @Override public String format(Object value) {
      if (value instanceof Double) {
        return String.valueOf(value);
      }
      return null;
    }

    @Override
    public void write(DataOutput out, Double value) throws IOException {
      out.writeDouble(value);
    }

    @Override
    public Double read(DataInput in) throws IOException {
      return in.readDouble();
    }
  };

  public static final PropertyType<Object> JSON = new PropertyType<Object>("json"){
    @Override public Object cast(Object value) {
      if (value instanceof JsonElement) {
        return value;
      }
      if (value == null) {
        return null;
      }
      return parse(value.toString());
    }

    @Override public Object parse(String str) {
      if (str == null) {
        return null;
      }
      try (Reader reader = new StringReader(str)) {
        return new Gson().fromJson(reader, Object.class);
      } catch (JsonIOException | JsonSyntaxException | IOException e) {
        return null;
      }
    }

    @Override public String format(Object value) {
      if (value == null) {
        return null;
      }
      return new Gson().toJson(value);
    }
  };

  /*
   * Register the basics.
   */
  static {
    register(BOOLEAN);
    register(INTEGER);
    register(LONG);
    register(DOUBLE);
    register(STRING);
    register(DATETIME);
    register(JSON);
  }
}
