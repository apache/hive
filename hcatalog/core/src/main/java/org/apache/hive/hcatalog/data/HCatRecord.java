/**
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hive.hcatalog.data;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.classification.InterfaceStability;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.schema.HCatSchema;

/**
 * Abstract class exposing get and set semantics for basic record usage.
 * Note :
 *   HCatRecord is designed only to be used as in-memory representation only.
 *   Don't use it to store data on the physical device.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class HCatRecord implements HCatRecordable {

  public abstract Object get(String fieldName, HCatSchema recordSchema) throws HCatException;

  public abstract void set(String fieldName, HCatSchema recordSchema, Object value) throws HCatException;

  public abstract void remove(int idx) throws HCatException;

  public abstract void copy(HCatRecord r) throws HCatException;

  protected Object get(String fieldName, HCatSchema recordSchema, Class clazz) throws HCatException {
    // TODO : if needed, verify that recordschema entry for fieldname matches appropriate type.
    return get(fieldName, recordSchema);
  }

  public Boolean getBoolean(String fieldName, HCatSchema recordSchema) throws HCatException {
    return (Boolean) get(fieldName, recordSchema, Boolean.class);
  }

  public void setBoolean(String fieldName, HCatSchema recordSchema, Boolean value) throws HCatException {
    set(fieldName, recordSchema, value);
  }

  public byte[] getByteArray(String fieldName, HCatSchema recordSchema) throws HCatException {
    return (byte[]) get(fieldName, recordSchema, byte[].class);
  }

  public void setByteArray(String fieldName, HCatSchema recordSchema, byte[] value) throws HCatException {
    set(fieldName, recordSchema, value);
  }

  public Byte getByte(String fieldName, HCatSchema recordSchema) throws HCatException {
    //TINYINT
    return (Byte) get(fieldName, recordSchema, Byte.class);
  }

  public void setByte(String fieldName, HCatSchema recordSchema, Byte value) throws HCatException {
    set(fieldName, recordSchema, value);
  }

  public Short getShort(String fieldName, HCatSchema recordSchema) throws HCatException {
    // SMALLINT
    return (Short) get(fieldName, recordSchema, Short.class);
  }

  public void setShort(String fieldName, HCatSchema recordSchema, Short value) throws HCatException {
    set(fieldName, recordSchema, value);
  }

  public Integer getInteger(String fieldName, HCatSchema recordSchema) throws HCatException {
    return (Integer) get(fieldName, recordSchema, Integer.class);
  }

  public void setInteger(String fieldName, HCatSchema recordSchema, Integer value) throws HCatException {
    set(fieldName, recordSchema, value);
  }

  public Long getLong(String fieldName, HCatSchema recordSchema) throws HCatException {
    // BIGINT
    return (Long) get(fieldName, recordSchema, Long.class);
  }

  public void setLong(String fieldName, HCatSchema recordSchema, Long value) throws HCatException {
    set(fieldName, recordSchema, value);
  }

  public Float getFloat(String fieldName, HCatSchema recordSchema) throws HCatException {
    return (Float) get(fieldName, recordSchema, Float.class);
  }

  public void setFloat(String fieldName, HCatSchema recordSchema, Float value) throws HCatException {
    set(fieldName, recordSchema, value);
  }

  public Double getDouble(String fieldName, HCatSchema recordSchema) throws HCatException {
    return (Double) get(fieldName, recordSchema, Double.class);
  }

  public void setDouble(String fieldName, HCatSchema recordSchema, Double value) throws HCatException {
    set(fieldName, recordSchema, value);
  }

  public String getString(String fieldName, HCatSchema recordSchema) throws HCatException {
    return (String) get(fieldName, recordSchema, String.class);
  }

  public void setString(String fieldName, HCatSchema recordSchema, String value) throws HCatException {
    set(fieldName, recordSchema, value);
  }

  @SuppressWarnings("unchecked")
  public List<? extends Object> getStruct(String fieldName, HCatSchema recordSchema) throws HCatException {
    return (List<? extends Object>) get(fieldName, recordSchema, List.class);
  }

  public void setStruct(String fieldName, HCatSchema recordSchema, List<? extends Object> value) throws HCatException {
    set(fieldName, recordSchema, value);
  }

  public List<?> getList(String fieldName, HCatSchema recordSchema) throws HCatException {
    return (List<?>) get(fieldName, recordSchema, List.class);
  }

  public void setList(String fieldName, HCatSchema recordSchema, List<?> value) throws HCatException {
    set(fieldName, recordSchema, value);
  }

  public Map<?, ?> getMap(String fieldName, HCatSchema recordSchema) throws HCatException {
    return (Map<?, ?>) get(fieldName, recordSchema, Map.class);
  }

  public void setMap(String fieldName, HCatSchema recordSchema, Map<?, ?> value) throws HCatException {
    set(fieldName, recordSchema, value);
  }

}
