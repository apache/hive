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
package org.apache.hadoop.hive.llap;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;

public class Row {
  private final Schema schema;
  private final Object[] colValues;
  private Map<String, Integer> nameToIndexMapping;

  public Row(Schema schema) {
    this.schema = schema;
    this.colValues = new Object[schema.getColumns().size()];
    this.nameToIndexMapping = new HashMap<String, Integer>(schema.getColumns().size());

    List<FieldDesc> colDescs = schema.getColumns();
    for (int idx = 0; idx < colDescs.size(); ++idx) {
      FieldDesc colDesc = colDescs.get(idx);
      nameToIndexMapping.put(colDesc.getName(), idx);
    }
  }

  public Object getValue(int colIndex) {
    return colValues[colIndex];
  }

  public Object getValue(String colName) {
    Integer idx = nameToIndexMapping.get(colName);
    Preconditions.checkArgument(idx != null);
    return getValue(idx);
  }

  public Boolean getBoolean(int idx) {
    return (Boolean) getValue(idx);
  }

  public Boolean getBoolean(String colName) {
    return (Boolean) getValue(colName);
  }

  public Byte getByte(int idx) {
    return (Byte) getValue(idx);
  }

  public Byte getByte(String colName) {
    return (Byte) getValue(colName);
  }

  public Short getShort(int idx) {
    return (Short) getValue(idx);
  }

  public Short getShort(String colName) {
    return (Short) getValue(colName);
  }

  public Integer getInt(int idx) {
    return (Integer) getValue(idx);
  }

  public Integer getInt(String colName) {
    return (Integer) getValue(colName);
  }

  public Long getLong(int idx) {
    return (Long) getValue(idx);
  }

  public Long getLong(String colName) {
    return (Long) getValue(colName);
  }

  public Float getFloat(int idx) {
    return (Float) getValue(idx);
  }

  public Float getFloat(String colName) {
    return (Float) getValue(colName);
  }

  public Double getDouble(int idx) {
    return (Double) getValue(idx);
  }

  public Double getDouble(String colName) {
    return (Double) getValue(colName);
  }

  public String getString(int idx) {
    return (String) getValue(idx);
  }

  public String getString(String colName) {
    return (String) getValue(colName);
  }

  public Date getDate(int idx) {
    return (Date) getValue(idx);
  }

  public Date getDate(String colName) {
    return (Date) getValue(colName);
  }

  public Timestamp getTimestamp(int idx) {
    return (Timestamp) getValue(idx);
  }

  public Timestamp getTimestamp(String colName) {
    return (Timestamp) getValue(colName);
  }

  public byte[] getBytes(int idx) {
    return (byte[]) getValue(idx);
  }

  public byte[] getBytes(String colName) {
    return (byte[]) getValue(colName);
  }

  public BigDecimal getDecimal(int idx) {
    return (BigDecimal) getValue(idx);
  }

  public BigDecimal getDecimal(String colName) {
    return (BigDecimal) getValue(colName);
  }

  public List<?> getList(int idx) {
    return (List<?>) getValue(idx);
  }

  public List<?> getList(String colName) {
    return (List<?>) getValue(colName);
  }

  public Map<?, ?> getMap(int idx) {
    return (Map<?, ?>) getValue(idx);
  }

  public Map<?, ?> getMap(String colName) {
    return (Map<?, ?>) getValue(colName);
  }

  // Struct value is simply a list of values.
  // The schema can be used to map the field name to the position in the list.
  public List<?> getStruct(int idx) {
    return (List<?>) getValue(idx);
  }

  public List<?> getStruct(String colName) {
    return (List<?>) getValue(colName);
  }

  public Schema getSchema() {
    return schema;
  }

  void setValue(int colIdx, Object obj) {
    colValues[colIdx] = obj;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    for (int idx = 0; idx < schema.getColumns().size(); ++idx) {
      if (idx > 0) {
        sb.append(", ");
      }
      Object val = getValue(idx);
      sb.append(val == null ? "null" : val.toString());
    }
    sb.append("]");
    return sb.toString();
  }
}
