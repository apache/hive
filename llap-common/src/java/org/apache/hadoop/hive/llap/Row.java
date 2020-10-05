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
    this.nameToIndexMapping = new HashMap(schema.getColumns().size());
    List<FieldDesc> colDescs = schema.getColumns();

    for(int idx = 0; idx < colDescs.size(); ++idx) {
      FieldDesc colDesc = (FieldDesc)colDescs.get(idx);
      this.nameToIndexMapping.put(colDesc.getName(), idx);
    }
  }

  public Object getValue(int colIndex) {
    return this.colValues[colIndex];
  }

  public Object getValue(String colName) {
    Integer idx = (Integer)this.nameToIndexMapping.get(colName);
    Preconditions.checkArgument(idx != null);
    return this.getValue(idx);
  }

  public Boolean getBoolean(int idx) {
    return (Boolean)this.getValue(idx);
  }

  public Boolean getBoolean(String colName) {
    return (Boolean)this.getValue(colName);
  }

  public Byte getByte(int idx) {
    return (Byte)this.getValue(idx);
  }

  public Byte getByte(String colName) {
    return (Byte)this.getValue(colName);
  }

  public Short getShort(int idx) {
    return (Short)this.getValue(idx);
  }

  public Short getShort(String colName) {
    return (Short)this.getValue(colName);
  }

  public Integer getInt(int idx) {
    return (Integer)this.getValue(idx);
  }

  public Integer getInt(String colName) {
    return (Integer)this.getValue(colName);
  }

  public Long getLong(int idx) {
    return (Long)this.getValue(idx);
  }

  public Long getLong(String colName) {
    return (Long)this.getValue(colName);
  }

  public Float getFloat(int idx) {
    return (Float)this.getValue(idx);
  }

  public Float getFloat(String colName) {
    return (Float)this.getValue(colName);
  }

  public Double getDouble(int idx) {
    return (Double)this.getValue(idx);
  }

  public Double getDouble(String colName) {
    return (Double)this.getValue(colName);
  }

  public String getString(int idx) {
    return (String)this.getValue(idx);
  }

  public String getString(String colName) {
    return (String)this.getValue(colName);
  }

  public Date getDate(int idx) {
    return (Date)this.getValue(idx);
  }

  public Date getDate(String colName) {
    return (Date)this.getValue(colName);
  }

  public Timestamp getTimestamp(int idx) {
    return (Timestamp)this.getValue(idx);
  }

  public Timestamp getTimestamp(String colName) {
    return (Timestamp)this.getValue(colName);
  }

  public byte[] getBytes(int idx) {
    return (byte[])((byte[])this.getValue(idx));
  }

  public byte[] getBytes(String colName) {
    return (byte[])((byte[])this.getValue(colName));
  }

  public BigDecimal getDecimal(int idx) {
    return (BigDecimal)this.getValue(idx);
  }

  public BigDecimal getDecimal(String colName) {
    return (BigDecimal)this.getValue(colName);
  }

  public List<?> getList(int idx) {
    return (List)this.getValue(idx);
  }

  public List<?> getList(String colName) {
    return (List)this.getValue(colName);
  }

  public Map<?, ?> getMap(int idx) {
    return (Map)this.getValue(idx);
  }

  public Map<?, ?> getMap(String colName) {
    return (Map)this.getValue(colName);
  }

  public List<?> getStruct(int idx) {
    return (List)this.getValue(idx);
  }

  public List<?> getStruct(String colName) {
    return (List)this.getValue(colName);
  }

  public Schema getSchema() {
    return this.schema;
  }

  void setValue(int colIdx, Object obj) {
    this.colValues[colIdx] = obj;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("[");

    for(int idx = 0; idx < this.schema.getColumns().size(); ++idx) {
      if (idx > 0) {
        sb.append(", ");
      }

      Object val = this.getValue(idx);
      sb.append(val == null ? "null" : val.toString());
    }

    sb.append("]");
    return sb.toString();
  }
}
