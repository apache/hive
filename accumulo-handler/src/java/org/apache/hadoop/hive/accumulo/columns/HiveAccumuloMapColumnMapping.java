/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.accumulo.columns;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.hive.accumulo.AccumuloHiveConstants;

import com.google.common.base.Preconditions;

/**
 * ColumnMapping for combining Accumulo columns into a single Hive Map. Expects ColumnEncoding
 * values for both the Key and Value of the Map.
 */
public class HiveAccumuloMapColumnMapping extends ColumnMapping {

  protected final String columnFamily, columnQualifierPrefix;
  protected final ColumnEncoding keyEncoding, valueEncoding;

  /**
   * @param columnFamily
   *          The column family that all qualifiers within should be placed into the same Hive map
   * @param columnQualifierPrefix
   *          The column qualifier prefix to include in the map, null is treated as an empty prefix
   * @param keyEncoding
   *          The encoding scheme for keys in this column family
   * @param valueEncoding
   *          The encoding scheme for the Accumulo values
   */
  public HiveAccumuloMapColumnMapping(String columnFamily, String columnQualifierPrefix,
      ColumnEncoding keyEncoding, ColumnEncoding valueEncoding, String columnName,
      String columnType) {
    // Try to make something reasonable to pass up to the base class
    super((null == columnFamily ? "" : columnFamily) + AccumuloHiveConstants.COLON, valueEncoding,
        columnName, columnType);

    Preconditions.checkNotNull(columnFamily, "Must provide a column family");

    this.columnFamily = columnFamily;
    this.columnQualifierPrefix = (null == columnQualifierPrefix) ? "" : columnQualifierPrefix;
    this.keyEncoding = keyEncoding;
    this.valueEncoding = valueEncoding;
  }

  public String getColumnFamily() {
    return columnFamily;
  }

  public String getColumnQualifierPrefix() {
    return columnQualifierPrefix;
  }

  public ColumnEncoding getKeyEncoding() {
    return keyEncoding;
  }

  public ColumnEncoding getValueEncoding() {
    return valueEncoding;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof HiveAccumuloMapColumnMapping) {
      HiveAccumuloMapColumnMapping other = (HiveAccumuloMapColumnMapping) o;
      return columnFamily.equals(other.columnFamily)
          && columnQualifierPrefix.equals(other.columnQualifierPrefix)
          && keyEncoding.equals(other.keyEncoding) && valueEncoding.equals(other.valueEncoding);
    }

    return false;
  }

  @Override
  public int hashCode() {
    HashCodeBuilder hcb = new HashCodeBuilder(23, 31);
    hcb.append(columnFamily).append(columnQualifierPrefix).append(keyEncoding)
        .append(valueEncoding);
    return hcb.toHashCode();
  }

  @Override
  public String toString() {
    return "[" + this.getClass().getSimpleName() + ": " + columnFamily + ":"
        + columnQualifierPrefix + "* encoding: " + keyEncoding + ":" + valueEncoding + "]";
  }
}
