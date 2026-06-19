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

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import com.google.common.base.Preconditions;

/**
 *
 */
public abstract class ColumnMapping {

  // SerDe property for how the Hive column maps to Accumulo
  protected final String mappingSpec;

  // The manner in which the values in this column are de/serialized from/to Accumulo
  protected final ColumnEncoding encoding;

  // The name of the Hive column
  protected final String columnName;

  // The type of the Hive column
  // Cannot store the actual TypeInfo because that would require
  // Hive jars on the Accumulo classpath which we don't want
  protected final String columnType;

  protected ColumnMapping(String mappingSpec, ColumnEncoding encoding, String columnName,
      String columnType) {
    Preconditions.checkNotNull(mappingSpec);
    Preconditions.checkNotNull(encoding);
    Preconditions.checkNotNull(columnName);
    Preconditions.checkNotNull(columnType);

    this.mappingSpec = mappingSpec;
    this.encoding = encoding;
    this.columnName = columnName;
    this.columnType = columnType;
  }

  protected ColumnMapping(String mappingSpec, ColumnEncoding encoding, String columnName,
      TypeInfo columnType) {
    Preconditions.checkNotNull(mappingSpec);
    Preconditions.checkNotNull(encoding);
    Preconditions.checkNotNull(columnName);
    Preconditions.checkNotNull(columnType);

    this.mappingSpec = mappingSpec;
    this.encoding = encoding;
    this.columnName = columnName;
    this.columnType = columnType.getTypeName();
  }

  /**
   * The property defining how this Column is mapped into Accumulo
   */
  public String getMappingSpec() {
    return mappingSpec;
  }

  /**
   * The manner in which the value is encoded in Accumulo
   */
  public ColumnEncoding getEncoding() {
    return encoding;
  }

  /**
   * The name of the Hive column this is mapping
   */
  public String getColumnName() {
    return columnName;
  }

  /**
   * The @{link TypeInfo} of the Hive column this is mapping
   */
  public String getColumnType() {
    return columnType;
  }
}
