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

package org.apache.hadoop.hive.ql.exec;

import java.io.Serializable;

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

/**
 * Implementation for ColumnInfo which contains the internal name for the column
 * (the one that is used by the operator to access the column) and the type
 * (identified by a java class).
 **/

public class ColumnInfo implements Serializable {

  private static final long serialVersionUID = 1L;

  private String internalName;

  private String alias = null; // [optional] alias of the column (external name
                               // as seen by the users)

  /**
   * Store the alias of the table where available.
   */
  private String tabAlias;

  /**
   * Indicates whether the column is a partition column.
   */
  private boolean isPartitionCol;

  transient private TypeInfo type;

  public ColumnInfo() {
  }

  public ColumnInfo(String internalName, TypeInfo type, String tabAlias,
      boolean isPartitionCol) {
    this.internalName = internalName;
    this.type = type;
    this.tabAlias = tabAlias;
    this.isPartitionCol = isPartitionCol;
  }

  public ColumnInfo(String internalName, Class type, String tabAlias,
      boolean isPartitionCol) {
    this.internalName = internalName;
    this.type = TypeInfoFactory.getPrimitiveTypeInfoFromPrimitiveWritable(type);
    this.tabAlias = tabAlias;
    this.isPartitionCol = isPartitionCol;
  }

  public TypeInfo getType() {
    return type;
  }

  public String getInternalName() {
    return internalName;
  }

  public void setType(TypeInfo type) {
    this.type = type;
  }

  public void setInternalName(String internalName) {
    this.internalName = internalName;
  }

  public String getTabAlias() {
    return tabAlias;
  }

  public boolean getIsPartitionCol() {
    return isPartitionCol;
  }

  /**
   * Returns the string representation of the ColumnInfo.
   */
  @Override
  public String toString() {
    return internalName + ": " + type;
  }

  public void setAlias(String col_alias) {
    alias = col_alias;
  }

  public String getAlias() {
    return alias;
  }

}
