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

package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

/**
 * ExprNodeColumnDesc.
 *
 */
public class ExprNodeColumnDesc extends ExprNodeDesc implements Serializable {
  private static final long serialVersionUID = 1L;

  /**
   * The column name.
   */
  private String column;

  /**
   * The alias of the table.
   */
  private String tabAlias;

  /**
   * Is the column a partitioned column.
   */
  private boolean isPartitionCol;

  public ExprNodeColumnDesc() {
  }

  public ExprNodeColumnDesc(TypeInfo typeInfo, String column, String tabAlias,
      boolean isPartitionCol) {
    super(typeInfo);
    this.column = column;
    this.tabAlias = tabAlias;
    this.isPartitionCol = isPartitionCol;
  }

  public ExprNodeColumnDesc(Class<?> c, String column, String tabAlias,
      boolean isPartitionCol) {
    super(TypeInfoFactory.getPrimitiveTypeInfoFromJavaPrimitive(c));
    this.column = column;
    this.tabAlias = tabAlias;
    this.isPartitionCol = isPartitionCol;
  }

  public String getColumn() {
    return column;
  }

  public void setColumn(String column) {
    this.column = column;
  }

  public String getTabAlias() {
    return tabAlias;
  }

  public void setTabAlias(String tabAlias) {
    this.tabAlias = tabAlias;
  }

  public boolean getIsParititonCol() {
    return isPartitionCol;
  }

  public void setIsPartitionCol(boolean isPartitionCol) {
    this.isPartitionCol = isPartitionCol;
  }

  @Override
  public String toString() {
    return "Column[" + column + "]";
  }

  @Explain(displayName = "expr")
  @Override
  public String getExprString() {
    return getColumn();
  }

  @Override
  public List<String> getCols() {
    List<String> lst = new ArrayList<String>();
    lst.add(column);
    return lst;
  }

  @Override
  public ExprNodeDesc clone() {
    return new ExprNodeColumnDesc(typeInfo, column, tabAlias, isPartitionCol);
  }

  @Override
  public boolean isSame(Object o) {
    if (!(o instanceof ExprNodeColumnDesc)) {
      return false;
    }
    ExprNodeColumnDesc dest = (ExprNodeColumnDesc) o;
    if (!column.equals(dest.getColumn())) {
      return false;
    }
    if (!typeInfo.equals(dest.getTypeInfo())) {
      return false;
    }
    return true;
  }
}
