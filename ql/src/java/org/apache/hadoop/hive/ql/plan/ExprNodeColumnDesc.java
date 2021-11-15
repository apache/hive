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

package org.apache.hadoop.hive.ql.plan;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

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
  private boolean isPartitionColOrVirtualCol;

  /**
   * Is the column a skewed column
   */
  private boolean isSkewedCol;

  /**
   * Is this column a generated column, i.e., a column
   * that is generated from table schema when Hive inserting SEL op for column pruning.
   * This column has no relation with the input query.
   *
   * This is used for nested column pruning where we could have the following scenario:
   *   ...
   *    |
   *   SEL (use a)
   *    |
   *   OP (use a.f)
   *    |
   *   ...
   * Without this field we do not know whether the column 'a' is actually specified in
   * the input query or an inserted op by Hive. For the former case, the pruning needs
   * to produce 'a', while for the latter case, it should produce 'a.f'.
   */
  private transient boolean isGenerated;

  public ExprNodeColumnDesc() {
  }

  public ExprNodeColumnDesc(ColumnInfo ci) {
    this(ci, false);
  }

  public ExprNodeColumnDesc(ColumnInfo ci, boolean isGenerated) {
    this(ci.getType(), ci.getInternalName(), ci.getTabAlias(), ci.getIsVirtualCol(), false, isGenerated);
  }

  public ExprNodeColumnDesc(TypeInfo typeInfo, String column, String tabAlias,
      boolean isPartitionColOrVirtualCol) {
    this(typeInfo, column, tabAlias, isPartitionColOrVirtualCol, false, false);
  }

  public ExprNodeColumnDesc(Class<?> c, String column, String tabAlias,
      boolean isPartitionColOrVirtualCol) {
    this(TypeInfoFactory.getPrimitiveTypeInfoFromJavaPrimitive(c),
        column, tabAlias, isPartitionColOrVirtualCol, false, false);
  }

  public ExprNodeColumnDesc(TypeInfo typeInfo, String column, String tabAlias,
      boolean isPartitionColOrVirtualCol, boolean isSkewedCol) {
    this(typeInfo, column, tabAlias, isPartitionColOrVirtualCol, isSkewedCol, false);
  }

  public ExprNodeColumnDesc(TypeInfo typeInfo, String column, String tabAlias,
      boolean isPartitionColOrVirtualCol, boolean isSkewedCol, boolean isGenerated) {
    super(typeInfo);
    this.column = column;
    this.tabAlias = tabAlias;
    this.isPartitionColOrVirtualCol = isPartitionColOrVirtualCol;
    this.isSkewedCol = isSkewedCol;
    this.isGenerated = isGenerated;
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

  public boolean getIsPartitionColOrVirtualCol() {
    return isPartitionColOrVirtualCol;
  }

  public void setIsPartitionColOrVirtualCol(boolean isPartitionCol) {
    this.isPartitionColOrVirtualCol = isPartitionCol;
  }

  public boolean getIsGenerated() {
    return this.isGenerated;
  }

  public void setIsGenerated(boolean isGenerated) {
    this.isGenerated = isGenerated;
  }

  @Override
  public String toString() {
    return "Column[" + column + "]";
  }

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
    return new ExprNodeColumnDesc(typeInfo, column, tabAlias, isPartitionColOrVirtualCol,
        isSkewedCol);
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
    if ( tabAlias != null && dest.tabAlias != null ) {
      if ( !tabAlias.equals(dest.tabAlias) ) {
        return false;
      }
    }
    return true;
  }

  /**
   * @return the isSkewedCol
   */
  public boolean isSkewedCol() {
    return isSkewedCol;
  }

  /**
   * @param isSkewedCol the isSkewedCol to set
   */
  public void setSkewedCol(boolean isSkewedCol) {
    this.isSkewedCol = isSkewedCol;
  }

  @Override
  public int hashCode() {
    int superHashCode = super.hashCode();
    HashCodeBuilder builder = new HashCodeBuilder();
    builder.appendSuper(superHashCode);
    builder.append(column);
    builder.append(tabAlias);
    return builder.toHashCode();
  }
}
