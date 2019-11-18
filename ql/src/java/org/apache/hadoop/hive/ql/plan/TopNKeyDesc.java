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

import org.apache.hadoop.hive.ql.plan.Explain.Level;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * TopNKeyDesc.
 *
 */
@Explain(displayName = "Top N Key Operator", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class TopNKeyDesc extends AbstractOperatorDesc {
  private static final long serialVersionUID = 1L;

  private int topN;
  private String columnSortOrder;
  private String nullOrder;
  private List<ExprNodeDesc> keyColumns;

  public TopNKeyDesc() {
  }

  public TopNKeyDesc(
      final int topN,
      final String columnSortOrder,
      final String nullOrder,
      final List<ExprNodeDesc> keyColumns) {

    this.topN = topN;
    this.columnSortOrder = columnSortOrder;
    this.nullOrder = nullOrder;
    this.keyColumns = keyColumns;
  }

  @Explain(displayName = "top n", explainLevels = { Level.DEFAULT, Level.EXTENDED, Level.USER })
  public int getTopN() {
    return topN;
  }

  public void setTopN(int topN) {
    this.topN = topN;
  }

  @Explain(displayName = "sort order", explainLevels = { Level.DEFAULT, Level.EXTENDED, Level.USER })
  public String getColumnSortOrder() {
    return columnSortOrder;
  }

  public void setColumnSortOrder(String columnSortOrder) {
    this.columnSortOrder = columnSortOrder;
  }

  @Explain(displayName = "null sort order", explainLevels = { Level.EXTENDED })
  public String getNullOrder() {
    return nullOrder;
  }

  public void setNullOrder(String nullOrder) {
    this.nullOrder = nullOrder;
  }

  @Explain(displayName = "keys")
  public String getKeyString() {
    return PlanUtils.getExprListString(keyColumns);
  }

  @Explain(displayName = "keys", explainLevels = { Level.USER })
  public String getUserLevelExplainKeyString() {
    return PlanUtils.getExprListString(keyColumns, true);
  }

  public List<ExprNodeDesc> getKeyColumns() {
    return keyColumns;
  }

  public void setKeyColumns(List<ExprNodeDesc> keyColumns) {
    this.keyColumns = keyColumns;
  }

  public List<String> getKeyColumnNames() {
    List<String> ret = new ArrayList<>();
    for (ExprNodeDesc keyColumn : keyColumns) {
      ret.add(keyColumn.getExprString());
    }
    return ret;
  }

  @Override
  public boolean isSame(OperatorDesc other) {
    if (getClass().getName().equals(other.getClass().getName())) {
      TopNKeyDesc otherDesc = (TopNKeyDesc) other;
      return getTopN() == otherDesc.getTopN() &&
          Objects.equals(columnSortOrder, otherDesc.columnSortOrder) &&
          Objects.equals(nullOrder, otherDesc.nullOrder) &&
          ExprNodeDescUtils.isSame(keyColumns, otherDesc.keyColumns);
    }
    return false;
  }

  @Override
  public Object clone() {
    TopNKeyDesc ret = new TopNKeyDesc();
    ret.setTopN(topN);
    ret.setColumnSortOrder(columnSortOrder);
    ret.setNullOrder(nullOrder);
    ret.setKeyColumns(getKeyColumns() == null ? null : new ArrayList<>(getKeyColumns()));
    return ret;
  }

  public class TopNKeyDescExplainVectorization extends OperatorExplainVectorization {
    private final TopNKeyDesc topNKeyDesc;
    private final VectorTopNKeyDesc vectorTopNKeyDesc;

    public TopNKeyDescExplainVectorization(TopNKeyDesc topNKeyDesc, VectorTopNKeyDesc vectorTopNKeyDesc) {
      super(vectorTopNKeyDesc, true);
      this.topNKeyDesc = topNKeyDesc;
      this.vectorTopNKeyDesc = vectorTopNKeyDesc;
    }

    @Explain(vectorization = Explain.Vectorization.OPERATOR, displayName = "keyExpressions", explainLevels = { Level.DEFAULT, Level.EXTENDED })
    public List<String> getKeyExpressions() {
      return vectorExpressionsToStringList(vectorTopNKeyDesc.getKeyExpressions());
    }
  }

  @Explain(vectorization = Explain.Vectorization.OPERATOR, displayName = "Top N Key Vectorization", explainLevels = { Level.DEFAULT, Level.EXTENDED })
  public TopNKeyDescExplainVectorization getTopNKeyVectorization() {
    VectorTopNKeyDesc vectorTopNKeyDesc = (VectorTopNKeyDesc) getVectorDesc();
    if (vectorTopNKeyDesc == null) {
      return null;
    }
    return new TopNKeyDescExplainVectorization(this, vectorTopNKeyDesc);
  }
}
