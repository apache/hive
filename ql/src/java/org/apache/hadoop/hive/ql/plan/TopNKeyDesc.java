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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.optimizer.topnkey.CommonKeyPrefix;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

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
  private List<ExprNodeDesc> partitionKeyColumns;
  private float efficiencyThreshold;
  private long checkEfficiencyNumBatches;
  private long checkEfficiencyNumRows;
  private int maxNumberOfPartitions;

  public TopNKeyDesc() {
  }

  public TopNKeyDesc(
    final int topN,
    final String columnSortOrder,
    final String nullOrder,
    final List<ExprNodeDesc> keyColumns,
    final List<ExprNodeDesc> partitionKeyColumns,
    float efficiencyThreshold,
    long checkEfficiencyNumBatches,
    int maxNumberOfPartitions) {

    this.topN = topN;
    this.keyColumns = new ArrayList<>(keyColumns.size());
    this.efficiencyThreshold = efficiencyThreshold;
    this.checkEfficiencyNumBatches = checkEfficiencyNumBatches;
    this.checkEfficiencyNumRows = checkEfficiencyNumBatches * VectorizedRowBatch.DEFAULT_SIZE;
    this.maxNumberOfPartitions = maxNumberOfPartitions;
    StringBuilder sortOrder = new StringBuilder(columnSortOrder.length());
    StringBuilder nullSortOrder = new StringBuilder(nullOrder.length());
    this.partitionKeyColumns = new ArrayList<>(partitionKeyColumns.size());

    for (int i = 0; i < keyColumns.size(); ++i) {
      ExprNodeDesc keyExpression = keyColumns.get(i);
      if (keyExpression instanceof ExprNodeConstantDesc) {
        continue;
      }
      this.keyColumns.add(keyExpression);
      sortOrder.append(columnSortOrder.charAt(i));
      nullSortOrder.append(nullOrder.charAt(i));
    }

    this.columnSortOrder = sortOrder.toString();
    this.nullOrder = nullSortOrder.toString();

    for (ExprNodeDesc keyExpression : partitionKeyColumns) {
      if (keyExpression instanceof ExprNodeConstantDesc) {
        continue;
      }
      this.partitionKeyColumns.add(keyExpression);
    }
  }

  @Explain(displayName = "top n", explainLevels = { Level.DEFAULT, Level.EXTENDED, Level.USER })
  public int getTopN() {
    return topN;
  }

  public float getEfficiencyThreshold() {
    return efficiencyThreshold;
  }

  public long getCheckEfficiencyNumBatches() {
    return checkEfficiencyNumBatches;
  }

  public long getCheckEfficiencyNumRows() {
    return checkEfficiencyNumRows;
  }

  public int getMaxNumberOfPartitions() {
    return maxNumberOfPartitions;
  }

  public void setTopN(int topN) {
    this.topN = topN;
  }

  @Explain(displayName = "sort order", explainLevels = { Level.DEFAULT, Level.EXTENDED })
  public String getColumnSortOrder() {
    return columnSortOrder;
  }

  public void setColumnSortOrder(String columnSortOrder) {
    this.columnSortOrder = columnSortOrder;
  }

  @Explain(displayName = "null sort order", explainLevels = { Level.DEFAULT, Level.EXTENDED })
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

  public List<ExprNodeDesc> getPartitionKeyColumns() {
    return partitionKeyColumns;
  }

  public void setPartitionKeyColumns(List<ExprNodeDesc> partitionKeyColumns) {
    this.partitionKeyColumns = partitionKeyColumns;
  }

  @Explain(displayName = "Map-reduce partition columns")
  public String getPartitionKeyString() {
    return PlanUtils.getExprListString(partitionKeyColumns);
  }

  @Explain(displayName = "PartitionCols", explainLevels = { Level.USER })
  public String getUserLevelExplainPartitionKeyString() {
    return PlanUtils.getExprListString(partitionKeyColumns, true);
  }


  @Override
  public boolean isSame(OperatorDesc other) {
    if (getClass().getName().equals(other.getClass().getName())) {
      TopNKeyDesc otherDesc = (TopNKeyDesc) other;
      return getTopN() == otherDesc.getTopN() &&
          getEfficiencyThreshold() == otherDesc.getEfficiencyThreshold() &&
          getCheckEfficiencyNumRows() == otherDesc.getCheckEfficiencyNumRows() &&
          getCheckEfficiencyNumBatches() == otherDesc.getCheckEfficiencyNumBatches() &&
          getMaxNumberOfPartitions() == otherDesc.getMaxNumberOfPartitions() &&
          ExprNodeDescUtils.isSame(partitionKeyColumns, otherDesc.partitionKeyColumns) &&
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
    ret.setPartitionKeyColumns(getPartitionKeyColumns() == null ? null : new ArrayList<>(getPartitionKeyColumns()));
    ret.setCheckEfficiencyNumRows(checkEfficiencyNumRows);
    ret.setCheckEfficiencyNumBatches(checkEfficiencyNumBatches);
    ret.setEfficiencyThreshold(efficiencyThreshold);
    ret.setMaxNumberOfPartitions(maxNumberOfPartitions);
    return ret;
  }

  public void setEfficiencyThreshold(float efficiencyThreshold) {
    this.efficiencyThreshold = efficiencyThreshold;
  }

  public void setCheckEfficiencyNumBatches(long checkEfficiencyNumBatches) {
    this.checkEfficiencyNumBatches = checkEfficiencyNumBatches;
  }

  public void setCheckEfficiencyNumRows(long checkEfficiencyNumRows) {
    this.checkEfficiencyNumRows = checkEfficiencyNumRows;
  }

  public void setMaxNumberOfPartitions(int maxNumberOfPartitions) {
    this.maxNumberOfPartitions = maxNumberOfPartitions;
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

  public TopNKeyDesc combine(CommonKeyPrefix commonKeyPrefix) {
    return new TopNKeyDesc(topN, commonKeyPrefix.getMappedOrder(),
            commonKeyPrefix.getMappedNullOrder(), commonKeyPrefix.getMappedColumns(),
            commonKeyPrefix.getMappedColumns().subList(0, partitionKeyColumns.size()),
            efficiencyThreshold, checkEfficiencyNumBatches, maxNumberOfPartitions);
  }

  public TopNKeyDesc withKeyColumns(int prefixLength) {
    return new TopNKeyDesc(topN,
            columnSortOrder,
            nullOrder,
            keyColumns.subList(0, prefixLength),
            partitionKeyColumns,
            efficiencyThreshold,
            checkEfficiencyNumBatches,
            maxNumberOfPartitions);
  }

}
