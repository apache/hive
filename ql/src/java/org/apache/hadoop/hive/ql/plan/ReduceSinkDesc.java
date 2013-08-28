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


/**
 * ReduceSinkDesc.
 *
 */
@Explain(displayName = "Reduce Output Operator")
public class ReduceSinkDesc extends AbstractOperatorDesc {
  private static final long serialVersionUID = 1L;
  /**
   * Key columns are passed to reducer in the "key".
   */
  private java.util.ArrayList<ExprNodeDesc> keyCols;
  private java.util.ArrayList<java.lang.String> outputKeyColumnNames;
  private List<List<Integer>> distinctColumnIndices;
  /**
   * Value columns are passed to reducer in the "value".
   */
  private java.util.ArrayList<ExprNodeDesc> valueCols;
  private java.util.ArrayList<java.lang.String> outputValueColumnNames;
  /**
   * Describe how to serialize the key.
   */
  private TableDesc keySerializeInfo;
  /**
   * Describe how to serialize the value.
   */
  private TableDesc valueSerializeInfo;

  /**
   * The tag for this reducesink descriptor.
   */
  private int tag;

  /**
   * Number of distribution keys.
   */
  private int numDistributionKeys;

  /**
   * The partition columns (CLUSTER BY or DISTRIBUTE BY in Hive language).
   * Partition columns decide the reducer that the current row goes to.
   * Partition columns are not passed to reducer.
   */
  private java.util.ArrayList<ExprNodeDesc> partitionCols;

  private int numReducers;

  private int topN = -1;
  private float topNMemoryUsage = -1;
  private boolean mapGroupBy;  // for group-by, values with same key on top-K should be forwarded

  public ReduceSinkDesc() {
  }

  public ReduceSinkDesc(ArrayList<ExprNodeDesc> keyCols,
      int numDistributionKeys,
      ArrayList<ExprNodeDesc> valueCols,
      ArrayList<String> outputKeyColumnNames,
      List<List<Integer>> distinctColumnIndices,
      ArrayList<String> outputValueColumnNames, int tag,
      ArrayList<ExprNodeDesc> partitionCols, int numReducers,
      final TableDesc keySerializeInfo, final TableDesc valueSerializeInfo) {
    this.keyCols = keyCols;
    this.numDistributionKeys = numDistributionKeys;
    this.valueCols = valueCols;
    this.outputKeyColumnNames = outputKeyColumnNames;
    this.outputValueColumnNames = outputValueColumnNames;
    this.tag = tag;
    this.numReducers = numReducers;
    this.partitionCols = partitionCols;
    this.keySerializeInfo = keySerializeInfo;
    this.valueSerializeInfo = valueSerializeInfo;
    this.distinctColumnIndices = distinctColumnIndices;
  }

  @Override
  public Object clone() {
    ReduceSinkDesc desc = new ReduceSinkDesc();
    desc.setKeyCols((ArrayList<ExprNodeDesc>) getKeyCols().clone());
    desc.setValueCols((ArrayList<ExprNodeDesc>) getValueCols().clone());
    desc.setOutputKeyColumnNames((ArrayList<String>) getOutputKeyColumnNames().clone());
    List<List<Integer>> distinctColumnIndicesClone = new ArrayList<List<Integer>>();
    for (List<Integer> distinctColumnIndex : getDistinctColumnIndices()) {
      List<Integer> tmp = new ArrayList<Integer>();
      tmp.addAll(distinctColumnIndex);
      distinctColumnIndicesClone.add(tmp);
    }
    desc.setDistinctColumnIndices(distinctColumnIndicesClone);
    desc.setOutputValueColumnNames((ArrayList<String>) getOutputValueColumnNames().clone());
    desc.setNumDistributionKeys(getNumDistributionKeys());
    desc.setTag(getTag());
    desc.setNumReducers(getNumReducers());
    desc.setPartitionCols((ArrayList<ExprNodeDesc>) getPartitionCols().clone());
    desc.setKeySerializeInfo((TableDesc) getKeySerializeInfo().clone());
    desc.setValueSerializeInfo((TableDesc) getValueSerializeInfo().clone());
    return desc;
  }

  public java.util.ArrayList<java.lang.String> getOutputKeyColumnNames() {
    return outputKeyColumnNames;
  }

  public void setOutputKeyColumnNames(
      java.util.ArrayList<java.lang.String> outputKeyColumnNames) {
    this.outputKeyColumnNames = outputKeyColumnNames;
  }

  public java.util.ArrayList<java.lang.String> getOutputValueColumnNames() {
    return outputValueColumnNames;
  }

  public void setOutputValueColumnNames(
      java.util.ArrayList<java.lang.String> outputValueColumnNames) {
    this.outputValueColumnNames = outputValueColumnNames;
  }

  @Explain(displayName = "key expressions")
  public java.util.ArrayList<ExprNodeDesc> getKeyCols() {
    return keyCols;
  }

  public void setKeyCols(final java.util.ArrayList<ExprNodeDesc> keyCols) {
    this.keyCols = keyCols;
  }

  public int getNumDistributionKeys() {
    return this.numDistributionKeys;
  }

  public void setNumDistributionKeys(int numKeys) {
    this.numDistributionKeys = numKeys;
  }

  @Explain(displayName = "value expressions")
  public java.util.ArrayList<ExprNodeDesc> getValueCols() {
    return valueCols;
  }

  public void setValueCols(final java.util.ArrayList<ExprNodeDesc> valueCols) {
    this.valueCols = valueCols;
  }

  @Explain(displayName = "Map-reduce partition columns")
  public java.util.ArrayList<ExprNodeDesc> getPartitionCols() {
    return partitionCols;
  }

  public void setPartitionCols(
      final java.util.ArrayList<ExprNodeDesc> partitionCols) {
    this.partitionCols = partitionCols;
  }

  @Explain(displayName = "tag")
  public int getTag() {
    return tag;
  }

  public void setTag(int tag) {
    this.tag = tag;
  }

  public int getTopN() {
    return topN;
  }

  public void setTopN(int topN) {
    this.topN = topN;
  }

  @Explain(displayName = "TopN")
  public Integer getTopNExplain() {
    return topN > 0 ? topN : null;
  }

  public float getTopNMemoryUsage() {
    return topNMemoryUsage;
  }

  public void setTopNMemoryUsage(float topNMemoryUsage) {
    this.topNMemoryUsage = topNMemoryUsage;
  }

  @Explain(displayName = "TopN Hash Memory Usage")
  public Float getTopNMemoryUsageExplain() {
    return topN > 0 && topNMemoryUsage > 0 ? topNMemoryUsage : null;
  }

  public boolean isMapGroupBy() {
    return mapGroupBy;
  }

  public void setMapGroupBy(boolean mapGroupBy) {
    this.mapGroupBy = mapGroupBy;
  }

  /**
   * Returns the number of reducers for the map-reduce job. -1 means to decide
   * the number of reducers at runtime. This enables Hive to estimate the number
   * of reducers based on the map-reduce input data size, which is only
   * available right before we start the map-reduce job.
   */
  public int getNumReducers() {
    return numReducers;
  }

  public void setNumReducers(int numReducers) {
    this.numReducers = numReducers;
  }

  public TableDesc getKeySerializeInfo() {
    return keySerializeInfo;
  }

  public void setKeySerializeInfo(TableDesc keySerializeInfo) {
    this.keySerializeInfo = keySerializeInfo;
  }

  public TableDesc getValueSerializeInfo() {
    return valueSerializeInfo;
  }

  public void setValueSerializeInfo(TableDesc valueSerializeInfo) {
    this.valueSerializeInfo = valueSerializeInfo;
  }

  /**
   * Returns the sort order of the key columns.
   *
   * @return null, which means ascending order for all key columns, or a String
   *         of the same length as key columns, that consists of only "+"
   *         (ascending order) and "-" (descending order).
   */
  @Explain(displayName = "sort order")
  public String getOrder() {
    return keySerializeInfo.getProperties().getProperty(
        org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_SORT_ORDER);
  }

  public void setOrder(String orderStr) {
    keySerializeInfo.getProperties().setProperty(
        org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_SORT_ORDER,
        orderStr);
  }

  public List<List<Integer>> getDistinctColumnIndices() {
    return distinctColumnIndices;
  }

  public void setDistinctColumnIndices(
      List<List<Integer>> distinctColumnIndices) {
    this.distinctColumnIndices = distinctColumnIndices;
  }
}
