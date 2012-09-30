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
 * BaseReduceSinkDesc.
 *
 */
@Explain(displayName = "Base Reduce Output Operator")
public class BaseReduceSinkDesc extends AbstractOperatorDesc {
  private static final long serialVersionUID = 1L;
  /**
   * Key columns are passed to reducer in the "key".
   */
  protected ArrayList<ExprNodeDesc> keyCols;
  protected ArrayList<String> outputKeyColumnNames;
  protected List<List<Integer>> distinctColumnIndices;
  /**
   * Value columns are passed to reducer in the "value".
   */
  protected ArrayList<ExprNodeDesc> valueCols;
  protected ArrayList<java.lang.String> outputValueColumnNames;
  /**
   * Describe how to serialize the key.
   */
  protected TableDesc keySerializeInfo;
  /**
   * Describe how to serialize the value.
   */
  protected TableDesc valueSerializeInfo;

  /**
   * The tag for this reducesink descriptor.
   */
  protected int tag;

  /**
   * Number of distribution keys.
   */
  protected int numDistributionKeys;

  /**
   * The partition columns (CLUSTER BY or DISTRIBUTE BY in Hive language).
   * Partition columns decide the reducer that the current row goes to.
   * Partition columns are not passed to reducer.
   */
  protected ArrayList<ExprNodeDesc> partitionCols;

  protected int numReducers;

  public BaseReduceSinkDesc() {
  }

  public ArrayList<String> getOutputKeyColumnNames() {
    return outputKeyColumnNames;
  }

  public void setOutputKeyColumnNames(
      ArrayList<String> outputKeyColumnNames) {
    this.outputKeyColumnNames = outputKeyColumnNames;
  }

  public ArrayList<String> getOutputValueColumnNames() {
    return outputValueColumnNames;
  }

  public void setOutputValueColumnNames(
      ArrayList<String> outputValueColumnNames) {
    this.outputValueColumnNames = outputValueColumnNames;
  }

  @Explain(displayName = "key expressions")
  public ArrayList<ExprNodeDesc> getKeyCols() {
    return keyCols;
  }

  public void setKeyCols(final ArrayList<ExprNodeDesc> keyCols) {
    this.keyCols = keyCols;
  }

  public int getNumDistributionKeys() {
    return this.numDistributionKeys;
  }

  public void setNumDistributionKeys(int numKeys) {
    this.numDistributionKeys = numKeys;
  }

  @Explain(displayName = "value expressions")
  public ArrayList<ExprNodeDesc> getValueCols() {
    return valueCols;
  }

  public void setValueCols(final ArrayList<ExprNodeDesc> valueCols) {
    this.valueCols = valueCols;
  }

  @Explain(displayName = "Map-reduce partition columns")
  public ArrayList<ExprNodeDesc> getPartitionCols() {
    return partitionCols;
  }

  public void setPartitionCols(
      final ArrayList<ExprNodeDesc> partitionCols) {
    this.partitionCols = partitionCols;
  }

  @Explain(displayName = "tag")
  public int getTag() {
    return tag;
  }

  public void setTag(int tag) {
    this.tag = tag;
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
        org.apache.hadoop.hive.serde.Constants.SERIALIZATION_SORT_ORDER);
  }

  public void setOrder(String orderStr) {
    keySerializeInfo.getProperties().setProperty(
        org.apache.hadoop.hive.serde.Constants.SERIALIZATION_SORT_ORDER,
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
