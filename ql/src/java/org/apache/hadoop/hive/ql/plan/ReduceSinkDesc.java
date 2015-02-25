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
import java.util.EnumSet;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.io.AcidUtils;


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
   * Used in tez. Holds the name of the output
   * that this reduce sink is writing to.
   */
  private String outputName;

  /**
   * The partition columns (CLUSTER BY or DISTRIBUTE BY in Hive language).
   * Partition columns decide the reducer that the current row goes to.
   * Partition columns are not passed to reducer.
   */
  private java.util.ArrayList<ExprNodeDesc> partitionCols;

  private int numReducers;

  /**
   * Bucket information
   */
  private int numBuckets;
  private List<ExprNodeDesc> bucketCols;

  private int topN = -1;
  private float topNMemoryUsage = -1;
  private boolean mapGroupBy;  // for group-by, values with same key on top-K should be forwarded
  //flag used to control how TopN handled for PTF/Windowing partitions.
  private boolean isPTFReduceSink = false; 
  private boolean skipTag; // Skip writing tags when feeding into mapjoin hashtable

  public static enum ReducerTraits {
    UNSET(0), // unset
    FIXED(1), // distribution of keys is fixed
    AUTOPARALLEL(2), // can change reducer count (ORDER BY can concat adjacent buckets)
    UNIFORM(3); // can redistribute into buckets uniformly (GROUP BY can)

    private final int trait;

    private ReducerTraits(int trait) {
      this.trait = trait;
    }
  };

  // Is reducer auto-parallelism unset (FIXED, UNIFORM, PARALLEL)
  private EnumSet<ReducerTraits> reduceTraits = EnumSet.of(ReducerTraits.UNSET);

  // Write type, since this needs to calculate buckets differently for updates and deletes
  private AcidUtils.Operation writeType;

  // whether we'll enforce the sort order of the RS
  private transient boolean enforceSort = false;

  // used by spark mode to decide whether global order is needed
  private transient boolean hasOrderBy = false;

  private static transient Log LOG = LogFactory.getLog(ReduceSinkDesc.class);
  public ReduceSinkDesc() {
  }

  public ReduceSinkDesc(ArrayList<ExprNodeDesc> keyCols,
      int numDistributionKeys,
      ArrayList<ExprNodeDesc> valueCols,
      ArrayList<String> outputKeyColumnNames,
      List<List<Integer>> distinctColumnIndices,
      ArrayList<String> outputValueColumnNames, int tag,
      ArrayList<ExprNodeDesc> partitionCols, int numReducers,
      final TableDesc keySerializeInfo, final TableDesc valueSerializeInfo,
      AcidUtils.Operation writeType) {
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
    this.setNumBuckets(-1);
    this.setBucketCols(null);
    this.writeType = writeType;
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
    desc.setNumBuckets(numBuckets);
    desc.setBucketCols(bucketCols);
    desc.setStatistics(this.getStatistics());
    desc.setSkipTag(skipTag);
    desc.reduceTraits = reduceTraits.clone();
    desc.setEnforceSort(enforceSort);
    desc.setHasOrderBy(hasOrderBy);
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
  public String getKeyColString() {
    return PlanUtils.getExprListString(keyCols);
  }

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
  public String getValueColsString() {
    return PlanUtils.getExprListString(valueCols);
  }

  public java.util.ArrayList<ExprNodeDesc> getValueCols() {
    return valueCols;
  }

  public void setValueCols(final java.util.ArrayList<ExprNodeDesc> valueCols) {
    this.valueCols = valueCols;
  }

  @Explain(displayName = "Map-reduce partition columns")
  public String getParitionColsString() {
    return PlanUtils.getExprListString(partitionCols);
  }

  public java.util.ArrayList<ExprNodeDesc> getPartitionCols() {
    return partitionCols;
  }

  public void setPartitionCols(
      final java.util.ArrayList<ExprNodeDesc> partitionCols) {
    this.partitionCols = partitionCols;
  }

  @Explain(displayName = "tag", normalExplain = false)
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

  @Explain(displayName = "TopN", normalExplain = false)
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

  public boolean isPTFReduceSink() {
    return isPTFReduceSink;
  }

  public void setPTFReduceSink(boolean isPTFReduceSink) {
    this.isPTFReduceSink = isPTFReduceSink;
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

  public String getOutputName() {
    return outputName;
  }

  public void setOutputName(String outputName) {
    this.outputName = outputName;
  }

  public int getNumBuckets() {
    return numBuckets;
  }

  public void setNumBuckets(int numBuckets) {
    this.numBuckets = numBuckets;
  }

  public List<ExprNodeDesc> getBucketCols() {
    return bucketCols;
  }

  public void setBucketCols(List<ExprNodeDesc> bucketCols) {
    this.bucketCols = bucketCols;
  }

  public void setSkipTag(boolean value) {
    this.skipTag = value;
  }

  public boolean getSkipTag() {
    return skipTag;
  }

  @Explain(displayName = "auto parallelism", normalExplain = false)
  public final boolean isAutoParallel() {
    return (this.reduceTraits.contains(ReducerTraits.AUTOPARALLEL));
  }

  public final EnumSet<ReducerTraits> getReducerTraits() {
    return this.reduceTraits;
  }

  public final void setReducerTraits(EnumSet<ReducerTraits> traits) {
    // we don't allow turning on auto parallel once it has been
    // explicitly turned off. That is to avoid scenarios where
    // auto parallelism could break assumptions about number of
    // reducers or hash function.

    boolean wasUnset = this.reduceTraits.remove(ReducerTraits.UNSET);
    
    if (this.reduceTraits.contains(ReducerTraits.FIXED)) {
      return;
    } else if (traits.contains(ReducerTraits.FIXED)) {
      this.reduceTraits.removeAll(EnumSet.of(
          ReducerTraits.AUTOPARALLEL,
          ReducerTraits.UNIFORM));
      this.reduceTraits.addAll(traits);
    } else {
      this.reduceTraits.addAll(traits);
    }
  }

  public AcidUtils.Operation getWriteType() {
    return writeType;
  }

  public boolean isEnforceSort() {
    return enforceSort;
  }

  public void setEnforceSort(boolean isDeduplicated) {
    this.enforceSort = isDeduplicated;
  }

  public boolean hasOrderBy() {
    return hasOrderBy;
  }

  public void setHasOrderBy(boolean hasOrderBy) {
    this.hasOrderBy = hasOrderBy;
  }
}
