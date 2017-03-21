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
import java.util.Arrays;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.plan.Explain.Level;
import org.apache.hadoop.hive.ql.plan.Explain.Vectorization;
import org.apache.hadoop.hive.ql.plan.VectorReduceSinkDesc.ReduceSinkKeyType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * ReduceSinkDesc.
 *
 */
@Explain(displayName = "Reduce Output Operator", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
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
   * Holds the name of the output operators
   * that this reduce sink is outputing to.
   */
  private List<String> outputOperators;

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

  // whether this RS is deduplicated
  private transient boolean isDeduplicated = false;

  // used by spark mode to decide whether global order is needed
  private transient boolean hasOrderBy = false;

  private static transient Logger LOG = LoggerFactory.getLogger(ReduceSinkDesc.class);

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
    this.setNumBuckets(-1);
    this.setBucketCols(null);
    this.vectorDesc = null;
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
    desc.setDeduplicated(isDeduplicated);
    desc.setHasOrderBy(hasOrderBy);
    if (vectorDesc != null) {
      throw new RuntimeException("Clone with vectorization desc not supported");
    }
    desc.vectorDesc = null;
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

  @Explain(displayName = "PartitionCols", explainLevels = { Level.USER })
  public String getUserLevelExplainParitionColsString() {
    return PlanUtils.getExprListString(partitionCols, true);
  }

  public java.util.ArrayList<ExprNodeDesc> getPartitionCols() {
    return partitionCols;
  }

  public void setPartitionCols(
      final java.util.ArrayList<ExprNodeDesc> partitionCols) {
    this.partitionCols = partitionCols;
  }

  public boolean isPartitioning() {
    if (partitionCols != null && !partitionCols.isEmpty()) {
      return true;
    }
    return false;
  }

  @Explain(displayName = "tag", explainLevels = { Level.EXTENDED })
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

  @Explain(displayName = "TopN", explainLevels = { Level.EXTENDED })
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

  public boolean isOrdering() {
    if (this.getOrder() != null && !this.getOrder().isEmpty()) {
      return true;
    }
    return false;
  }

  /**
   * Returns the null order in the key columns.
   *
   * @return null, which means default for all key columns, or a String
   *         of the same length as key columns, that consists of only "a"
   *         (null first) and "z" (null last).
   */
  @Explain(displayName = "null sort order", explainLevels = { Level.EXTENDED })
  public String getNullOrder() {
    return keySerializeInfo.getProperties().getProperty(
        org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_NULL_SORT_ORDER);
  }

  public void setNullOrder(String nullOrderStr) {
    keySerializeInfo.getProperties().setProperty(
        org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_NULL_SORT_ORDER,
        nullOrderStr);
  }

  public List<List<Integer>> getDistinctColumnIndices() {
    return distinctColumnIndices;
  }

  public void setDistinctColumnIndices(
      List<List<Integer>> distinctColumnIndices) {
    this.distinctColumnIndices = distinctColumnIndices;
  }

  @Explain(displayName = "outputname", explainLevels = { Level.USER })
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

  @Explain(displayName = "auto parallelism", explainLevels = { Level.EXTENDED })
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

  public boolean isDeduplicated() {
    return isDeduplicated;
  }

  public void setDeduplicated(boolean isDeduplicated) {
    this.isDeduplicated = isDeduplicated;
  }

  public boolean hasOrderBy() {
    return hasOrderBy;
  }

  public void setHasOrderBy(boolean hasOrderBy) {
    this.hasOrderBy = hasOrderBy;
  }

  // Use LinkedHashSet to give predictable display order.
  private static final Set<String> vectorizableReduceSinkNativeEngines =
      new LinkedHashSet<String>(Arrays.asList("tez", "spark"));

  public class ReduceSinkOperatorExplainVectorization extends OperatorExplainVectorization {

    private final ReduceSinkDesc reduceSinkDesc;
    private final VectorReduceSinkDesc vectorReduceSinkDesc;
    private final VectorReduceSinkInfo vectorReduceSinkInfo; 

    private VectorizationCondition[] nativeConditions;

    public ReduceSinkOperatorExplainVectorization(ReduceSinkDesc reduceSinkDesc, VectorDesc vectorDesc) {
      // VectorReduceSinkOperator is not native vectorized.
      super(vectorDesc, ((VectorReduceSinkDesc) vectorDesc).reduceSinkKeyType()!= ReduceSinkKeyType.NONE);
      this.reduceSinkDesc = reduceSinkDesc;
      vectorReduceSinkDesc = (VectorReduceSinkDesc) vectorDesc;
      vectorReduceSinkInfo = vectorReduceSinkDesc.getVectorReduceSinkInfo();
    }

    @Explain(vectorization = Vectorization.EXPRESSION, displayName = "keyExpressions", explainLevels = { Level.DEFAULT, Level.EXTENDED })
    public List<String> getKeyExpression() {
      if (!isNative) {
        return null;
      }
      return vectorExpressionsToStringList(vectorReduceSinkInfo.getReduceSinkKeyExpressions());
    }

    @Explain(vectorization = Vectorization.EXPRESSION, displayName = "valueExpressions", explainLevels = { Level.DEFAULT, Level.EXTENDED })
    public List<String> getValueExpression() {
      if (!isNative) {
        return null;
      }
      return vectorExpressionsToStringList(vectorReduceSinkInfo.getReduceSinkValueExpressions());
    }

    private VectorizationCondition[] createNativeConditions() {

      boolean enabled = vectorReduceSinkDesc.getIsVectorizationReduceSinkNativeEnabled();
 
      String engine = vectorReduceSinkDesc.getEngine();
      String engineInSupportedCondName =
          HiveConf.ConfVars.HIVE_EXECUTION_ENGINE.varname + " " + engine + " IN " + vectorizableReduceSinkNativeEngines;
      boolean engineInSupported = vectorizableReduceSinkNativeEngines.contains(engine);

      VectorizationCondition[] conditions = new VectorizationCondition[] {
          new VectorizationCondition(
              enabled,
              HiveConf.ConfVars.HIVE_VECTORIZATION_REDUCESINK_NEW_ENABLED.varname),
          new VectorizationCondition(
              engineInSupported,
              engineInSupportedCondName),
          new VectorizationCondition(
              !vectorReduceSinkDesc.getHasTopN(),
              "No TopN"),
          new VectorizationCondition(
              !vectorReduceSinkDesc.getHasDistinctColumns(),
              "No DISTINCT columns"),
          new VectorizationCondition(
              vectorReduceSinkDesc.getIsKeyBinarySortable(),
              "BinarySortableSerDe for keys"),
          new VectorizationCondition(
              vectorReduceSinkDesc.getIsValueLazyBinary(),
              "LazyBinarySerDe for values")
      };
      if (vectorReduceSinkDesc.getIsUnexpectedCondition()) {
        VectorizationCondition[] newConditions = new VectorizationCondition[conditions.length + 1];
        System.arraycopy(conditions, 0, newConditions, 0, conditions.length);
        newConditions[conditions.length] =
            new VectorizationCondition(
                false,
                "NOT UnexpectedCondition");
        conditions = newConditions;
      }
      return conditions;
    }

    @Explain(vectorization = Vectorization.OPERATOR, displayName = "nativeConditionsMet", explainLevels = { Level.DEFAULT, Level.EXTENDED })
    public List<String> getNativeConditionsMet() {
      if (nativeConditions == null) {
        nativeConditions = createNativeConditions();
      }
      return VectorizationCondition.getConditionsMet(nativeConditions);
    }

    @Explain(vectorization = Vectorization.OPERATOR, displayName = "nativeConditionsNotMet", explainLevels = { Level.DEFAULT, Level.EXTENDED })
    public List<String> getNativeConditionsNotMet() {
      if (nativeConditions == null) {
        nativeConditions = createNativeConditions();
      }
      return VectorizationCondition.getConditionsNotMet(nativeConditions);
    }
  }

  @Explain(vectorization = Vectorization.OPERATOR, displayName = "Reduce Sink Vectorization", explainLevels = { Level.DEFAULT, Level.EXTENDED })
  public ReduceSinkOperatorExplainVectorization getReduceSinkVectorization() {
    if (vectorDesc == null) {
      return null;
    }
    return new ReduceSinkOperatorExplainVectorization(this, vectorDesc);
  }

  public List<String> getOutputOperators() {
    return outputOperators;
  }

  public void setOutputOperators(List<String> outputOperators) {
    this.outputOperators = outputOperators;
  }
}
