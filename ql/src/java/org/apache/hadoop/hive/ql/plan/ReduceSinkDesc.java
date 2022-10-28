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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.optimizer.signature.Signature;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.plan.Explain.Level;
import org.apache.hadoop.hive.ql.plan.Explain.Vectorization;
import org.apache.hadoop.hive.ql.plan.VectorReduceSinkDesc.ReduceSinkKeyType;


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
  private List<ExprNodeDesc> keyCols;
  private List<java.lang.String> outputKeyColumnNames;
  private List<List<Integer>> distinctColumnIndices;
  /**
   * Value columns are passed to reducer in the "value".
   */
  private List<ExprNodeDesc> valueCols;
  private List<java.lang.String> outputValueColumnNames;
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
  private List<ExprNodeDesc> partitionCols;

  private int numReducers;

  /**
   * Bucket information
   */
  private int numBuckets;
  private List<ExprNodeDesc> bucketCols;
  private boolean isCompaction;

  private int topN = -1;
  private float topNMemoryUsage = -1;
  private boolean mapGroupBy;  // for group-by, values with same key on top-K should be forwarded
  //flag used to control how TopN handled for PTF/Windowing partitions.
  private boolean isPTFReduceSink = false;
  private boolean skipTag; // Skip writing tags when feeding into mapjoin hashtable
  private boolean forwarding; // Whether this RS can forward records directly instead of shuffling/sorting

  public static enum ReducerTraits {
    UNSET(0), // unset
    FIXED(1), // distribution of keys is fixed
    AUTOPARALLEL(2), // can change reducer count (ORDER BY can concat adjacent buckets)
    UNIFORM(3), // can redistribute into buckets uniformly (GROUP BY can)
    QUICKSTART(4); // do not wait for downstream tasks

    private final int trait;

    private ReducerTraits(int trait) {
      this.trait = trait;
    }
  };

  // Is reducer auto-parallelism unset (FIXED, UNIFORM, PARALLEL)
  private EnumSet<ReducerTraits> reduceTraits = EnumSet.of(ReducerTraits.UNSET);

  // whether this RS is deduplicated
  private transient boolean isDeduplicated = false;

  // used to decide whether global order is needed
  private transient boolean hasOrderBy = false;

  private AcidUtils.Operation writeType;

  public ReduceSinkDesc() {
  }

  public ReduceSinkDesc(List<ExprNodeDesc> keyCols,
      int numDistributionKeys,
      List<ExprNodeDesc> valueCols, List<String> outputKeyColumnNames,
      List<List<Integer>> distinctColumnIndices,
      List<String> outputValueColumnNames, int tag, List<ExprNodeDesc> partitionCols, int numReducers,
      final TableDesc keySerializeInfo, final TableDesc valueSerializeInfo,
      AcidUtils.Operation writeType) {
    setKeyCols(keyCols);
    this.numDistributionKeys = numDistributionKeys;
    setValueCols(valueCols);
    this.outputKeyColumnNames = outputKeyColumnNames;
    this.outputValueColumnNames = outputValueColumnNames;
    this.tag = tag;
    this.numReducers = numReducers;
    setPartitionCols(partitionCols);
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
    desc.setKeyCols(new ArrayList<ExprNodeDesc>(getKeyCols()));
    desc.setValueCols(new ArrayList<ExprNodeDesc>(getValueCols()));
    desc.setOutputKeyColumnNames(new ArrayList<String>(getOutputKeyColumnNames()));
    desc.setColumnExprMap(new HashMap<>(getColumnExprMap()));
    List<List<Integer>> distinctColumnIndicesClone = new ArrayList<List<Integer>>();
    for (List<Integer> distinctColumnIndex : getDistinctColumnIndices()) {
      List<Integer> tmp = new ArrayList<Integer>();
      tmp.addAll(distinctColumnIndex);
      distinctColumnIndicesClone.add(tmp);
    }
    desc.setDistinctColumnIndices(distinctColumnIndicesClone);
    desc.setOutputValueColumnNames(new ArrayList<String>(getOutputValueColumnNames()));
    desc.setNumDistributionKeys(getNumDistributionKeys());
    desc.setTag(getTag());
    desc.setNumReducers(getNumReducers());
    desc.setPartitionCols(new ArrayList<ExprNodeDesc>(getPartitionCols()));
    desc.setKeySerializeInfo((TableDesc) getKeySerializeInfo().clone());
    desc.setValueSerializeInfo((TableDesc) getValueSerializeInfo().clone());
    desc.setNumBuckets(numBuckets);
    desc.setBucketCols(bucketCols);
    desc.setStatistics(this.getStatistics());
    desc.setSkipTag(skipTag);
    desc.reduceTraits = reduceTraits.clone();
    desc.setDeduplicated(isDeduplicated);
    desc.setHasOrderBy(hasOrderBy);
    desc.outputName = outputName;
    return desc;
  }

  public List<String> getOutputKeyColumnNames() {
    return outputKeyColumnNames;
  }

  // NOTE: Debugging only.
  @Explain(displayName = "output key column names", explainLevels = { Level.DEBUG })
  public List<String> getOutputKeyColumnNamesDisplay() {
    List<String> result = new ArrayList<String>();
    for (String name : outputKeyColumnNames) {
      result.add(Utilities.ReduceField.KEY.name() + "." + name);
    }
    return result;
  }


  public void setOutputKeyColumnNames(
      java.util.ArrayList<java.lang.String> outputKeyColumnNames) {
    this.outputKeyColumnNames = outputKeyColumnNames;
  }

  public List<String> getOutputValueColumnNames() {
    return outputValueColumnNames;
  }

  // NOTE: Debugging only.
  @Explain(displayName = "output value column names", explainLevels = { Level.DEBUG })
  public List<String> getOutputValueColumnNamesDisplay() {
    List<String> result = new ArrayList<String>();
    for (String name : outputValueColumnNames) {
      result.add(Utilities.ReduceField.VALUE.name() + "." + name);
    }
    return result;
  }

  public void setOutputValueColumnNames(
      java.util.ArrayList<java.lang.String> outputValueColumnNames) {
    this.outputValueColumnNames = outputValueColumnNames;
  }

  @Explain(displayName = "key expressions")
  @Signature
  public String getKeyColString() {
    return PlanUtils.getExprListString(keyCols);
  }

  public List<ExprNodeDesc> getKeyCols() {
    return keyCols;
  }

  public void setKeyCols(List<ExprNodeDesc> keyCols) {
    assert keyCols == null || keyCols.stream().allMatch(Objects::nonNull);
    this.keyCols = keyCols;
  }

  public int getNumDistributionKeys() {
    return this.numDistributionKeys;
  }

  public void setNumDistributionKeys(int numKeys) {
    this.numDistributionKeys = numKeys;
  }

  @Explain(displayName = "value expressions")
  @Signature
  public String getValueColsString() {
    return PlanUtils.getExprListString(valueCols);
  }

  public List<ExprNodeDesc> getValueCols() {
    return valueCols;
  }

  public void setValueCols(List<ExprNodeDesc> valueCols) {
    assert valueCols == null || valueCols.stream().allMatch(Objects::nonNull);
    this.valueCols = valueCols;
  }

  @Explain(displayName = "Map-reduce partition columns")
  public String getParitionColsString() {
    return PlanUtils.getExprListString(partitionCols);
  }

  @Explain(displayName = "PartitionCols", explainLevels = { Level.USER })
  @Signature
  public String getUserLevelExplainParitionColsString() {
    return PlanUtils.getExprListString(partitionCols, true);
  }

  public List<ExprNodeDesc> getPartitionCols() {
    return partitionCols;
  }

  public void setPartitionCols(
      final List<ExprNodeDesc> partitionCols) {
    assert partitionCols == null || partitionCols.stream().allMatch(Objects::nonNull);
    this.partitionCols = partitionCols;
  }

  public boolean isPartitioning() {
    if (partitionCols != null && !partitionCols.isEmpty()) {
      return true;
    }
    return false;
  }

  @Signature
  @Explain(displayName = "tag", explainLevels = { Level.EXTENDED })
  public int getTag() {
    return tag;
  }

  public void setTag(int tag) {
    this.tag = tag;
  }

  @Signature
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
  @Signature
  @Explain(displayName = "sort order", explainLevels = { Level.DEFAULT, Level.EXTENDED })
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
  @Explain(displayName = "null sort order", explainLevels = { Level.DEFAULT, Level.EXTENDED })
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

  public boolean hasADistinctColumnIndex() {
    if (this.distinctColumnIndices == null) {
      return false;
    }
    for (List<Integer> distinctColumnIndex : this.distinctColumnIndices) {
      if (CollectionUtils.isNotEmpty(distinctColumnIndex)) {
        return true;
      }
    }
    return false;
  }

  @Explain(displayName = "outputname", explainLevels = { Level.USER })
  public String getOutputName() {
    return outputName;
  }

  public void setOutputName(String outputName) {
    this.outputName = outputName;
  }

  @Explain(displayName = "numBuckets", explainLevels = { Level.EXTENDED })
  public int getNumBuckets() {
    return numBuckets;
  }

  public void setNumBuckets(int numBuckets) {
    this.numBuckets = numBuckets;
  }

  public boolean isCompaction() {
    return isCompaction;
  }

  public void setIsCompaction(boolean isCompaction) {
    this.isCompaction = isCompaction;
  }

  @Explain(displayName = "bucketingVersion", explainLevels = { Level.EXTENDED })
  public int getBucketingVersionForExplain() {
    return getBucketingVersion();
  }

  public List<ExprNodeDesc> getBucketCols() {
    return bucketCols;
  }

  public void setBucketCols(List<ExprNodeDesc> bucketCols) {
    assert bucketCols == null || bucketCols.stream().allMatch(Objects::nonNull);
    this.bucketCols = bucketCols;
  }

  public void setSkipTag(boolean value) {
    this.skipTag = value;
  }

  public boolean getSkipTag() {
    return skipTag;
  }

  public void setForwarding(boolean forwarding) {
    this.forwarding = forwarding;
  }

  public boolean isForwarding() {
    return forwarding;
  }

  @Explain(displayName = "auto parallelism", explainLevels = { Level.EXTENDED })
  public final boolean isAutoParallel() {
    return (this.reduceTraits.contains(ReducerTraits.AUTOPARALLEL));
  }

  public final boolean isSlowStart() {
    return !(this.reduceTraits.contains(ReducerTraits.QUICKSTART));
  }

  @Explain(displayName = "quick start", displayOnlyOnTrue = true, explainLevels = {Explain.Level.EXTENDED })
  public final boolean isQuickStart() {
    return !isSlowStart();
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
      new LinkedHashSet<String>(Arrays.asList("tez"));

  public class ReduceSinkOperatorExplainVectorization extends OperatorExplainVectorization {

    private final ReduceSinkDesc reduceSinkDesc;
    private final VectorReduceSinkDesc vectorReduceSinkDesc;
    private final VectorReduceSinkInfo vectorReduceSinkInfo;

    private VectorizationCondition[] nativeConditions;

    public ReduceSinkOperatorExplainVectorization(ReduceSinkDesc reduceSinkDesc,
        VectorReduceSinkDesc vectorReduceSinkDesc) {
      // VectorReduceSinkOperator is not native vectorized.
      super(vectorReduceSinkDesc, vectorReduceSinkDesc.reduceSinkKeyType()!= ReduceSinkKeyType.NONE);
      this.reduceSinkDesc = reduceSinkDesc;
      this.vectorReduceSinkDesc = vectorReduceSinkDesc;
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

    @Explain(vectorization = Vectorization.DETAIL, displayName = "keyColumns",
        explainLevels = { Level.DEFAULT, Level.EXTENDED })
    public List<String> getKeyColumns() {
      if (!isNative) {
        return null;
      }
      int[] keyColumnMap = vectorReduceSinkInfo.getReduceSinkKeyColumnMap();
      if (keyColumnMap == null) {
        // Always show an array.
        return Collections.emptyList();
      }
      return outputColumnsAndTypesToStringList(
          vectorReduceSinkInfo.getReduceSinkKeyColumnMap(),
          vectorReduceSinkInfo.getReduceSinkKeyTypeInfos());
    }

    @Explain(vectorization = Vectorization.DETAIL, displayName = "valueColumns",
        explainLevels = { Level.DEFAULT, Level.EXTENDED })
    public List<String> getValueColumns() {
      if (!isNative) {
        return null;
      }
      int[] valueColumnMap = vectorReduceSinkInfo.getReduceSinkValueColumnMap();
      if (valueColumnMap == null) {
        // Always show an array.
        return Collections.emptyList();
      }
      return outputColumnsAndTypesToStringList(
          vectorReduceSinkInfo.getReduceSinkValueColumnMap(),
          vectorReduceSinkInfo.getReduceSinkValueTypeInfos());
    }

    @Explain(vectorization = Vectorization.DETAIL, displayName = "bucketColumns",
        explainLevels = { Level.DEFAULT, Level.EXTENDED })
    public List<String> getBucketColumns() {
      if (!isNative) {
        return null;
      }
      int[] bucketColumnMap = vectorReduceSinkInfo.getReduceSinkBucketColumnMap();
      if (bucketColumnMap == null || bucketColumnMap.length == 0) {
        // Suppress empty column map.
        return null;
      }
      return outputColumnsAndTypesToStringList(
          vectorReduceSinkInfo.getReduceSinkBucketColumnMap(),
          vectorReduceSinkInfo.getReduceSinkBucketTypeInfos());
    }

    @Explain(vectorization = Vectorization.DETAIL, displayName = "partitionColumns",
        explainLevels = { Level.DEFAULT, Level.EXTENDED })
    public List<String> getPartitionColumns() {
      if (!isNative) {
        return null;
      }
      int[] partitionColumnMap = vectorReduceSinkInfo.getReduceSinkPartitionColumnMap();
      if (partitionColumnMap == null || partitionColumnMap.length == 0) {
       // Suppress empty column map.
        return null;
      }
      return outputColumnsAndTypesToStringList(
          vectorReduceSinkInfo.getReduceSinkPartitionColumnMap(),
          vectorReduceSinkInfo.getReduceSinkPartitionTypeInfos());
    }

    private VectorizationCondition[] createNativeConditions() {

      boolean enabled = vectorReduceSinkDesc.getIsVectorizationReduceSinkNativeEnabled();

      String engine = vectorReduceSinkDesc.getEngine();
      String engineInSupportedCondName =
          HiveConf.ConfVars.HIVE_EXECUTION_ENGINE.varname + " " + engine + " IN " +
              vectorizableReduceSinkNativeEngines;
      boolean engineInSupported = vectorizableReduceSinkNativeEngines.contains(engine);

      VectorizationCondition[] conditions = new VectorizationCondition[] {
          new VectorizationCondition(
              enabled,
              HiveConf.ConfVars.HIVE_VECTORIZATION_REDUCESINK_NEW_ENABLED.varname),
          new VectorizationCondition(
              engineInSupported,
              engineInSupportedCondName),
          new VectorizationCondition(
              !vectorReduceSinkDesc.getHasPTFTopN(),
              "No PTF TopN"),
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

    @Explain(vectorization = Vectorization.OPERATOR, displayName = "nativeConditionsMet",
        explainLevels = { Level.DEFAULT, Level.EXTENDED })
    public List<String> getNativeConditionsMet() {
      if (nativeConditions == null) {
        nativeConditions = createNativeConditions();
      }
      return VectorizationCondition.getConditionsMet(nativeConditions);
    }

    @Explain(vectorization = Vectorization.OPERATOR, displayName = "nativeConditionsNotMet",
        explainLevels = { Level.DEFAULT, Level.EXTENDED })
    public List<String> getNativeConditionsNotMet() {
      if (nativeConditions == null) {
        nativeConditions = createNativeConditions();
      }
      return VectorizationCondition.getConditionsNotMet(nativeConditions);
    }
  }

  @Explain(vectorization = Vectorization.OPERATOR, displayName = "Reduce Sink Vectorization",
      explainLevels = { Level.DEFAULT, Level.EXTENDED })
  public ReduceSinkOperatorExplainVectorization getReduceSinkVectorization() {
    VectorReduceSinkDesc vectorReduceSinkDesc = (VectorReduceSinkDesc) getVectorDesc();
    if (vectorReduceSinkDesc == null) {
      return null;
    }
    return new ReduceSinkOperatorExplainVectorization(this, vectorReduceSinkDesc);
  }

  @Override
  public boolean isSame(OperatorDesc other) {
    if (getClass().getName().equals(other.getClass().getName())) {
      ReduceSinkDesc otherDesc = (ReduceSinkDesc) other;
      return ExprNodeDescUtils.isSame(getKeyCols(), otherDesc.getKeyCols()) &&
          ExprNodeDescUtils.isSame(getValueCols(), otherDesc.getValueCols()) &&
          ExprNodeDescUtils.isSame(getPartitionCols(), otherDesc.getPartitionCols()) &&
          getTag() == otherDesc.getTag() &&
          Objects.equals(getOrder(), otherDesc.getOrder()) &&
          getTopN() == otherDesc.getTopN() &&
          isAutoParallel() == otherDesc.isAutoParallel();
    }
    return false;
  }

  public AcidUtils.Operation getWriteType() {
    return writeType;
  }
}
