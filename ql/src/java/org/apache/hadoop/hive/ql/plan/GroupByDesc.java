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
import java.util.List;
import java.util.Objects;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.vector.VectorAggregationDesc;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.VectorAggregateExpression;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hive.common.util.AnnotationUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.optimizer.physical.Vectorizer;
import org.apache.hadoop.hive.ql.plan.Explain.Level;
import org.apache.hadoop.hive.ql.plan.Explain.Vectorization;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;


/**
 * GroupByDesc.
 *
 */
@Explain(displayName = "Group By Operator", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class GroupByDesc extends AbstractOperatorDesc {
  /**
   * Group-by Mode: COMPLETE: complete 1-phase aggregation: iterate, terminate
   * PARTIAL1: partial aggregation - first phase: iterate, terminatePartial
   * PARTIAL2: partial aggregation - second phase: merge, terminatePartial
   * PARTIALS: For non-distinct the same as PARTIAL2, for distinct the same as
   * PARTIAL1
   * FINAL: partial aggregation - final phase: merge, terminate
   * HASH: For non-distinct the same as PARTIAL1 but use hash-table-based aggregation
   * MERGEPARTIAL: FINAL for non-distinct aggregations, COMPLETE for distinct
   * aggregations.
   */
  private static final long serialVersionUID = 1L;

  /**
   * Mode.
   *
   */
  public static enum Mode {
    COMPLETE, PARTIAL1, PARTIAL2, PARTIALS, FINAL, HASH, MERGEPARTIAL
  };

  private Mode mode;

  // no hash aggregations for group by
  private boolean bucketGroup;

  private ArrayList<ExprNodeDesc> keys;
  private List<Long> listGroupingSets;
  private boolean groupingSetsPresent;
  private int groupingSetPosition = -1; //  /* in case of grouping sets; groupby1 will output values for every setgroup; this is the index of the column that information will be sent */
  private ArrayList<org.apache.hadoop.hive.ql.plan.AggregationDesc> aggregators;
  private ArrayList<java.lang.String> outputColumnNames;
  private float groupByMemoryUsage;
  private float memoryThreshold;
  transient private boolean isDistinct;
  private boolean dontResetAggrsDistinct;

  public GroupByDesc() {
  }

  public GroupByDesc(
      final Mode mode,
      final ArrayList<java.lang.String> outputColumnNames,
      final ArrayList<ExprNodeDesc> keys,
      final ArrayList<org.apache.hadoop.hive.ql.plan.AggregationDesc> aggregators,
      final float groupByMemoryUsage,
      final float memoryThreshold,
      final List<Long> listGroupingSets,
      final boolean groupingSetsPresent,
      final int groupingSetsPosition,
      final boolean isDistinct) {
    this(mode, outputColumnNames, keys, aggregators,
        false, groupByMemoryUsage, memoryThreshold, listGroupingSets,
        groupingSetsPresent, groupingSetsPosition, isDistinct);
  }

  public GroupByDesc(
      final Mode mode,
      final ArrayList<java.lang.String> outputColumnNames,
      final ArrayList<ExprNodeDesc> keys,
      final ArrayList<org.apache.hadoop.hive.ql.plan.AggregationDesc> aggregators,
      final boolean bucketGroup,
      final float groupByMemoryUsage,
      final float memoryThreshold,
      final List<Long> listGroupingSets,
      final boolean groupingSetsPresent,
      final int groupingSetsPosition,
      final boolean isDistinct) {
    this.mode = mode;
    this.outputColumnNames = outputColumnNames;
    this.keys = keys;
    this.aggregators = aggregators;
    this.bucketGroup = bucketGroup;
    this.groupByMemoryUsage = groupByMemoryUsage;
    this.memoryThreshold = memoryThreshold;
    this.listGroupingSets = listGroupingSets;
    this.groupingSetsPresent = groupingSetsPresent;
    this.groupingSetPosition = groupingSetsPosition;
    this.isDistinct = isDistinct;
  }

  public Mode getMode() {
    return mode;
  }

  @Explain(displayName = "mode")
  public String getModeString() {
    switch (mode) {
    case COMPLETE:
      return "complete";
    case PARTIAL1:
      return "partial1";
    case PARTIAL2:
      return "partial2";
    case PARTIALS:
      return "partials";
    case HASH:
      return "hash";
    case FINAL:
      return "final";
    case MERGEPARTIAL:
      return "mergepartial";
    }

    return "unknown";
  }

  public void setMode(final Mode mode) {
    this.mode = mode;
  }

  @Explain(displayName = "keys")
  public String getKeyString() {
    return PlanUtils.getExprListString(keys);
  }

  @Explain(displayName = "keys", explainLevels = { Level.USER })
  public String getUserLevelExplainKeyString() {
    return PlanUtils.getExprListString(keys, true);
  }

  public ArrayList<ExprNodeDesc> getKeys() {
    return keys;
  }

  public void setKeys(final ArrayList<ExprNodeDesc> keys) {
    this.keys = keys;
  }

  @Explain(displayName = "outputColumnNames")
  public ArrayList<java.lang.String> getOutputColumnNames() {
    return outputColumnNames;
  }

  @Explain(displayName = "Output", explainLevels = { Level.USER })
  public ArrayList<java.lang.String> getUserLevelExplainOutputColumnNames() {
    return outputColumnNames;
  }

  @Explain(displayName = "pruneGroupingSetId", displayOnlyOnTrue = true)
  public boolean pruneGroupingSetId() {
    return groupingSetPosition >= 0 &&
        outputColumnNames.size() != keys.size() + aggregators.size();
  }

  public void setOutputColumnNames(
      ArrayList<java.lang.String> outputColumnNames) {
    this.outputColumnNames = outputColumnNames;
  }

  public float getGroupByMemoryUsage() {
    return groupByMemoryUsage;
  }

  public void setGroupByMemoryUsage(float groupByMemoryUsage) {
    this.groupByMemoryUsage = groupByMemoryUsage;
  }

  public float getMemoryThreshold() {
    return memoryThreshold;
  }

  public void setMemoryThreshold(float memoryThreshold) {
    this.memoryThreshold = memoryThreshold;
  }

  @Explain(displayName = "aggregations", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public List<String> getAggregatorStrings() {
    List<String> res = new ArrayList<String>();
    for (AggregationDesc agg: aggregators) {
      res.add(agg.getExprString());
    }
    return res;
  }

  public ArrayList<org.apache.hadoop.hive.ql.plan.AggregationDesc> getAggregators() {
    return aggregators;
  }

  public void setAggregators(
      final ArrayList<org.apache.hadoop.hive.ql.plan.AggregationDesc> aggregators) {
    this.aggregators = aggregators;
  }

  public boolean isAggregate() {
    if (this.aggregators != null && !this.aggregators.isEmpty()) {
      return true;
    }
    return false;
  }

  @Explain(displayName = "bucketGroup", displayOnlyOnTrue = true)
  public boolean getBucketGroup() {
    return bucketGroup;
  }

  public void setBucketGroup(boolean bucketGroup) {
    this.bucketGroup = bucketGroup;
  }

  /**
   * Checks if this grouping is like distinct, which means that all non-distinct grouping
   * columns behave like they were distinct - for example min and max operators.
   */
  public boolean isDistinctLike() {
    ArrayList<AggregationDesc> aggregators = getAggregators();
    for (AggregationDesc ad : aggregators) {
      if (!ad.getDistinct()) {
        GenericUDAFEvaluator udafEval = ad.getGenericUDAFEvaluator();
        UDFType annot = AnnotationUtils.getAnnotation(udafEval.getClass(), UDFType.class);
        if (annot == null || !annot.distinctLike()) {
          return false;
        }
      }
    }
    return true;
  }

  // Consider a query like:
  // select a, b, count(distinct c) from T group by a,b with rollup;
  // Assume that hive.map.aggr is set to true and hive.groupby.skewindata is false,
  // in which case the group by would execute as a single map-reduce job.
  // For the group-by, the group by keys should be: a,b,groupingSet(for rollup), c
  // So, the starting position of grouping set need to be known
  public List<Long> getListGroupingSets() {
    return listGroupingSets;
  }

  public void setListGroupingSets(final List<Long> listGroupingSets) {
    this.listGroupingSets = listGroupingSets;
  }

  public boolean isGroupingSetsPresent() {
    return groupingSetsPresent;
  }

  public void setGroupingSetsPresent(boolean groupingSetsPresent) {
    this.groupingSetsPresent = groupingSetsPresent;
  }

  public int getGroupingSetPosition() {
    return groupingSetPosition;
  }

  public void setGroupingSetPosition(int groupingSetPosition) {
    this.groupingSetPosition = groupingSetPosition;
  }

  public boolean isDontResetAggrsDistinct() {
    return dontResetAggrsDistinct;
  }

  public void setDontResetAggrsDistinct(boolean dontResetAggrsDistinct) {
    this.dontResetAggrsDistinct = dontResetAggrsDistinct;
  }

  public boolean isDistinct() {
    return isDistinct;
  }

  public void setDistinct(boolean isDistinct) {
    this.isDistinct = isDistinct;
  }

  @Override
  public Object clone() {
    ArrayList<java.lang.String> outputColumnNames = new ArrayList<>();
    outputColumnNames.addAll(this.outputColumnNames);
    ArrayList<ExprNodeDesc> keys = new ArrayList<>();
    keys.addAll(this.keys);
    ArrayList<org.apache.hadoop.hive.ql.plan.AggregationDesc> aggregators = new ArrayList<>();
    aggregators.addAll(this.aggregators);
    List<Long> listGroupingSets = new ArrayList<>();
    listGroupingSets.addAll(this.listGroupingSets);
    return new GroupByDesc(this.mode, outputColumnNames, keys, aggregators,
        this.groupByMemoryUsage, this.memoryThreshold, listGroupingSets, this.groupingSetsPresent,
        this.groupingSetPosition, this.isDistinct);
  }

  public class GroupByOperatorExplainVectorization extends OperatorExplainVectorization {

    private final GroupByDesc groupByDesc;
    private final VectorGroupByDesc vectorGroupByDesc;

    public GroupByOperatorExplainVectorization(GroupByDesc groupByDesc,
        VectorGroupByDesc vectorGroupByDesc) {
      // Native vectorization not supported.
      super(vectorGroupByDesc, false);
      this.groupByDesc = groupByDesc;
      this.vectorGroupByDesc = vectorGroupByDesc;
    }

    @Explain(vectorization = Vectorization.EXPRESSION, displayName = "keyExpressions", explainLevels = { Level.DEFAULT, Level.EXTENDED })
    public List<String> getKeysExpression() {
      return vectorExpressionsToStringList(vectorGroupByDesc.getKeyExpressions());
    }

    @Explain(vectorization = Vectorization.EXPRESSION, displayName = "aggregators", explainLevels = { Level.DEFAULT, Level.EXTENDED })
    public List<String> getAggregators() {
      VectorAggregationDesc[] vecAggrDescs = vectorGroupByDesc.getVecAggrDescs();
      List<String> vecAggrList = new ArrayList<String>(vecAggrDescs.length);
      for (VectorAggregationDesc vecAggrDesc : vecAggrDescs) {
        vecAggrList.add(vecAggrDesc.toString());
      }
      return vecAggrList;
    }

    @Explain(vectorization = Vectorization.OPERATOR, displayName = "vectorProcessingMode", explainLevels = { Level.DEFAULT, Level.EXTENDED })
    public String getProcessingMode() {
      return vectorGroupByDesc.getProcessingMode().name();
    }

    @Explain(vectorization = Vectorization.OPERATOR, displayName = "groupByMode", explainLevels = { Level.DEFAULT, Level.EXTENDED })
    public String getGroupByMode() {
      return groupByDesc.getMode().name();
    }

    @Explain(vectorization = Vectorization.OPERATOR, displayName = "vectorOutputConditionsNotMet", explainLevels = { Level.DEFAULT, Level.EXTENDED })
    public List<String> getVectorOutputConditionsNotMet() {
      List<String> results = new ArrayList<String>();

      boolean isVectorizationComplexTypesEnabled = vectorGroupByDesc.getIsVectorizationComplexTypesEnabled();
      boolean isVectorizationGroupByComplexTypesEnabled = vectorGroupByDesc.getIsVectorizationGroupByComplexTypesEnabled();

      if (isVectorizationComplexTypesEnabled && isVectorizationGroupByComplexTypesEnabled) {
        return null;
      }

      results.add(
          getComplexTypeWithGroupByEnabledCondition(
              isVectorizationComplexTypesEnabled, isVectorizationGroupByComplexTypesEnabled));
      return results;
    }

    @Explain(vectorization = Vectorization.EXPRESSION, displayName = "projectedOutputColumnNums", explainLevels = { Level.DEFAULT, Level.EXTENDED })
    public String getProjectedOutputColumnNums() {
      return Arrays.toString(vectorGroupByDesc.getProjectedOutputColumns());
    }
  }

  @Explain(vectorization = Vectorization.OPERATOR, displayName = "Group By Vectorization", explainLevels = { Level.DEFAULT, Level.EXTENDED })
  public GroupByOperatorExplainVectorization getGroupByVectorization() {
    VectorGroupByDesc vectorGroupByDesc = (VectorGroupByDesc) getVectorDesc();
    if (vectorGroupByDesc == null) {
      return null;
    }
    return new GroupByOperatorExplainVectorization(this, vectorGroupByDesc);
  }

  public static String getComplexTypeEnabledCondition(
      boolean isVectorizationComplexTypesEnabled) {
    return
        HiveConf.ConfVars.HIVE_VECTORIZATION_COMPLEX_TYPES_ENABLED.varname +
        " IS " + isVectorizationComplexTypesEnabled;
  }

  public static String getComplexTypeWithGroupByEnabledCondition(
      boolean isVectorizationComplexTypesEnabled,
      boolean isVectorizationGroupByComplexTypesEnabled) {
    final boolean enabled = (isVectorizationComplexTypesEnabled && isVectorizationGroupByComplexTypesEnabled);
    return "(" +
        HiveConf.ConfVars.HIVE_VECTORIZATION_COMPLEX_TYPES_ENABLED.varname + " " + isVectorizationComplexTypesEnabled +
        " AND " +
        HiveConf.ConfVars.HIVE_VECTORIZATION_GROUPBY_COMPLEX_TYPES_ENABLED.varname + " " + isVectorizationGroupByComplexTypesEnabled +
        ") IS " + enabled;
  }

  @Override
  public boolean isSame(OperatorDesc other) {
    if (getClass().getName().equals(other.getClass().getName())) {
      GroupByDesc otherDesc = (GroupByDesc) other;
      return Objects.equals(getModeString(), otherDesc.getModeString()) &&
          Objects.equals(getKeyString(), otherDesc.getKeyString()) &&
          Objects.equals(getOutputColumnNames(), otherDesc.getOutputColumnNames()) &&
          pruneGroupingSetId() == otherDesc.pruneGroupingSetId() &&
          Objects.equals(getAggregatorStrings(), otherDesc.getAggregatorStrings()) &&
          getBucketGroup() == otherDesc.getBucketGroup();
    }
    return false;
  }

}
