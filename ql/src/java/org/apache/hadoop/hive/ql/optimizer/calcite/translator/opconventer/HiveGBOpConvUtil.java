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

package org.apache.hadoop.hive.ql.optimizer.calcite.translator.opconventer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Aggregate.Group;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveGroupingID;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.ExprNodeConverter;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.opconventer.HiveOpConverter.OpAttr;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer.GenericUDAFInfo;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.Mode;
import org.apache.hadoop.hive.ql.util.NullOrdering;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import com.google.common.collect.ImmutableList;

/**
 * TODO:<br>
 * 1. Change the output col/ExprNodeColumn names to external names.<br>
 * 2. Verify if we need to use the "KEY."/"VALUE." in RS cols; switch to
 * external names if possible.<br>
 * 3. In ExprNode &amp; in ColumnInfo the tableAlias/VirtualColumn is specified
 * differently for different GB/RS in pipeline. Remove the different treatments.
 * 4. VirtualColMap needs to be maintained
 *
 */
final class HiveGBOpConvUtil {

  private HiveGBOpConvUtil() {
    throw new UnsupportedOperationException("HiveGBOpConvUtil should not be instantiated!");
  }

  private static enum HIVEGBPHYSICALMODE {
    MAP_SIDE_GB_NO_SKEW_NO_ADD_MR_JOB,
    MAP_SIDE_GB_NO_SKEW_ADD_MR_JOB,
    MAP_SIDE_GB_SKEW_GBKEYS_OR_DIST_UDAF_PRESENT,
    MAP_SIDE_GB_SKEW_GBKEYS_AND_DIST_UDAF_NOT_PRESENT,
    NO_MAP_SIDE_GB_NO_SKEW, NO_MAP_SIDE_GB_SKEW
  };

  private static class UDAFAttrs {
    private boolean                 isDistinctUDAF;
    private String                  udafName;
    private GenericUDAFEvaluator    udafEvaluator;
    private final ArrayList<ExprNodeDesc> udafParams                      = new ArrayList<ExprNodeDesc>();
    private List<Integer>           udafParamsIndxInGBInfoDistExprs = new ArrayList<Integer>();
    // We store the position of the argument for the function in the input.
  };

  private static class GBInfo {
    private final List<String>        outputColNames       = new ArrayList<String>();

    private final List<String>        gbKeyColNamesInInput = new ArrayList<String>();
    private final List<TypeInfo>      gbKeyTypes           = new ArrayList<TypeInfo>();
    private final List<ExprNodeDesc>  gbKeys               = new ArrayList<ExprNodeDesc>();

    private final List<Long>       grpSets              = new ArrayList<Long>();
    private boolean                   grpSetRqrAdditionalMRJob;
    private boolean                   grpIdFunctionNeeded;

    private final List<String>        distExprNames        = new ArrayList<String>();
    private final List<TypeInfo>      distExprTypes        = new ArrayList<TypeInfo>();
    private final List<ExprNodeDesc>  distExprNodes        = new ArrayList<ExprNodeDesc>();
    private final List<List<Integer>> distColIndices       = new ArrayList<List<Integer>>();

    private final List<ExprNodeDesc>  deDupedNonDistIrefs  = new ArrayList<ExprNodeDesc>();

    private final List<UDAFAttrs>     udafAttrs            = new ArrayList<UDAFAttrs>();
    private boolean                   containsDistinctAggr = false;

    float                             groupByMemoryUsage;
    float                             memoryThreshold;
    float                             minReductionHashAggr;
    float                             minReductionHashAggrLowerBound;

    private HIVEGBPHYSICALMODE        gbPhysicalPipelineMode;

    private NullOrdering defaultNullOrder = NullOrdering.NULLS_LAST;
  };

  private static HIVEGBPHYSICALMODE getAggOPMode(HiveConf hc, GBInfo gbInfo) {
    HIVEGBPHYSICALMODE gbPhysicalPipelineMode = HIVEGBPHYSICALMODE.MAP_SIDE_GB_NO_SKEW_NO_ADD_MR_JOB;

    if (hc.getBoolVar(HiveConf.ConfVars.HIVE_MAPSIDE_AGGREGATE)) {
      if (!hc.getBoolVar(HiveConf.ConfVars.HIVE_GROUPBY_SKEW)) {
        if (!gbInfo.grpSetRqrAdditionalMRJob) {
          gbPhysicalPipelineMode = HIVEGBPHYSICALMODE.MAP_SIDE_GB_NO_SKEW_NO_ADD_MR_JOB;
        } else {
          gbPhysicalPipelineMode = HIVEGBPHYSICALMODE.MAP_SIDE_GB_NO_SKEW_ADD_MR_JOB;
        }
      } else {
        if (gbInfo.containsDistinctAggr || !gbInfo.gbKeys.isEmpty()) {
          gbPhysicalPipelineMode = HIVEGBPHYSICALMODE.MAP_SIDE_GB_SKEW_GBKEYS_OR_DIST_UDAF_PRESENT;
        } else {
          gbPhysicalPipelineMode = HIVEGBPHYSICALMODE.MAP_SIDE_GB_SKEW_GBKEYS_AND_DIST_UDAF_NOT_PRESENT;
        }
      }
    } else {
      if (!hc.getBoolVar(HiveConf.ConfVars.HIVE_GROUPBY_SKEW)) {
        gbPhysicalPipelineMode = HIVEGBPHYSICALMODE.NO_MAP_SIDE_GB_NO_SKEW;
      } else {
        gbPhysicalPipelineMode = HIVEGBPHYSICALMODE.NO_MAP_SIDE_GB_SKEW;
      }
    }

    return gbPhysicalPipelineMode;
  }

  // For each of the GB op in the logical GB this should be called separately;
  // otherwise GBevaluator and expr nodes may get shared among multiple GB ops
  private static GBInfo getGBInfo(HiveAggregate aggRel, OpAttr inputOpAf, HiveConf hc) throws SemanticException {
    GBInfo gbInfo = new GBInfo();

    // 0. Collect AggRel output col Names
    gbInfo.outputColNames.addAll(aggRel.getRowType().getFieldNames());

    // 1. Collect GB Keys
    RelNode aggInputRel = aggRel.getInput();
    ExprNodeConverter exprConv = new ExprNodeConverter(inputOpAf.tabAlias,
        aggInputRel.getRowType(), new HashSet<Integer>(), aggRel.getCluster().getTypeFactory(),
        true);

    ExprNodeDesc tmpExprNodeDesc;
    for (int i : aggRel.getGroupSet()) {
      RexInputRef iRef = new RexInputRef(i, aggInputRel.getRowType().getFieldList()
          .get(i).getType());
      tmpExprNodeDesc = iRef.accept(exprConv);
      gbInfo.gbKeys.add(tmpExprNodeDesc);
      gbInfo.gbKeyColNamesInInput.add(aggInputRel.getRowType().getFieldNames().get(i));
      gbInfo.gbKeyTypes.add(tmpExprNodeDesc.getTypeInfo());
    }

    // 2. Collect Grouping Set info
    if (aggRel.getGroupType() != Group.SIMPLE) {
      // 2.1 Translate Grouping set col bitset
      ImmutableList<ImmutableBitSet> lstGrpSet = aggRel.getGroupSets();
      long bitmap = 0;
      for (ImmutableBitSet grpSet : lstGrpSet) {
        bitmap = 0;
        for (Integer bitIdx : grpSet.asList()) {
          bitmap = SemanticAnalyzer.setBit(bitmap, bitIdx);
        }
        gbInfo.grpSets.add(bitmap);
      }
      Collections.sort(gbInfo.grpSets);

      // 2.2 Check if GRpSet require additional MR Job
      gbInfo.grpSetRqrAdditionalMRJob = gbInfo.grpSets.size() > hc
          .getIntVar(HiveConf.ConfVars.HIVE_NEW_JOB_GROUPING_SET_CARDINALITY);

      // 2.3 Check if GROUPING_ID needs to be projected out
      if (!aggRel.getAggCallList().isEmpty()
          && (aggRel.getAggCallList().get(aggRel.getAggCallList().size() - 1).getAggregation() == HiveGroupingID.INSTANCE)) {
        gbInfo.grpIdFunctionNeeded = true;
      }
    }

    // 3. Walk through UDAF & Collect Distinct Info
    Set<Integer> distinctRefs = new HashSet<Integer>();
    Map<Integer, Integer> distParamInRefsToOutputPos = new HashMap<Integer, Integer>();
    for (AggregateCall aggCall : aggRel.getAggCallList()) {
      if ((aggCall.getAggregation() == HiveGroupingID.INSTANCE) || !aggCall.isDistinct()) {
        continue;
      }

      List<Integer> argLst = new ArrayList<Integer>(aggCall.getArgList());
      List<String> argNames = HiveCalciteUtil.getFieldNames(argLst, aggInputRel);
      ExprNodeDesc distinctExpr;
      for (int i = 0; i < argLst.size(); i++) {
        if (!distinctRefs.contains(argLst.get(i))) {
          distinctRefs.add(argLst.get(i));
          distinctExpr = HiveCalciteUtil.getExprNode(argLst.get(i), aggInputRel, exprConv);
          // Only distinct nodes that are NOT part of the key should be added to distExprNodes
          if (ExprNodeDescUtils.indexOf(distinctExpr, gbInfo.gbKeys) < 0) {
            distParamInRefsToOutputPos.put(argLst.get(i), gbInfo.distExprNodes.size());
            gbInfo.distExprNodes.add(distinctExpr);
            gbInfo.distExprNames.add(argNames.get(i));
            gbInfo.distExprTypes.add(distinctExpr.getTypeInfo());
          }
        }
      }
    }

    // 4. Walk through UDAF & Collect UDAF Info
    Set<Integer> deDupedNonDistIrefsSet = new HashSet<Integer>();
    for (AggregateCall aggCall : aggRel.getAggCallList()) {
      if (aggCall.getAggregation() == HiveGroupingID.INSTANCE) {
        continue;
      }

      UDAFAttrs udafAttrs = new UDAFAttrs();
      List<ExprNodeDesc> argExps = HiveCalciteUtil.getExprNodes(aggCall.getArgList(), aggInputRel,
          inputOpAf.tabAlias);
      udafAttrs.udafParams.addAll(argExps);
      udafAttrs.udafName = aggCall.getAggregation().getName();
      udafAttrs.isDistinctUDAF = aggCall.isDistinct();
      List<Integer> argLst = new ArrayList<Integer>(aggCall.getArgList());
      List<Integer> distColIndicesOfUDAF = new ArrayList<Integer>();
      List<Integer> distUDAFParamsIndxInDistExprs = new ArrayList<Integer>();
      for (int i = 0; i < argLst.size(); i++) {
        // NOTE: distinct expr can be part of of GB key
        if (udafAttrs.isDistinctUDAF) {
          ExprNodeDesc argExpr = argExps.get(i);
          int found = ExprNodeDescUtils.indexOf(argExpr, gbInfo.gbKeys);
          distColIndicesOfUDAF.add(found < 0 ? distParamInRefsToOutputPos.get(argLst.get(i)) + gbInfo.gbKeys.size() +
              (gbInfo.grpSets.size() > 0 ? 1 : 0) : found);
          distUDAFParamsIndxInDistExprs.add(distParamInRefsToOutputPos.get(argLst.get(i)));
        } else {
          // TODO: this seems wrong (following what Hive Regular does)
          if (!distParamInRefsToOutputPos.containsKey(argLst.get(i))
              && !deDupedNonDistIrefsSet.contains(argLst.get(i))) {
            deDupedNonDistIrefsSet.add(argLst.get(i));
            gbInfo.deDupedNonDistIrefs.add(udafAttrs.udafParams.get(i));
          }
        }
      }

      if (udafAttrs.isDistinctUDAF) {
        gbInfo.containsDistinctAggr = true;

        udafAttrs.udafParamsIndxInGBInfoDistExprs = distUDAFParamsIndxInDistExprs;
        gbInfo.distColIndices.add(distColIndicesOfUDAF);
      }

      // special handling for count, similar to PlanModifierForASTConv::replaceEmptyGroupAggr()
      udafAttrs.udafEvaluator = SemanticAnalyzer.getGenericUDAFEvaluator(udafAttrs.udafName,
          new ArrayList<ExprNodeDesc>(udafAttrs.udafParams), new ASTNode(),
          udafAttrs.isDistinctUDAF, udafAttrs.udafParams.size() == 0 &&
          "count".equalsIgnoreCase(udafAttrs.udafName) ? true : false);
      gbInfo.udafAttrs.add(udafAttrs);
    }

    // 4. Gather GB Memory threshold
    gbInfo.groupByMemoryUsage = HiveConf.getFloatVar(hc, HiveConf.ConfVars.HIVE_MAP_AGGR_HASH_MEMORY);
    gbInfo.memoryThreshold = HiveConf.getFloatVar(hc, HiveConf.ConfVars.HIVE_MAP_AGGR_MEMORY_THRESHOLD);
    gbInfo.minReductionHashAggr = HiveConf.getFloatVar(hc, HiveConf.ConfVars.HIVE_MAP_AGGR_HASH_MIN_REDUCTION);
    gbInfo.minReductionHashAggrLowerBound =
            HiveConf.getFloatVar(hc, HiveConf.ConfVars.HIVE_MAP_AGGR_HASH_MIN_REDUCTION_LOWER_BOUND);

    // 5. Gather GB Physical pipeline (based on user config & Grping Sets size)
    gbInfo.gbPhysicalPipelineMode = getAggOPMode(hc, gbInfo);
    gbInfo.defaultNullOrder = NullOrdering.defaultNullOrder(hc);

    return gbInfo;
  }

  static OpAttr translateGB(OpAttr inputOpAf, HiveAggregate aggRel, HiveConf hc)
      throws SemanticException {
    OpAttr translatedGBOpAttr = null;
    GBInfo gbInfo = getGBInfo(aggRel, inputOpAf, hc);

    switch (gbInfo.gbPhysicalPipelineMode) {
    case MAP_SIDE_GB_NO_SKEW_NO_ADD_MR_JOB:
      translatedGBOpAttr = genMapSideGBNoSkewNoAddMRJob(inputOpAf, aggRel, gbInfo);
      break;
    case MAP_SIDE_GB_NO_SKEW_ADD_MR_JOB:
      translatedGBOpAttr = genMapSideGBNoSkewAddMRJob(inputOpAf, aggRel, gbInfo);
      break;
    case MAP_SIDE_GB_SKEW_GBKEYS_OR_DIST_UDAF_PRESENT:
      translatedGBOpAttr = genMapSideGBSkewGBKeysOrDistUDAFPresent(inputOpAf, aggRel, gbInfo);
      break;
    case MAP_SIDE_GB_SKEW_GBKEYS_AND_DIST_UDAF_NOT_PRESENT:
      translatedGBOpAttr = genMapSideGBSkewGBKeysAndDistUDAFNotPresent(inputOpAf, aggRel, gbInfo);
      break;
    case NO_MAP_SIDE_GB_NO_SKEW:
      translatedGBOpAttr = genNoMapSideGBNoSkew(inputOpAf, aggRel, gbInfo);
      break;
    case NO_MAP_SIDE_GB_SKEW:
      translatedGBOpAttr = genNoMapSideGBSkew(inputOpAf, aggRel, gbInfo);
      break;
    }

    return translatedGBOpAttr;
  }

  /**
   * GB-RS-GB1
   *
   * Construct GB-RS-GB Pipe line. User has enabled Map Side GB, specified no
   * skew and Grp Set is below the threshold.
   *
   * @param inputOpAf
   * @param aggRel
   * @param gbInfo
   * @return
   * @throws SemanticException
   */
  private static OpAttr genMapSideGBNoSkewNoAddMRJob(OpAttr inputOpAf, HiveAggregate aggRel,
      GBInfo gbInfo) throws SemanticException {
    OpAttr mapSideGB = null;
    OpAttr mapSideRS = null;
    OpAttr reduceSideGB = null;

    // 1. Insert MapSide GB
    mapSideGB = genMapSideGB(inputOpAf, gbInfo);

    // 2. Insert MapSide RS
    mapSideRS = genMapSideGBRS(mapSideGB, gbInfo);

    // 3. Insert ReduceSide GB
    reduceSideGB = genReduceSideGB1(mapSideRS, gbInfo, false, false, GroupByDesc.Mode.MERGEPARTIAL);

    return reduceSideGB;
  }

  /**
   * GB-RS-GB1-RS-GB2
   */
  private static OpAttr genGBRSGBRSGBOpPipeLine(OpAttr inputOpAf, HiveAggregate aggRel,
      GBInfo gbInfo) throws SemanticException {
    OpAttr mapSideGB = null;
    OpAttr mapSideRS = null;
    OpAttr reduceSideGB1 = null;
    OpAttr reduceSideRS = null;
    OpAttr reduceSideGB2 = null;

    // 1. Insert MapSide GB
    mapSideGB = genMapSideGB(inputOpAf, gbInfo);

    // 2. Insert MapSide RS
    mapSideRS = genMapSideGBRS(mapSideGB, gbInfo);

    // 3. Insert ReduceSide GB1
    boolean computeGrpSet = (gbInfo.gbPhysicalPipelineMode == HIVEGBPHYSICALMODE.MAP_SIDE_GB_SKEW_GBKEYS_OR_DIST_UDAF_PRESENT) ? false : true;
    reduceSideGB1 = genReduceSideGB1(mapSideRS, gbInfo, computeGrpSet, false, GroupByDesc.Mode.PARTIALS);

    // 4. Insert RS on reduce side with Reduce side GB as input
    reduceSideRS = genReduceGBRS(reduceSideGB1, gbInfo);

    // 5. Insert ReduceSide GB2
    reduceSideGB2 = genReduceSideGB2(reduceSideRS, gbInfo);

    return reduceSideGB2;
  }

  /**
   * GB-RS-GB1-RS-GB2
   *
   * @param inputOpAf
   * @param aggRel
   * @param gbInfo
   * @return
   * @throws SemanticException
   */
  private static OpAttr genMapSideGBNoSkewAddMRJob(OpAttr inputOpAf, HiveAggregate aggRel,
      GBInfo gbInfo) throws SemanticException {
    // 1. Sanity check
    if (gbInfo.containsDistinctAggr) {
      String errorMsg = "The number of rows per input row due to grouping sets is "
          + gbInfo.grpSets.size();
      throw new SemanticException(
          ErrorMsg.HIVE_GROUPING_SETS_THRESHOLD_NOT_ALLOWED_WITH_DISTINCTS.getMsg(errorMsg));
    }

    // 2. Gen GB-RS-GB-RS-GB pipeline
    return genGBRSGBRSGBOpPipeLine(inputOpAf, aggRel, gbInfo);
  }

  /**
   * GB-RS-GB1-RS-GB2
   *
   * @param inputOpAf
   * @param aggRel
   * @param gbInfo
   * @return
   * @throws SemanticException
   */
  private static OpAttr genMapSideGBSkewGBKeysOrDistUDAFPresent(OpAttr inputOpAf,
      HiveAggregate aggRel, GBInfo gbInfo) throws SemanticException {
    // 1. Sanity check
    if (gbInfo.grpSetRqrAdditionalMRJob) {
      String errorMsg = "The number of rows per input row due to grouping sets is "
          + gbInfo.grpSets.size();
      throw new SemanticException(
          ErrorMsg.HIVE_GROUPING_SETS_THRESHOLD_NOT_ALLOWED_WITH_SKEW.getMsg(errorMsg));
    }

    // 2. Gen GB-RS-GB-RS-GB pipeline
    return genGBRSGBRSGBOpPipeLine(inputOpAf, aggRel, gbInfo);
  }

  /**
   * GB-RS-GB2
   *
   * @param inputOpAf
   * @param aggRel
   * @param gbInfo
   * @return
   * @throws SemanticException
   */
  private static OpAttr genMapSideGBSkewGBKeysAndDistUDAFNotPresent(OpAttr inputOpAf,
      HiveAggregate aggRel, GBInfo gbInfo) throws SemanticException {
    OpAttr mapSideGB = null;
    OpAttr mapSideRS = null;
    OpAttr reduceSideGB2 = null;

    // 1. Sanity check
    if (gbInfo.grpSetRqrAdditionalMRJob) {
      String errorMsg = "The number of rows per input row due to grouping sets is "
          + gbInfo.grpSets.size();
      throw new SemanticException(
          ErrorMsg.HIVE_GROUPING_SETS_THRESHOLD_NOT_ALLOWED_WITH_SKEW.getMsg(errorMsg));
    }

    // 1. Insert MapSide GB
    mapSideGB = genMapSideGB(inputOpAf, gbInfo);

    // 2. Insert MapSide RS
    mapSideRS = genMapSideGBRS(mapSideGB, gbInfo);

    // 3. Insert ReduceSide GB2
    reduceSideGB2 = genReduceSideGB2(mapSideRS, gbInfo);

    return reduceSideGB2;
  }

  /**
   * RS-Gb1
   *
   * @param inputOpAf
   * @param aggRel
   * @param gbInfo
   * @return
   * @throws SemanticException
   */
  private static OpAttr genNoMapSideGBNoSkew(OpAttr inputOpAf, HiveAggregate aggRel, GBInfo gbInfo)
      throws SemanticException {
    OpAttr mapSideRS = null;
    OpAttr reduceSideGB1NoMapGB = null;

    // 1. Insert MapSide RS
    mapSideRS = genMapSideRS(inputOpAf, gbInfo);

    // 2. Insert ReduceSide GB
    reduceSideGB1NoMapGB = genReduceSideGB1NoMapGB(mapSideRS, gbInfo, GroupByDesc.Mode.COMPLETE);

    return reduceSideGB1NoMapGB;
  }

  /**
   * RS-GB1-RS-GB2
   *
   * @param inputOpAf
   * @param aggRel
   * @param gbInfo
   * @return
   * @throws SemanticException
   */
  private static OpAttr genNoMapSideGBSkew(OpAttr inputOpAf, HiveAggregate aggRel, GBInfo gbInfo)
      throws SemanticException {
    OpAttr mapSideRS = null;
    OpAttr reduceSideGB1NoMapGB = null;
    OpAttr reduceSideRS = null;
    OpAttr reduceSideGB2 = null;

    // 1. Insert MapSide RS
    mapSideRS = genMapSideRS(inputOpAf, gbInfo);

    // 2. Insert ReduceSide GB
    reduceSideGB1NoMapGB = genReduceSideGB1NoMapGB(mapSideRS, gbInfo, GroupByDesc.Mode.PARTIAL1);

    // 3. Insert RS on reduce side with Reduce side GB as input
    reduceSideRS = genReduceGBRS(reduceSideGB1NoMapGB, gbInfo);

    // 4. Insert ReduceSide GB2
    reduceSideGB2 = genReduceSideGB2(reduceSideRS, gbInfo);

    return reduceSideGB2;
  }

  private static int getParallelismForReduceSideRS(GBInfo gbInfo) {
    int degreeOfParallelism = 0;

    switch (gbInfo.gbPhysicalPipelineMode) {
    case MAP_SIDE_GB_NO_SKEW_ADD_MR_JOB:
    case MAP_SIDE_GB_SKEW_GBKEYS_OR_DIST_UDAF_PRESENT:
    case NO_MAP_SIDE_GB_SKEW:
      if (gbInfo.gbKeys.isEmpty()) {
        degreeOfParallelism = 1;
      } else {
        degreeOfParallelism = -1;
      }
      break;
    default:
      throw new RuntimeException(
          "Unable to determine Reducer Parallelism - Invalid Physical Mode: "
              + gbInfo.gbPhysicalPipelineMode);
    }

    return degreeOfParallelism;
  }

  private static int getParallelismForMapSideRS(GBInfo gbInfo) {
    int degreeOfParallelism = 0;

    switch (gbInfo.gbPhysicalPipelineMode) {
    case MAP_SIDE_GB_NO_SKEW_NO_ADD_MR_JOB:
    case MAP_SIDE_GB_NO_SKEW_ADD_MR_JOB:
    case NO_MAP_SIDE_GB_NO_SKEW:
      if (gbInfo.gbKeys.isEmpty()) {
        degreeOfParallelism = 1;
      } else {
        degreeOfParallelism = -1;
      }
      break;
    case NO_MAP_SIDE_GB_SKEW:
    case MAP_SIDE_GB_SKEW_GBKEYS_OR_DIST_UDAF_PRESENT:
      degreeOfParallelism = -1;
      break;
    case MAP_SIDE_GB_SKEW_GBKEYS_AND_DIST_UDAF_NOT_PRESENT:
      degreeOfParallelism = 1;
      break;
    default:
      throw new RuntimeException(
          "Unable to determine Reducer Parallelism - Invalid Physical Mode: "
              + gbInfo.gbPhysicalPipelineMode);
    }

    return degreeOfParallelism;
  }

  private static int getNumPartFieldsForReduceSideRS(GBInfo gbInfo) {
    int numPartFields = 0;

    switch (gbInfo.gbPhysicalPipelineMode) {
    case MAP_SIDE_GB_NO_SKEW_ADD_MR_JOB:
      numPartFields = gbInfo.gbKeys.size() + 1;
      break;
    case MAP_SIDE_GB_SKEW_GBKEYS_OR_DIST_UDAF_PRESENT:
    case NO_MAP_SIDE_GB_SKEW:
      numPartFields = gbInfo.gbKeys.size();
      break;
    default:
      throw new RuntimeException(
          "Unable to determine Number of Partition Fields - Invalid Physical Mode: "
              + gbInfo.gbPhysicalPipelineMode);
    }

    return numPartFields;
  }

  private static int getNumPartFieldsForMapSideRS(GBInfo gbInfo) {
    int numPartFields = 0;

    switch (gbInfo.gbPhysicalPipelineMode) {
    case MAP_SIDE_GB_NO_SKEW_NO_ADD_MR_JOB:
    case MAP_SIDE_GB_NO_SKEW_ADD_MR_JOB:
    case MAP_SIDE_GB_SKEW_GBKEYS_AND_DIST_UDAF_NOT_PRESENT:
    case NO_MAP_SIDE_GB_NO_SKEW:
      numPartFields += gbInfo.gbKeys.size();
      break;
    case NO_MAP_SIDE_GB_SKEW:
    case MAP_SIDE_GB_SKEW_GBKEYS_OR_DIST_UDAF_PRESENT:
      if (gbInfo.containsDistinctAggr) {
        numPartFields = Integer.MAX_VALUE;
      } else {
        numPartFields = -1;
      }
      break;
    default:
      throw new RuntimeException(
          "Unable to determine Number of Partition Fields - Invalid Physical Mode: "
              + gbInfo.gbPhysicalPipelineMode);
    }

    return numPartFields;
  }

  private static boolean inclGrpSetInReduceSide(GBInfo gbInfo) {
    boolean inclGrpSet = false;

    if (gbInfo.grpSets.size() > 0
        && (gbInfo.gbPhysicalPipelineMode == HIVEGBPHYSICALMODE.MAP_SIDE_GB_NO_SKEW_ADD_MR_JOB || gbInfo.gbPhysicalPipelineMode == HIVEGBPHYSICALMODE.MAP_SIDE_GB_SKEW_GBKEYS_OR_DIST_UDAF_PRESENT)) {
      inclGrpSet = true;
    }

    return inclGrpSet;
  }

  private static boolean inclGrpSetInMapSide(GBInfo gbInfo) {
    boolean inclGrpSet = false;

    if (gbInfo.grpSets.size() > 0
        && ((gbInfo.gbPhysicalPipelineMode == HIVEGBPHYSICALMODE.MAP_SIDE_GB_NO_SKEW_NO_ADD_MR_JOB) ||
            gbInfo.gbPhysicalPipelineMode == HIVEGBPHYSICALMODE.MAP_SIDE_GB_SKEW_GBKEYS_OR_DIST_UDAF_PRESENT)) {
      inclGrpSet = true;
    }

    return inclGrpSet;
  }

  private static OpAttr genReduceGBRS(OpAttr inputOpAf, GBInfo gbInfo) throws SemanticException {
    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
    ArrayList<String> outputColumnNames = new ArrayList<String>();
    ArrayList<ColumnInfo> colInfoLst = new ArrayList<ColumnInfo>();
    GroupByOperator reduceSideGB1 = (GroupByOperator) inputOpAf.inputs.get(0);
    List<ColumnInfo> gb1ColInfoLst = reduceSideGB1.getSchema().getSignature();

    ArrayList<ExprNodeDesc> reduceKeys = getReduceKeysForRS(reduceSideGB1, 0,
        gbInfo.gbKeys.size() - 1, outputColumnNames, false, colInfoLst, colExprMap, true, true);
    if (inclGrpSetInReduceSide(gbInfo)) {
      addGrpSetCol(false, gb1ColInfoLst.get(reduceKeys.size()).getInternalName(), true, reduceKeys,
          outputColumnNames, colInfoLst, colExprMap);
    }

    ArrayList<ExprNodeDesc> reduceValues = getValueKeysForRS(reduceSideGB1, reduceSideGB1.getConf()
        .getKeys().size(), outputColumnNames, colInfoLst, colExprMap, true, true);

    ReduceSinkOperator rsOp = (ReduceSinkOperator) OperatorFactory.getAndMakeChild(PlanUtils
        .getReduceSinkDesc(reduceKeys, reduceValues, outputColumnNames, true, -1,
            getNumPartFieldsForReduceSideRS(gbInfo), getParallelismForReduceSideRS(gbInfo),
            AcidUtils.Operation.NOT_ACID, gbInfo.defaultNullOrder), new RowSchema(colInfoLst), reduceSideGB1);

    rsOp.setColumnExprMap(colExprMap);

    return new OpAttr("", new HashSet<Integer>(), rsOp);
  }

  private static OpAttr genMapSideGBRS(OpAttr inputOpAf, GBInfo gbInfo) throws SemanticException {
    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
    List<String> outputKeyColumnNames = new ArrayList<String>();
    List<String> outputValueColumnNames = new ArrayList<String>();
    ArrayList<ColumnInfo> colInfoLst = new ArrayList<ColumnInfo>();
    GroupByOperator mapGB = (GroupByOperator) inputOpAf.inputs.get(0);

    ArrayList<ExprNodeDesc> reduceKeys = getReduceKeysForRS(mapGB, 0, gbInfo.gbKeys.size() - 1,
        outputKeyColumnNames, false, colInfoLst, colExprMap, false, false);
    int keyLength = reduceKeys.size();

    if (inclGrpSetInMapSide(gbInfo)) {
      addGrpSetCol(false, SemanticAnalyzer.getColumnInternalName(reduceKeys.size()), true,
          reduceKeys, outputKeyColumnNames, colInfoLst, colExprMap);
      keyLength++;
    }
    if (mapGB.getConf().getKeys().size() > reduceKeys.size()) {
      // NOTE: All dist cols have single output col name;
      reduceKeys.addAll(getReduceKeysForRS(mapGB, reduceKeys.size(), mapGB.getConf().getKeys()
          .size() - 1, outputKeyColumnNames, true, colInfoLst, colExprMap, false, false));
    } else if (!gbInfo.distColIndices.isEmpty()) {
      // This is the case where distinct cols are part of GB Keys in which case
      // we still need to add it to out put col names
      outputKeyColumnNames.add(SemanticAnalyzer.getColumnInternalName(reduceKeys.size()));
    }

    ArrayList<ExprNodeDesc> reduceValues = getValueKeysForRS(mapGB, mapGB.getConf().getKeys()
        .size(), outputValueColumnNames, colInfoLst, colExprMap, false, false);

    ReduceSinkOperator rsOp = (ReduceSinkOperator) OperatorFactory.getAndMakeChild(PlanUtils
        .getReduceSinkDesc(reduceKeys, keyLength, reduceValues, gbInfo.distColIndices,
        outputKeyColumnNames, outputValueColumnNames, true, -1, getNumPartFieldsForMapSideRS(
        gbInfo), getParallelismForMapSideRS(gbInfo), AcidUtils.Operation.NOT_ACID, gbInfo.defaultNullOrder),
        new RowSchema(colInfoLst), mapGB);

    rsOp.setColumnExprMap(colExprMap);

    return new OpAttr("", new HashSet<Integer>(), rsOp);
  }

  private static OpAttr genMapSideRS(OpAttr inputOpAf, GBInfo gbInfo) throws SemanticException {
    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
    List<String> outputKeyColumnNames = new ArrayList<String>();
    List<String> outputValueColumnNames = new ArrayList<String>();
    ArrayList<ColumnInfo> colInfoLst = new ArrayList<ColumnInfo>();
    String outputColName;

    // 1. Add GB Keys to reduce keys
    ArrayList<ExprNodeDesc> reduceKeys= new ArrayList<ExprNodeDesc>();
    for (int i = 0; i < gbInfo.gbKeys.size(); i++) {
      //gbInfo already has ExprNode for gbkeys
      reduceKeys.add(gbInfo.gbKeys.get(i));
      String colOutputName = SemanticAnalyzer.getColumnInternalName(i);
      outputKeyColumnNames.add(colOutputName);
      colInfoLst.add(new ColumnInfo(Utilities.ReduceField.KEY.toString() + "." + colOutputName, gbInfo.gbKeyTypes.get(i), "", false));
      colExprMap.put(colOutputName, gbInfo.gbKeys.get(i));
    }

    // Note: GROUPING SETS are not allowed with map side aggregation set to false so we don't have to worry about it

    int keyLength = reduceKeys.size();

    // 2. Add Dist UDAF args to reduce keys
    if (gbInfo.containsDistinctAggr) {
      // TODO: Why is this needed (doesn't represent any cols)
      String udafName = SemanticAnalyzer.getColumnInternalName(reduceKeys.size());
      outputKeyColumnNames.add(udafName);
      for (int i = 0; i < gbInfo.distExprNodes.size(); i++) {
        reduceKeys.add(gbInfo.distExprNodes.get(i));
        //this part of reduceKeys is later used to create column names strictly for non-distinct aggregates
        // with parameters same as distinct keys which expects _col0 at the end. So we always append
        // _col0 at the end instead of _col<i>
        outputColName = SemanticAnalyzer.getColumnInternalName(0);
        String field = Utilities.ReduceField.KEY.toString() + "." + udafName + ":" + i + "."
            + outputColName;
        ColumnInfo colInfo = new ColumnInfo(field, gbInfo.distExprNodes.get(i).getTypeInfo(), null,
            false);
        colInfoLst.add(colInfo);
        colExprMap.put(field, gbInfo.distExprNodes.get(i));
      }
    }

    // 3. Add UDAF args deduped to reduce values
    ArrayList<ExprNodeDesc> reduceValues = new ArrayList<ExprNodeDesc>();
    for (int i = 0; i < gbInfo.deDupedNonDistIrefs.size(); i++) {
      reduceValues.add(gbInfo.deDupedNonDistIrefs.get(i));
      outputColName = SemanticAnalyzer.getColumnInternalName(reduceValues.size() - 1);
      outputValueColumnNames.add(outputColName);
      String field = Utilities.ReduceField.VALUE.toString() + "." + outputColName;
      colInfoLst.add(new ColumnInfo(field, reduceValues.get(reduceValues.size() - 1).getTypeInfo(),
          null, false));
      colExprMap.put(field, reduceValues.get(reduceValues.size() - 1));
    }

    // 4. Gen RS
    ReduceSinkOperator rsOp = (ReduceSinkOperator) OperatorFactory.getAndMakeChild(PlanUtils
        .getReduceSinkDesc(reduceKeys, keyLength, reduceValues,
            gbInfo.distColIndices, outputKeyColumnNames,
            outputValueColumnNames, true, -1, getNumPartFieldsForMapSideRS(gbInfo),
            getParallelismForMapSideRS(gbInfo), AcidUtils.Operation.NOT_ACID, gbInfo.defaultNullOrder), new RowSchema(
        colInfoLst), inputOpAf.inputs.get(0));

    rsOp.setColumnExprMap(colExprMap);

    return new OpAttr("", new HashSet<Integer>(), rsOp);
  }

  private static OpAttr genReduceSideGB2(OpAttr inputOpAf, GBInfo gbInfo) throws SemanticException {
    ArrayList<String> outputColNames = new ArrayList<String>();
    ArrayList<ColumnInfo> colInfoLst = new ArrayList<ColumnInfo>();
    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
    String colOutputName = null;
    ReduceSinkOperator rs = (ReduceSinkOperator) inputOpAf.inputs.get(0);
    List<ColumnInfo> rsColInfoLst = rs.getSchema().getSignature();
    ColumnInfo ci;

    // 1. Build GB Keys, grouping set starting position
    // 1.1 First Add original GB Keys
    ArrayList<ExprNodeDesc> gbKeys = ExprNodeDescUtils.genExprNodeDesc(rs, 0,
        gbInfo.gbKeys.size() - 1, false, false);
    for (int i = 0; i < gbInfo.gbKeys.size(); i++) {
      ci = rsColInfoLst.get(i);
      colOutputName = gbInfo.outputColNames.get(i);
      outputColNames.add(colOutputName);
      colInfoLst.add(new ColumnInfo(colOutputName, ci.getType(), "", false));
      colExprMap.put(colOutputName, gbKeys.get(i));
    }
    // 1.2 Add GrpSet Col
    int groupingSetsPosition = -1;
    if (inclGrpSetInReduceSide(gbInfo) && gbInfo.grpIdFunctionNeeded) {
      groupingSetsPosition = gbKeys.size();
      ExprNodeDesc grpSetColExpr = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo,
          rsColInfoLst.get(groupingSetsPosition).getInternalName(), null, false);
      gbKeys.add(grpSetColExpr);
      colOutputName = gbInfo.outputColNames.get(gbInfo.outputColNames.size() - 1);
      ;
      outputColNames.add(colOutputName);
      colInfoLst.add(new ColumnInfo(colOutputName, TypeInfoFactory.stringTypeInfo, null, true));
      colExprMap.put(colOutputName, grpSetColExpr);
    }

    // 2. Add UDAF
    UDAFAttrs udafAttr;
    ArrayList<AggregationDesc> aggregations = new ArrayList<AggregationDesc>();
    int udafStartPosInGBInfOutputColNames = gbInfo.grpSets.isEmpty() ? gbInfo.gbKeys.size()
        : gbInfo.gbKeys.size() * 2;
    int udafStartPosInInputRS = gbInfo.grpSets.isEmpty() ? gbInfo.gbKeys.size() : gbInfo.gbKeys.size() + 1;

    for (int i = 0; i < gbInfo.udafAttrs.size(); i++) {
      udafAttr = gbInfo.udafAttrs.get(i);
      ArrayList<ExprNodeDesc> aggParameters = new ArrayList<ExprNodeDesc>();
      aggParameters.add(new ExprNodeColumnDesc(rsColInfoLst.get(udafStartPosInInputRS + i)));
      colOutputName = gbInfo.outputColNames.get(udafStartPosInGBInfOutputColNames + i);
      outputColNames.add(colOutputName);
      Mode udafMode = SemanticAnalyzer.groupByDescModeToUDAFMode(GroupByDesc.Mode.FINAL,
          udafAttr.isDistinctUDAF);
      GenericUDAFInfo udaf = SemanticAnalyzer.getGenericUDAFInfo(udafAttr.udafEvaluator, udafMode,
          aggParameters);
      aggregations.add(new AggregationDesc(udafAttr.udafName.toLowerCase(),
          udaf.genericUDAFEvaluator, udaf.convertedParameters, false, udafMode));
      colInfoLst.add(new ColumnInfo(colOutputName, udaf.returnType, "", false));
    }

    Operator rsGBOp2 = OperatorFactory.getAndMakeChild(new GroupByDesc(GroupByDesc.Mode.FINAL,
        outputColNames, gbKeys, aggregations, false, gbInfo.groupByMemoryUsage,
        gbInfo.memoryThreshold, gbInfo.minReductionHashAggr, gbInfo.minReductionHashAggrLowerBound,
        null, false, groupingSetsPosition, gbInfo.containsDistinctAggr),
        new RowSchema(colInfoLst), rs);

    rsGBOp2.setColumnExprMap(colExprMap);

    // TODO: Shouldn't we propgate vc? is it vc col from tab or all vc
    return new OpAttr("", new HashSet<Integer>(), rsGBOp2);
  }

  private static OpAttr genReduceSideGB1(OpAttr inputOpAf, GBInfo gbInfo, boolean computeGrpSet,
      boolean propagateConstInDistinctUDAF, GroupByDesc.Mode gbMode) throws SemanticException {
    ArrayList<String> outputColNames = new ArrayList<String>();
    ArrayList<ColumnInfo> colInfoLst = new ArrayList<ColumnInfo>();
    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
    String colOutputName = null;
    ReduceSinkOperator rs = (ReduceSinkOperator) inputOpAf.inputs.get(0);
    List<ColumnInfo> rsColInfoLst = rs.getSchema().getSignature();
    ColumnInfo ci;
    boolean finalGB = (gbInfo.gbPhysicalPipelineMode == HIVEGBPHYSICALMODE.MAP_SIDE_GB_NO_SKEW_NO_ADD_MR_JOB);

    // 1. Build GB Keys, grouping set starting position
    // 1.1 First Add original GB Keys
    ArrayList<ExprNodeDesc> gbKeys = ExprNodeDescUtils.genExprNodeDesc(rs, 0,
        gbInfo.gbKeys.size() - 1, false, false);
    for (int i = 0; i < gbInfo.gbKeys.size(); i++) {
      ci = rsColInfoLst.get(i);
      if (finalGB) {
        colOutputName = gbInfo.outputColNames.get(i);
      } else {
        colOutputName = SemanticAnalyzer.getColumnInternalName(i);
      }
      outputColNames.add(colOutputName);
      colInfoLst.add(new ColumnInfo(colOutputName, ci.getType(), "", false));
      colExprMap.put(colOutputName, gbKeys.get(i));
    }

    // 1.2 Add GrpSet Col
    int groupingSetsColPosition = -1;
    if ((!finalGB && gbInfo.grpSets.size() > 0) || (finalGB && gbInfo.grpIdFunctionNeeded)) {
      groupingSetsColPosition = gbInfo.gbKeys.size();
      if (computeGrpSet) {
        // GrpSet Col needs to be constructed
        gbKeys.add(new ExprNodeConstantDesc("0L"));
      } else {
        // GrpSet Col already part of input RS
        // TODO: Can't we just copy the ExprNodeDEsc from input (Do we need to
        // explicitly set table alias to null & VC to false
        gbKeys.addAll(ExprNodeDescUtils.genExprNodeDesc(rs, groupingSetsColPosition,
            groupingSetsColPosition, false, true));
      }

      colOutputName = SemanticAnalyzer.getColumnInternalName(groupingSetsColPosition);
      if (finalGB) {
        colOutputName = gbInfo.outputColNames.get(gbInfo.outputColNames.size() - 1);
      }
      outputColNames.add(colOutputName);
      colInfoLst.add(new ColumnInfo(colOutputName, TypeInfoFactory.stringTypeInfo, null, true));
      colExprMap.put(colOutputName, gbKeys.get(groupingSetsColPosition));
    }

    // 2. Walk through UDAF and add them to GB
    String lastReduceKeyColName = null;
    if (!rs.getConf().getOutputKeyColumnNames().isEmpty()) {
      lastReduceKeyColName = rs.getConf().getOutputKeyColumnNames()
          .get(rs.getConf().getOutputKeyColumnNames().size() - 1);
    }
    int numDistinctUDFs = 0;

    int distinctStartPosInReduceKeys = gbKeys.size();
    List<ExprNodeDesc> reduceValues = rs.getConf().getValueCols();
    ArrayList<AggregationDesc> aggregations = new ArrayList<AggregationDesc>();
    int udafColStartPosInOriginalGB = (gbInfo.grpSets.size() > 0) ? gbInfo.gbKeys.size() * 2
        : gbInfo.gbKeys.size();
    int udafColStartPosInRS = rs.getConf().getKeyCols().size();
    for (int i = 0; i < gbInfo.udafAttrs.size(); i++) {
      UDAFAttrs udafAttr = gbInfo.udafAttrs.get(i);
      ArrayList<ExprNodeDesc> aggParameters = new ArrayList<ExprNodeDesc>();

      if (udafAttr.isDistinctUDAF) {
        ColumnInfo rsDistUDAFParamColInfo;
        ExprNodeDesc distinctUDAFParam;
        ExprNodeDesc constantPropDistinctUDAFParam;
        for (int j = 0; j < udafAttr.udafParamsIndxInGBInfoDistExprs.size(); j++) {
          rsDistUDAFParamColInfo = rsColInfoLst.get(distinctStartPosInReduceKeys + j);
          String rsDistUDAFParamName = rsDistUDAFParamColInfo.getInternalName();
          // TODO: verify if this is needed
          if (lastReduceKeyColName != null) {
            rsDistUDAFParamName = Utilities.ReduceField.KEY.name() + "." + lastReduceKeyColName
                + ":" + numDistinctUDFs + "." + SemanticAnalyzer.getColumnInternalName(j);
          }

          distinctUDAFParam = new ExprNodeColumnDesc(rsDistUDAFParamColInfo.getType(),
              rsDistUDAFParamName, rsDistUDAFParamColInfo.getTabAlias(),
              rsDistUDAFParamColInfo.getIsVirtualCol());
          if (propagateConstInDistinctUDAF) {
            // TODO: Implement propConstDistUDAFParams
            constantPropDistinctUDAFParam = SemanticAnalyzer
                .isConstantParameterInAggregationParameters(
                    rsDistUDAFParamColInfo.getInternalName(), reduceValues);
            if (constantPropDistinctUDAFParam != null) {
              distinctUDAFParam = constantPropDistinctUDAFParam;
            }
          }
          aggParameters.add(distinctUDAFParam);
        }
        numDistinctUDFs++;
      } else {
        aggParameters.add(new ExprNodeColumnDesc(rsColInfoLst.get(udafColStartPosInRS + i)));
      }
      Mode udafMode = SemanticAnalyzer.groupByDescModeToUDAFMode(gbMode, udafAttr.isDistinctUDAF);
      GenericUDAFInfo udaf = SemanticAnalyzer.getGenericUDAFInfo(udafAttr.udafEvaluator, udafMode,
          aggParameters);
      aggregations.add(new AggregationDesc(udafAttr.udafName.toLowerCase(),
          udaf.genericUDAFEvaluator, udaf.convertedParameters,
          (gbMode != GroupByDesc.Mode.FINAL && udafAttr.isDistinctUDAF), udafMode));

      if (finalGB) {
        colOutputName = gbInfo.outputColNames.get(udafColStartPosInOriginalGB + i);
      } else {
        colOutputName = SemanticAnalyzer.getColumnInternalName(gbKeys.size() + aggregations.size()
            - 1);
      }

      colInfoLst.add(new ColumnInfo(colOutputName, udaf.returnType, "", false));
      outputColNames.add(colOutputName);
    }

    // Nothing special needs to be done for grouping sets if
    // this is the final group by operator, and multiple rows corresponding to
    // the
    // grouping sets have been generated upstream.
    // However, if an addition MR job has been created to handle grouping sets,
    // additional rows corresponding to grouping sets need to be created here.
    //TODO: Clean up/refactor assumptions
    boolean includeGrpSetInGBDesc = (gbInfo.grpSets.size() > 0)
        && !finalGB
        && !(gbInfo.gbPhysicalPipelineMode == HIVEGBPHYSICALMODE.MAP_SIDE_GB_SKEW_GBKEYS_OR_DIST_UDAF_PRESENT);
    Operator rsGBOp = OperatorFactory.getAndMakeChild(new GroupByDesc(gbMode, outputColNames,
        gbKeys, aggregations, gbInfo.groupByMemoryUsage, gbInfo.memoryThreshold,
        gbInfo.minReductionHashAggr, gbInfo.minReductionHashAggrLowerBound,
        gbInfo.grpSets, includeGrpSetInGBDesc, groupingSetsColPosition, gbInfo.containsDistinctAggr),
        new RowSchema(colInfoLst), rs);

    rsGBOp.setColumnExprMap(colExprMap);

    return new OpAttr("", new HashSet<Integer>(), rsGBOp);
  }

  /**
   * RS-GB0
   *
   * @param inputOpAf
   * @param gbInfo
   * @param gbMode
   * @return
   * @throws SemanticException
   */
  private static OpAttr genReduceSideGB1NoMapGB(OpAttr inputOpAf, GBInfo gbInfo,
      GroupByDesc.Mode gbMode) throws SemanticException {
    ArrayList<String> outputColNames = new ArrayList<String>();
    ArrayList<ColumnInfo> colInfoLst = new ArrayList<ColumnInfo>();
    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
    String colOutputName = null;
    ReduceSinkOperator rs = (ReduceSinkOperator) inputOpAf.inputs.get(0);
    List<ColumnInfo> rsColInfoLst = rs.getSchema().getSignature();
    ColumnInfo ci;
    boolean useOriginalGBNames = (gbInfo.gbPhysicalPipelineMode == HIVEGBPHYSICALMODE.NO_MAP_SIDE_GB_NO_SKEW);

    // 1. Build GB Keys, grouping set starting position
    // 1.1 First Add original GB Keys
    ArrayList<ExprNodeDesc> gbKeys = ExprNodeDescUtils.genExprNodeDesc(rs, 0,
        gbInfo.gbKeys.size() - 1, true, false);
    for (int i = 0; i < gbInfo.gbKeys.size(); i++) {
      ci = rsColInfoLst.get(i);
      if (useOriginalGBNames) {
        colOutputName = gbInfo.outputColNames.get(i);
      } else {
        colOutputName = SemanticAnalyzer.getColumnInternalName(i);
      }
      outputColNames.add(colOutputName);
      colInfoLst.add(new ColumnInfo(colOutputName, ci.getType(), null, false));
      colExprMap.put(colOutputName, gbKeys.get(i));
    }

    // 2. Walk through UDAF and add them to GB
    String lastReduceKeyColName = null;
    if (!rs.getConf().getOutputKeyColumnNames().isEmpty()) {
      lastReduceKeyColName = rs.getConf().getOutputKeyColumnNames()
          .get(rs.getConf().getOutputKeyColumnNames().size() - 1);
    }
    int numDistinctUDFs = 0;
    List<ExprNodeDesc> reduceValues = rs.getConf().getValueCols();
    ArrayList<AggregationDesc> aggregations = new ArrayList<AggregationDesc>();
    int udafColStartPosInOriginalGB = gbInfo.gbKeys.size();

    final List<List<ColumnInfo>> paramColInfoTable = new ArrayList<>(gbInfo.udafAttrs.size());
    final List<List<String>> distinctColumnNameTable = new ArrayList<>(gbInfo.udafAttrs.size());
    final Map<ColumnInfo, String> distinctColumnMapping = new HashMap<>();
    for (int i = 0; i < gbInfo.udafAttrs.size(); i++) {
      final UDAFAttrs udafAttr = gbInfo.udafAttrs.get(i);
      final List<ColumnInfo> paramColInfo = new ArrayList<>(udafAttr.udafParams.size());
      final List<String> distinctColNames = new ArrayList<>(udafAttr.udafParams.size());

      for (int j = 0; j < udafAttr.udafParams.size(); j++) {
        final int argPos = getColInfoPos(udafAttr.udafParams.get(j), gbInfo);
        final ColumnInfo rsUDAFParamColInfo = rsColInfoLst.get(argPos);
        paramColInfo.add(rsUDAFParamColInfo);

        final String distinctColumnName;
        if (udafAttr.isDistinctUDAF && lastReduceKeyColName != null) {
          distinctColumnName = Utilities.ReduceField.KEY.name() + "." + lastReduceKeyColName
                  + ":" + numDistinctUDFs + "." + SemanticAnalyzer.getColumnInternalName(j);
          distinctColumnMapping.putIfAbsent(rsUDAFParamColInfo, distinctColumnName);
        } else {
          distinctColumnName = null;
        }
        distinctColNames.add(distinctColumnName);
      }

      paramColInfoTable.add(paramColInfo);
      distinctColumnNameTable.add(distinctColNames);

      if(udafAttr.isDistinctUDAF) {
        numDistinctUDFs++;
      }
    }

    // the positions in rsColInfoLst are as follows
    // --grpkey--,--distkey--,--values--
    // but distUDAF may be before/after some non-distUDAF,
    // i.e., their positions can be mixed.
    // so for all UDAF we first check to see if it is groupby key, if not is it distinct key
    // if not it should be value
    final Map<Integer, List<ExprNodeDesc>> indexToParameter = new TreeMap<>();
    for (int i = 0; i < paramColInfoTable.size(); i++) {
      final ArrayList<ExprNodeDesc> aggParameters = new ArrayList<>();

      for (int j = 0; j < paramColInfoTable.get(i).size(); j++) {
        final ColumnInfo rsUDAFParamColInfo = paramColInfoTable.get(i).get(j);

        final String rsUDAFParamName;
        if (distinctColumnNameTable.get(i).get(j) != null) {
          rsUDAFParamName = distinctColumnNameTable.get(i).get(j);
        } else if (distinctColumnMapping.containsKey(rsUDAFParamColInfo)) {
          // This UDAF is not labeled with DISTINCT, but it refers to a DISTINCT key.
          // The original internal name could be already obsolete as any DISTINCT keys are renamed.
          rsUDAFParamName = distinctColumnMapping.get(rsUDAFParamColInfo);
        } else {
          rsUDAFParamName = rsUDAFParamColInfo.getInternalName();
        }
        ExprNodeDesc udafParam = new ExprNodeColumnDesc(rsUDAFParamColInfo.getType(), rsUDAFParamName,
                rsUDAFParamColInfo.getTabAlias(), rsUDAFParamColInfo.getIsVirtualCol());
        final ExprNodeDesc constantPropDistinctUDAFParam = SemanticAnalyzer
                .isConstantParameterInAggregationParameters(rsUDAFParamColInfo.getInternalName(),
                        reduceValues);
        if (constantPropDistinctUDAFParam != null) {
          udafParam = constantPropDistinctUDAFParam;
        }
        aggParameters.add(udafParam);
      }
      indexToParameter.put(i, aggParameters);
    }

    for(Map.Entry<Integer, List<ExprNodeDesc>> e : indexToParameter.entrySet()){
      UDAFAttrs udafAttr = gbInfo.udafAttrs.get(e.getKey());
      Mode udafMode = SemanticAnalyzer.groupByDescModeToUDAFMode(gbMode, udafAttr.isDistinctUDAF);
      GenericUDAFInfo udaf = SemanticAnalyzer.getGenericUDAFInfo(udafAttr.udafEvaluator, udafMode,
          e.getValue());
      aggregations.add(new AggregationDesc(udafAttr.udafName.toLowerCase(),
          udaf.genericUDAFEvaluator, udaf.convertedParameters, udafAttr.isDistinctUDAF, udafMode));
      if (useOriginalGBNames) {
        colOutputName = gbInfo.outputColNames.get(udafColStartPosInOriginalGB + e.getKey());
      } else {
        colOutputName = SemanticAnalyzer.getColumnInternalName(gbKeys.size() + aggregations.size()
            - 1);
      }
      colInfoLst.add(new ColumnInfo(colOutputName, udaf.returnType, "", false));
      outputColNames.add(colOutputName);
    }

    Operator rsGB1 = OperatorFactory.getAndMakeChild(new GroupByDesc(gbMode, outputColNames,
        gbKeys, aggregations, false, gbInfo.groupByMemoryUsage, gbInfo.minReductionHashAggrLowerBound,
        gbInfo.memoryThreshold, gbInfo.minReductionHashAggr, null,
        false, -1, numDistinctUDFs > 0), new RowSchema(colInfoLst), rs);
    rsGB1.setColumnExprMap(colExprMap);

    return new OpAttr("", new HashSet<Integer>(), rsGB1);
  }

  private static int getColInfoPos(ExprNodeDesc aggExpr, GBInfo gbInfo ) {
    //first see if it is gbkeys
    int gbKeyIndex = ExprNodeDescUtils.indexOf(aggExpr, gbInfo.gbKeys);
    if(gbKeyIndex < 0 )  {
        //then check if it is distinct key
      int distinctKeyIndex = ExprNodeDescUtils.indexOf(aggExpr, gbInfo.distExprNodes);
      if(distinctKeyIndex < 0) {
        // lastly it should be in deDupedNonDistIrefs
        int deDupValIndex = ExprNodeDescUtils.indexOf(aggExpr, gbInfo.deDupedNonDistIrefs);
        assert(deDupValIndex >= 0);
        return gbInfo.gbKeys.size() + gbInfo.distExprNodes.size() + deDupValIndex;
      }
      else {
        //aggExpr is part of distinct key
        return gbInfo.gbKeys.size() + distinctKeyIndex;
      }

    }
    else {
        return gbKeyIndex;
    }
  }

  @SuppressWarnings("unchecked")
  private static OpAttr genMapSideGB(OpAttr inputOpAf, GBInfo gbAttrs) throws SemanticException {
    ArrayList<String> outputColNames = new ArrayList<String>();
    ArrayList<ColumnInfo> colInfoLst = new ArrayList<ColumnInfo>();
    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
    Set<String> gbKeyColsAsNamesFrmIn = new HashSet<String>();
    String colOutputName = null;

    // 1. Build GB Keys, grouping set starting position
    // 1.1 First Add original GB Keys
    ArrayList<ExprNodeDesc> gbKeys = new ArrayList<ExprNodeDesc>();
    for (int i = 0; i < gbAttrs.gbKeys.size(); i++) {
      gbKeys.add(gbAttrs.gbKeys.get(i));
      colOutputName = SemanticAnalyzer.getColumnInternalName(i);
      colInfoLst.add(new ColumnInfo(colOutputName, gbAttrs.gbKeyTypes.get(i), "", false));
      outputColNames.add(colOutputName);
      gbKeyColsAsNamesFrmIn.add(gbAttrs.gbKeyColNamesInInput.get(i));
      colExprMap.put(colOutputName, gbKeys.get(i));
    }
    // 1.2. Adjust GroupingSet Position, GBKeys for GroupingSet Position if
    // needed. NOTE: GroupingID is added to map side GB only if we don't GrpSet
    // doesn't require additional MR Jobs
    int groupingSetsPosition = -1;
    boolean inclGrpID = inclGrpSetInMapSide(gbAttrs);
    if (inclGrpID) {
      groupingSetsPosition = gbKeys.size();
      addGrpSetCol(true, null, false, gbKeys, outputColNames, colInfoLst, colExprMap);
    }
    // 1.3. Add all distinct params
    // NOTE: distinct expr can not be part of of GB key (we assume plan
    // gen would have prevented it)
    for (int i = 0; i < gbAttrs.distExprNodes.size(); i++) {
      if (!gbKeyColsAsNamesFrmIn.contains(gbAttrs.distExprNames.get(i))) {
        gbKeys.add(gbAttrs.distExprNodes.get(i));
        colOutputName = SemanticAnalyzer.getColumnInternalName(gbKeys.size() - 1);
        colInfoLst.add(new ColumnInfo(colOutputName, gbAttrs.distExprTypes.get(i), "", false));
        outputColNames.add(colOutputName);
        gbKeyColsAsNamesFrmIn.add(gbAttrs.distExprNames.get(i));
        colExprMap.put(colOutputName, gbKeys.get(gbKeys.size() - 1));
      }
    }

    // 2. Build Aggregations
    ArrayList<AggregationDesc> aggregations = new ArrayList<AggregationDesc>();
    for (UDAFAttrs udafAttr : gbAttrs.udafAttrs) {
      Mode amode = SemanticAnalyzer.groupByDescModeToUDAFMode(GroupByDesc.Mode.HASH,
          udafAttr.isDistinctUDAF);
      aggregations.add(new AggregationDesc(udafAttr.udafName.toLowerCase(), udafAttr.udafEvaluator,
          udafAttr.udafParams, udafAttr.isDistinctUDAF, amode));
      GenericUDAFInfo udafInfo;
      try {
        udafInfo = SemanticAnalyzer.getGenericUDAFInfo(udafAttr.udafEvaluator, amode,
            udafAttr.udafParams);
      } catch (SemanticException e) {
        throw new RuntimeException(e);
      }
      colOutputName = SemanticAnalyzer.getColumnInternalName(gbKeys.size() + aggregations.size()
          - 1);
      colInfoLst.add(new ColumnInfo(colOutputName, udafInfo.returnType, "", false));
      outputColNames.add(colOutputName);
    }

    // 3. Create GB
    @SuppressWarnings("rawtypes")
    Operator gbOp = OperatorFactory.getAndMakeChild(new GroupByDesc(GroupByDesc.Mode.HASH,
        outputColNames, gbKeys, aggregations, false, gbAttrs.groupByMemoryUsage,
        gbAttrs.memoryThreshold, gbAttrs.minReductionHashAggr, gbAttrs.minReductionHashAggrLowerBound,
        gbAttrs.grpSets, inclGrpID, groupingSetsPosition,
        gbAttrs.containsDistinctAggr), new RowSchema(colInfoLst), inputOpAf.inputs.get(0));

    // 5. Setup Expr Col Map
    // NOTE: UDAF is not included in ExprColMap
    gbOp.setColumnExprMap(colExprMap);

    return new OpAttr("", new HashSet<Integer>(), gbOp);
  }

  private static void addGrpSetCol(boolean createConstantExpr, String grpSetIDExprName,
      boolean addReducePrefixToColInfoName, List<ExprNodeDesc> exprLst,
      List<String> outputColumnNames, List<ColumnInfo> colInfoLst,
      Map<String, ExprNodeDesc> colExprMap) throws SemanticException {
    String outputColName = null;
    ExprNodeDesc grpSetColExpr = null;

    if (createConstantExpr) {
      grpSetColExpr = new ExprNodeConstantDesc("0L");
    } else {
      grpSetColExpr = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, grpSetIDExprName,
          null, false);
    }
    exprLst.add(grpSetColExpr);

    outputColName = SemanticAnalyzer.getColumnInternalName(exprLst.size() - 1);
    outputColumnNames.add(outputColName);
    String internalColName = outputColName;
    if (addReducePrefixToColInfoName) {
      internalColName = Utilities.ReduceField.KEY.toString() + "." + outputColName;
    }
    colInfoLst.add(new ColumnInfo(internalColName, grpSetColExpr.getTypeInfo(), null, true));
    colExprMap.put(internalColName, grpSetColExpr);
  }

  /**
   * Get Reduce Keys for RS following MapSide GB
   *
   * @param reduceKeys
   *          assumed to be deduped list of exprs
   * @param outputKeyColumnNames
   * @param colExprMap
   * @return List of ExprNodeDesc of ReduceKeys
   * @throws SemanticException
   */
  private static ArrayList<ExprNodeDesc> getReduceKeysForRS(Operator inOp, int startPos,
      int endPos, List<String> outputKeyColumnNames, boolean addOnlyOneKeyColName,
      ArrayList<ColumnInfo> colInfoLst, Map<String, ExprNodeDesc> colExprMap,
      boolean addEmptyTabAlias, boolean setColToNonVirtual) throws SemanticException {
    ArrayList<ExprNodeDesc> reduceKeys = null;
    if (endPos < 0) {
      reduceKeys = new ArrayList<ExprNodeDesc>();
    } else {
      reduceKeys = ExprNodeDescUtils.genExprNodeDesc(inOp, startPos, endPos, addEmptyTabAlias,
          setColToNonVirtual);
      int outColNameIndx = startPos;
      for (int i = 0; i < reduceKeys.size(); ++i) {
        String outputColName = SemanticAnalyzer.getColumnInternalName(outColNameIndx);
        outColNameIndx++;
        if (!addOnlyOneKeyColName || i == 0) {
          outputKeyColumnNames.add(outputColName);
        }

        // TODO: Verify if this is needed (Why can't it be always null/empty
        String tabAlias = addEmptyTabAlias ? "" : null;
        ColumnInfo colInfo = new ColumnInfo(Utilities.ReduceField.KEY.toString() + "."
            + outputColName, reduceKeys.get(i).getTypeInfo(), tabAlias, false);
        colInfoLst.add(colInfo);
        colExprMap.put(colInfo.getInternalName(), reduceKeys.get(i));
      }
    }

    return reduceKeys;
  }

  /**
   * Get Value Keys for RS following MapSide GB
   *
   * @param GroupByOperator
   *          MapSide GB
   * @param outputKeyColumnNames
   * @param colExprMap
   * @return List of ExprNodeDesc of Values
   * @throws SemanticException
   */
  private static ArrayList<ExprNodeDesc> getValueKeysForRS(Operator inOp, int aggStartPos,
      List<String> outputKeyColumnNames, ArrayList<ColumnInfo> colInfoLst,
      Map<String, ExprNodeDesc> colExprMap, boolean addEmptyTabAlias, boolean setColToNonVirtual)
      throws SemanticException {
    List<ColumnInfo> mapGBColInfoLst = inOp.getSchema().getSignature();
    ArrayList<ExprNodeDesc> valueKeys = null;
    if (aggStartPos >= mapGBColInfoLst.size()) {
      valueKeys = new ArrayList<ExprNodeDesc>();
    } else {
      valueKeys = ExprNodeDescUtils.genExprNodeDesc(inOp, aggStartPos, mapGBColInfoLst.size() - 1,
          true, setColToNonVirtual);
      for (int i = 0; i < valueKeys.size(); ++i) {
        String outputColName = SemanticAnalyzer.getColumnInternalName(i);
        outputKeyColumnNames.add(outputColName);
        // TODO: Verify if this is needed (Why can't it be always null/empty
        String tabAlias = addEmptyTabAlias ? "" : null;
        ColumnInfo colInfo = new ColumnInfo(Utilities.ReduceField.VALUE.toString() + "."
            + outputColName, valueKeys.get(i).getTypeInfo(), tabAlias, false);
        colInfoLst.add(colInfo);
        colExprMap.put(colInfo.getInternalName(), valueKeys.get(i));
      }
    }

    return valueKeys;
  }

  // TODO: Implement this
  private static ExprNodeDesc propConstDistUDAFParams() {
    return null;
  }
}
