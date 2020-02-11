/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.optimizer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.hadoop.hive.ql.util.NullOrdering;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.SemanticDispatcher;
import org.apache.hadoop.hive.ql.lib.SemanticGraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.SemanticNodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.SemanticRule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer.GenericUDAFInfo;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.Mode;

/**
 * Queries of form : select max(c), count(distinct c) from T; generates a plan
 * of form TS-&gt;mGBy-&gt;RS-&gt;rGBy-&gt;FS This plan suffers from a problem that vertex
 * containing rGBy-&gt;FS necessarily need to have 1 task. This limitation results
 * in slow execution because that task gets all the data. This optimization if
 * successful will rewrite above plan to mGby1-rs1-mGby2-mGby3-rs2-rGby1 This
 * introduces extra vertex of mGby2-mGby3-rs2. Note this vertex can have
 * multiple tasks and since we are doing aggregation, output of this must
 * necessarily be smaller than its input, which results in much less data going
 * in to original rGby-&gt;FS vertex, which continues to have single task. Also
 * note on calcite tree we have HiveExpandDistinctAggregatesRule rule which does
 * similar plan transformation but has different conditions which needs to be
 * satisfied. Additionally, we don't do any costing here but this is possibly
 * that this transformation may slow down query a bit since if data is small
 * enough to fit in a single task of last reducer, injecting additional vertex
 * in pipeline may make query slower. If this happens, users can use the
 * configuration hive.optimize.countdistinct to turn it off.
 */
public class CountDistinctRewriteProc extends Transform {

  private static final Logger LOG = LoggerFactory.getLogger(CountDistinctRewriteProc.class
      .getName());

  public CountDistinctRewriteProc() {
  }

  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {

    Map<SemanticRule, SemanticNodeProcessor> opRules = new LinkedHashMap<SemanticRule, SemanticNodeProcessor>();
    // process group-by pattern
    opRules
        .put(
            new RuleRegExp("R1", GroupByOperator.getOperatorName() + "%"
                + ReduceSinkOperator.getOperatorName() + "%" + GroupByOperator.getOperatorName()
                + "%"), getCountDistinctProc(pctx));

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    SemanticDispatcher disp = new DefaultRuleDispatcher(getDefaultProc(), opRules, null);
    SemanticGraphWalker ogw = new DefaultGraphWalker(disp);

    // Create a list of topop nodes
    List<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pctx.getTopOps().values());
    ogw.startWalking(topNodes, null);

    return pctx;
  }

  private SemanticNodeProcessor getDefaultProc() {
    return new SemanticNodeProcessor() {
      @Override
      public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
          Object... nodeOutputs) throws SemanticException {
        return null;
      }
    };
  }

  private SemanticNodeProcessor getCountDistinctProc(ParseContext pctx) {
    return new CountDistinctProcessor(pctx);
  }

  /**
   * CountDistinctProcessor.
   *
   */
  public class CountDistinctProcessor implements SemanticNodeProcessor {

    protected ParseContext pGraphContext;

    public CountDistinctProcessor(ParseContext pGraphContext) {
      this.pGraphContext = pGraphContext;
    }


    // Check if we can process it or not by the index of distinct
    protected int checkCountDistinct(GroupByOperator mGby, ReduceSinkOperator rs,
        GroupByOperator rGby) {
      // Position of distinct column in aggregator list of map Gby before rewrite.
      int indexOfDist = -1;
      List<ExprNodeDesc> keys = mGby.getConf().getKeys();
      if (!(mGby.getConf().getMode() == GroupByDesc.Mode.HASH
          && !mGby.getConf().isGroupingSetsPresent() && rs.getConf().getKeyCols().size() == 1
          && rs.getConf().getPartitionCols().size() == 0
          && rs.getConf().getDistinctColumnIndices().size() == 1
          && rGby.getConf().getMode() == GroupByDesc.Mode.MERGEPARTIAL && keys.size() == 1
          && rGby.getConf().getKeys().size() == 0 && mGby.getConf().getOutputColumnNames().size() == mGby
          .getConf().getAggregators().size() + 1)) {
        return -1;
      }
      for (int pos = 0; pos < mGby.getConf().getAggregators().size(); pos++) {
        AggregationDesc aggr = mGby.getConf().getAggregators().get(pos);
        if (aggr.getDistinct()) {
          if (indexOfDist != -1 || !aggr.getGenericUDAFName().equalsIgnoreCase("count")) {
            // there are 2 or more distincts, or distinct is not on count
            // TODO: may be the same count(distinct key), count(distinct key)
            // TODO: deal with duplicate count distinct key
            return -1;
          }
          indexOfDist = pos;
          if (!(aggr.getParameters().size() == 1
              && aggr.getParameters().get(0) instanceof ExprNodeColumnDesc && mGby.getConf()
              .getKeys().get(0) instanceof ExprNodeColumnDesc)) {
            return -1;
          } else {
            ExprNodeColumnDesc agg = (ExprNodeColumnDesc) aggr.getParameters().get(0);
            ExprNodeColumnDesc key = (ExprNodeColumnDesc) mGby.getConf().getKeys().get(0);
            if (!agg.isSame(key)) {
              return -1;
            }
          }
        }
      }
      if (indexOfDist == -1) {
        return -1;
      }
      // check if it is potential to trigger nullscan
      if (pGraphContext.getConf().getBoolVar(HiveConf.ConfVars.HIVEMETADATAONLYQUERIES)) {
        for (TableScanOperator tsOp : pGraphContext.getTopOps().values()) {
          List<Integer> colIDs = tsOp.getNeededColumnIDs();
          TableScanDesc desc = tsOp.getConf();
          boolean noColNeeded = (colIDs == null) || (colIDs.isEmpty());
          boolean noVCneeded = (desc == null) || (desc.getVirtualCols() == null)
              || (desc.getVirtualCols().isEmpty());
          boolean isSkipHF = desc.isNeedSkipHeaderFooters();
          if (noColNeeded && noVCneeded && !isSkipHF) {
            // it is possible that nullscan can fire, we skip this rule.
            return -1;
          }
        }
      }
      return indexOfDist;
    }

    /*
     * We will transform GB-RS-GBY to mGby1-rs1-mGby2-mGby3-rs2-rGby1
     */
    @SuppressWarnings("unchecked")
    protected void processGroupBy(GroupByOperator mGby, ReduceSinkOperator rs, GroupByOperator rGby, int indexOfDist)
        throws SemanticException, CloneNotSupportedException {
      // remove count(distinct) in map-side gby
      List<Operator<? extends OperatorDesc>> parents = mGby.getParentOperators();
      List<Operator<? extends OperatorDesc>> children = rGby.getChildOperators();
      mGby.removeParents();
      rs.removeParents();
      rGby.removeParents();

      GroupByOperator mGby1 = genMapGroupby1(mGby, indexOfDist);
      ReduceSinkOperator rs1 = genReducesink1(mGby1, rs, indexOfDist);
      GroupByOperator mGby2 = genMapGroupby2(rs1, mGby, indexOfDist);
      GroupByOperator mGby3 = genMapGroupby3(mGby2, mGby, indexOfDist);
      ReduceSinkOperator rs2 = genReducesink2(mGby3, rs);
      GroupByOperator rGby1 = genReduceGroupby(rs2, rGby, indexOfDist);
      for (Operator<? extends OperatorDesc> parent : parents) {
        OperatorFactory.makeChild(parent, mGby1);
      }
      OperatorFactory.makeChild(mGby1, rs1);
      OperatorFactory.makeChild(rs1, mGby2);
      OperatorFactory.makeChild(mGby2, mGby3);
      OperatorFactory.makeChild(mGby3, rs2);
      OperatorFactory.makeChild(rs2, rGby1);
      for (Operator<? extends OperatorDesc> child : children) {
        child.removeParents();
        OperatorFactory.makeChild(rGby1, child);
      }
    }

    // mGby1 ---already contains group by key, we need to remove distinct column
    private GroupByOperator genMapGroupby1(Operator<? extends OperatorDesc> mGby, int indexOfDist)
        throws CloneNotSupportedException {
      GroupByOperator mGby1 = (GroupByOperator) mGby.clone();
      // distinct is at lost position.
      String fieldString = mGby1.getConf().getOutputColumnNames().get(indexOfDist + 1);
      mGby1.getColumnExprMap().remove(fieldString);
      mGby1.getConf().getOutputColumnNames().remove(indexOfDist + 1);
      mGby1.getConf().getAggregators().remove(indexOfDist);
      mGby1.getConf().setDistinct(false);
      mGby1.getSchema().getColumnNames().remove(indexOfDist + 1);
      mGby1.getSchema().getSignature().remove(indexOfDist + 1);
      return mGby1;
    }

    // rs1 --- remove distinctColIndices, set #reducer as -1, reset keys,
    // values, colexpmap and rowschema
    private ReduceSinkOperator genReducesink1(GroupByOperator mGby1,
        Operator<? extends OperatorDesc> rs, int indexOfDist) throws CloneNotSupportedException,
        SemanticException {
      ReduceSinkOperator rs1 = (ReduceSinkOperator) rs.clone();
      Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
      ArrayList<String> outputKeyColumnNames = new ArrayList<String>();
      ArrayList<String> outputValueColumnNames = new ArrayList<String>();
      ArrayList<ExprNodeDesc> reduceKeys = new ArrayList<ExprNodeDesc>();
      ArrayList<ExprNodeDesc> reduceValues = new ArrayList<ExprNodeDesc>();
      ArrayList<ColumnInfo> rowSchema = new ArrayList<>();

      List<String> internalNames = new ArrayList<>();
      for (int index = 0; index < mGby1.getSchema().getSignature().size(); index++) {
        ColumnInfo paraExprInfo = mGby1.getSchema().getSignature().get(index);
        String paraExpression = paraExprInfo.getInternalName();
        assert (paraExpression != null);
        ExprNodeColumnDesc exprDesc = new ExprNodeColumnDesc(paraExprInfo.getType(),
            paraExpression, paraExprInfo.getTabAlias(), paraExprInfo.getIsVirtualCol());
        // index==0 means this is key
        if (index == 0) {
          reduceKeys.add(exprDesc);
          String outputColName = SemanticAnalyzer.getColumnInternalName(index);
          outputKeyColumnNames.add(outputColName);
          String internalName = Utilities.ReduceField.KEY.toString() + "." + outputColName;
          colExprMap.put(internalName, exprDesc);
          internalNames.add(internalName);
          rowSchema.add(new ColumnInfo(internalName, mGby1.getSchema().getSignature().get(index).getType(), "", false));
        } else {
          reduceValues.add(exprDesc);
          String outputColName = SemanticAnalyzer.getColumnInternalName(index - 1);
          outputValueColumnNames.add(outputColName);
          String internalName = Utilities.ReduceField.VALUE.toString() + "." + outputColName;
          colExprMap.put(internalName, exprDesc);
          internalNames.add(internalName);
          rowSchema.add(new ColumnInfo(internalName, mGby1.getSchema().getSignature().get(index).getType(), "", false));
        }
      }
      List<List<Integer>> distinctColIndices = new ArrayList<>();
      rs1.setConf(PlanUtils.getReduceSinkDesc(reduceKeys, 1, reduceValues, distinctColIndices,
          outputKeyColumnNames, outputValueColumnNames, true, -1, 1, -1,
          AcidUtils.Operation.NOT_ACID, NullOrdering.defaultNullOrder(pGraphContext.getConf())));
      rs1.setColumnExprMap(colExprMap);
      
      rs1.setSchema(new RowSchema(rowSchema));
      return rs1;
    }

    // mGby2 ---already contains key, remove distinct and change all the others
    private GroupByOperator genMapGroupby2(ReduceSinkOperator rs1,
        Operator<? extends OperatorDesc> mGby, int indexOfDist) throws CloneNotSupportedException, SemanticException {
      GroupByOperator mGby2 = (GroupByOperator) mGby.clone();
      ArrayList<ColumnInfo> rowSchema = new ArrayList<>();
      ArrayList<ExprNodeDesc> groupByKeys = new ArrayList<ExprNodeDesc>();
      ArrayList<String> outputColumnNames = new ArrayList<String>();
      Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();

      ColumnInfo exprInfo = rs1.getSchema().getSignature().get(0);
      ExprNodeDesc key = new ExprNodeColumnDesc(exprInfo);
      groupByKeys.add(key);
      String field = SemanticAnalyzer.getColumnInternalName(0);
      outputColumnNames.add(field);
      ColumnInfo oColInfo = new ColumnInfo(field, exprInfo.getType(), "", false);
      colExprMap.put(field, key);
      rowSchema.add(oColInfo);

      ArrayList<AggregationDesc> aggregations = new ArrayList<AggregationDesc>();
      for (int index = 0; index < mGby2.getConf().getAggregators().size(); index++) {
        ArrayList<ExprNodeDesc> aggParameters = new ArrayList<ExprNodeDesc>();
        if (index != indexOfDist) {
          AggregationDesc desc = mGby2.getConf().getAggregators().get(index);
          ColumnInfo paraExprInfo = null;
          // for example, original it is max 0, dist 1, min 2
          // rs1's schema is key 0, max 1, min 2
          if (index < indexOfDist) {
            paraExprInfo = rs1.getSchema().getSignature().get(index + 1);
          } else {
            paraExprInfo = rs1.getSchema().getSignature().get(index);
          }

          String paraExpression = paraExprInfo.getInternalName();
          assert (paraExpression != null);
          aggParameters.add(new ExprNodeColumnDesc(paraExprInfo.getType(), paraExpression,
              paraExprInfo.getTabAlias(), paraExprInfo.getIsVirtualCol()));

          // for all the other aggregations, we set the mode to PARTIAL2
          Mode amode = SemanticAnalyzer.groupByDescModeToUDAFMode(GroupByDesc.Mode.PARTIAL2, false);
          GenericUDAFEvaluator genericUDAFEvaluator = desc.getGenericUDAFEvaluator();
          GenericUDAFInfo udaf = SemanticAnalyzer.getGenericUDAFInfo(genericUDAFEvaluator, amode,
              aggParameters);
          aggregations.add(new AggregationDesc(desc.getGenericUDAFName(),
              udaf.genericUDAFEvaluator, udaf.convertedParameters, false, amode));
          String f = SemanticAnalyzer.getColumnInternalName(aggregations.size());
          outputColumnNames.add(f);
          rowSchema.add(new ColumnInfo(f, udaf.returnType, "", false));
        }
      }
      mGby2.getConf().setMode(GroupByDesc.Mode.PARTIAL2);
      mGby2.getConf().setOutputColumnNames(outputColumnNames);
      mGby2.getConf().getKeys().clear();
      mGby2.getConf().getKeys().addAll(groupByKeys);
      mGby2.getConf().getAggregators().clear();
      mGby2.getConf().getAggregators().addAll(aggregations);
      mGby2.getConf().setDistinct(false);
      mGby2.setSchema(new RowSchema(rowSchema));
      mGby2.setColumnExprMap(colExprMap);
      return mGby2;
    }

    // mGby3 is a follow up of mGby2. Here we start to count(key).
    private GroupByOperator genMapGroupby3(GroupByOperator mGby2,
        Operator<? extends OperatorDesc> mGby, int indexOfDist) throws CloneNotSupportedException, SemanticException {
      GroupByOperator mGby3 = (GroupByOperator) mGby.clone();
      ArrayList<ColumnInfo> rowSchema = new ArrayList<>();
      ArrayList<String> outputColumnNames = new ArrayList<String>();
      Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();

      // exprInfo is the key
      ArrayList<AggregationDesc> aggregations = new ArrayList<AggregationDesc>();
      for (int index = 0; index <= mGby2.getConf().getAggregators().size(); index++) {
        if (index == indexOfDist) {
          ArrayList<ExprNodeDesc> aggParameters = new ArrayList<ExprNodeDesc>();
          // add count(KEY._col0) to replace distinct
          ColumnInfo paraExprInfo = mGby2.getSchema().getSignature().get(0);
          String paraExpression = paraExprInfo.getInternalName();
          assert (paraExpression != null);
          aggParameters.add(new ExprNodeColumnDesc(paraExprInfo.getType(), paraExpression,
              paraExprInfo.getTabAlias(), paraExprInfo.getIsVirtualCol()));
          Mode amode = SemanticAnalyzer.groupByDescModeToUDAFMode(GroupByDesc.Mode.HASH, false);
          GenericUDAFEvaluator genericUDAFEvaluator = SemanticAnalyzer.getGenericUDAFEvaluator(
              "count", aggParameters, null, false, false);
          assert (genericUDAFEvaluator != null);
          GenericUDAFInfo udaf = SemanticAnalyzer.getGenericUDAFInfo(genericUDAFEvaluator, amode,
              aggParameters);
          AggregationDesc newDesc = new AggregationDesc("count", udaf.genericUDAFEvaluator,
              udaf.convertedParameters, false, amode);
          String f = SemanticAnalyzer.getColumnInternalName(aggregations.size());
          aggregations.add(newDesc);
          outputColumnNames.add(f);
          rowSchema.add(new ColumnInfo(f, udaf.returnType, "", false));
        }
        if (index == mGby2.getConf().getAggregators().size()) {
          break;
        }
        ArrayList<ExprNodeDesc> aggParameters = new ArrayList<ExprNodeDesc>();
        AggregationDesc desc = mGby2.getConf().getAggregators().get(index);
        ColumnInfo paraExprInfo = null;
        // for example, original it is max 0, dist 1, min 2
        // rs1's schema is key 0, max 1, min 2
        paraExprInfo = mGby2.getSchema().getSignature().get(index + 1);
        String paraExpression = paraExprInfo.getInternalName();
        assert (paraExpression != null);
        aggParameters.add(new ExprNodeColumnDesc(paraExprInfo.getType(), paraExpression,
            paraExprInfo.getTabAlias(), paraExprInfo.getIsVirtualCol()));

        // for all the other aggregations, we set the mode to PARTIAL2
        Mode amode = SemanticAnalyzer.groupByDescModeToUDAFMode(GroupByDesc.Mode.PARTIAL2, false);
        GenericUDAFEvaluator genericUDAFEvaluator = desc.getGenericUDAFEvaluator();
        GenericUDAFInfo udaf = SemanticAnalyzer.getGenericUDAFInfo(genericUDAFEvaluator, amode,
            aggParameters);
        String f = SemanticAnalyzer.getColumnInternalName(aggregations.size());
        aggregations.add(new AggregationDesc(desc.getGenericUDAFName(), udaf.genericUDAFEvaluator,
            udaf.convertedParameters, false, amode));
        outputColumnNames.add(f);
        rowSchema.add(new ColumnInfo(f, udaf.returnType, "", false));
      }
      mGby3.getConf().setMode(GroupByDesc.Mode.PARTIAL2);
      mGby3.getConf().setOutputColumnNames(outputColumnNames);
      mGby3.getConf().getKeys().clear();
      mGby3.getConf().getAggregators().clear();
      mGby3.getConf().getAggregators().addAll(aggregations);
      mGby3.getConf().setDistinct(false);
      mGby3.setSchema(new RowSchema(rowSchema));
      mGby3.setColumnExprMap(colExprMap);
      return mGby3;
    }

    // #reducer is already 1
    private ReduceSinkOperator genReducesink2(GroupByOperator mGby2,
        Operator<? extends OperatorDesc> rs) throws SemanticException, CloneNotSupportedException {
      ReduceSinkOperator rs2 = (ReduceSinkOperator) rs.clone();
      Map<String, ExprNodeDesc> colExprMap = new HashMap<>();

      ArrayList<String> outputKeyColumnNames = new ArrayList<String>();
      ArrayList<String> outputValueColumnNames = new ArrayList<String>();
      ArrayList<ExprNodeDesc> reduceValues = new ArrayList<ExprNodeDesc>();
      ArrayList<ColumnInfo> rowSchema = new ArrayList<>();
      for (int index = 0; index < mGby2.getSchema().getSignature().size(); index++) {
        ColumnInfo paraExprInfo = mGby2.getSchema().getSignature().get(index);
        String paraExpression = paraExprInfo.getInternalName();
        assert (paraExpression != null);
        ExprNodeColumnDesc exprDesc = new ExprNodeColumnDesc(paraExprInfo.getType(),
            paraExpression, paraExprInfo.getTabAlias(), paraExprInfo.getIsVirtualCol());
        reduceValues.add(exprDesc);
        String outputColName = SemanticAnalyzer.getColumnInternalName(index);
        outputValueColumnNames.add(outputColName);
        String internalName = Utilities.ReduceField.VALUE.toString() + "." + outputColName;
        colExprMap.put(internalName, exprDesc);
        rowSchema.add(new ColumnInfo(internalName, paraExprInfo.getType(), "", false));
      }
      List<List<Integer>> distinctColIndices = new ArrayList<>();
      ArrayList<ExprNodeDesc> reduceKeys = new ArrayList<>();
      rs2.setConf(PlanUtils.getReduceSinkDesc(reduceKeys, 0, reduceValues, distinctColIndices,
          outputKeyColumnNames, outputValueColumnNames, false, -1, 0, 1,
          AcidUtils.Operation.NOT_ACID, NullOrdering.defaultNullOrder(pGraphContext.getConf())));
      rs2.setColumnExprMap(colExprMap);
      rs2.setSchema(new RowSchema(rowSchema));
      return rs2;
    }

    // replace the distinct with the count aggregation
    private GroupByOperator genReduceGroupby(ReduceSinkOperator rs2,
        Operator<? extends OperatorDesc> rGby, int indexOfDist) throws SemanticException,
        CloneNotSupportedException {
      GroupByOperator rGby1 = (GroupByOperator) rGby.clone();
      ColumnInfo paraExprInfo = rs2.getSchema().getSignature().get(indexOfDist);
      String paraExpression = paraExprInfo.getInternalName();
      assert (paraExpression != null);
      ArrayList<ExprNodeDesc> aggParameters = new ArrayList<ExprNodeDesc>();
      aggParameters.add(new ExprNodeColumnDesc(paraExprInfo.getType(), paraExpression, paraExprInfo
          .getTabAlias(), paraExprInfo.getIsVirtualCol()));
      GenericUDAFEvaluator genericUDAFEvaluator = SemanticAnalyzer.getGenericUDAFEvaluator("count",
          aggParameters, null, false, false);
      assert (genericUDAFEvaluator != null);
      Mode amode = SemanticAnalyzer.groupByDescModeToUDAFMode(GroupByDesc.Mode.MERGEPARTIAL, false);
      GenericUDAFInfo udaf = SemanticAnalyzer.getGenericUDAFInfo(genericUDAFEvaluator, amode,
          aggParameters);
      AggregationDesc newDesc = new AggregationDesc("count", udaf.genericUDAFEvaluator,
          udaf.convertedParameters, false, amode);
      rGby1.getConf().getAggregators().set(indexOfDist, newDesc);
      rGby1.getConf().setDistinct(false);
      return rGby1;
    }

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      GroupByOperator mGby = (GroupByOperator) stack.get(stack.size() - 3);
      ReduceSinkOperator rs = (ReduceSinkOperator) stack.get(stack.size() - 2);
      GroupByOperator rGby = (GroupByOperator) stack.get(stack.size() - 1);
      int applicableDistPos = checkCountDistinct(mGby, rs, rGby);
      if (applicableDistPos != -1) {
        LOG.info("trigger count distinct rewrite");
        try {
          processGroupBy(mGby, rs, rGby, applicableDistPos);
        } catch (CloneNotSupportedException e) {
          throw new SemanticException(e.getMessage());
        }
      }
      return null;
    }

  }

}
