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

package org.apache.hadoop.hive.ql.optimizer.calcite.translator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.SemiJoin;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.LimitOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.AcidUtils.Operation;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil.JoinLeafPredicateInfo;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil.JoinPredicateInfo;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSort;
import org.apache.hadoop.hive.ql.parse.JoinCond;
import org.apache.hadoop.hive.ql.parse.JoinType;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.JoinCondDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.LimitDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class HiveOpConverter {

  private static final Log LOG = LogFactory.getLog(HiveOpConverter.class);

  public static enum HIVEAGGOPMODE {
    NO_SKEW_NO_MAP_SIDE_AGG, // Corresponds to SemAnalyzer genGroupByPlan1MR
    SKEW_NO_MAP_SIDE_AGG, // Corresponds to SemAnalyzer genGroupByPlan2MR
    NO_SKEW_MAP_SIDE_AGG, // Corresponds to SemAnalyzer
                          // genGroupByPlanMapAggrNoSkew
    SKEW_MAP_SIDE_AGG // Corresponds to SemAnalyzer genGroupByPlanMapAggr2MR
  };

  // TODO: remove this after stashing only rqd pieces from opconverter
  private final Map<String, Operator<? extends OperatorDesc>> topOps;
  private final HIVEAGGOPMODE                                 aggMode;
  private final boolean                                       strictMode;

  public static HIVEAGGOPMODE getAggOPMode(HiveConf hc) {
    HIVEAGGOPMODE aggOpMode = HIVEAGGOPMODE.NO_SKEW_NO_MAP_SIDE_AGG;

    if (hc.getBoolVar(HiveConf.ConfVars.HIVEMAPSIDEAGGREGATE)) {
      if (!hc.getBoolVar(HiveConf.ConfVars.HIVEGROUPBYSKEW)) {
        aggOpMode = HIVEAGGOPMODE.NO_SKEW_MAP_SIDE_AGG;
      } else {
        aggOpMode = HIVEAGGOPMODE.SKEW_MAP_SIDE_AGG;
      }
    } else if (hc.getBoolVar(HiveConf.ConfVars.HIVEGROUPBYSKEW)) {
      aggOpMode = HIVEAGGOPMODE.SKEW_NO_MAP_SIDE_AGG;
    }

    return aggOpMode;
  }

  public HiveOpConverter(Map<String, Operator<? extends OperatorDesc>> topOps,
          HIVEAGGOPMODE aggMode, boolean strictMode) {
    this.topOps = topOps;
    this.aggMode = aggMode;
    this.strictMode = strictMode;
  }

  private class OpAttr {
    private final String                 tabAlias;
    ImmutableList<Operator>              inputs;
    ImmutableMap<Integer, VirtualColumn> vcolMap;

    private OpAttr(String tabAlias, Map<Integer, VirtualColumn> vcolMap, Operator... inputs) {
      this.tabAlias = tabAlias;
      this.vcolMap = ImmutableMap.copyOf(vcolMap);
      this.inputs = ImmutableList.copyOf(inputs);
    }

    private OpAttr clone(Operator... inputs) {
      return new OpAttr(tabAlias, this.vcolMap, inputs);
    }
  }

  public Operator convert(RelNode root) throws SemanticException {
    OpAttr opAf = dispatch(root);
    return opAf.inputs.get(0);
  }

  OpAttr dispatch(RelNode rn) throws SemanticException {
    if (rn instanceof HiveJoin) {
      return visit((HiveJoin) rn);
    } else if (rn instanceof SemiJoin) {
      SemiJoin sj = (SemiJoin) rn;
      HiveJoin hj = HiveJoin.getJoin(sj.getCluster(), sj.getLeft(), sj.getRight(),
              sj.getCondition(), sj.getJoinType(), true);
      return visit(hj);
    } else if (rn instanceof HiveSort) {
      return visit((HiveSort) rn);
    }
    LOG.error(rn.getClass().getCanonicalName() + "operator translation not supported"
            + " yet in return path.");
    return null;
  }

  OpAttr visit(HiveJoin joinRel) throws SemanticException {
    // 1. Convert inputs
    OpAttr[] inputs = new OpAttr[joinRel.getInputs().size()];
    for (int i=0; i<inputs.length; i++) {
      inputs[i] = dispatch(joinRel.getInput(i));
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Translating operator rel#" + joinRel.getId() + ":" + joinRel.getRelTypeName() +
              " with row type: [" + joinRel.getRowType() + "]");
    }

    // 2. Convert join condition
    JoinPredicateInfo joinPredInfo = JoinPredicateInfo.constructJoinPredicateInfo(joinRel);

    // 3. Extract join keys from condition
    ExprNodeDesc[][] joinKeys = extractJoinKeys(joinPredInfo, joinRel.getInputs());

    // 4. For each input, we generate the corresponding ReduceSink child
    List<Operator<?>> children = new ArrayList<Operator<?>>();
    for (int i = 0; i < inputs.length; i++) {
      // Generate a ReduceSink operator for each join child
      ReduceSinkOperator child = genReduceSink(inputs[i].inputs.get(0), joinKeys[i],
              i, -1, Operation.NOT_ACID, strictMode);
      children.add(child);
    }

    // 5. Generate Join operator
    JoinOperator joinOp = genJoin(joinRel, joinPredInfo, children, joinKeys);

    // 6. TODO: Extract condition for non-equi join elements (if any) and add it

    // 7. Virtual columns
    Map<Integer,VirtualColumn> vcolMap = new HashMap<Integer,VirtualColumn>();
    vcolMap.putAll(inputs[0].vcolMap);
    if (extractJoinType(joinRel) != JoinType.LEFTSEMI) {
      int shift = inputs[0].inputs.get(0).getSchema().getSignature().size();
      for (int i=1; i<inputs.length; i++) {
        vcolMap.putAll(HiveCalciteUtil.shiftVColsMap(inputs[i].vcolMap,shift));
        shift += inputs[i].inputs.get(0).getSchema().getSignature().size();
      }
    }

    // 8. Return result
    return new OpAttr(null, vcolMap, joinOp);
  }
  
  OpAttr visit(HiveSort sortRel) throws SemanticException {
    OpAttr inputOpAf = dispatch(sortRel.getInput());
  
    if (LOG.isDebugEnabled()) {
      LOG.debug("Translating operator rel#" + sortRel.getId() + ":" + sortRel.getRelTypeName() +
              " with row type: [" + sortRel.getRowType() + "]");
      if (sortRel.getCollation() == RelCollations.EMPTY) {
        LOG.debug("Operator rel#" + sortRel.getId() + ":" + sortRel.getRelTypeName() +
              " consists of limit");
      }
      else if (sortRel.fetch == null) {
        LOG.debug("Operator rel#" + sortRel.getId() + ":" + sortRel.getRelTypeName() +
              " consists of sort");
      }
      else {
        LOG.debug("Operator rel#" + sortRel.getId() + ":" + sortRel.getRelTypeName() +
              " consists of sort+limit");
      }
    }
  
    Operator<?> inputOp = inputOpAf.inputs.get(0);
    Operator<?> resultOp = inputOpAf.inputs.get(0);
    // 1. If we need to sort tuples based on the value of some
    //    of their columns
    if (sortRel.getCollation() != RelCollations.EMPTY) {
  
      // In strict mode, in the presence of order by, limit must be specified
      if (strictMode && sortRel.fetch == null) {
        throw new SemanticException(ErrorMsg.NO_LIMIT_WITH_ORDERBY.getMsg());
      }
  
      // 1.a. Extract order for each column from collation
      //      Generate sortCols and order
      List<ExprNodeDesc> sortCols = new ArrayList<ExprNodeDesc>();
      StringBuilder order = new StringBuilder();
      for (RelCollation collation : sortRel.getCollationList()) {
        for (RelFieldCollation sortInfo : collation.getFieldCollations()) {
          int sortColumnPos = sortInfo.getFieldIndex();
          ColumnInfo columnInfo = new ColumnInfo(inputOp.getSchema().getSignature().
                  get(sortColumnPos));
          ExprNodeColumnDesc sortColumn = new ExprNodeColumnDesc(columnInfo.getType(),
              columnInfo.getInternalName(), columnInfo.getTabAlias(),
              columnInfo.getIsVirtualCol());
          sortCols.add(sortColumn);
          if (sortInfo.getDirection() == RelFieldCollation.Direction.DESCENDING) {
            order.append("-");
          }
          else {
            order.append("+");
          }
        }
      }
      // Use only 1 reducer for order by
      int numReducers = 1;
  
      // 1.b. Generate reduce sink 
      resultOp = genReduceSink(resultOp, sortCols.toArray(new ExprNodeDesc[sortCols.size()]),
              -1, new ArrayList<ExprNodeDesc>(), order.toString(), numReducers,
              Operation.NOT_ACID, strictMode);
  
      // 1.c. Generate project operator
      Map<String, ExprNodeDesc> descriptors = buildBacktrackFromReduceSink(
              (ReduceSinkOperator) resultOp, inputOp);
      SelectDesc selectDesc = new SelectDesc(
              new ArrayList<ExprNodeDesc>(descriptors.values()),
              new ArrayList<String>(descriptors.keySet()));
      ArrayList<ColumnInfo> cinfoLst = createColInfos(inputOp);
      resultOp = OperatorFactory.getAndMakeChild(selectDesc,
              new RowSchema(cinfoLst), resultOp);
      resultOp.setColumnExprMap(descriptors);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Generated " + resultOp + " with row schema: [" + resultOp.getSchema() + "]");
      }
    }
  
    // 2. If we need to generate limit
    if (sortRel.fetch != null) {
      int limit = RexLiteral.intValue(sortRel.fetch);
      LimitDesc limitDesc = new LimitDesc(limit);
      // TODO: Set 'last limit' global property
      ArrayList<ColumnInfo> cinfoLst = createColInfos(inputOp);
      resultOp = (LimitOperator) OperatorFactory.getAndMakeChild(
          limitDesc, new RowSchema(cinfoLst), resultOp);
  
      if (LOG.isDebugEnabled()) {
        LOG.debug("Generated " + resultOp + " with row schema: [" + resultOp.getSchema() + "]");
      }
    }
  
    // 3. Return result
    return inputOpAf.clone(resultOp);
  }

  private static ExprNodeDesc[][] extractJoinKeys(JoinPredicateInfo joinPredInfo,
          List<RelNode> inputs) {
    ExprNodeDesc[][] joinKeys = new ExprNodeDesc[inputs.size()][];
    for (int i = 0; i < inputs.size(); i++) {
      joinKeys[i] = new ExprNodeDesc[joinPredInfo.getEquiJoinPredicateElements().size()];
      for (int j = 0; j < joinPredInfo.getEquiJoinPredicateElements().size(); j++) {
        JoinLeafPredicateInfo joinLeafPredInfo = joinPredInfo.getEquiJoinPredicateElements().get(j);
        RexNode key = joinLeafPredInfo.getJoinKeyExprs(j).get(0);
        joinKeys[i][j] = convertToExprNode(key, inputs.get(j), null);
      }
    }
    return joinKeys;
  }

  private static ReduceSinkOperator genReduceSink(Operator<?> input, ExprNodeDesc[] keys,
          int tag, int numReducers, Operation acidOperation,
          boolean strictMode) throws SemanticException {
    return genReduceSink(input, keys, tag, new ArrayList<ExprNodeDesc>(),
            "", numReducers, acidOperation, strictMode);
  }
          
  @SuppressWarnings({ "rawtypes", "unchecked" })
  private static ReduceSinkOperator genReduceSink(Operator<?> input,
      ExprNodeDesc[] keys, int tag, ArrayList<ExprNodeDesc> partitionCols,
      String order, int numReducers, Operation acidOperation,
      boolean strictMode) throws SemanticException {
    Operator dummy = Operator.createDummy();  // dummy for backtracking
    dummy.setParentOperators(Arrays.asList(input));

    ArrayList<ExprNodeDesc> reduceKeys = new ArrayList<ExprNodeDesc>();
    ArrayList<ExprNodeDesc> reduceKeysBack = new ArrayList<ExprNodeDesc>();

    // Compute join keys and store in reduceKeys
    for (ExprNodeDesc key : keys) {
      reduceKeys.add(key);
      reduceKeysBack.add(ExprNodeDescUtils.backtrack(key, dummy, input));
    }

    // Walk over the input schema and copy in the output
    ArrayList<ExprNodeDesc> reduceValues = new ArrayList<ExprNodeDesc>();
    ArrayList<ExprNodeDesc> reduceValuesBack = new ArrayList<ExprNodeDesc>();
    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();

    List<ColumnInfo> inputColumns = input.getSchema().getSignature();
    ArrayList<ColumnInfo> outputColumns = new ArrayList<ColumnInfo>();
    List<String> outputColumnNames = new ArrayList<String>();
    int[] index = new int[inputColumns.size()];
    for (int i = 0; i < inputColumns.size(); i++) {
      ColumnInfo colInfo = inputColumns.get(i);
      String outputColName = colInfo.getInternalName();
      ExprNodeDesc expr = new ExprNodeColumnDesc(colInfo);

      // backtrack can be null when input is script operator
      ExprNodeDesc exprBack = ExprNodeDescUtils.backtrack(expr, dummy, input);
      int kindex = exprBack == null ? -1 : ExprNodeDescUtils.indexOf(exprBack, reduceKeysBack);
      if (kindex >= 0) {
        ColumnInfo newColInfo = new ColumnInfo(colInfo);
        newColInfo.setInternalName(Utilities.ReduceField.KEY + ".reducesinkkey" + kindex);
        newColInfo.setAlias(outputColName);
        newColInfo.setTabAlias(colInfo.getTabAlias());
        outputColumns.add(newColInfo);
        index[i] = kindex;
        continue;
      }
      int vindex = exprBack == null ? -1 : ExprNodeDescUtils.indexOf(exprBack, reduceValuesBack);
      if (kindex >= 0) {
        index[i] = -vindex - 1;
        continue;
      }
      index[i] = -reduceValues.size() - 1;

      reduceValues.add(expr);
      reduceValuesBack.add(exprBack);

      ColumnInfo newColInfo = new ColumnInfo(colInfo);
      newColInfo.setInternalName(Utilities.ReduceField.VALUE + "." + outputColName);
      newColInfo.setAlias(outputColName);
      newColInfo.setTabAlias(colInfo.getTabAlias());

      outputColumns.add(newColInfo);
      outputColumnNames.add(outputColName);
    }
    dummy.setParentOperators(null);

    // Use only 1 reducer if no reduce keys
    if (reduceKeys.size() == 0) {
      numReducers = 1;

      // Cartesian product is not supported in strict mode
      if (strictMode) {
        throw new SemanticException(ErrorMsg.NO_CARTESIAN_PRODUCT.getMsg());
      }
    }

    ReduceSinkDesc rsDesc;
    if (order.isEmpty()) {
      rsDesc = PlanUtils.getReduceSinkDesc(reduceKeys, reduceValues,
              outputColumnNames, false, tag, reduceKeys.size(),
              numReducers, acidOperation);
    } else {
      rsDesc = PlanUtils.getReduceSinkDesc(reduceKeys, reduceValues,
              outputColumnNames, false, tag, partitionCols,
              order, numReducers, acidOperation);
    }

    ReduceSinkOperator rsOp = (ReduceSinkOperator) OperatorFactory.getAndMakeChild(rsDesc,
            new RowSchema(outputColumns), input);

    List<String> keyColNames = rsDesc.getOutputKeyColumnNames();
    for (int i = 0 ; i < keyColNames.size(); i++) {
      colExprMap.put(Utilities.ReduceField.KEY + "." + keyColNames.get(i), reduceKeys.get(i));
    }
    List<String> valColNames = rsDesc.getOutputValueColumnNames();
    for (int i = 0 ; i < valColNames.size(); i++) {
      colExprMap.put(Utilities.ReduceField.VALUE + "." + valColNames.get(i), reduceValues.get(i));
    }

    rsOp.setValueIndex(index);
    rsOp.setColumnExprMap(colExprMap);
    rsOp.setInputAliases(input.getSchema().getColumnNames().toArray(
            new String[input.getSchema().getColumnNames().size()]));

    if (LOG.isDebugEnabled()) {
      LOG.debug("Generated " + rsOp + " with row schema: [" + rsOp.getSchema() + "]");
    }

    return rsOp;
  }

  private static JoinOperator genJoin(HiveJoin hiveJoin, JoinPredicateInfo joinPredInfo,
      List<Operator<?>> children, ExprNodeDesc[][] joinKeys) throws SemanticException {

    // Extract join type
    JoinType joinType = extractJoinType(hiveJoin);
    
    // NOTE: Currently binary joins only
    JoinCondDesc[] joinCondns = new JoinCondDesc[1];
    joinCondns[0] = new JoinCondDesc(new JoinCond(0, 1, joinType));

    ArrayList<ColumnInfo> outputColumns = new ArrayList<ColumnInfo>();
    ArrayList<String> outputColumnNames =
            new ArrayList<String>(hiveJoin.getRowType().getFieldNames());
    Operator<?>[] childOps = new Operator[children.size()];

    Map<String, Byte> reversedExprs = new HashMap<String, Byte>();
    HashMap<Byte, List<ExprNodeDesc>> exprMap = new HashMap<Byte, List<ExprNodeDesc>>();
    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
    HashMap<Integer, Set<String>> posToAliasMap = new HashMap<Integer, Set<String>>();

    int outputPos = 0;
    for (int pos = 0; pos < children.size(); pos++) {
      ReduceSinkOperator inputRS = (ReduceSinkOperator) children.get(pos);
      if (inputRS.getNumParent() != 1) {
        throw new SemanticException("RS should have single parent");
      }
      Operator<?> parent = inputRS.getParentOperators().get(0);
      ReduceSinkDesc rsDesc = inputRS.getConf();

      int[] index = inputRS.getValueIndex();

      Byte tag = (byte) rsDesc.getTag();

      // Semijoin
      if (joinType == JoinType.LEFTSEMI && pos != 0) {
        exprMap.put(tag, new ArrayList<ExprNodeDesc>());
        childOps[pos] = inputRS;
        continue;
      }

      List<String> keyColNames = rsDesc.getOutputKeyColumnNames();
      List<String> valColNames = rsDesc.getOutputValueColumnNames();

      posToAliasMap.put(pos, new HashSet<String>(inputRS.getSchema().getTableNames()));

      Map<String, ExprNodeDesc> descriptors = buildBacktrackFromReduceSink(outputPos,
              outputColumnNames, keyColNames, valColNames, index, parent);

      List<ColumnInfo> parentColumns = parent.getSchema().getSignature();
      for (int i = 0; i < index.length; i++) {
        ColumnInfo info = new ColumnInfo(parentColumns.get(i));
        info.setInternalName(outputColumnNames.get(outputPos));
        outputColumns.add(info);
        reversedExprs.put(outputColumnNames.get(outputPos), tag);
        outputPos++;
      }

      exprMap.put(tag, new ArrayList<ExprNodeDesc>(descriptors.values()));
      colExprMap.putAll(descriptors);
      childOps[pos] = inputRS;
    }

    boolean noOuterJoin = joinType != JoinType.FULLOUTER
            && joinType != JoinType.LEFTOUTER
            && joinType != JoinType.RIGHTOUTER;
    JoinDesc desc = new JoinDesc(exprMap, outputColumnNames, noOuterJoin,
            joinCondns, joinKeys);
    desc.setReversedExprs(reversedExprs);

    JoinOperator joinOp = (JoinOperator) OperatorFactory.getAndMakeChild(desc,
        new RowSchema(outputColumns), childOps);
    joinOp.setColumnExprMap(colExprMap);
    joinOp.setPosToAliasMap(posToAliasMap);

    // TODO: null safes?

    if (LOG.isDebugEnabled()) {
      LOG.debug("Generated " + joinOp + " with row schema: [" + joinOp.getSchema() + "]");
    }

    return joinOp;
  }

  private static JoinType extractJoinType(HiveJoin join) {
    // UNIQUE
    if (join.isDistinct()) {
      return JoinType.UNIQUE;
    }
    // SEMIJOIN
    if (join.isLeftSemiJoin()) {
      return JoinType.LEFTSEMI;
    }
    // OUTER AND INNER JOINS
    JoinType resultJoinType;
    switch (join.getJoinType()) {
      case FULL:
        resultJoinType = JoinType.FULLOUTER;
        break;
      case LEFT:
        resultJoinType = JoinType.LEFTOUTER;
        break;
      case RIGHT:
        resultJoinType = JoinType.RIGHTOUTER;
        break;
      default:
        resultJoinType = JoinType.INNER;
        break;
    }
    return resultJoinType;
  }

  private static Map<String, ExprNodeDesc> buildBacktrackFromReduceSink(
          ReduceSinkOperator rsOp, Operator<?> inputOp) {
    return buildBacktrackFromReduceSink(0, inputOp.getSchema().getColumnNames(),
            rsOp.getConf().getOutputKeyColumnNames(), rsOp.getConf().getOutputValueColumnNames(),
            rsOp.getValueIndex(), inputOp);
  }

  private static Map<String, ExprNodeDesc> buildBacktrackFromReduceSink(int initialPos,
          List<String> outputColumnNames, List<String> keyColNames, List<String> valueColNames,
          int[] index, Operator<?> inputOp) {
    Map<String, ExprNodeDesc> columnDescriptors = new LinkedHashMap<String, ExprNodeDesc>();
    for (int i = 0; i < index.length; i++) {
      ColumnInfo info = new ColumnInfo(inputOp.getSchema().getSignature().get(i));
      String field;
      if (index[i] >= 0) {
        field = Utilities.ReduceField.KEY + "." + keyColNames.get(index[i]);
      } else {
        field = Utilities.ReduceField.VALUE + "." + valueColNames.get(-index[i] - 1);
      }
      ExprNodeColumnDesc desc = new ExprNodeColumnDesc(info.getType(),
          field, info.getTabAlias(), info.getIsVirtualCol());
      columnDescriptors.put(outputColumnNames.get(initialPos+i), desc);
    }
    return columnDescriptors;
  }

  private static ExprNodeDesc convertToExprNode(RexNode rn, RelNode inputRel,
      String tabAlias) {
    return (ExprNodeDesc) rn.accept(new ExprNodeConverter(tabAlias,
            inputRel.getRowType(), false));
  }

  private static ArrayList<ColumnInfo> createColInfos(Operator<?> input) {
    ArrayList<ColumnInfo> cInfoLst = new ArrayList<ColumnInfo>();
    for (ColumnInfo ci : input.getSchema().getSignature()) {
      cInfoLst.add(new ColumnInfo(ci));
    }
    return cInfoLst;
  }

  private static Pair<ArrayList<ColumnInfo>, Map<Integer, VirtualColumn>> createColInfos(
      List<RexNode> calciteExprs, List<ExprNodeDesc> hiveExprs, List<String> projNames,
      OpAttr inpOpAf) {
    if (hiveExprs.size() != projNames.size()) {
      throw new RuntimeException("Column expressions list doesn't match Column Names list");
    }

    RexNode rexN;
    ExprNodeDesc pe;
    ArrayList<ColumnInfo> colInfos = new ArrayList<ColumnInfo>();
    VirtualColumn vc;
    Map<Integer, VirtualColumn> newVColMap = new HashMap<Integer, VirtualColumn>();
    for (int i = 0; i < hiveExprs.size(); i++) {
      pe = hiveExprs.get(i);
      rexN = calciteExprs.get(i);
      vc = null;
      if (rexN instanceof RexInputRef) {
        vc = inpOpAf.vcolMap.get(((RexInputRef) rexN).getIndex());
        if (vc != null) {
          newVColMap.put(i, vc);
        }
      }
      colInfos
          .add(new ColumnInfo(projNames.get(i), pe.getTypeInfo(), inpOpAf.tabAlias, vc != null));
    }

    return new Pair<ArrayList<ColumnInfo>, Map<Integer, VirtualColumn>>(colInfos, newVColMap);
  }

}
