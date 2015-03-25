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
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistribution.Type;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.SemiJoin;
import org.apache.calcite.rel.logical.LogicalExchange;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.LimitOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.AcidUtils.Operation;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil.JoinLeafPredicateInfo;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil.JoinPredicateInfo;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSort;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveUnion;
import org.apache.hadoop.hive.ql.parse.JoinCond;
import org.apache.hadoop.hive.ql.parse.JoinType;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.OrderExpression;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.PartitionExpression;
import org.apache.hadoop.hive.ql.parse.PTFTranslator;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.UnparseTranslator;
import org.apache.hadoop.hive.ql.parse.WindowingComponentizer;
import org.apache.hadoop.hive.ql.parse.WindowingSpec;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.apache.hadoop.hive.ql.plan.JoinCondDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.LimitDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PTFDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.plan.UnionDesc;

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
  private final SemanticAnalyzer                              semanticAnalyzer;
  private final HiveConf                                      hiveConf;
  private final UnparseTranslator                             unparseTranslator;
  private final Map<String, Operator<? extends OperatorDesc>> topOps;
  private final boolean                                       strictMode;
  private int                                                 reduceSinkTagGenerator;

  public HiveOpConverter(SemanticAnalyzer semanticAnalyzer, HiveConf hiveConf,
      UnparseTranslator unparseTranslator, Map<String, Operator<? extends OperatorDesc>> topOps,
      boolean strictMode) {
    this.semanticAnalyzer = semanticAnalyzer;
    this.hiveConf = hiveConf;
    this.unparseTranslator = unparseTranslator;
    this.topOps = topOps;
    this.strictMode = strictMode;
    this.reduceSinkTagGenerator = 0;
  }

  static class OpAttr {
    final String                         tabAlias;
    ImmutableList<Operator>              inputs;
    ImmutableMap<Integer, VirtualColumn> vcolMap;

    OpAttr(String tabAlias, Map<Integer, VirtualColumn> vcolMap, Operator... inputs) {
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
    if (rn instanceof HiveTableScan) {
      return visit((HiveTableScan) rn);
    } else if (rn instanceof HiveProject) {
      return visit((HiveProject) rn);
    } else if (rn instanceof HiveJoin) {
      return visit((HiveJoin) rn);
    } else if (rn instanceof SemiJoin) {
      SemiJoin sj = (SemiJoin) rn;
      HiveJoin hj = HiveJoin.getJoin(sj.getCluster(), sj.getLeft(), sj.getRight(),
          sj.getCondition(), sj.getJoinType(), true);
      return visit(hj);
    } else if (rn instanceof HiveFilter) {
      return visit((HiveFilter) rn);
    } else if (rn instanceof HiveSort) {
      return visit((HiveSort) rn);
    } else if (rn instanceof HiveUnion) {
      return visit((HiveUnion) rn);
    } else if (rn instanceof LogicalExchange) {
      return visit((LogicalExchange) rn);
    } else if (rn instanceof HiveAggregate) {
      return visit((HiveAggregate) rn);
    }
    LOG.error(rn.getClass().getCanonicalName() + "operator translation not supported"
        + " yet in return path.");
    return null;
  }

  /**
   * TODO: 1. PPD needs to get pushed in to TS
   * 
   * @param scanRel
   * @return
   */
  OpAttr visit(HiveTableScan scanRel) {

    if (LOG.isDebugEnabled()) {
      LOG.debug("Translating operator rel#" + scanRel.getId() + ":" + scanRel.getRelTypeName()
          + " with row type: [" + scanRel.getRowType() + "]");
    }

    RelOptHiveTable ht = (RelOptHiveTable) scanRel.getTable();
    Map<Integer, VirtualColumn> newVColMap = new HashMap<Integer, VirtualColumn>();

    // 1. Setup TableScan Desc
    // 1.1 Create TableScanDesc
    String tableAlias = ht.getTableAlias();
    List<VirtualColumn> virtualCols = new ArrayList<VirtualColumn>(ht.getVirtualCols());
    TableScanDesc tsd = new TableScanDesc(tableAlias, virtualCols, ht.getHiveTableMD());

    // 1.2. Set Partition cols in TSDesc
    List<ColumnInfo> partColInfos = ht.getPartColumns();
    List<String> partColNames = new ArrayList<String>();
    for (ColumnInfo ci : partColInfos) {
      partColNames.add(ci.getInternalName());
    }
    tsd.setPartColumns(partColNames);

    // 1.3. Set needed cols in TSDesc
    List<Integer> neededColumnIDs = new ArrayList<Integer>();
    List<String> neededColumns = new ArrayList<String>();
    Map<String, Integer> colNameToIndxMap = HiveCalciteUtil.getColNameIndxMap(ht.getHiveTableMD()
        .getCols());
    for (RelDataTypeField rdtf : scanRel.getRowType().getFieldList()) {
      neededColumnIDs.add(colNameToIndxMap.get(rdtf.getName()));
      neededColumns.add(rdtf.getName());
    }
    tsd.setNeededColumnIDs(neededColumnIDs);
    tsd.setNeededColumns(neededColumns);

    // 2. Setup TableScan
    TableScanOperator ts = null;
    // 2.1 Construct ordered colInfo list for TS RowSchema & update vcolMap
    ArrayList<ColumnInfo> colInfos = new ArrayList<ColumnInfo>(ht.getNonPartColumns());
    colInfos.addAll(ht.getPartColumns());
    ColumnInfo ci;
    for (VirtualColumn vc : virtualCols) {
      ci = new ColumnInfo(vc.getName(), vc.getTypeInfo(), tableAlias, true, vc.getIsHidden());
      colInfos.add(ci);
      newVColMap.put(colInfos.size(), vc);
    }

    // 2.2. Create TS OP
    ts = (TableScanOperator) OperatorFactory.get(tsd, new RowSchema(colInfos));

    topOps.put(ht.getQBID(), ts);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Generated " + ts + " with row schema: [" + ts.getSchema() + "]");
    }

    return new OpAttr(tableAlias, newVColMap, ts);
  }

  OpAttr visit(HiveProject projectRel) throws SemanticException {
    OpAttr inputOpAf = dispatch(projectRel.getInput());

    if (LOG.isDebugEnabled()) {
      LOG.debug("Translating operator rel#" + projectRel.getId() + ":"
          + projectRel.getRelTypeName() + " with row type: [" + projectRel.getRowType() + "]");
    }

    WindowingSpec windowingSpec = new WindowingSpec();
    List<ExprNodeDesc> exprCols = new ArrayList<ExprNodeDesc>();
    for (int pos = 0; pos < projectRel.getChildExps().size(); pos++) {
      ExprNodeConverter converter = new ExprNodeConverter(inputOpAf.tabAlias, projectRel
          .getRowType().getFieldNames().get(pos), projectRel.getInput().getRowType(),
          projectRel.getRowType(), false, projectRel.getCluster().getTypeFactory());
      exprCols.add((ExprNodeDesc) projectRel.getChildExps().get(pos).accept(converter));
      if (converter.getWindowFunctionSpec() != null) {
        windowingSpec.addWindowFunction(converter.getWindowFunctionSpec());
      }
    }
    if (windowingSpec.getWindowExpressions() != null
        && !windowingSpec.getWindowExpressions().isEmpty()) {
      inputOpAf = genPTF(inputOpAf, windowingSpec);
    }
    // TODO: is this a safe assumption (name collision, external names...)
    List<String> exprNames = new ArrayList<String>(projectRel.getRowType().getFieldNames());
    SelectDesc sd = new SelectDesc(exprCols, exprNames);
    Pair<ArrayList<ColumnInfo>, Map<Integer, VirtualColumn>> colInfoVColPair = createColInfos(
        projectRel.getChildExps(), exprCols, exprNames, inputOpAf);
    SelectOperator selOp = (SelectOperator) OperatorFactory.getAndMakeChild(sd, new RowSchema(
        colInfoVColPair.getKey()), inputOpAf.inputs.get(0));

    if (LOG.isDebugEnabled()) {
      LOG.debug("Generated " + selOp + " with row schema: [" + selOp.getSchema() + "]");
    }

    return new OpAttr(inputOpAf.tabAlias, colInfoVColPair.getValue(), selOp);
  }

  OpAttr visit(HiveJoin joinRel) throws SemanticException {
    // 1. Convert inputs
    OpAttr[] inputs = new OpAttr[joinRel.getInputs().size()];
    List<Operator<?>> children = new ArrayList<Operator<?>>(joinRel.getInputs().size());
    for (int i = 0; i < inputs.length; i++) {
      inputs[i] = dispatch(joinRel.getInput(i));
      children.add(inputs[i].inputs.get(0));
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Translating operator rel#" + joinRel.getId() + ":" + joinRel.getRelTypeName()
          + " with row type: [" + joinRel.getRowType() + "]");
    }

    // 2. Convert join condition
    JoinPredicateInfo joinPredInfo = JoinPredicateInfo.constructJoinPredicateInfo(joinRel);

    // 3. Extract join keys from condition
    ExprNodeDesc[][] joinKeys = extractJoinKeys(joinPredInfo, joinRel.getInputs());

    // 4. Generate Join operator
    JoinOperator joinOp = genJoin(joinRel, joinPredInfo, children, joinKeys);

    // 5. TODO: Extract condition for non-equi join elements (if any) and
    // add it

    // 6. Virtual columns
    Map<Integer, VirtualColumn> vcolMap = new HashMap<Integer, VirtualColumn>();
    vcolMap.putAll(inputs[0].vcolMap);
    if (extractJoinType(joinRel) != JoinType.LEFTSEMI) {
      int shift = inputs[0].inputs.get(0).getSchema().getSignature().size();
      for (int i = 1; i < inputs.length; i++) {
        vcolMap.putAll(HiveCalciteUtil.shiftVColsMap(inputs[i].vcolMap, shift));
        shift += inputs[i].inputs.get(0).getSchema().getSignature().size();
      }
    }

    // 8. Return result
    return new OpAttr(null, vcolMap, joinOp);
  }

  OpAttr visit(HiveAggregate aggRel) throws SemanticException {
    OpAttr inputOpAf = dispatch(aggRel.getInput());
    return HiveGBOpConvUtil.translateGB(inputOpAf, aggRel, hiveConf);
  }

  OpAttr visit(HiveSort sortRel) throws SemanticException {
    OpAttr inputOpAf = dispatch(sortRel.getInput());

    if (LOG.isDebugEnabled()) {
      LOG.debug("Translating operator rel#" + sortRel.getId() + ":" + sortRel.getRelTypeName()
          + " with row type: [" + sortRel.getRowType() + "]");
      if (sortRel.getCollation() == RelCollations.EMPTY) {
        LOG.debug("Operator rel#" + sortRel.getId() + ":" + sortRel.getRelTypeName()
            + " consists of limit");
      } else if (sortRel.fetch == null) {
        LOG.debug("Operator rel#" + sortRel.getId() + ":" + sortRel.getRelTypeName()
            + " consists of sort");
      } else {
        LOG.debug("Operator rel#" + sortRel.getId() + ":" + sortRel.getRelTypeName()
            + " consists of sort+limit");
      }
    }

    Operator<?> inputOp = inputOpAf.inputs.get(0);
    Operator<?> resultOp = inputOpAf.inputs.get(0);
    // 1. If we need to sort tuples based on the value of some
    // of their columns
    if (sortRel.getCollation() != RelCollations.EMPTY) {

      // In strict mode, in the presence of order by, limit must be
      // specified
      if (strictMode && sortRel.fetch == null) {
        throw new SemanticException(ErrorMsg.NO_LIMIT_WITH_ORDERBY.getMsg());
      }

      // 1.a. Extract order for each column from collation
      // Generate sortCols and order
      List<ExprNodeDesc> sortCols = new ArrayList<ExprNodeDesc>();
      StringBuilder order = new StringBuilder();
      for (RelCollation collation : sortRel.getCollationList()) {
        for (RelFieldCollation sortInfo : collation.getFieldCollations()) {
          int sortColumnPos = sortInfo.getFieldIndex();
          ColumnInfo columnInfo = new ColumnInfo(inputOp.getSchema().getSignature()
              .get(sortColumnPos));
          ExprNodeColumnDesc sortColumn = new ExprNodeColumnDesc(columnInfo.getType(),
              columnInfo.getInternalName(), columnInfo.getTabAlias(), columnInfo.getIsVirtualCol());
          sortCols.add(sortColumn);
          if (sortInfo.getDirection() == RelFieldCollation.Direction.DESCENDING) {
            order.append("-");
          } else {
            order.append("+");
          }
        }
      }
      // Use only 1 reducer for order by
      int numReducers = 1;

      // 1.b. Generate reduce sink and project operator
      resultOp = genReduceSinkAndBacktrackSelect(resultOp,
          sortCols.toArray(new ExprNodeDesc[sortCols.size()]), -1, new ArrayList<ExprNodeDesc>(),
          order.toString(), numReducers, Operation.NOT_ACID, strictMode);
    }

    // 2. If we need to generate limit
    if (sortRel.fetch != null) {
      int limit = RexLiteral.intValue(sortRel.fetch);
      LimitDesc limitDesc = new LimitDesc(limit);
      // TODO: Set 'last limit' global property
      ArrayList<ColumnInfo> cinfoLst = createColInfos(inputOp);
      resultOp = (LimitOperator) OperatorFactory.getAndMakeChild(limitDesc,
          new RowSchema(cinfoLst), resultOp);

      if (LOG.isDebugEnabled()) {
        LOG.debug("Generated " + resultOp + " with row schema: [" + resultOp.getSchema() + "]");
      }
    }

    // 3. Return result
    return inputOpAf.clone(resultOp);
  }

  /**
   * TODO: 1) isSamplingPred 2) sampleDesc 3) isSortedFilter
   */
  OpAttr visit(HiveFilter filterRel) throws SemanticException {
    OpAttr inputOpAf = dispatch(filterRel.getInput());

    if (LOG.isDebugEnabled()) {
      LOG.debug("Translating operator rel#" + filterRel.getId() + ":" + filterRel.getRelTypeName()
          + " with row type: [" + filterRel.getRowType() + "]");
    }

    ExprNodeDesc filCondExpr = filterRel.getCondition().accept(
        new ExprNodeConverter(inputOpAf.tabAlias, filterRel.getInput().getRowType(), false,
            filterRel.getCluster().getTypeFactory()));
    FilterDesc filDesc = new FilterDesc(filCondExpr, false);
    ArrayList<ColumnInfo> cinfoLst = createColInfos(inputOpAf.inputs.get(0));
    FilterOperator filOp = (FilterOperator) OperatorFactory.getAndMakeChild(filDesc, new RowSchema(
        cinfoLst), inputOpAf.inputs.get(0));

    if (LOG.isDebugEnabled()) {
      LOG.debug("Generated " + filOp + " with row schema: [" + filOp.getSchema() + "]");
    }

    return inputOpAf.clone(filOp);
  }

  OpAttr visit(HiveUnion unionRel) throws SemanticException {
    // 1. Convert inputs
    OpAttr[] inputs = new OpAttr[unionRel.getInputs().size()];
    for (int i = 0; i < inputs.length; i++) {
      inputs[i] = dispatch(unionRel.getInput(i));
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Translating operator rel#" + unionRel.getId() + ":" + unionRel.getRelTypeName()
          + " with row type: [" + unionRel.getRowType() + "]");
    }

    // 2. Create a new union operator
    UnionDesc unionDesc = new UnionDesc();
    unionDesc.setNumInputs(inputs.length);
    ArrayList<ColumnInfo> cinfoLst = createColInfos(inputs[0].inputs.get(0));
    Operator<?>[] children = new Operator<?>[inputs.length];
    for (int i = 0; i < children.length; i++) {
      children[i] = inputs[i].inputs.get(0);
    }
    Operator<? extends OperatorDesc> unionOp = OperatorFactory.getAndMakeChild(unionDesc,
        new RowSchema(cinfoLst), children);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Generated " + unionOp + " with row schema: [" + unionOp.getSchema() + "]");
    }

    // 3. Return result
    return inputs[0].clone(unionOp);
  }

  OpAttr visit(LogicalExchange exchangeRel) throws SemanticException {
    OpAttr inputOpAf = dispatch(exchangeRel.getInput());

    if (LOG.isDebugEnabled()) {
      LOG.debug("Translating operator rel#" + exchangeRel.getId() + ":"
          + exchangeRel.getRelTypeName() + " with row type: [" + exchangeRel.getRowType() + "]");
    }

    RelDistribution distribution = exchangeRel.getDistribution();
    if (distribution.getType() != Type.HASH_DISTRIBUTED) {
      throw new SemanticException("Only hash distribution supported for LogicalExchange");
    }
    ExprNodeDesc[] expressions = new ExprNodeDesc[distribution.getKeys().size()];
    for (int i = 0; i < distribution.getKeys().size(); i++) {
      int key = distribution.getKeys().get(i);
      ColumnInfo colInfo = inputOpAf.inputs.get(0).getSchema().getSignature().get(key);
      ExprNodeDesc column = new ExprNodeColumnDesc(colInfo);
      expressions[i] = column;
    }

    ReduceSinkOperator rsOp = genReduceSink(inputOpAf.inputs.get(0), expressions,
        reduceSinkTagGenerator++, -1, Operation.NOT_ACID, strictMode);

    return inputOpAf.clone(rsOp);
  }

  private OpAttr genPTF(OpAttr inputOpAf, WindowingSpec wSpec) throws SemanticException {
    Operator<?> input = inputOpAf.inputs.get(0);

    wSpec.validateAndMakeEffective();
    WindowingComponentizer groups = new WindowingComponentizer(wSpec);
    RowResolver rr = new RowResolver();
    for (ColumnInfo ci : input.getSchema().getSignature()) {
      rr.put(ci.getTabAlias(), ci.getInternalName(), ci);
    }

    while (groups.hasNext()) {
      wSpec = groups.next(hiveConf, semanticAnalyzer, unparseTranslator, rr);

      // 1. Create RS and backtrack Select operator on top
      ArrayList<ExprNodeDesc> keyCols = new ArrayList<ExprNodeDesc>();
      ArrayList<ExprNodeDesc> partCols = new ArrayList<ExprNodeDesc>();
      StringBuilder order = new StringBuilder();

      for (PartitionExpression partCol : wSpec.getQueryPartitionSpec().getExpressions()) {
        ExprNodeDesc partExpr = semanticAnalyzer.genExprNodeDesc(partCol.getExpression(), rr);
        if (ExprNodeDescUtils.indexOf(partExpr, partCols) < 0) {
          keyCols.add(partExpr);
          partCols.add(partExpr);
          order.append('+');
        }
      }

      if (wSpec.getQueryOrderSpec() != null) {
        for (OrderExpression orderCol : wSpec.getQueryOrderSpec().getExpressions()) {
          ExprNodeDesc orderExpr = semanticAnalyzer.genExprNodeDesc(orderCol.getExpression(), rr);
          char orderChar = orderCol.getOrder() == PTFInvocationSpec.Order.ASC ? '+' : '-';
          int index = ExprNodeDescUtils.indexOf(orderExpr, keyCols);
          if (index >= 0) {
            order.setCharAt(index, orderChar);
            continue;
          }
          keyCols.add(orderExpr);
          order.append(orderChar);
        }
      }

      SelectOperator selectOp = genReduceSinkAndBacktrackSelect(input,
          keyCols.toArray(new ExprNodeDesc[keyCols.size()]), reduceSinkTagGenerator++, partCols,
          order.toString(), -1, Operation.NOT_ACID, strictMode);

      // 2. Finally create PTF
      PTFTranslator translator = new PTFTranslator();
      PTFDesc ptfDesc = translator.translate(wSpec, semanticAnalyzer, hiveConf, rr,
          unparseTranslator);
      RowResolver ptfOpRR = ptfDesc.getFuncDef().getOutputShape().getRr();

      Operator<?> ptfOp = OperatorFactory.getAndMakeChild(ptfDesc,
          new RowSchema(ptfOpRR.getColumnInfos()), selectOp);

      if (LOG.isDebugEnabled()) {
        LOG.debug("Generated " + ptfOp + " with row schema: [" + ptfOp.getSchema() + "]");
      }

      // 3. Prepare for next iteration (if any)
      rr = ptfOpRR;
      input = ptfOp;
    }

    return inputOpAf.clone(input);
  }

  private ExprNodeDesc[][] extractJoinKeys(JoinPredicateInfo joinPredInfo, List<RelNode> inputs) {
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

  private static SelectOperator genReduceSinkAndBacktrackSelect(Operator<?> input,
      ExprNodeDesc[] keys, int tag, ArrayList<ExprNodeDesc> partitionCols, String order,
      int numReducers, Operation acidOperation, boolean strictMode) throws SemanticException {
    // 1. Generate RS operator
    ReduceSinkOperator rsOp = genReduceSink(input, keys, tag, partitionCols, order, numReducers,
        acidOperation, strictMode);

    // 2. Generate backtrack Select operator
    Map<String, ExprNodeDesc> descriptors = buildBacktrackFromReduceSink((ReduceSinkOperator) rsOp,
        input);
    SelectDesc selectDesc = new SelectDesc(new ArrayList<ExprNodeDesc>(descriptors.values()),
        new ArrayList<String>(descriptors.keySet()));
    ArrayList<ColumnInfo> cinfoLst = createColInfos(input);
    SelectOperator selectOp = (SelectOperator) OperatorFactory.getAndMakeChild(selectDesc,
        new RowSchema(cinfoLst), rsOp);
    selectOp.setColumnExprMap(descriptors);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Generated " + selectOp + " with row schema: [" + selectOp.getSchema() + "]");
    }

    return selectOp;
  }

  private static ReduceSinkOperator genReduceSink(Operator<?> input, ExprNodeDesc[] keys, int tag,
      int numReducers, Operation acidOperation, boolean strictMode) throws SemanticException {
    return genReduceSink(input, keys, tag, new ArrayList<ExprNodeDesc>(), "", numReducers,
        acidOperation, strictMode);
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  private static ReduceSinkOperator genReduceSink(Operator<?> input, ExprNodeDesc[] keys, int tag,
      ArrayList<ExprNodeDesc> partitionCols, String order, int numReducers,
      Operation acidOperation, boolean strictMode) throws SemanticException {
    Operator dummy = Operator.createDummy(); // dummy for backtracking
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
      rsDesc = PlanUtils.getReduceSinkDesc(reduceKeys, reduceValues, outputColumnNames, false, tag,
          reduceKeys.size(), numReducers, acidOperation);
    } else {
      rsDesc = PlanUtils.getReduceSinkDesc(reduceKeys, reduceValues, outputColumnNames, false, tag,
          partitionCols, order, numReducers, acidOperation);
    }

    ReduceSinkOperator rsOp = (ReduceSinkOperator) OperatorFactory.getAndMakeChild(rsDesc,
        new RowSchema(outputColumns), input);

    List<String> keyColNames = rsDesc.getOutputKeyColumnNames();
    for (int i = 0; i < keyColNames.size(); i++) {
      colExprMap.put(Utilities.ReduceField.KEY + "." + keyColNames.get(i), reduceKeys.get(i));
    }
    List<String> valColNames = rsDesc.getOutputValueColumnNames();
    for (int i = 0; i < valColNames.size(); i++) {
      colExprMap.put(Utilities.ReduceField.VALUE + "." + valColNames.get(i), reduceValues.get(i));
    }

    rsOp.setValueIndex(index);
    rsOp.setColumnExprMap(colExprMap);
    rsOp.setInputAliases(input.getSchema().getColumnNames()
        .toArray(new String[input.getSchema().getColumnNames().size()]));

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
    ArrayList<String> outputColumnNames = new ArrayList<String>(hiveJoin.getRowType()
        .getFieldNames());
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

    boolean noOuterJoin = joinType != JoinType.FULLOUTER && joinType != JoinType.LEFTOUTER
        && joinType != JoinType.RIGHTOUTER;
    JoinDesc desc = new JoinDesc(exprMap, outputColumnNames, noOuterJoin, joinCondns, joinKeys);
    desc.setReversedExprs(reversedExprs);

    JoinOperator joinOp = (JoinOperator) OperatorFactory.getAndMakeChild(desc, new RowSchema(
        outputColumns), childOps);
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

  private static Map<String, ExprNodeDesc> buildBacktrackFromReduceSink(ReduceSinkOperator rsOp,
      Operator<?> inputOp) {
    return buildBacktrackFromReduceSink(0, inputOp.getSchema().getColumnNames(), rsOp.getConf()
        .getOutputKeyColumnNames(), rsOp.getConf().getOutputValueColumnNames(),
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
      ExprNodeColumnDesc desc = new ExprNodeColumnDesc(info.getType(), field, info.getTabAlias(),
          info.getIsVirtualCol());
      columnDescriptors.put(outputColumnNames.get(initialPos + i), desc);
    }
    return columnDescriptors;
  }

  private static ExprNodeDesc convertToExprNode(RexNode rn, RelNode inputRel, String tabAlias) {
    return (ExprNodeDesc) rn.accept(new ExprNodeConverter(tabAlias, inputRel.getRowType(), false,
        inputRel.getCluster().getTypeFactory()));
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
