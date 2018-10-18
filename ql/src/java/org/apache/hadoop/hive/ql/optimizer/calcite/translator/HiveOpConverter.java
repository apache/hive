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

package org.apache.hadoop.hive.ql.optimizer.calcite.translator;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistribution.Type;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.SemiJoin;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.StrictChecks;
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
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveMultiJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSemiJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortExchange;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortLimit;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveUnion;
import org.apache.hadoop.hive.ql.parse.JoinCond;
import org.apache.hadoop.hive.ql.parse.JoinType;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.OrderExpression;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.PartitionExpression;
import org.apache.hadoop.hive.ql.parse.PTFTranslator;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.UnparseTranslator;
import org.apache.hadoop.hive.ql.parse.WindowingComponentizer;
import org.apache.hadoop.hive.ql.parse.WindowingSpec;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.WindowFunctionSpec;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
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
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

public class HiveOpConverter {

  private static final Logger LOG = LoggerFactory.getLogger(HiveOpConverter.class);

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
  private final Map<String, TableScanOperator>                topOps;
  private int                                                 uniqueCounter;

  public HiveOpConverter(SemanticAnalyzer semanticAnalyzer, HiveConf hiveConf,
      UnparseTranslator unparseTranslator, Map<String, TableScanOperator> topOps) {
    this.semanticAnalyzer = semanticAnalyzer;
    this.hiveConf = hiveConf;
    this.unparseTranslator = unparseTranslator;
    this.topOps = topOps;
    this.uniqueCounter = 0;
  }

  static class OpAttr {
    final String                         tabAlias;
    ImmutableList<Operator>              inputs;
    ImmutableSet<Integer>                vcolsInCalcite;

    OpAttr(String tabAlias, Set<Integer> vcols, Operator... inputs) {
      this.tabAlias = tabAlias;
      this.inputs = ImmutableList.copyOf(inputs);
      this.vcolsInCalcite = ImmutableSet.copyOf(vcols);
    }

    private OpAttr clone(Operator... inputs) {
      return new OpAttr(tabAlias, vcolsInCalcite, inputs);
    }
  }

  private void handleTopLimit(Operator<?> rootOp) {
    if (rootOp instanceof LimitOperator) {
      // this can happen only on top most limit, not while visiting Limit Operator
      // since that can be within subquery.
      this.semanticAnalyzer.getQB().getParseInfo().setOuterQueryLimit(((LimitOperator) rootOp).getConf().getLimit());
    }
  }

  public Operator convert(RelNode root) throws SemanticException {
    OpAttr opAf = dispatch(root);
    Operator rootOp = opAf.inputs.get(0);
    handleTopLimit(rootOp);
    return rootOp;
  }

  OpAttr dispatch(RelNode rn) throws SemanticException {
    if (rn instanceof HiveTableScan) {
      return visit((HiveTableScan) rn);
    } else if (rn instanceof HiveProject) {
      return visit((HiveProject) rn);
    } else if (rn instanceof HiveMultiJoin) {
      return visit((HiveMultiJoin) rn);
    } else if (rn instanceof HiveJoin) {
      return visit((HiveJoin) rn);
    } else if (rn instanceof SemiJoin) {
      return visit((SemiJoin)rn);
    } else if (rn instanceof HiveFilter) {
      return visit((HiveFilter) rn);
    } else if (rn instanceof HiveSortLimit) {
      return visit((HiveSortLimit) rn);
    } else if (rn instanceof HiveUnion) {
      return visit((HiveUnion) rn);
    } else if (rn instanceof HiveSortExchange) {
      return visit((HiveSortExchange) rn);
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

    // 1. Setup TableScan Desc
    // 1.1 Build col details used by scan
    ArrayList<ColumnInfo> colInfos = new ArrayList<ColumnInfo>();
    List<VirtualColumn> virtualCols = new ArrayList<VirtualColumn>();
    List<Integer> neededColumnIDs = new ArrayList<Integer>();
    List<String> neededColumnNames = new ArrayList<String>();
    Set<Integer> vcolsInCalcite = new HashSet<Integer>();

    List<String> partColNames = new ArrayList<String>();
    Map<Integer, VirtualColumn> VColsMap = HiveCalciteUtil.getVColsMap(ht.getVirtualCols(),
        ht.getNoOfNonVirtualCols());
    Map<Integer, ColumnInfo> posToPartColInfo = ht.getPartColInfoMap();
    Map<Integer, ColumnInfo> posToNonPartColInfo = ht.getNonPartColInfoMap();
    List<Integer> neededColIndxsFrmReloptHT = scanRel.getNeededColIndxsFrmReloptHT();
    List<String> scanColNames = scanRel.getRowType().getFieldNames();
    String tableAlias = scanRel.getConcatQbIDAlias();

    String colName;
    ColumnInfo colInfo;
    VirtualColumn vc;

    for (int index = 0; index < scanRel.getRowType().getFieldList().size(); index++) {
      colName = scanColNames.get(index);
      if (VColsMap.containsKey(index)) {
        vc = VColsMap.get(index);
        virtualCols.add(vc);
        colInfo = new ColumnInfo(vc.getName(), vc.getTypeInfo(), tableAlias, true, vc.getIsHidden());
        vcolsInCalcite.add(index);
      } else if (posToPartColInfo.containsKey(index)) {
        partColNames.add(colName);
        colInfo = posToPartColInfo.get(index);
        vcolsInCalcite.add(index);
      } else {
        colInfo = posToNonPartColInfo.get(index);
      }
      colInfos.add(colInfo);
      if (neededColIndxsFrmReloptHT.contains(index)) {
        neededColumnIDs.add(index);
        neededColumnNames.add(colName);
      }
    }

    // 1.2 Create TableScanDesc
    TableScanDesc tsd = new TableScanDesc(tableAlias, virtualCols, ht.getHiveTableMD());

    // 1.3. Set Partition cols in TSDesc
    tsd.setPartColumns(partColNames);

    // 1.4. Set needed cols in TSDesc
    tsd.setNeededColumnIDs(neededColumnIDs);
    tsd.setNeededColumns(neededColumnNames);

    // 2. Setup TableScan
    TableScanOperator ts = (TableScanOperator) OperatorFactory.get(
        semanticAnalyzer.getOpContext(), tsd, new RowSchema(colInfos));

    //now that we let Calcite process subqueries we might have more than one
    // tablescan with same alias.
    if(topOps.get(tableAlias) != null) {
      tableAlias = tableAlias +  this.uniqueCounter ;
    }
    topOps.put(tableAlias, ts);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Generated " + ts + " with row schema: [" + ts.getSchema() + "]");
    }

    return new OpAttr(tableAlias, vcolsInCalcite, ts);
  }

  OpAttr visit(HiveProject projectRel) throws SemanticException {
    OpAttr inputOpAf = dispatch(projectRel.getInput());

    if (LOG.isDebugEnabled()) {
      LOG.debug("Translating operator rel#" + projectRel.getId() + ":"
          + projectRel.getRelTypeName() + " with row type: [" + projectRel.getRowType() + "]");
    }

    WindowingSpec windowingSpec = new WindowingSpec();
    List<String> exprNames = new ArrayList<String>(projectRel.getRowType().getFieldNames());
    List<ExprNodeDesc> exprCols = new ArrayList<ExprNodeDesc>();
    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
    for (int pos = 0; pos < projectRel.getChildExps().size(); pos++) {
      ExprNodeConverter converter = new ExprNodeConverter(inputOpAf.tabAlias, projectRel
          .getRowType().getFieldNames().get(pos), projectRel.getInput().getRowType(),
          projectRel.getRowType(), inputOpAf.vcolsInCalcite, projectRel.getCluster().getTypeFactory(),
          true);
      ExprNodeDesc exprCol = projectRel.getChildExps().get(pos).accept(converter);
      colExprMap.put(exprNames.get(pos), exprCol);
      exprCols.add(exprCol);
      //TODO: Cols that come through PTF should it retain (VirtualColumness)?
      if (converter.getWindowFunctionSpec() != null) {
        for (WindowFunctionSpec wfs : converter.getWindowFunctionSpec()) {
          windowingSpec.addWindowFunction(wfs);
        }
      }
    }
    if (windowingSpec.getWindowExpressions() != null
        && !windowingSpec.getWindowExpressions().isEmpty()) {
      inputOpAf = genPTF(inputOpAf, windowingSpec);
    }
    // TODO: is this a safe assumption (name collision, external names...)
    SelectDesc sd = new SelectDesc(exprCols, exprNames);
    Pair<ArrayList<ColumnInfo>, Set<Integer>> colInfoVColPair = createColInfos(
        projectRel.getChildExps(), exprCols, exprNames, inputOpAf);
    SelectOperator selOp = (SelectOperator) OperatorFactory.getAndMakeChild(sd, new RowSchema(
        colInfoVColPair.getKey()), inputOpAf.inputs.get(0));
    selOp.setColumnExprMap(colExprMap);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Generated " + selOp + " with row schema: [" + selOp.getSchema() + "]");
    }

    return new OpAttr(inputOpAf.tabAlias, colInfoVColPair.getValue(), selOp);
  }

  OpAttr visit(HiveMultiJoin joinRel) throws SemanticException {
    return translateJoin(joinRel);
  }

  OpAttr visit(HiveJoin joinRel) throws SemanticException {
    return translateJoin(joinRel);
  }


  OpAttr visit(SemiJoin joinRel) throws SemanticException {
    return translateJoin(joinRel);
  }

  private String getHiveDerivedTableAlias() {
    return "$hdt$_" + (this.uniqueCounter++);
  }

  private OpAttr translateJoin(RelNode joinRel) throws SemanticException {
    // 0. Additional data structures needed for the join optimization
    // through Hive
    String[] baseSrc = new String[joinRel.getInputs().size()];
    String tabAlias = getHiveDerivedTableAlias();

    // 1. Convert inputs
    OpAttr[] inputs = new OpAttr[joinRel.getInputs().size()];
    List<Operator<?>> children = new ArrayList<Operator<?>>(joinRel.getInputs().size());
    for (int i = 0; i < inputs.length; i++) {
      inputs[i] = dispatch(joinRel.getInput(i));
      children.add(inputs[i].inputs.get(0));
      baseSrc[i] = inputs[i].tabAlias;
    }

    // 2. Generate tags
    for (int tag=0; tag<children.size(); tag++) {
      ReduceSinkOperator reduceSinkOp = (ReduceSinkOperator) children.get(tag);
      reduceSinkOp.getConf().setTag(tag);
    }

    // 3. Virtual columns
    Set<Integer> newVcolsInCalcite = new HashSet<Integer>();
    newVcolsInCalcite.addAll(inputs[0].vcolsInCalcite);
    if (joinRel instanceof HiveMultiJoin ||
            !(joinRel instanceof SemiJoin)) {
      int shift = inputs[0].inputs.get(0).getSchema().getSignature().size();
      for (int i = 1; i < inputs.length; i++) {
        newVcolsInCalcite.addAll(HiveCalciteUtil.shiftVColsSet(inputs[i].vcolsInCalcite, shift));
        shift += inputs[i].inputs.get(0).getSchema().getSignature().size();
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Translating operator rel#" + joinRel.getId() + ":" + joinRel.getRelTypeName()
          + " with row type: [" + joinRel.getRowType() + "]");
    }

    // 4. Extract join key expressions from HiveSortExchange
    ExprNodeDesc[][] joinExpressions = new ExprNodeDesc[inputs.length][];
    for (int i = 0; i < inputs.length; i++) {
      joinExpressions[i] = ((HiveSortExchange) joinRel.getInput(i)).getJoinExpressions();
    }

    // 5. Extract rest of join predicate info. We infer the rest of join condition
    //    that will be added to the filters (join conditions that are not part of
    //    the join key)
    List<RexNode> joinFilters;
    if (joinRel instanceof HiveJoin) {
      joinFilters = ImmutableList.of(((HiveJoin)joinRel).getJoinFilter());
    } else if (joinRel instanceof HiveMultiJoin){
      joinFilters = ((HiveMultiJoin)joinRel).getJoinFilters();
    } else if (joinRel instanceof HiveSemiJoin){
      joinFilters = ImmutableList.of(((HiveSemiJoin)joinRel).getJoinFilter());
    } else {
      throw new SemanticException ("Can't handle join type: " + joinRel.getClass().getName());
    }
    List<List<ExprNodeDesc>> filterExpressions = Lists.newArrayList();
    for (int i = 0; i< joinFilters.size(); i++) {
      List<ExprNodeDesc> filterExpressionsForInput = new ArrayList<ExprNodeDesc>();
      if (joinFilters.get(i) != null) {
        for (RexNode conj : RelOptUtil.conjunctions(joinFilters.get(i))) {
          ExprNodeDesc expr = convertToExprNode(conj, joinRel, null, newVcolsInCalcite);
          filterExpressionsForInput.add(expr);
        }
      }
      filterExpressions.add(filterExpressionsForInput);
    }

    // 6. Generate Join operator
    JoinOperator joinOp = genJoin(joinRel, joinExpressions, filterExpressions, children,
            baseSrc, tabAlias);

    // 7. Return result
    return new OpAttr(tabAlias, newVcolsInCalcite, joinOp);
  }

  OpAttr visit(HiveAggregate aggRel) throws SemanticException {
    OpAttr inputOpAf = dispatch(aggRel.getInput());
    return HiveGBOpConvUtil.translateGB(inputOpAf, aggRel, hiveConf);
  }

  OpAttr visit(HiveSortLimit sortRel) throws SemanticException {
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

      // In strict mode, in the presence of order by, limit must be specified.
      if (sortRel.fetch == null) {
        String error = StrictChecks.checkNoLimit(hiveConf);
        if (error != null) throw new SemanticException(error);
      }

      // 1.a. Extract order for each column from collation
      // Generate sortCols and order
      ImmutableBitSet.Builder sortColsPosBuilder = ImmutableBitSet.builder();
      ImmutableBitSet.Builder sortOutputColsPosBuilder = ImmutableBitSet.builder();
      Map<Integer, RexNode> obRefToCallMap = sortRel.getInputRefToCallMap();
      List<ExprNodeDesc> sortCols = new ArrayList<ExprNodeDesc>();
      StringBuilder order = new StringBuilder();
      StringBuilder nullOrder = new StringBuilder();
      for (RelFieldCollation sortInfo : sortRel.getCollation().getFieldCollations()) {
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
        if (sortInfo.nullDirection == RelFieldCollation.NullDirection.FIRST) {
          nullOrder.append("a");
        } else if (sortInfo.nullDirection == RelFieldCollation.NullDirection.LAST) {
          nullOrder.append("z");
        } else {
          // Default
          nullOrder.append(sortInfo.getDirection() == RelFieldCollation.Direction.DESCENDING ? "z" : "a");
        }

        if (obRefToCallMap != null) {
          RexNode obExpr = obRefToCallMap.get(sortColumnPos);
          sortColsPosBuilder.set(sortColumnPos);
          if (obExpr == null) {
            sortOutputColsPosBuilder.set(sortColumnPos);
          }
        }
      }
      // Use only 1 reducer for order by
      int numReducers = 1;

      // We keep the columns only the columns that are part of the final output
      List<String> keepColumns = new ArrayList<String>();
      final ImmutableBitSet sortColsPos = sortColsPosBuilder.build();
      final ImmutableBitSet sortOutputColsPos = sortOutputColsPosBuilder.build();
      final ArrayList<ColumnInfo> inputSchema = inputOp.getSchema().getSignature();
      for (int pos=0; pos<inputSchema.size(); pos++) {
        if ((sortColsPos.get(pos) && sortOutputColsPos.get(pos)) ||
                (!sortColsPos.get(pos) && !sortOutputColsPos.get(pos))) {
          keepColumns.add(inputSchema.get(pos).getInternalName());
        }
      }

      // 1.b. Generate reduce sink and project operator
      resultOp = genReduceSinkAndBacktrackSelect(resultOp,
          sortCols.toArray(new ExprNodeDesc[sortCols.size()]), 0, new ArrayList<ExprNodeDesc>(),
          order.toString(), nullOrder.toString(), numReducers, Operation.NOT_ACID, hiveConf, keepColumns);
    }

    // 2. If we need to generate limit
    if (sortRel.fetch != null) {
      int limit = RexLiteral.intValue(sortRel.fetch);
      int offset = sortRel.offset == null ? 0 : RexLiteral.intValue(sortRel.offset);
      LimitDesc limitDesc = new LimitDesc(offset,limit);
      ArrayList<ColumnInfo> cinfoLst = createColInfos(resultOp);
      resultOp = OperatorFactory.getAndMakeChild(limitDesc, new RowSchema(cinfoLst), resultOp);

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
        new ExprNodeConverter(inputOpAf.tabAlias, filterRel.getInput().getRowType(), inputOpAf.vcolsInCalcite,
            filterRel.getCluster().getTypeFactory(), true));
    FilterDesc filDesc = new FilterDesc(filCondExpr, false);
    ArrayList<ColumnInfo> cinfoLst = createColInfos(inputOpAf.inputs.get(0));
    FilterOperator filOp = (FilterOperator) OperatorFactory.getAndMakeChild(filDesc,
        new RowSchema(cinfoLst), inputOpAf.inputs.get(0));

    if (LOG.isDebugEnabled()) {
      LOG.debug("Generated " + filOp + " with row schema: [" + filOp.getSchema() + "]");
    }

    return inputOpAf.clone(filOp);
  }

  // use this function to make the union "flat" for both execution and explain
  // purpose
  private List<RelNode> extractRelNodeFromUnion(HiveUnion unionRel) {
    List<RelNode> ret = new ArrayList<RelNode>();
    for (RelNode input : unionRel.getInputs()) {
      if (input instanceof HiveUnion) {
        ret.addAll(extractRelNodeFromUnion((HiveUnion) input));
      } else {
        ret.add(input);
      }
    }
    return ret;
  }

  OpAttr visit(HiveUnion unionRel) throws SemanticException {
    // 1. Convert inputs
    List<RelNode> inputsList = extractRelNodeFromUnion(unionRel);
    OpAttr[] inputs = new OpAttr[inputsList.size()];
    for (int i = 0; i < inputs.length; i++) {
      inputs[i] = dispatch(inputsList.get(i));
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Translating operator rel#" + unionRel.getId() + ":" + unionRel.getRelTypeName()
          + " with row type: [" + unionRel.getRowType() + "]");
    }

    // 2. Create a new union operator
    UnionDesc unionDesc = new UnionDesc();
    unionDesc.setNumInputs(inputs.length);
    String tableAlias = getHiveDerivedTableAlias();
    ArrayList<ColumnInfo> cinfoLst = createColInfos(inputs[0].inputs.get(0), tableAlias);
    Operator<?>[] children = new Operator<?>[inputs.length];
    for (int i = 0; i < children.length; i++) {
      if (i == 0) {
        children[i] = inputs[i].inputs.get(0);
      } else {
        Operator<?> op = inputs[i].inputs.get(0);
        // We need to check if the other input branches for union is following the first branch
        // We may need to cast the data types for specific columns.
        children[i] = genInputSelectForUnion(op, cinfoLst);
      }
    }
    Operator<? extends OperatorDesc> unionOp = OperatorFactory.getAndMakeChild(
        semanticAnalyzer.getOpContext(), unionDesc, new RowSchema(cinfoLst), children);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Generated " + unionOp + " with row schema: [" + unionOp.getSchema() + "]");
    }

    //TODO: Can columns retain virtualness out of union
    // 3. Return result
    return new OpAttr(tableAlias, inputs[0].vcolsInCalcite, unionOp);

  }

  OpAttr visit(HiveSortExchange exchangeRel) throws SemanticException {
    OpAttr inputOpAf = dispatch(exchangeRel.getInput());
    String tabAlias = inputOpAf.tabAlias;
    if (tabAlias == null || tabAlias.length() == 0) {
      tabAlias = getHiveDerivedTableAlias();
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Translating operator rel#" + exchangeRel.getId() + ":"
          + exchangeRel.getRelTypeName() + " with row type: [" + exchangeRel.getRowType() + "]");
    }

    RelDistribution distribution = exchangeRel.getDistribution();
    if (distribution.getType() != Type.HASH_DISTRIBUTED) {
      throw new SemanticException("Only hash distribution supported for LogicalExchange");
    }
    ExprNodeDesc[] expressions = new ExprNodeDesc[exchangeRel.getJoinKeys().size()];
    for (int index = 0; index < exchangeRel.getJoinKeys().size(); index++) {
      expressions[index] = convertToExprNode(exchangeRel.getJoinKeys().get(index),
          exchangeRel.getInput(), inputOpAf.tabAlias, inputOpAf);
    }
    exchangeRel.setJoinExpressions(expressions);

    ReduceSinkOperator rsOp = genReduceSink(inputOpAf.inputs.get(0), tabAlias, expressions,
        -1, -1, Operation.NOT_ACID, hiveConf);

    return new OpAttr(tabAlias, inputOpAf.vcolsInCalcite, rsOp);
  }

  private OpAttr genPTF(OpAttr inputOpAf, WindowingSpec wSpec) throws SemanticException {
    Operator<?> input = inputOpAf.inputs.get(0);

    wSpec.validateAndMakeEffective();
    WindowingComponentizer groups = new WindowingComponentizer(wSpec);
    RowResolver rr = new RowResolver();
    for (ColumnInfo ci : input.getSchema().getSignature()) {
      rr.put(inputOpAf.tabAlias, ci.getInternalName(), ci);
    }

    while (groups.hasNext()) {
      wSpec = groups.next(hiveConf, semanticAnalyzer, unparseTranslator, rr);

      // 1. Create RS and backtrack Select operator on top
      ArrayList<ExprNodeDesc> keyCols = new ArrayList<ExprNodeDesc>();
      ArrayList<ExprNodeDesc> partCols = new ArrayList<ExprNodeDesc>();
      StringBuilder order = new StringBuilder();
      StringBuilder nullOrder = new StringBuilder();

      for (PartitionExpression partCol : wSpec.getQueryPartitionSpec().getExpressions()) {
        ExprNodeDesc partExpr = semanticAnalyzer.genExprNodeDesc(partCol.getExpression(), rr);
        if (ExprNodeDescUtils.indexOf(partExpr, partCols) < 0) {
          keyCols.add(partExpr);
          partCols.add(partExpr);
          order.append('+');
          nullOrder.append('a');
        }
      }

      if (wSpec.getQueryOrderSpec() != null) {
        for (OrderExpression orderCol : wSpec.getQueryOrderSpec().getExpressions()) {
          ExprNodeDesc orderExpr = semanticAnalyzer.genExprNodeDesc(orderCol.getExpression(), rr);
          char orderChar = orderCol.getOrder() == PTFInvocationSpec.Order.ASC ? '+' : '-';
          char nullOrderChar = orderCol.getNullOrder() == PTFInvocationSpec.NullOrder.NULLS_FIRST ? 'a' : 'z';
          int index = ExprNodeDescUtils.indexOf(orderExpr, keyCols);
          if (index >= 0) {
            order.setCharAt(index, orderChar);
            nullOrder.setCharAt(index, nullOrderChar);
            continue;
          }
          keyCols.add(orderExpr);
          order.append(orderChar);
          nullOrder.append(nullOrderChar);
        }
      }

      SelectOperator selectOp = genReduceSinkAndBacktrackSelect(input,
          keyCols.toArray(new ExprNodeDesc[keyCols.size()]), 0, partCols,
          order.toString(), nullOrder.toString(), -1, Operation.NOT_ACID, hiveConf);

      // 2. Finally create PTF
      PTFTranslator translator = new PTFTranslator();
      PTFDesc ptfDesc = translator.translate(wSpec, semanticAnalyzer, hiveConf, rr,
          unparseTranslator);
      RowResolver ptfOpRR = ptfDesc.getFuncDef().getOutputShape().getRr();

      Operator<?> ptfOp = OperatorFactory.getAndMakeChild(
          ptfDesc, new RowSchema(ptfOpRR.getColumnInfos()), selectOp);

      if (LOG.isDebugEnabled()) {
        LOG.debug("Generated " + ptfOp + " with row schema: [" + ptfOp.getSchema() + "]");
      }

      // 3. Prepare for next iteration (if any)
      rr = ptfOpRR;
      input = ptfOp;
    }

    return inputOpAf.clone(input);
  }

  private static SelectOperator genReduceSinkAndBacktrackSelect(Operator<?> input,
          ExprNodeDesc[] keys, int tag, ArrayList<ExprNodeDesc> partitionCols, String order,
          String nullOrder, int numReducers, Operation acidOperation, HiveConf hiveConf)
              throws SemanticException {
    return genReduceSinkAndBacktrackSelect(input, keys, tag, partitionCols, order, nullOrder,
        numReducers, acidOperation, hiveConf, input.getSchema().getColumnNames());
  }

  private static SelectOperator genReduceSinkAndBacktrackSelect(Operator<?> input,
      ExprNodeDesc[] keys, int tag, ArrayList<ExprNodeDesc> partitionCols, String order, String nullOrder,
      int numReducers, Operation acidOperation, HiveConf hiveConf,
      List<String> keepColNames) throws SemanticException {
    // 1. Generate RS operator
    // 1.1 Prune the tableNames, only count the tableNames that are not empty strings
  // as empty string in table aliases is only allowed for virtual columns.
    String tableAlias = null;
    Set<String> tableNames = input.getSchema().getTableNames();
    for (String tableName : tableNames) {
      if (tableName != null) {
        if (tableName.length() == 0) {
          if (tableAlias == null) {
            tableAlias = tableName;
          }
        } else {
          if (tableAlias == null || tableAlias.length() == 0) {
            tableAlias = tableName;
          } else {
            if (!tableName.equals(tableAlias)) {
              throw new SemanticException(
                  "In CBO return path, genReduceSinkAndBacktrackSelect is expecting only one tableAlias but there is more than one");
            }
          }
        }
      }
    }
    if (tableAlias == null) {
      throw new SemanticException(
          "In CBO return path, genReduceSinkAndBacktrackSelect is expecting only one tableAlias but there is none");
    }
    // 1.2 Now generate RS operator
    ReduceSinkOperator rsOp = genReduceSink(input, tableAlias, keys, tag, partitionCols, order,
            nullOrder, numReducers, acidOperation, hiveConf);

    // 2. Generate backtrack Select operator
    Map<String, ExprNodeDesc> descriptors = buildBacktrackFromReduceSink(keepColNames,
        rsOp.getConf().getOutputKeyColumnNames(), rsOp.getConf().getOutputValueColumnNames(),
        rsOp.getValueIndex(), input);
    SelectDesc selectDesc = new SelectDesc(new ArrayList<ExprNodeDesc>(descriptors.values()),
        new ArrayList<String>(descriptors.keySet()));
    ArrayList<ColumnInfo> cinfoLst = createColInfosSubset(input, keepColNames);
    SelectOperator selectOp = (SelectOperator) OperatorFactory.getAndMakeChild(
        selectDesc, new RowSchema(cinfoLst), rsOp);
    selectOp.setColumnExprMap(descriptors);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Generated " + selectOp + " with row schema: [" + selectOp.getSchema() + "]");
    }

    return selectOp;
  }

  private static ReduceSinkOperator genReduceSink(Operator<?> input, String tableAlias, ExprNodeDesc[] keys, int tag,
      int numReducers, Operation acidOperation, HiveConf hiveConf) throws SemanticException {
    return genReduceSink(input, tableAlias, keys, tag, new ArrayList<ExprNodeDesc>(), "", "", numReducers,
        acidOperation, hiveConf);
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  private static ReduceSinkOperator genReduceSink(Operator<?> input, String tableAlias, ExprNodeDesc[] keys, int tag,
      ArrayList<ExprNodeDesc> partitionCols, String order, String nullOrder, int numReducers,
      Operation acidOperation, HiveConf hiveConf) throws SemanticException {
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
      ExprNodeColumnDesc expr = new ExprNodeColumnDesc(colInfo);

      // backtrack can be null when input is script operator
      ExprNodeDesc exprBack = ExprNodeDescUtils.backtrack(expr, dummy, input);
      int kindex = exprBack == null ? -1 : ExprNodeDescUtils.indexOf(exprBack, reduceKeysBack);
      if (kindex >= 0) {
        ColumnInfo newColInfo = new ColumnInfo(colInfo);
        newColInfo.setInternalName(Utilities.ReduceField.KEY + ".reducesinkkey" + kindex);
        newColInfo.setAlias(outputColName);
        newColInfo.setTabAlias(tableAlias);
        outputColumns.add(newColInfo);
        index[i] = kindex;
        continue;
      }
      int vindex = exprBack == null ? -1 : ExprNodeDescUtils.indexOf(exprBack, reduceValuesBack);
      if (vindex >= 0) {
        index[i] = -vindex - 1;
        continue;
      }
      index[i] = -reduceValues.size() - 1;

      reduceValues.add(expr);
      reduceValuesBack.add(exprBack);

      ColumnInfo newColInfo = new ColumnInfo(colInfo);
      newColInfo.setInternalName(Utilities.ReduceField.VALUE + "." + outputColName);
      newColInfo.setAlias(outputColName);
      newColInfo.setTabAlias(tableAlias);

      outputColumns.add(newColInfo);
      outputColumnNames.add(outputColName);
    }
    dummy.setParentOperators(null);

    // Use only 1 reducer if no reduce keys
    if (reduceKeys.size() == 0) {
      numReducers = 1;

      // Cartesian product is not supported in strict mode
      String error = StrictChecks.checkCartesian(hiveConf);
      if (error != null) throw new SemanticException(error);
    }

    ReduceSinkDesc rsDesc;
    if (order.isEmpty()) {
      rsDesc = PlanUtils.getReduceSinkDesc(reduceKeys, reduceValues, outputColumnNames, false, tag,
          reduceKeys.size(), numReducers, acidOperation);
    } else {
      rsDesc = PlanUtils.getReduceSinkDesc(reduceKeys, reduceValues, outputColumnNames, false, tag,
          partitionCols, order, nullOrder, numReducers, acidOperation);
    }

    ReduceSinkOperator rsOp = (ReduceSinkOperator) OperatorFactory.getAndMakeChild(
        rsDesc, new RowSchema(outputColumns), input);

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
    rsOp.setInputAliases(input.getSchema().getTableNames()
        .toArray(new String[input.getSchema().getTableNames().size()]));

    if (LOG.isDebugEnabled()) {
      LOG.debug("Generated " + rsOp + " with row schema: [" + rsOp.getSchema() + "]");
    }

    return rsOp;
  }

  private static JoinOperator genJoin(RelNode join, ExprNodeDesc[][] joinExpressions,
      List<List<ExprNodeDesc>> filterExpressions, List<Operator<?>> children,
      String[] baseSrc, String tabAlias)
          throws SemanticException {

    // 1. Extract join type
    JoinCondDesc[] joinCondns;
    boolean semiJoin;
    boolean noOuterJoin;
    if (join instanceof HiveMultiJoin) {
      HiveMultiJoin hmj = (HiveMultiJoin) join;
      joinCondns = new JoinCondDesc[hmj.getJoinInputs().size()];
      for (int i = 0; i < hmj.getJoinInputs().size(); i++) {
        joinCondns[i] = new JoinCondDesc(new JoinCond(
                hmj.getJoinInputs().get(i).left,
                hmj.getJoinInputs().get(i).right,
                transformJoinType(hmj.getJoinTypes().get(i))));
      }
      semiJoin = false;
      noOuterJoin = !hmj.isOuterJoin();
    } else {
      joinCondns = new JoinCondDesc[1];
      semiJoin = join instanceof SemiJoin;
      JoinType joinType;
      if (semiJoin) {
        joinType = JoinType.LEFTSEMI;
      } else {
        joinType = extractJoinType((Join)join);
      }
      joinCondns[0] = new JoinCondDesc(new JoinCond(0, 1, joinType));
      noOuterJoin = joinType != JoinType.FULLOUTER && joinType != JoinType.LEFTOUTER
              && joinType != JoinType.RIGHTOUTER;
    }

    // 2. We create the join aux structures
    ArrayList<ColumnInfo> outputColumns = new ArrayList<ColumnInfo>();
    ArrayList<String> outputColumnNames = new ArrayList<String>(join.getRowType()
        .getFieldNames());
    Operator<?>[] childOps = new Operator[children.size()];

    Map<String, Byte> reversedExprs = new HashMap<String, Byte>();
    Map<Byte, List<ExprNodeDesc>> exprMap = new HashMap<Byte, List<ExprNodeDesc>>();
    Map<Byte, List<ExprNodeDesc>> filters = new HashMap<Byte, List<ExprNodeDesc>>();
    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
    HashMap<Integer, Set<String>> posToAliasMap = new HashMap<Integer, Set<String>>();

    int outputPos = 0;
    for (int pos = 0; pos < children.size(); pos++) {
      // 2.1. Backtracking from RS
      ReduceSinkOperator inputRS = (ReduceSinkOperator) children.get(pos);
      if (inputRS.getNumParent() != 1) {
        throw new SemanticException("RS should have single parent");
      }
      Operator<?> parent = inputRS.getParentOperators().get(0);
      ReduceSinkDesc rsDesc = inputRS.getConf();
      int[] index = inputRS.getValueIndex();
      Byte tag = (byte) rsDesc.getTag();

      // 2.1.1. If semijoin...
      if (semiJoin && pos != 0) {
        exprMap.put(tag, new ArrayList<ExprNodeDesc>());
        childOps[pos] = inputRS;
        continue;
      }

      posToAliasMap.put(pos, new HashSet<String>(inputRS.getSchema().getTableNames()));
      List<String> keyColNames = rsDesc.getOutputKeyColumnNames();
      List<String> valColNames = rsDesc.getOutputValueColumnNames();

      Map<String, ExprNodeDesc> descriptors = buildBacktrackFromReduceSinkForJoin(outputPos,
          outputColumnNames, keyColNames, valColNames, index, parent, baseSrc[pos]);

      List<ColumnInfo> parentColumns = parent.getSchema().getSignature();
      for (int i = 0; i < index.length; i++) {
        ColumnInfo info = new ColumnInfo(parentColumns.get(i));
        info.setInternalName(outputColumnNames.get(outputPos));
        info.setTabAlias(tabAlias);
        outputColumns.add(info);
        reversedExprs.put(outputColumnNames.get(outputPos), tag);
        outputPos++;
      }

      exprMap.put(tag, new ArrayList<ExprNodeDesc>(descriptors.values()));
      colExprMap.putAll(descriptors);
      childOps[pos] = inputRS;
    }

    // 3. We populate the filters and filterMap structure needed in the join descriptor
    List<List<ExprNodeDesc>> filtersPerInput = Lists.newArrayList();
    int[][] filterMap = new int[children.size()][];
    for (int i=0; i<children.size(); i++) {
      filtersPerInput.add(new ArrayList<ExprNodeDesc>());
    }
    // 3. We populate the filters structure
    for (int i=0; i<filterExpressions.size(); i++) {
      int leftPos = joinCondns[i].getLeft();
      int rightPos = joinCondns[i].getRight();

      for (ExprNodeDesc expr : filterExpressions.get(i)) {
        // We need to update the exprNode, as currently
        // they refer to columns in the output of the join;
        // they should refer to the columns output by the RS
        int inputPos = updateExprNode(expr, reversedExprs, colExprMap);
        if (inputPos == -1) {
          inputPos = leftPos;
        }
        filtersPerInput.get(inputPos).add(expr);

        if (joinCondns[i].getType() == JoinDesc.FULL_OUTER_JOIN ||
                joinCondns[i].getType() == JoinDesc.LEFT_OUTER_JOIN ||
                joinCondns[i].getType() == JoinDesc.RIGHT_OUTER_JOIN) {
          if (inputPos == leftPos) {
            updateFilterMap(filterMap, leftPos, rightPos);
          } else {
            updateFilterMap(filterMap, rightPos, leftPos);
          }
        }
      }
    }
    for (int pos = 0; pos < children.size(); pos++) {
      ReduceSinkOperator inputRS = (ReduceSinkOperator) children.get(pos);
      ReduceSinkDesc rsDesc = inputRS.getConf();
      Byte tag = (byte) rsDesc.getTag();
      filters.put(tag, filtersPerInput.get(pos));
    }

    // 4. We create the join operator with its descriptor
    JoinDesc desc = new JoinDesc(exprMap, outputColumnNames, noOuterJoin, joinCondns,
            filters, joinExpressions, null);
    desc.setReversedExprs(reversedExprs);
    desc.setFilterMap(filterMap);

    JoinOperator joinOp = (JoinOperator) OperatorFactory.getAndMakeChild(
        childOps[0].getCompilationOpContext(), desc, new RowSchema(outputColumns), childOps);
    joinOp.setColumnExprMap(colExprMap);
    joinOp.setPosToAliasMap(posToAliasMap);
    joinOp.getConf().setBaseSrc(baseSrc);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Generated " + joinOp + " with row schema: [" + joinOp.getSchema() + "]");
    }

    return joinOp;
  }

  /*
   * This method updates the input expr, changing all the
   * ExprNodeColumnDesc in it to refer to columns given by the
   * colExprMap.
   *
   * For instance, "col_0 = 1" would become "VALUE.col_0 = 1";
   * the execution engine expects filters in the Join operators
   * to be expressed that way.
   */
  private static int updateExprNode(ExprNodeDesc expr, final Map<String, Byte> reversedExprs,
      final Map<String, ExprNodeDesc> colExprMap) throws SemanticException {
    int inputPos = -1;
    if (expr instanceof ExprNodeGenericFuncDesc) {
      ExprNodeGenericFuncDesc func = (ExprNodeGenericFuncDesc) expr;
      List<ExprNodeDesc> newChildren = new ArrayList<ExprNodeDesc>();
      for (ExprNodeDesc functionChild : func.getChildren()) {
        if (functionChild instanceof ExprNodeColumnDesc) {
          String colRef = functionChild.getExprString();
          int pos = reversedExprs.get(colRef);
          if (pos != -1) {
            if (inputPos == -1) {
              inputPos = pos;
            } else if (inputPos != pos) {
              throw new SemanticException(
                  "UpdateExprNode is expecting only one position for join operator convert. But there are more than one.");
            }
          }
          newChildren.add(colExprMap.get(colRef));
        } else {
          int pos = updateExprNode(functionChild, reversedExprs, colExprMap);
          if (pos != -1) {
            if (inputPos == -1) {
              inputPos = pos;
            } else if (inputPos != pos) {
              throw new SemanticException(
                  "UpdateExprNode is expecting only one position for join operator convert. But there are more than one.");
            }
          }
          newChildren.add(functionChild);
        }
      }
      func.setChildren(newChildren);
    }
    return inputPos;
  }

  private static void updateFilterMap(int[][] filterMap, int inputPos, int joinPos) {
    int[] map = filterMap[inputPos];
    if (map == null) {
      filterMap[inputPos] = new int[2];
      filterMap[inputPos][0] = joinPos;
      filterMap[inputPos][1]++;
    } else {
      boolean inserted = false;
      for (int j=0; j<map.length/2 && !inserted; j++) {
        if (map[j*2] == joinPos) {
          map[j*2+1]++;
          inserted = true;
        }
      }
      if (!inserted) {
        int[] newMap = new int[map.length + 2];
        System.arraycopy(map, 0, newMap, 0, map.length);
        newMap[map.length] = joinPos;
        newMap[map.length+1]++;
        filterMap[inputPos] = newMap;
      }
    }
  }

  private static JoinType extractJoinType(Join join) {
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
      // TODO: UNIQUE JOIN
      resultJoinType = JoinType.INNER;
      break;
    }
    return resultJoinType;
  }

  private static JoinType transformJoinType(JoinRelType type) {
    JoinType resultJoinType;
    switch (type) {
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

  private static Map<String, ExprNodeDesc> buildBacktrackFromReduceSinkForJoin(int initialPos,
      List<String> outputColumnNames, List<String> keyColNames, List<String> valueColNames,
      int[] index, Operator<?> inputOp, String tabAlias) {
    Map<String, ExprNodeDesc> columnDescriptors = new LinkedHashMap<String, ExprNodeDesc>();
    for (int i = 0; i < index.length; i++) {
      ColumnInfo info = new ColumnInfo(inputOp.getSchema().getSignature().get(i));
      String field;
      if (index[i] >= 0) {
        field = Utilities.ReduceField.KEY + "." + keyColNames.get(index[i]);
      } else {
        field = Utilities.ReduceField.VALUE + "." + valueColNames.get(-index[i] - 1);
      }
      ExprNodeColumnDesc desc = new ExprNodeColumnDesc(info.getType(), field, tabAlias,
          info.getIsVirtualCol());
      columnDescriptors.put(outputColumnNames.get(initialPos + i), desc);
    }
    return columnDescriptors;
  }

  private static Map<String, ExprNodeDesc> buildBacktrackFromReduceSink(List<String> keepColNames,
      List<String> keyColNames, List<String> valueColNames, int[] index, Operator<?> inputOp) {
    Map<String, ExprNodeDesc> columnDescriptors = new LinkedHashMap<String, ExprNodeDesc>();
    int pos = 0;
    for (int i = 0; i < index.length; i++) {
      ColumnInfo info = inputOp.getSchema().getSignature().get(i);
      if (pos < keepColNames.size() &&
              info.getInternalName().equals(keepColNames.get(pos))) {
        String field;
        if (index[i] >= 0) {
          field = Utilities.ReduceField.KEY + "." + keyColNames.get(index[i]);
        } else {
          field = Utilities.ReduceField.VALUE + "." + valueColNames.get(-index[i] - 1);
        }
        ExprNodeColumnDesc desc = new ExprNodeColumnDesc(info.getType(), field, info.getTabAlias(),
            info.getIsVirtualCol());
        columnDescriptors.put(keepColNames.get(pos), desc);
        pos++;
      }
    }
    return columnDescriptors;
  }

  private static ExprNodeDesc convertToExprNode(RexNode rn, RelNode inputRel, String tabAlias, OpAttr inputAttr) {
    return convertToExprNode(rn, inputRel, tabAlias, inputAttr.vcolsInCalcite);
  }

  private static ExprNodeDesc convertToExprNode(RexNode rn, RelNode inputRel, String tabAlias,
          Set<Integer> vcolsInCalcite) {
    return rn.accept(new ExprNodeConverter(tabAlias, inputRel.getRowType(), vcolsInCalcite,
        inputRel.getCluster().getTypeFactory(), true));
  }

  private static ArrayList<ColumnInfo> createColInfos(Operator<?> input) {
    ArrayList<ColumnInfo> cInfoLst = new ArrayList<ColumnInfo>();
    for (ColumnInfo ci : input.getSchema().getSignature()) {
      cInfoLst.add(new ColumnInfo(ci));
    }
    return cInfoLst;
  }

  //create column info with new tableAlias
  private static ArrayList<ColumnInfo> createColInfos(Operator<?> input, String tableAlias) {
    ArrayList<ColumnInfo> cInfoLst = new ArrayList<ColumnInfo>();
    for (ColumnInfo ci : input.getSchema().getSignature()) {
      ColumnInfo copyOfColumnInfo = new ColumnInfo(ci);
      copyOfColumnInfo.setTabAlias(tableAlias);
      cInfoLst.add(copyOfColumnInfo);
    }
    return cInfoLst;
  }

  private static ArrayList<ColumnInfo> createColInfosSubset(Operator<?> input,
          List<String> keepColNames) {
    ArrayList<ColumnInfo> cInfoLst = new ArrayList<ColumnInfo>();
    int pos = 0;
    for (ColumnInfo ci : input.getSchema().getSignature()) {
      if (pos < keepColNames.size() &&
              ci.getInternalName().equals(keepColNames.get(pos))) {
        cInfoLst.add(new ColumnInfo(ci));
        pos++;
      }
    }
    return cInfoLst;
  }

  private static Pair<ArrayList<ColumnInfo>, Set<Integer>> createColInfos(
      List<RexNode> calciteExprs, List<ExprNodeDesc> hiveExprs, List<String> projNames,
      OpAttr inpOpAf) {
    if (hiveExprs.size() != projNames.size()) {
      throw new RuntimeException("Column expressions list doesn't match Column Names list");
    }

    RexNode rexN;
    ExprNodeDesc pe;
    ArrayList<ColumnInfo> colInfos = new ArrayList<ColumnInfo>();
    boolean vc;
    Set<Integer> newVColSet = new HashSet<Integer>();
    for (int i = 0; i < hiveExprs.size(); i++) {
      pe = hiveExprs.get(i);
      rexN = calciteExprs.get(i);
      vc = false;
      if (rexN instanceof RexInputRef) {
        if (inpOpAf.vcolsInCalcite.contains(((RexInputRef) rexN).getIndex())) {
          newVColSet.add(i);
          vc = true;
        }
      }
      colInfos
          .add(new ColumnInfo(projNames.get(i), pe.getTypeInfo(), inpOpAf.tabAlias, vc));
    }

    return new Pair<ArrayList<ColumnInfo>, Set<Integer>>(colInfos, newVColSet);
  }

  private Operator<? extends OperatorDesc> genInputSelectForUnion(
      Operator<? extends OperatorDesc> origInputOp, ArrayList<ColumnInfo> uColumnInfo)
      throws SemanticException {
    Iterator<ColumnInfo> oIter = origInputOp.getSchema().getSignature().iterator();
    Iterator<ColumnInfo> uIter = uColumnInfo.iterator();
    List<ExprNodeDesc> columns = new ArrayList<ExprNodeDesc>();
    List<String> colName = new ArrayList<String>();
    Map<String, ExprNodeDesc> columnExprMap = new HashMap<String, ExprNodeDesc>();
    boolean needSelectOp = false;
    while (oIter.hasNext()) {
      ColumnInfo oInfo = oIter.next();
      ColumnInfo uInfo = uIter.next();
      if (!oInfo.isSameColumnForRR(uInfo)) {
        needSelectOp = true;
      }
      ExprNodeDesc column = new ExprNodeColumnDesc(oInfo.getType(), oInfo.getInternalName(),
          oInfo.getTabAlias(), oInfo.getIsVirtualCol(), oInfo.isSkewedCol());
      if (!oInfo.getType().equals(uInfo.getType())) {
        column = ParseUtils.createConversionCast(column, (PrimitiveTypeInfo) uInfo.getType());
      }
      columns.add(column);
      colName.add(uInfo.getInternalName());
      columnExprMap.put(uInfo.getInternalName(), column);
    }
    if (needSelectOp) {
      return OperatorFactory.getAndMakeChild(new SelectDesc(
          columns, colName), new RowSchema(uColumnInfo), columnExprMap, origInputOp);
    } else {
      return origInputOp;
    }
  }
}
