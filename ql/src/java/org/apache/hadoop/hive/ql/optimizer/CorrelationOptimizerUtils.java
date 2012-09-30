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

package org.apache.hadoop.hive.ql.optimizer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.CorrelationCompositeOperator;
import org.apache.hadoop.hive.ql.exec.CorrelationLocalSimulativeReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.CorrelationOptimizer.IntraQueryCorrelation;
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.TypeCheckProcFactory;
import org.apache.hadoop.hive.ql.plan.CorrelationCompositeDesc;
import org.apache.hadoop.hive.ql.plan.CorrelationLocalSimulativeReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.CorrelationReducerDispatchDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.apache.hadoop.hive.ql.plan.ForwardDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;


public final class CorrelationOptimizerUtils {

  static final private Log LOG = LogFactory.getLog(CorrelationOptimizerUtils.class.getName());

  public static boolean isExisted(ExprNodeDesc expr, List<ExprNodeDesc> col_list) {
    for (ExprNodeDesc thisExpr : col_list) {
      if (expr.getExprString().equals(thisExpr.getExprString())) {
        return true;
      }
    }
    return false;
  }

  public static String getColumnName(Map<String, ExprNodeDesc> opColumnExprMap, ExprNodeDesc expr) {
    for (Entry<String, ExprNodeDesc> entry : opColumnExprMap.entrySet()) {
      if (expr.getExprString().equals(entry.getValue().getExprString())) {
        return entry.getKey();
      }
    }
    return null;
  }


  public static Operator<? extends OperatorDesc> unionUsedColumnsAndMakeNewSelect(
      List<ReduceSinkOperator> rsops,
      IntraQueryCorrelation correlation, Map<Operator<? extends OperatorDesc>,
      Map<String, ExprNodeDesc>> originalOpColumnExprMap, TableScanOperator input,
      ParseContext pGraphContext,
      Map<Operator<? extends OperatorDesc>, OpParseContext> originalOpParseCtx) {

    ArrayList<String> columnNames = new ArrayList<String>();
    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
    ArrayList<ExprNodeDesc> col_list = new ArrayList<ExprNodeDesc>();
    RowResolver out_rwsch = new RowResolver();
    boolean isSelectAll = false;

    int pos = 0;
    for (ReduceSinkOperator rsop : rsops) {
      Operator<? extends OperatorDesc> curr = correlation.getBottom2TSops().get(rsop).get(0)
          .getChildOperators().get(0);
      while (true) {
        if (curr.getName().equals(SelectOperator.getOperatorName())) {
          SelectOperator selOp = (SelectOperator) curr;
          if (selOp.getColumnExprMap() != null) {
            for (Entry<String, ExprNodeDesc> entry : selOp.getColumnExprMap().entrySet()) {
              ExprNodeDesc expr = entry.getValue();
              if (!isExisted(expr, col_list)
                  && originalOpParseCtx.get(selOp).getRowResolver().getInvRslvMap()
                  .containsKey(entry.getKey())) {
                col_list.add(expr);
                String[] colRef = originalOpParseCtx.get(selOp).getRowResolver().getInvRslvMap()
                    .get(entry.getKey());
                String tabAlias = colRef[0];
                String colAlias = colRef[1];
                String outputName = entry.getKey();
                out_rwsch.put(tabAlias, colAlias, new ColumnInfo(
                    outputName, expr.getTypeInfo(), tabAlias, false));
                pos++;
                columnNames.add(outputName);
                colExprMap.put(outputName, expr);
              }
            }
          } else {
            for (ExprNodeDesc expr : selOp.getConf().getColList()) {
              if (!isExisted(expr, col_list)) {
                col_list.add(expr);
                String[] colRef = pGraphContext.getOpParseCtx().get(selOp).getRowResolver()
                    .getInvRslvMap().get(expr.getCols().get(0));
                String tabAlias = colRef[0];
                String colAlias = colRef[1];
                String outputName = expr.getCols().get(0);
                out_rwsch.put(tabAlias, colAlias, new ColumnInfo(
                    outputName, expr.getTypeInfo(), tabAlias, false));
                columnNames.add(outputName);
                colExprMap.put(outputName, expr);
                pos++;
              }
            }
          }
          break;
        } else if (curr.getName().equals(FilterOperator.getOperatorName())) {
          isSelectAll = true;
          break;
        } else if (curr.getName().equals(ReduceSinkOperator.getOperatorName())) {
          ReduceSinkOperator thisRSop = (ReduceSinkOperator) curr;
          for (ExprNodeDesc expr : thisRSop.getConf().getKeyCols()) {
            if (!isExisted(expr, col_list)) {
              col_list.add(expr);
              assert expr.getCols().size() == 1;
              String columnName = getColumnName(originalOpColumnExprMap.get(thisRSop), expr);
              String[] colRef = pGraphContext.getOpParseCtx().get(thisRSop).getRowResolver()
                  .getInvRslvMap().get(columnName);
              String tabAlias = colRef[0];
              String colAlias = colRef[1];
              String outputName = expr.getCols().get(0);
              out_rwsch.put(tabAlias, colAlias, new ColumnInfo(
                  outputName, expr.getTypeInfo(), tabAlias, false));
              columnNames.add(outputName);
              colExprMap.put(outputName, expr);
              pos++;
            }
          }
          for (ExprNodeDesc expr : thisRSop.getConf().getValueCols()) {
            if (!isExisted(expr, col_list)) {
              col_list.add(expr);
              assert expr.getCols().size() == 1;
              String columnName = getColumnName(originalOpColumnExprMap.get(thisRSop), expr);
              String[] colRef = pGraphContext.getOpParseCtx().get(thisRSop).getRowResolver()
                  .getInvRslvMap().get(columnName);
              String tabAlias = colRef[0];
              String colAlias = colRef[1];
              String outputName = expr.getCols().get(0);
              out_rwsch.put(tabAlias, colAlias, new ColumnInfo(
                  outputName, expr.getTypeInfo(), tabAlias, false));
              columnNames.add(outputName);
              colExprMap.put(outputName, expr);
              pos++;
            }
          }

          break;
        } else {
          curr = curr.getChildOperators().get(0);
        }
      }
    }

    Operator<? extends OperatorDesc> output;
    if (isSelectAll) {
      output = input;
    } else {
      output = putOpInsertMap(OperatorFactory.getAndMakeChild(
          new SelectDesc(col_list, columnNames, false), new RowSchema(
              out_rwsch.getColumnInfos()), input), out_rwsch, pGraphContext.getOpParseCtx());
      output.setColumnExprMap(colExprMap);
      output.setChildOperators(Utilities.makeList());

    }

    return output;
  }


  public static Operator<? extends OperatorDesc> putOpInsertMap(
      Operator<? extends OperatorDesc> op,
      RowResolver rr, LinkedHashMap<Operator<? extends OperatorDesc>, OpParseContext> opParseCtx) {
    OpParseContext ctx = new OpParseContext(rr);
    opParseCtx.put(op, ctx);
    op.augmentPlan();
    return op;
  }

  public static Map<Operator<? extends OperatorDesc>, String> getAliasIDtTopOps(
      Map<String, Operator<? extends OperatorDesc>> topOps) {
    Map<Operator<? extends OperatorDesc>, String> aliasIDtTopOps =
        new HashMap<Operator<? extends OperatorDesc>, String>();
    for (Entry<String, Operator<? extends OperatorDesc>> entry : topOps.entrySet()) {
      assert !aliasIDtTopOps.containsKey(entry.getValue());
      aliasIDtTopOps.put(entry.getValue(), entry.getKey());
    }
    return aliasIDtTopOps;
  }

  /**
   * Find all peer ReduceSinkOperators (which have the same child operator of op) of op (op
   * included).
   */
  public static List<ReduceSinkOperator> findPeerReduceSinkOperators(ReduceSinkOperator op) {
    List<ReduceSinkOperator> peerReduceSinkOperators = new ArrayList<ReduceSinkOperator>();
    List<Operator<? extends OperatorDesc>> children = op.getChildOperators();
    assert children.size() == 1; // A ReduceSinkOperator should have only one child
    for (Operator<? extends OperatorDesc> parent : children.get(0).getParentOperators()) {
      assert (parent instanceof ReduceSinkOperator);
      peerReduceSinkOperators.add((ReduceSinkOperator) parent);
    }
    return peerReduceSinkOperators;
  }

  public static List<CorrelationLocalSimulativeReduceSinkOperator> findPeerFakeReduceSinkOperators(
      CorrelationLocalSimulativeReduceSinkOperator op) {

    List<CorrelationLocalSimulativeReduceSinkOperator> peerReduceSinkOperators =
        new ArrayList<CorrelationLocalSimulativeReduceSinkOperator>();

    List<Operator<? extends OperatorDesc>> children = op.getChildOperators();
    assert children.size() == 1;

    for (Operator<? extends OperatorDesc> parent : children.get(0).getParentOperators()) {
      assert (parent instanceof ReduceSinkOperator);
      peerReduceSinkOperators.add((CorrelationLocalSimulativeReduceSinkOperator) parent);
    }

    return peerReduceSinkOperators;
  }

  public static boolean applyCorrelation(
      IntraQueryCorrelation correlation,
      ParseContext inputpGraphContext,
      Map<Operator<? extends OperatorDesc>, Map<String, ExprNodeDesc>> originalOpColumnExprMap,
      Map<Operator<? extends OperatorDesc>, RowResolver> originalOpRowResolver,
      Map<ReduceSinkOperator, GroupByOperator> groupbyRegular2MapSide,
      Map<Operator<? extends OperatorDesc>, OpParseContext> originalOpParseCtx)
          throws SemanticException {

    ParseContext pGraphContext = inputpGraphContext;

    // 0: if necessary, replace RS-GBY to GBY-RS-GBY. In GBY-RS-GBY, the type of the first GBY is
    // hash, so it can group records
    LOG.info("apply correlation step 0: replace RS-GBY to GBY-RS-GBY");
    for (ReduceSinkOperator rsop : correlation.getRSGBYToBeReplacedByGBYRSGBY()) {
      LOG.info("operator " + rsop.getIdentifier() + " should be replaced");
      assert !correlation.getBottomReduceSinkOperators().contains(rsop);
      GroupByOperator mapSideGBY = groupbyRegular2MapSide.get(rsop);
      assert (mapSideGBY.getChildOperators().get(0).getChildOperators().get(0) instanceof GroupByOperator);
      ReduceSinkOperator newRsop = (ReduceSinkOperator) mapSideGBY.getChildOperators().get(0);
      GroupByOperator reduceSideGBY = (GroupByOperator) newRsop.getChildOperators().get(0);
      GroupByOperator oldReduceSideGBY = (GroupByOperator) rsop.getChildOperators().get(0);
      List<Operator<? extends OperatorDesc>> parents = rsop.getParentOperators();
      List<Operator<? extends OperatorDesc>> children = oldReduceSideGBY.getChildOperators();
      mapSideGBY.setParentOperators(parents);
      for (Operator<? extends OperatorDesc> parent : parents) {
        parent.replaceChild(rsop, mapSideGBY);
      }
      reduceSideGBY.setChildOperators(children);
      for (Operator<? extends OperatorDesc> child : children) {
        child.replaceParent(oldReduceSideGBY, reduceSideGBY);
      }
      correlation.getAllReduceSinkOperators().remove(rsop);
      correlation.getAllReduceSinkOperators().add(newRsop);
    }


    Operator<? extends OperatorDesc> curr;

    // 1: Create table scan operator
    LOG.info("apply correlation step 1: create table scan operator");
    Map<TableScanOperator, TableScanOperator> oldTSOP2newTSOP =
        new HashMap<TableScanOperator, TableScanOperator>();
    Map<String, Operator<? extends OperatorDesc>> oldTopOps = pGraphContext.getTopOps();
    Map<Operator<? extends OperatorDesc>, String> oldAliasIDtTopOps =
        getAliasIDtTopOps(oldTopOps);
    Map<TableScanOperator, Table> oldTopToTable = pGraphContext.getTopToTable();
    Map<String, Operator<? extends OperatorDesc>> addedTopOps =
        new HashMap<String, Operator<? extends OperatorDesc>>();
    Map<TableScanOperator, Table> addedTopToTable = new HashMap<TableScanOperator, Table>();
    for (Entry<String, List<TableScanOperator>> entry : correlation.getTable2CorrelatedTSops()
        .entrySet()) {
      TableScanOperator oldTSop = entry.getValue().get(0);
      TableScanDesc tsDesc = new TableScanDesc(oldTSop.getConf().getAlias(), oldTSop.getConf()
          .getVirtualCols());
      tsDesc.setForwardRowNumber(true);
      OpParseContext opParseCtx = pGraphContext.getOpParseCtx().get(oldTSop);
      Operator<? extends OperatorDesc> top = putOpInsertMap(OperatorFactory.get(tsDesc,
          new RowSchema(opParseCtx.getRowResolver().getColumnInfos())),
          opParseCtx.getRowResolver(), pGraphContext.getOpParseCtx());
      top.setParentOperators(null);
      top.setChildOperators(Utilities.makeList());
      for (TableScanOperator tsop : entry.getValue()) {
        addedTopOps.put(oldAliasIDtTopOps.get(tsop), top);
        addedTopToTable.put((TableScanOperator) top, oldTopToTable.get(tsop));
        oldTSOP2newTSOP.put(tsop, (TableScanOperator) top);
      }
    }

    List<Operator<? extends OperatorDesc>> childrenOfDispatch =
        new ArrayList<Operator<? extends OperatorDesc>>();
    for (ReduceSinkOperator rsop : correlation.getBottomReduceSinkOperators()) {
      // TODO: currently, correlation optimizer can not handle the case that
      // a table is directly connected to a post computation operator. e.g.
      //   Join
      //  /    \
      // GBY   T2
      // |
      // T1
      if (!correlation.getBottomReduceSinkOperators()
          .containsAll(findPeerReduceSinkOperators(rsop))) {
        LOG.info("Can not handle the case that " +
            "a table is directly connected to a post computation operator. Use original plan");
        return false;
      }
      Operator<? extends OperatorDesc> op = rsop.getChildOperators().get(0);
      if (!childrenOfDispatch.contains(op)) {
        LOG.info("Add :" + op.getIdentifier() + " " + op.getName()
            + " to the children list of dispatch operator");
        childrenOfDispatch.add(op);
      }
    }

    int opTag = 0;
    Map<Integer, ReduceSinkOperator> operationPath2CorrelationReduceSinkOps =
        new HashMap<Integer, ReduceSinkOperator>();
    for (Entry<String, List<ReduceSinkOperator>> entry : correlation
        .getTable2CorrelatedRSops().entrySet()) {

      // 2: Create select operator for shared operation paths
      LOG.info("apply correlation step 2: create select operator for shared operation path for the table of "
          + entry.getKey());
      curr =
          unionUsedColumnsAndMakeNewSelect(entry.getValue(), correlation, originalOpColumnExprMap,
              oldTSOP2newTSOP
              .get(correlation.getBottom2TSops().get(entry.getValue().get(0)).get(0)),
              pGraphContext, originalOpParseCtx);

      // 3: Create CorrelationCompositeOperator, CorrelationReduceSinkOperator
      LOG.info("apply correlation step 3: create correlation composite Operator and correlation reduce sink operator for the table of "
          + entry.getKey());
      curr =
          createCorrelationCompositeReducesinkOperaotr(
              correlation.getTable2CorrelatedTSops().get(entry.getKey()), entry.getValue(),
              correlation, curr, pGraphContext,
              childrenOfDispatch, entry.getKey(), originalOpColumnExprMap, opTag,
              originalOpRowResolver);

      operationPath2CorrelationReduceSinkOps.put(new Integer(opTag), (ReduceSinkOperator) curr);
      opTag++;
    }


    // 4: Create correlation dispatch operator for operation paths
    LOG.info("apply correlation step 4: create correlation dispatch operator for operation paths");
    RowResolver outputRS = new RowResolver();
    List<Operator<? extends OperatorDesc>> correlationReduceSinkOps =
        new ArrayList<Operator<? extends OperatorDesc>>();
    for (Entry<Integer, ReduceSinkOperator> entry : operationPath2CorrelationReduceSinkOps
        .entrySet()) {
      curr = entry.getValue();
      correlationReduceSinkOps.add(curr);
      RowResolver inputRS = pGraphContext.getOpParseCtx().get(curr).getRowResolver();
      for (Entry<String, LinkedHashMap<String, ColumnInfo>> e1 : inputRS.getRslvMap().entrySet()) {
        for (Entry<String, ColumnInfo> e2 : e1.getValue().entrySet()) {
          outputRS.put(e1.getKey(), e2.getKey(), e2.getValue());
        }
      }
    }

    Operator<? extends OperatorDesc> dispatchOp = putOpInsertMap(OperatorFactory.get(
        new CorrelationReducerDispatchDesc(correlation.getDispatchConf(), correlation
            .getDispatchKeySelectDescConf(), correlation.getDispatchValueSelectDescConf()),
            new RowSchema(outputRS.getColumnInfos())),
            outputRS, pGraphContext.getOpParseCtx());

    dispatchOp.setParentOperators(correlationReduceSinkOps);
    for (Operator<? extends OperatorDesc> thisOp : correlationReduceSinkOps) {
      thisOp.setChildOperators(Utilities.makeList(dispatchOp));
    }

    // 5: Replace the old plan in the original plan tree with new plan
    LOG.info("apply correlation step 5: Replace the old plan in the original plan tree with the new plan");
    Set<Operator<? extends OperatorDesc>> processed =
        new HashSet<Operator<? extends OperatorDesc>>();
    for (Operator<? extends OperatorDesc> op : childrenOfDispatch) {
      List<Operator<? extends OperatorDesc>> parents =
          new ArrayList<Operator<? extends OperatorDesc>>();
      for (Operator<? extends OperatorDesc> oldParent : op.getParentOperators()) {
        if (!correlation.getBottomReduceSinkOperators().contains(oldParent)) {
          parents.add(oldParent);
        }
      }
      parents.add(dispatchOp);
      op.setParentOperators(parents);
    }
    dispatchOp.setChildOperators(childrenOfDispatch);
    HashMap<String, Operator<? extends OperatorDesc>> newTopOps =
        new HashMap<String, Operator<? extends OperatorDesc>>();
    for (Entry<String, Operator<? extends OperatorDesc>> entry : oldTopOps.entrySet()) {
      if (addedTopOps.containsKey(entry.getKey())) {
        newTopOps.put(entry.getKey(), addedTopOps.get(entry.getKey()));
      } else {
        newTopOps.put(entry.getKey(), entry.getValue());
      }
    }
    pGraphContext.setTopOps(newTopOps);
    HashMap<TableScanOperator, Table> newTopToTable = new HashMap<TableScanOperator, Table>();
    for (Entry<TableScanOperator, Table> entry : oldTopToTable.entrySet()) {
      if (addedTopToTable.containsKey(oldTSOP2newTSOP.get(entry.getKey()))) {
        newTopToTable.put(oldTSOP2newTSOP.get(entry.getKey()),
            addedTopToTable.get(oldTSOP2newTSOP.get(entry.getKey())));
      } else {
        newTopToTable.put(entry.getKey(), entry.getValue());
      }
    }
    pGraphContext.setTopToTable(newTopToTable);

    // 6: Change every JFC related ReduceSinkOperator to a
    // CorrelationLocalSimulativeReduceSinkOperator
    LOG.info("apply correlation step 6: Change every JFC related reduce sink operator to a " +
        "CorrelationLocalSimulativeReduceSinkOperator");
    for (ReduceSinkOperator rsop : correlation.getAllReduceSinkOperators()) {
      if (!correlation.getBottomReduceSinkOperators().contains(rsop)) {
        Operator<? extends OperatorDesc> childOP = rsop.getChildOperators().get(0);
        Operator<? extends OperatorDesc> parentOP = rsop.getParentOperators().get(0);
        Operator<? extends OperatorDesc> correlationLocalSimulativeReduceSinkOperator =
            putOpInsertMap(
                OperatorFactory.get(
                    new CorrelationLocalSimulativeReduceSinkDesc(rsop.getConf()),
                    new RowSchema(pGraphContext.getOpParseCtx().get(rsop).getRowResolver()
                        .getColumnInfos())),
                        pGraphContext.getOpParseCtx().get(rsop).getRowResolver(),
                        pGraphContext.getOpParseCtx());
        correlationLocalSimulativeReduceSinkOperator.setChildOperators(Utilities.makeList(childOP));
        correlationLocalSimulativeReduceSinkOperator.setParentOperators(Utilities.makeList(parentOP));
        parentOP.getChildOperators().set(parentOP.getChildOperators().indexOf(rsop),
            correlationLocalSimulativeReduceSinkOperator);
        childOP.getParentOperators().set(childOP.getParentOperators().indexOf(rsop),
            correlationLocalSimulativeReduceSinkOperator);
      }
    }
    return true;
  }

  public static Operator<? extends OperatorDesc> createCorrelationCompositeReducesinkOperaotr(
      List<TableScanOperator> tsops,
      List<ReduceSinkOperator> rsops,
      IntraQueryCorrelation correlation,
      Operator<? extends OperatorDesc> input,
      ParseContext pGraphContext,
      List<Operator<? extends OperatorDesc>> childrenOfDispatch,
      String tableName,
      Map<Operator<? extends OperatorDesc>, Map<String, ExprNodeDesc>> originalOpColumnExprMap,
      int newTag,
      Map<Operator<? extends OperatorDesc>, RowResolver> originalOpRowResolver)
          throws SemanticException {

    // Create CorrelationCompositeOperator
    RowResolver inputRR = pGraphContext.getOpParseCtx().get(input).getRowResolver();
    List<Operator<? extends OperatorDesc>> tops =
        new ArrayList<Operator<? extends OperatorDesc>>();
    List<Operator<? extends OperatorDesc>> bottoms =
        new ArrayList<Operator<? extends OperatorDesc>>();
    List<Integer> opTags = new ArrayList<Integer>();

    for (ReduceSinkOperator rsop : rsops) {
      TableScanOperator tsop = correlation.getBottom2TSops().get(rsop).get(0);
      Operator<? extends OperatorDesc> curr = tsop.getChildOperators().get(0);
      if (curr == rsop) {
        // no filter needed, just forward
        ForwardDesc forwardCtx = new ForwardDesc();
        Operator<ForwardDesc> forwardOp = OperatorFactory.get(ForwardDesc.class);
        forwardOp.setConf(forwardCtx);
        tops.add(forwardOp);
        bottoms.add(forwardOp);
        opTags.add(correlation.getBottomReduceSink2OperationPathMap().get(rsop));
      } else {
        // Add filter operator
        FilterOperator currFilOp = null;
        while (curr != rsop) {
          if (curr.getName().equals("FIL")) {
            FilterOperator fil = (FilterOperator) curr;
            FilterDesc filterCtx = new FilterDesc(fil.getConf().getPredicate(), false);
            Operator<FilterDesc> nowFilOp = OperatorFactory.get(FilterDesc.class);
            nowFilOp.setConf(filterCtx);
            if (currFilOp == null) {
              currFilOp = (FilterOperator) nowFilOp;
              tops.add(currFilOp);
            } else {
              nowFilOp.setParentOperators(Utilities.makeList(currFilOp));
              currFilOp.setChildOperators(Utilities.makeList(nowFilOp));
              currFilOp = (FilterOperator) nowFilOp;
            }
          }
          curr = curr.getChildOperators().get(0);
        }
        if (currFilOp == null) {
          ForwardDesc forwardCtx = new ForwardDesc();
          Operator<ForwardDesc> forwardOp = OperatorFactory.get(ForwardDesc.class);
          forwardOp.setConf(forwardCtx);
          tops.add(forwardOp);
          bottoms.add(forwardOp);
        } else {
          bottoms.add(currFilOp);
        }
        opTags.add(correlation.getBottomReduceSink2OperationPathMap().get(rsop));

      }
    }

    int[] opTagsArray = new int[opTags.size()];
    for (int i = 0; i < opTags.size(); i++) {
      opTagsArray[i] = opTags.get(i).intValue();
    }

    for (Operator<? extends OperatorDesc> op : bottoms) {
      op.setParentOperators(Utilities.makeList(input));
    }
    input.setChildOperators(bottoms);

    CorrelationCompositeDesc ycoCtx = new CorrelationCompositeDesc();
    ycoCtx.setAllOperationPathTags(opTagsArray);

    Operator<? extends OperatorDesc> ycop = putOpInsertMap(OperatorFactory.get(ycoCtx,
        new RowSchema(inputRR.getColumnInfos())),
        inputRR, pGraphContext.getOpParseCtx());
    ycop.setParentOperators(tops);
    for (Operator<? extends OperatorDesc> op : tops) {
      op.setChildOperators(Utilities.makeList(ycop));
    }

    // Create CorrelationReduceSinkOperator
    ArrayList<ExprNodeDesc> partitionCols = new ArrayList<ExprNodeDesc>();
    ArrayList<ExprNodeDesc> keyCols = new ArrayList<ExprNodeDesc>();
    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
    ArrayList<String> keyOutputColumnNames = new ArrayList<String>();
    ReduceSinkOperator firstRsop = rsops.get(0);

    RowResolver firstRsopRS = pGraphContext.getOpParseCtx().get(firstRsop).getRowResolver();
    RowResolver orginalFirstRsopRS = originalOpRowResolver.get(firstRsop);
    RowResolver outputRS = new RowResolver();
    Map<String, ExprNodeColumnDesc> keyCol2ExprForDispatch =
        new HashMap<String, ExprNodeColumnDesc>();
    Map<String, ExprNodeColumnDesc> valueCol2ExprForDispatch =
        new HashMap<String, ExprNodeColumnDesc>();

    for (ExprNodeDesc expr : firstRsop.getConf().getKeyCols()) {
      assert expr instanceof ExprNodeColumnDesc;
      ExprNodeColumnDesc encd = (ExprNodeColumnDesc) expr;
      String ouputName = getColumnName(originalOpColumnExprMap.get(firstRsop), expr);
      ColumnInfo cinfo = orginalFirstRsopRS.getColumnInfos().get(
          orginalFirstRsopRS.getPosition(ouputName));

      String col = SemanticAnalyzer.getColumnInternalName(keyCols.size());
      keyOutputColumnNames.add(col);
      ColumnInfo newColInfo = new ColumnInfo(col, cinfo.getType(), tableName, cinfo
          .getIsVirtualCol(), cinfo.isHiddenVirtualCol());

      colExprMap.put(newColInfo.getInternalName(), expr);

      outputRS.put(tableName, newColInfo.getInternalName(), newColInfo);
      keyCols.add(expr);

      keyCol2ExprForDispatch.put(encd.getColumn(), new ExprNodeColumnDesc(cinfo.getType(), col,
          tableName,
          encd.getIsPartitionColOrVirtualCol()));

    }

    ArrayList<ExprNodeDesc> valueCols = new ArrayList<ExprNodeDesc>();
    ArrayList<String> valueOutputColumnNames = new ArrayList<String>();

    correlation.addOperationPathToDispatchConf(newTag);
    correlation.addOperationPathToDispatchKeySelectDescConf(newTag);
    correlation.addOperationPathToDispatchValueSelectDescConf(newTag);


    for (ReduceSinkOperator rsop : rsops) {
      LOG.debug("Analyzing ReduceSinkOperator " + rsop.getIdentifier());
      RowResolver rs = pGraphContext.getOpParseCtx().get(rsop).getRowResolver();
      RowResolver orginalRS = originalOpRowResolver.get(rsop);
      Integer childOpIndex = childrenOfDispatch.indexOf(rsop.getChildOperators().get(0));
      int outputTag = rsop.getConf().getTag();
      if (outputTag == -1) {
        outputTag = 0;
      }
      if (!correlation.getDispatchConfForOperationPath(newTag).containsKey(childOpIndex)) {
        correlation.getDispatchConfForOperationPath(newTag).put(childOpIndex,
            new ArrayList<Integer>());
      }
      correlation.getDispatchConfForOperationPath(newTag).get(childOpIndex).add(outputTag);

      ArrayList<ExprNodeDesc> thisKeyColsInDispatch = new ArrayList<ExprNodeDesc>();
      ArrayList<String> outputKeyNamesInDispatch = new ArrayList<String>();
      for (ExprNodeDesc expr : rsop.getConf().getKeyCols()) {
        assert expr instanceof ExprNodeColumnDesc;
        ExprNodeColumnDesc encd = (ExprNodeColumnDesc) expr;
        String outputName = getColumnName(originalOpColumnExprMap.get(rsop), expr);
        LOG.debug("key column: " + outputName);
        thisKeyColsInDispatch.add(keyCol2ExprForDispatch.get(encd.getColumn()));
        String[] names = outputName.split("\\.");
        String outputKeyName = "";
        switch (names.length) {
        case 1:
          outputKeyName = names[0];
          break;
        case 2:
          outputKeyName = names[1];
          break;
        default:
          throw (new SemanticException("found a un-sopported internal key name structure"));
        }
        outputKeyNamesInDispatch.add(outputKeyName);
      }

      if (!correlation.getDispatchKeySelectDescConfForOperationPath(newTag).containsKey(
          childOpIndex)) {
        correlation.getDispatchKeySelectDescConfForOperationPath(newTag).put(childOpIndex,
            new ArrayList<SelectDesc>());
      }
      correlation.getDispatchKeySelectDescConfForOperationPath(newTag).get(childOpIndex).
      add(new SelectDesc(thisKeyColsInDispatch, outputKeyNamesInDispatch, false));

      ArrayList<ExprNodeDesc> thisValueColsInDispatch = new ArrayList<ExprNodeDesc>();
      ArrayList<String> outputValueNamesInDispatch = new ArrayList<String>();
      for (ExprNodeDesc expr : rsop.getConf().getValueCols()) {

        String outputName = getColumnName(originalOpColumnExprMap.get(rsop), expr);
        LOG.debug("value column: " + outputName);
        LOG.debug("originalOpColumnExprMap.get(rsop):" + originalOpColumnExprMap.get(rsop) +
            " expr:" + expr.toString() +
            " orginalRS.getColumnInfos().toString:" + orginalRS.getColumnInfos().toString() + " "
            + outputName);
        ColumnInfo cinfo = orginalRS.getColumnInfos().get(orginalRS.getPosition(outputName));
        if (!valueCol2ExprForDispatch.containsKey(expr.getExprString())) {

          String col = SemanticAnalyzer.getColumnInternalName(keyCols.size() + valueCols.size());
          valueOutputColumnNames.add(col);
          ColumnInfo newColInfo = new ColumnInfo(col, cinfo.getType(), tableName, cinfo
              .getIsVirtualCol(), cinfo.isHiddenVirtualCol());
          colExprMap.put(newColInfo.getInternalName(), expr);
          outputRS.put(tableName, newColInfo.getInternalName(), newColInfo);
          valueCols.add(expr);

          valueCol2ExprForDispatch.put(expr.getExprString(), new ExprNodeColumnDesc(
              cinfo.getType(), col, tableName,
              false));
        }

        thisValueColsInDispatch.add(valueCol2ExprForDispatch.get(expr.getExprString()));
        String[] names = outputName.split("\\.");
        String outputValueName = "";
        switch (names.length) {
        case 1:
          outputValueName = names[0];
          break;
        case 2:
          outputValueName = names[1];
          break;
        default:
          throw (new SemanticException("found a un-sopported internal value name structure"));
        }
        outputValueNamesInDispatch.add(outputValueName);
      }

      if (!correlation.getDispatchValueSelectDescConfForOperationPath(newTag).containsKey(
          childOpIndex)) {
        correlation.getDispatchValueSelectDescConfForOperationPath(newTag).put(childOpIndex,
            new ArrayList<SelectDesc>());
      }
      correlation.getDispatchValueSelectDescConfForOperationPath(newTag).get(childOpIndex).
      add(new SelectDesc(thisValueColsInDispatch, outputValueNamesInDispatch, false));
    }

    ReduceSinkOperator rsOp = null;
    rsOp = (ReduceSinkOperator) putOpInsertMap(
        OperatorFactory.getAndMakeChild(getReduceSinkDesc(keyCols,
            keyCols.size(), valueCols, new ArrayList<List<Integer>>(),
            keyOutputColumnNames, valueOutputColumnNames, true, newTag, keyCols.size(),
            -1), new RowSchema(outputRS
                .getColumnInfos()), ycop), outputRS, pGraphContext.getOpParseCtx());
    rsOp.setColumnExprMap(colExprMap);
    ((CorrelationCompositeOperator) ycop).getConf().setCorrespondingReduceSinkOperator(rsOp);

    return rsOp;
  }


  /**
   * Generate reduce sink descriptor.
   *
   * @param keyCols
   *          The columns to be stored in the key
   * @param numKeys
   *          number of distribution keys. Equals to group-by-key
   *          numbers usually.
   * @param valueCols
   *          The columns to be stored in the value
   * @param distinctColIndices
   *          column indices for distinct aggregates
   * @param outputKeyColumnNames
   *          The output key columns names
   * @param outputValueColumnNames
   *          The output value columns names
   * @param tag
   *          The tag for this ReduceSinkOperator
   * @param numPartitionFields
   *          The first numPartitionFields of keyCols will be partition columns.
   *          If numPartitionFields=-1, then partition randomly.
   * @param numReducers
   *          The number of reducers, set to -1 for automatic inference based on
   *          input data size.
   * @return ReduceSinkDesc.
   */
  public static ReduceSinkDesc getReduceSinkDesc(
      ArrayList<ExprNodeDesc> keyCols, int numKeys,
      ArrayList<ExprNodeDesc> valueCols,
      List<List<Integer>> distinctColIndices,
      ArrayList<String> outputKeyColumnNames, ArrayList<String> outputValueColumnNames,
      boolean includeKey, int tag,
      int numPartitionFields, int numReducers) throws SemanticException {
    ArrayList<ExprNodeDesc> partitionCols = null;

    if (numPartitionFields >= keyCols.size()) {
      partitionCols = keyCols;
    } else if (numPartitionFields >= 0) {
      partitionCols = new ArrayList<ExprNodeDesc>(numPartitionFields);
      for (int i = 0; i < numPartitionFields; i++) {
        partitionCols.add(keyCols.get(i));
      }
    } else {
      // numPartitionFields = -1 means random partitioning
      partitionCols = new ArrayList<ExprNodeDesc>(1);
      partitionCols.add(TypeCheckProcFactory.DefaultExprProcessor
          .getFuncExprNodeDesc("rand"));
    }

    StringBuilder order = new StringBuilder();
    for (int i = 0; i < keyCols.size(); i++) {
      order.append("+");
    }

    TableDesc keyTable = null;
    TableDesc valueTable = null;
    ArrayList<String> outputKeyCols = new ArrayList<String>();
    ArrayList<String> outputValCols = new ArrayList<String>();
    if (includeKey) {
      keyTable = PlanUtils.getReduceKeyTableDesc(PlanUtils.getFieldSchemasFromColumnListWithLength(
          keyCols, distinctColIndices, outputKeyColumnNames, numKeys, ""),
          order.toString());
      outputKeyCols.addAll(outputKeyColumnNames);
    } else {
      keyTable = PlanUtils.getReduceKeyTableDesc(PlanUtils.getFieldSchemasFromColumnList(
          keyCols, "reducesinkkey"), order.toString());
      for (int i = 0; i < keyCols.size(); i++) {
        outputKeyCols.add("reducesinkkey" + i);
      }
    }
    valueTable = PlanUtils.getReduceValueTableDesc(PlanUtils.getFieldSchemasFromColumnList(
        valueCols, outputValueColumnNames, 0, ""));
    outputValCols.addAll(outputValueColumnNames);

    return new ReduceSinkDesc(keyCols, numKeys, valueCols, outputKeyCols,
        distinctColIndices, outputValCols,
        tag, partitionCols, numReducers, keyTable,
        valueTable, true);
  }

}
