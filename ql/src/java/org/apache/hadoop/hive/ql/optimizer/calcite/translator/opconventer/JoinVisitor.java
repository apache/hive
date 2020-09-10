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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAntiJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveMultiJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSemiJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortExchange;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.opconventer.HiveOpConverter.OpAttr;
import org.apache.hadoop.hive.ql.parse.JoinCond;
import org.apache.hadoop.hive.ql.parse.JoinType;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.JoinCondDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

class JoinVisitor extends HiveRelNodeVisitor<RelNode> {
  JoinVisitor(HiveOpConverter hiveOpConverter) {
    super(hiveOpConverter);
  }

  @Override
  OpAttr visit(RelNode joinRel) throws SemanticException {
    // 0. Additional data structures needed for the join optimization
    // through Hive
    String[] baseSrc = new String[joinRel.getInputs().size()];
    String tabAlias = hiveOpConverter.getHiveDerivedTableAlias();

    // 1. Convert inputs
    OpAttr[] inputs = new OpAttr[joinRel.getInputs().size()];
    List<Operator<?>> children = new ArrayList<Operator<?>>(joinRel.getInputs().size());
    for (int i = 0; i < inputs.length; i++) {
      inputs[i] = hiveOpConverter.dispatch(joinRel.getInput(i));
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
    if (joinRel instanceof HiveMultiJoin || !((joinRel instanceof Join) &&
            ((((Join) joinRel).isSemiJoin()) || (((Join) joinRel).getJoinType() == JoinRelType.ANTI)))) {
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
      joinExpressions[i] = ((HiveSortExchange) joinRel.getInput(i)).getKeyExpressions();
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
    } else if (joinRel instanceof HiveAntiJoin){
      joinFilters = ImmutableList.of(((HiveAntiJoin)joinRel).getJoinFilter());
    } else {
      throw new SemanticException("Can't handle join type: " + joinRel.getClass().getName());
    }
    List<List<ExprNodeDesc>> filterExpressions = Lists.newArrayList();
    for (int i = 0; i< joinFilters.size(); i++) {
      List<ExprNodeDesc> filterExpressionsForInput = new ArrayList<ExprNodeDesc>();
      if (joinFilters.get(i) != null) {
        for (RexNode conj : RelOptUtil.conjunctions(joinFilters.get(i))) {
          ExprNodeDesc expr = HiveOpConverterUtils.convertToExprNode(conj, joinRel, null, newVcolsInCalcite);
          filterExpressionsForInput.add(expr);
        }
      }
      filterExpressions.add(filterExpressionsForInput);
    }

    // 6. Generate Join operator
    JoinOperator joinOp = genJoin(joinRel, joinExpressions, filterExpressions, children, baseSrc, tabAlias);

    // 7. Return result
    return new OpAttr(tabAlias, newVcolsInCalcite, joinOp);
  }

  private JoinOperator genJoin(RelNode join, ExprNodeDesc[][] joinExpressions,
      List<List<ExprNodeDesc>> filterExpressions, List<Operator<?>> children, String[] baseSrc, String tabAlias)
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
      JoinRelType joinRelType = JoinRelType.INNER;
      if (join instanceof Join) {
        joinRelType = ((Join) join).getJoinType();
      }
      JoinType joinType;
      switch (joinRelType) {
        case SEMI:
          joinType = JoinType.LEFTSEMI;
          semiJoin = true;
          break;
        case ANTI:
          joinType = JoinType.ANTI;
          semiJoin = true;
          break;
        default:
          assert join instanceof Join;
          joinType = transformJoinType(((Join)join).getJoinType());
          semiJoin = false;
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

      Map<String, ExprNodeDesc> descriptors = buildBacktrackFromReduceSinkForJoin(outputPos, outputColumnNames,
          keyColNames, valColNames, index, parent, baseSrc[pos]);

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
    JoinDesc desc = new JoinDesc(exprMap, outputColumnNames, noOuterJoin, joinCondns, filters, joinExpressions, null);
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

  private Map<String, ExprNodeDesc> buildBacktrackFromReduceSinkForJoin(int initialPos, List<String> outputColumnNames,
      List<String> keyColNames, List<String> valueColNames, int[] index, Operator<?> inputOp, String tabAlias) {
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

  /*
   * This method updates the input expr, changing all the
   * ExprNodeColumnDesc in it to refer to columns given by the
   * colExprMap.
   *
   * For instance, "col_0 = 1" would become "VALUE.col_0 = 1";
   * the execution engine expects filters in the Join operators
   * to be expressed that way.
   */
  private int updateExprNode(ExprNodeDesc expr, final Map<String, Byte> reversedExprs,
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

  private void updateFilterMap(int[][] filterMap, int inputPos, int joinPos) {
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

  private JoinType transformJoinType(JoinRelType type) {
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
}
