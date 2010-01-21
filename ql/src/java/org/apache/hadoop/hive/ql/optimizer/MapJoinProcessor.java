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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.parse.ErrorMsg;
import org.apache.hadoop.hive.ql.parse.GenMapRedWalker;
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.QBJoinTree;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.exprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.ql.plan.joinDesc;
import org.apache.hadoop.hive.ql.plan.mapJoinDesc;
import org.apache.hadoop.hive.ql.plan.reduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.selectDesc;
import org.apache.hadoop.hive.ql.plan.tableDesc;

/**
 * Implementation of one of the rule-based map join optimization. User passes
 * hints to specify map-joins and during this optimization, all user specified
 * map joins are converted to MapJoins - the reduce sink operator above the join
 * are converted to map sink operators. In future, once statistics are
 * implemented, this transformation can also be done based on costs.
 */
public class MapJoinProcessor implements Transform {
  private ParseContext pGraphContext;

  /**
   * empty constructor
   */
  public MapJoinProcessor() {
    pGraphContext = null;
  }

  @SuppressWarnings("nls")
  private Operator<? extends Serializable> putOpInsertMap(
      Operator<? extends Serializable> op, RowResolver rr) {
    OpParseContext ctx = new OpParseContext(rr);
    pGraphContext.getOpParseCtx().put(op, ctx);
    return op;
  }

  /**
   * convert a regular join to a a map-side join.
   * 
   * @param op
   *          join operator
   * @param qbJoin
   *          qb join tree
   * @param mapJoinPos
   *          position of the source to be read as part of map-reduce framework.
   *          All other sources are cached in memory
   */
  private MapJoinOperator convertMapJoin(ParseContext pctx, JoinOperator op,
      QBJoinTree joinTree, int mapJoinPos) throws SemanticException {
    // outer join cannot be performed on a table which is being cached
    joinDesc desc = op.getConf();
    org.apache.hadoop.hive.ql.plan.joinCond[] condns = desc.getConds();
    for (org.apache.hadoop.hive.ql.plan.joinCond condn : condns) {
      if (condn.getType() == joinDesc.FULL_OUTER_JOIN) {
        throw new SemanticException(ErrorMsg.NO_OUTER_MAPJOIN.getMsg());
      }
      if ((condn.getType() == joinDesc.LEFT_OUTER_JOIN)
          && (condn.getLeft() != mapJoinPos)) {
        throw new SemanticException(ErrorMsg.NO_OUTER_MAPJOIN.getMsg());
      }
      if ((condn.getType() == joinDesc.RIGHT_OUTER_JOIN)
          && (condn.getRight() != mapJoinPos)) {
        throw new SemanticException(ErrorMsg.NO_OUTER_MAPJOIN.getMsg());
      }
    }

    RowResolver oldOutputRS = pctx.getOpParseCtx().get(op).getRR();
    RowResolver outputRS = new RowResolver();
    ArrayList<String> outputColumnNames = new ArrayList<String>();
    Map<Byte, List<exprNodeDesc>> keyExprMap = new HashMap<Byte, List<exprNodeDesc>>();
    Map<Byte, List<exprNodeDesc>> valueExprMap = new HashMap<Byte, List<exprNodeDesc>>();

    // Walk over all the sources (which are guaranteed to be reduce sink
    // operators).
    // The join outputs a concatenation of all the inputs.
    QBJoinTree leftSrc = joinTree.getJoinSrc();

    List<Operator<? extends Serializable>> parentOps = op.getParentOperators();
    List<Operator<? extends Serializable>> newParentOps = new ArrayList<Operator<? extends Serializable>>();
    List<Operator<? extends Serializable>> oldReduceSinkParentOps = new ArrayList<Operator<? extends Serializable>>();
    Map<String, exprNodeDesc> colExprMap = new HashMap<String, exprNodeDesc>();
    // found a source which is not to be stored in memory
    if (leftSrc != null) {
      // assert mapJoinPos == 0;
      Operator<? extends Serializable> parentOp = parentOps.get(0);
      assert parentOp.getParentOperators().size() == 1;
      Operator<? extends Serializable> grandParentOp = parentOp
          .getParentOperators().get(0);
      oldReduceSinkParentOps.add(parentOp);
      grandParentOp.removeChild(parentOp);
      newParentOps.add(grandParentOp);
    }

    int pos = 0;
    // Remove parent reduce-sink operators
    for (String src : joinTree.getBaseSrc()) {
      if (src != null) {
        Operator<? extends Serializable> parentOp = parentOps.get(pos);
        assert parentOp.getParentOperators().size() == 1;
        Operator<? extends Serializable> grandParentOp = parentOp
            .getParentOperators().get(0);

        grandParentOp.removeChild(parentOp);
        oldReduceSinkParentOps.add(parentOp);
        newParentOps.add(grandParentOp);
      }
      pos++;
    }

    // get the join keys from old parent ReduceSink operators
    for (pos = 0; pos < newParentOps.size(); pos++) {
      ReduceSinkOperator oldPar = (ReduceSinkOperator) oldReduceSinkParentOps
          .get(pos);
      reduceSinkDesc rsconf = oldPar.getConf();
      Byte tag = (byte) rsconf.getTag();
      List<exprNodeDesc> keys = rsconf.getKeyCols();
      keyExprMap.put(tag, keys);
    }

    // create the map-join operator
    for (pos = 0; pos < newParentOps.size(); pos++) {
      RowResolver inputRS = pGraphContext.getOpParseCtx().get(
          newParentOps.get(pos)).getRR();

      List<exprNodeDesc> values = new ArrayList<exprNodeDesc>();

      Iterator<String> keysIter = inputRS.getTableNames().iterator();
      while (keysIter.hasNext()) {
        String key = keysIter.next();
        HashMap<String, ColumnInfo> rrMap = inputRS.getFieldMap(key);
        Iterator<String> fNamesIter = rrMap.keySet().iterator();
        while (fNamesIter.hasNext()) {
          String field = fNamesIter.next();
          ColumnInfo valueInfo = inputRS.get(key, field);
          ColumnInfo oldValueInfo = oldOutputRS.get(key, field);
          if (oldValueInfo == null) {
            continue;
          }
          String outputCol = oldValueInfo.getInternalName();
          if (outputRS.get(key, field) == null) {
            outputColumnNames.add(outputCol);
            exprNodeDesc colDesc = new exprNodeColumnDesc(valueInfo.getType(),
                valueInfo.getInternalName(), valueInfo.getTabAlias(), valueInfo
                    .getIsPartitionCol());
            values.add(colDesc);
            outputRS.put(key, field, new ColumnInfo(outputCol, valueInfo
                .getType(), valueInfo.getTabAlias(), valueInfo
                .getIsPartitionCol()));
            colExprMap.put(outputCol, colDesc);
          }
        }
      }

      valueExprMap.put(new Byte((byte) pos), values);
    }

    org.apache.hadoop.hive.ql.plan.joinCond[] joinCondns = op.getConf()
        .getConds();

    Operator[] newPar = new Operator[newParentOps.size()];
    pos = 0;
    for (Operator<? extends Serializable> o : newParentOps) {
      newPar[pos++] = o;
    }

    List<exprNodeDesc> keyCols = keyExprMap.get(new Byte((byte) 0));
    StringBuilder keyOrder = new StringBuilder();
    for (int i = 0; i < keyCols.size(); i++) {
      keyOrder.append("+");
    }

    tableDesc keyTableDesc = PlanUtils.getMapJoinKeyTableDesc(PlanUtils
        .getFieldSchemasFromColumnList(keyCols, "mapjoinkey"));

    List<tableDesc> valueTableDescs = new ArrayList<tableDesc>();

    for (pos = 0; pos < newParentOps.size(); pos++) {
      List<exprNodeDesc> valueCols = valueExprMap.get(new Byte((byte) pos));
      keyOrder = new StringBuilder();
      for (int i = 0; i < valueCols.size(); i++) {
        keyOrder.append("+");
      }

      tableDesc valueTableDesc = PlanUtils.getMapJoinValueTableDesc(PlanUtils
          .getFieldSchemasFromColumnList(valueCols, "mapjoinvalue"));

      valueTableDescs.add(valueTableDesc);
    }

    MapJoinOperator mapJoinOp = (MapJoinOperator) putOpInsertMap(
        OperatorFactory.getAndMakeChild(new mapJoinDesc(keyExprMap,
            keyTableDesc, valueExprMap, valueTableDescs, outputColumnNames,
            mapJoinPos, joinCondns), new RowSchema(outputRS.getColumnInfos()),
            newPar), outputRS);

    mapJoinOp.getConf().setReversedExprs(op.getConf().getReversedExprs());
    mapJoinOp.setColumnExprMap(colExprMap);

    // change the children of the original join operator to point to the map
    // join operator
    List<Operator<? extends Serializable>> childOps = op.getChildOperators();
    for (Operator<? extends Serializable> childOp : childOps) {
      childOp.replaceParent(op, mapJoinOp);
    }

    mapJoinOp.setChildOperators(childOps);
    mapJoinOp.setParentOperators(newParentOps);
    op.setChildOperators(null);
    op.setParentOperators(null);

    // create a dummy select to select all columns
    genSelectPlan(pctx, mapJoinOp);
    return mapJoinOp;
  }

  private void genSelectPlan(ParseContext pctx, MapJoinOperator input)
      throws SemanticException {
    List<Operator<? extends Serializable>> childOps = input.getChildOperators();
    input.setChildOperators(null);

    // create a dummy select - This select is needed by the walker to split the
    // mapJoin later on
    RowResolver inputRR = pctx.getOpParseCtx().get(input).getRR();

    ArrayList<exprNodeDesc> exprs = new ArrayList<exprNodeDesc>();
    ArrayList<String> outputs = new ArrayList<String>();
    List<String> outputCols = input.getConf().getOutputColumnNames();
    RowResolver outputRS = new RowResolver();

    Map<String, exprNodeDesc> colExprMap = new HashMap<String, exprNodeDesc>();

    for (int i = 0; i < outputCols.size(); i++) {
      String internalName = outputCols.get(i);
      String[] nm = inputRR.reverseLookup(internalName);
      ColumnInfo valueInfo = inputRR.get(nm[0], nm[1]);
      exprNodeDesc colDesc = new exprNodeColumnDesc(valueInfo.getType(),
          valueInfo.getInternalName(), nm[0], valueInfo.getIsPartitionCol());
      exprs.add(colDesc);
      outputs.add(internalName);
      outputRS.put(nm[0], nm[1], new ColumnInfo(internalName, valueInfo
          .getType(), nm[0], valueInfo.getIsPartitionCol()));
      colExprMap.put(internalName, colDesc);
    }

    selectDesc select = new selectDesc(exprs, outputs, false);

    SelectOperator sel = (SelectOperator) putOpInsertMap(
        OperatorFactory.getAndMakeChild(select, new RowSchema(inputRR
            .getColumnInfos()), input), inputRR);

    sel.setColumnExprMap(colExprMap);

    // Insert the select operator in between.
    sel.setChildOperators(childOps);
    for (Operator<? extends Serializable> ch : childOps) {
      ch.replaceParent(input, sel);
    }
  }

  /**
   * Is it a map-side join.
   * 
   * @param op
   *          join operator
   * @param qbJoin
   *          qb join tree
   * @return -1 if it cannot be converted to a map-side join, position of the
   *         map join node otherwise
   */
  private int mapSideJoin(JoinOperator op, QBJoinTree joinTree)
      throws SemanticException {
    int mapJoinPos = -1;
    if (joinTree.isMapSideJoin()) {
      int pos = 0;
      // In a map-side join, exactly one table is not present in memory.
      // The client provides the list of tables which can be cached in memory
      // via a hint.
      if (joinTree.getJoinSrc() != null) {
        mapJoinPos = pos;
      }
      for (String src : joinTree.getBaseSrc()) {
        if (src != null) {
          if (!joinTree.getMapAliases().contains(src)) {
            if (mapJoinPos >= 0) {
              return -1;
            }
            mapJoinPos = pos;
          }
        }
        pos++;
      }

      // All tables are to be cached - this is not possible. In future, we can
      // support this by randomly
      // leaving some table from the list of tables to be cached
      if (mapJoinPos == -1) {
        throw new SemanticException(ErrorMsg.INVALID_MAPJOIN_HINT
            .getMsg(pGraphContext.getQB().getParseInfo().getHints()));
      }
    }

    return mapJoinPos;
  }

  /**
   * Transform the query tree. For each join, check if it is a map-side join
   * (user specified). If yes, convert it to a map-side join.
   * 
   * @param pactx
   *          current parse context
   */
  public ParseContext transform(ParseContext pactx) throws SemanticException {
    pGraphContext = pactx;
    List<MapJoinOperator> listMapJoinOps = new ArrayList<MapJoinOperator>();

    // traverse all the joins and convert them if necessary
    if (pGraphContext.getJoinContext() != null) {
      Map<JoinOperator, QBJoinTree> joinMap = new HashMap<JoinOperator, QBJoinTree>();

      Set<Map.Entry<JoinOperator, QBJoinTree>> joinCtx = pGraphContext
          .getJoinContext().entrySet();
      Iterator<Map.Entry<JoinOperator, QBJoinTree>> joinCtxIter = joinCtx
          .iterator();
      while (joinCtxIter.hasNext()) {
        Map.Entry<JoinOperator, QBJoinTree> joinEntry = joinCtxIter.next();
        JoinOperator joinOp = joinEntry.getKey();
        QBJoinTree qbJoin = joinEntry.getValue();
        int mapJoinPos = mapSideJoin(joinOp, qbJoin);
        if (mapJoinPos >= 0) {
          listMapJoinOps.add(convertMapJoin(pactx, joinOp, qbJoin, mapJoinPos));
        } else {
          joinMap.put(joinOp, qbJoin);
        }
      }

      // store the new joinContext
      pGraphContext.setJoinContext(joinMap);
    }

    // Go over the list and find if a reducer is not needed
    List<MapJoinOperator> listMapJoinOpsNoRed = new ArrayList<MapJoinOperator>();

    // create a walker which walks the tree in a DFS manner while maintaining
    // the operator stack.
    // The dispatcher generates the plan from the operator tree
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    opRules.put(new RuleRegExp(new String("R0"), "MAPJOIN%"),
        getCurrentMapJoin());
    opRules.put(new RuleRegExp(new String("R1"), "MAPJOIN%.*FS%"),
        getMapJoinFS());
    opRules.put(new RuleRegExp(new String("R2"), "MAPJOIN%.*RS%"),
        getMapJoinDefault());
    opRules.put(new RuleRegExp(new String("R3"), "MAPJOIN%.*MAPJOIN%"),
        getMapJoinDefault());
    opRules.put(new RuleRegExp(new String("R4"), "MAPJOIN%.*UNION%"),
        getMapJoinDefault());

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(getDefault(), opRules,
        new MapJoinWalkerCtx(listMapJoinOpsNoRed));

    GraphWalker ogw = new GenMapRedWalker(disp);
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(listMapJoinOps);
    ogw.startWalking(topNodes, null);

    pGraphContext.setListMapJoinOpsNoReducer(listMapJoinOpsNoRed);
    return pGraphContext;
  }

  public static class CurrentMapJoin implements NodeProcessor {

    /**
     * Store the current mapjoin in the context
     */
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      MapJoinWalkerCtx ctx = (MapJoinWalkerCtx) procCtx;
      MapJoinOperator mapJoin = (MapJoinOperator) nd;
      ctx.setCurrMapJoinOp(mapJoin);
      return null;
    }
  }

  public static class MapJoinFS implements NodeProcessor {

    /**
     * Store the current mapjoin in a list of mapjoins followed by a filesink
     */
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      MapJoinWalkerCtx ctx = (MapJoinWalkerCtx) procCtx;
      MapJoinOperator mapJoin = ctx.getCurrMapJoinOp();
      List<MapJoinOperator> listRejectedMapJoins = ctx
          .getListRejectedMapJoins();

      // the mapjoin has already been handled
      if ((listRejectedMapJoins != null)
          && (listRejectedMapJoins.contains(mapJoin))) {
        return null;
      }

      List<MapJoinOperator> listMapJoinsNoRed = ctx.getListMapJoinsNoRed();
      if (listMapJoinsNoRed == null) {
        listMapJoinsNoRed = new ArrayList<MapJoinOperator>();
      }
      listMapJoinsNoRed.add(mapJoin);
      ctx.setListMapJoins(listMapJoinsNoRed);
      return null;
    }
  }

  public static class MapJoinDefault implements NodeProcessor {

    /**
     * Store the mapjoin in a rejected list
     */
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      MapJoinWalkerCtx ctx = (MapJoinWalkerCtx) procCtx;
      MapJoinOperator mapJoin = ctx.getCurrMapJoinOp();
      List<MapJoinOperator> listRejectedMapJoins = ctx
          .getListRejectedMapJoins();
      if (listRejectedMapJoins == null) {
        listRejectedMapJoins = new ArrayList<MapJoinOperator>();
      }
      listRejectedMapJoins.add(mapJoin);
      ctx.setListRejectedMapJoins(listRejectedMapJoins);
      return null;
    }
  }

  public static class Default implements NodeProcessor {

    /**
     * nothing to do
     */
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      return null;
    }
  }

  public static NodeProcessor getMapJoinFS() {
    return new MapJoinFS();
  }

  public static NodeProcessor getMapJoinDefault() {
    return new MapJoinDefault();
  }

  public static NodeProcessor getDefault() {
    return new Default();
  }

  public static NodeProcessor getCurrentMapJoin() {
    return new CurrentMapJoin();
  }

  public static class MapJoinWalkerCtx implements NodeProcessorCtx {
    List<MapJoinOperator> listMapJoinsNoRed;
    List<MapJoinOperator> listRejectedMapJoins;
    MapJoinOperator currMapJoinOp;

    /**
     * @param listMapJoinsNoRed
     */
    public MapJoinWalkerCtx(List<MapJoinOperator> listMapJoinsNoRed) {
      this.listMapJoinsNoRed = listMapJoinsNoRed;
      currMapJoinOp = null;
      listRejectedMapJoins = new ArrayList<MapJoinOperator>();
    }

    /**
     * @return the listMapJoins
     */
    public List<MapJoinOperator> getListMapJoinsNoRed() {
      return listMapJoinsNoRed;
    }

    /**
     * @param listMapJoinsNoRed
     *          the listMapJoins to set
     */
    public void setListMapJoins(List<MapJoinOperator> listMapJoinsNoRed) {
      this.listMapJoinsNoRed = listMapJoinsNoRed;
    }

    /**
     * @return the currMapJoinOp
     */
    public MapJoinOperator getCurrMapJoinOp() {
      return currMapJoinOp;
    }

    /**
     * @param currMapJoinOp
     *          the currMapJoinOp to set
     */
    public void setCurrMapJoinOp(MapJoinOperator currMapJoinOp) {
      this.currMapJoinOp = currMapJoinOp;
    }

    /**
     * @return the listRejectedMapJoins
     */
    public List<MapJoinOperator> getListRejectedMapJoins() {
      return listRejectedMapJoins;
    }

    /**
     * @param listRejectedMapJoins
     *          the listRejectedMapJoins to set
     */
    public void setListRejectedMapJoins(
        List<MapJoinOperator> listRejectedMapJoins) {
      this.listRejectedMapJoins = listRejectedMapJoins;
    }
  }
}
