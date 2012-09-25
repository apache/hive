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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.AbstractMapJoinOperator;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.LateralViewJoinOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.ScriptOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.UnionOperator;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.parse.GenMapRedWalker;
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.QBJoinTree;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.plan.JoinCondDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.MapredLocalWork;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

/**
 * Implementation of one of the rule-based map join optimization. User passes hints to specify
 * map-joins and during this optimization, all user specified map joins are converted to MapJoins -
 * the reduce sink operator above the join are converted to map sink operators. In future, once
 * statistics are implemented, this transformation can also be done based on costs.
 */
public class MapJoinProcessor implements Transform {

  private static final Log LOG = LogFactory.getLog(MapJoinProcessor.class.getName());

  private ParseContext pGraphContext;

  /**
   * empty constructor.
   */
  public MapJoinProcessor() {
    pGraphContext = null;
  }

  @SuppressWarnings("nls")
  private Operator<? extends OperatorDesc>
    putOpInsertMap(Operator<? extends OperatorDesc> op, RowResolver rr) {
    OpParseContext ctx = new OpParseContext(rr);
    pGraphContext.getOpParseCtx().put(op, ctx);
    return op;
  }

  /**
   * Generate the MapRed Local Work
   * @param newWork
   * @param mapJoinOp
   * @param bigTablePos
   * @return
   * @throws SemanticException
   */
  private static String genMapJoinLocalWork(MapredWork newWork, MapJoinOperator mapJoinOp,
      int bigTablePos) throws SemanticException {
    // keep the small table alias to avoid concurrent modification exception
    ArrayList<String> smallTableAliasList = new ArrayList<String>();
    String bigTableAlias = null;

    // create a new  MapredLocalWork
    MapredLocalWork newLocalWork = new MapredLocalWork(
        new LinkedHashMap<String, Operator<? extends OperatorDesc>>(),
        new LinkedHashMap<String, FetchWork>());

    for (Map.Entry<String, Operator<? extends OperatorDesc>> entry :
      newWork.getAliasToWork().entrySet()) {
      String alias = entry.getKey();
      Operator<? extends OperatorDesc> op = entry.getValue();

      // if the table scan is for big table; then skip it
      // tracing down the operator tree from the table scan operator
      Operator<? extends OperatorDesc> parentOp = op;
      Operator<? extends OperatorDesc> childOp = op.getChildOperators().get(0);
      while ((childOp != null) && (!childOp.equals(mapJoinOp))) {
        parentOp = childOp;
        assert parentOp.getChildOperators().size() == 1;
        childOp = parentOp.getChildOperators().get(0);
      }
      if (childOp == null) {
        throw new SemanticException(
            "Cannot find join op by tracing down the table scan operator tree");
      }
      // skip the big table pos
      int i = childOp.getParentOperators().indexOf(parentOp);
      if (i == bigTablePos) {
        bigTableAlias = alias;
        continue;
      }
      // set alias to work and put into smallTableAliasList
      newLocalWork.getAliasToWork().put(alias, op);
      smallTableAliasList.add(alias);
      // get input path and remove this alias from pathToAlias
      // because this file will be fetched by fetch operator
      LinkedHashMap<String, ArrayList<String>> pathToAliases = newWork.getPathToAliases();

      // keep record all the input path for this alias
      HashSet<String> pathSet = new HashSet<String>();
      HashSet<String> emptyPath = new HashSet<String>();
      for (Map.Entry<String, ArrayList<String>> entry2 : pathToAliases.entrySet()) {
        String path = entry2.getKey();
        ArrayList<String> list = entry2.getValue();
        if (list.contains(alias)) {
          // add to path set
          if (!pathSet.contains(path)) {
            pathSet.add(path);
          }
          //remove this alias from the alias list
          list.remove(alias);
          if(list.size() == 0) {
            emptyPath.add(path);
          }
        }
      }
      //remove the path, with which no alias associates
      for (String path : emptyPath) {
        pathToAliases.remove(path);
      }

      // create fetch work
      FetchWork fetchWork = null;
      List<String> partDir = new ArrayList<String>();
      List<PartitionDesc> partDesc = new ArrayList<PartitionDesc>();

      for (String tablePath : pathSet) {
        PartitionDesc partitionDesc = newWork.getPathToPartitionInfo().get(tablePath);
        // create fetchwork for non partitioned table
        if (partitionDesc.getPartSpec() == null || partitionDesc.getPartSpec().size() == 0) {
          fetchWork = new FetchWork(tablePath, partitionDesc.getTableDesc());
          break;
        }
        // if table is partitioned,add partDir and partitionDesc
        partDir.add(tablePath);
        partDesc.add(partitionDesc);
      }
      // create fetchwork for partitioned table
      if (fetchWork == null) {
        TableDesc table = newWork.getAliasToPartnInfo().get(alias).getTableDesc();
        fetchWork = new FetchWork(partDir, partDesc, table);
      }
      // set alias to fetch work
      newLocalWork.getAliasToFetchWork().put(alias, fetchWork);
    }
    // remove small table ailias from aliasToWork;Avoid concurrent modification
    for (String alias : smallTableAliasList) {
      newWork.getAliasToWork().remove(alias);
    }

    // set up local work
    newWork.setMapLocalWork(newLocalWork);
    // remove reducer
    newWork.setReducer(null);
    // return the big table alias
    if (bigTableAlias == null) {
      throw new SemanticException("Big Table Alias is null");
    }
    return bigTableAlias;
  }

  public static String genMapJoinOpAndLocalWork(MapredWork newWork, JoinOperator op, int mapJoinPos)
    throws SemanticException {
    try {
      LinkedHashMap<Operator<? extends OperatorDesc>, OpParseContext> opParseCtxMap =
        newWork.getOpParseCtxMap();
      QBJoinTree newJoinTree = newWork.getJoinTree();
      // generate the map join operator; already checked the map join
      MapJoinOperator newMapJoinOp = MapJoinProcessor.convertMapJoin(opParseCtxMap, op,
          newJoinTree, mapJoinPos, true);
      // generate the local work and return the big table alias
      String bigTableAlias = MapJoinProcessor
          .genMapJoinLocalWork(newWork, newMapJoinOp, mapJoinPos);
      // clean up the mapred work
      newWork.setOpParseCtxMap(null);
      newWork.setJoinTree(null);

      return bigTableAlias;

    } catch (Exception e) {
      e.printStackTrace();
      throw new SemanticException("Generate New MapJoin Opertor Exeception " + e.getMessage());
    }

  }

  /**
   * convert a regular join to a a map-side join.
   *
   * @param opParseCtxMap
   * @param op
   *          join operator
   * @param joinTree
   *          qb join tree
   * @param mapJoinPos
   *          position of the source to be read as part of map-reduce framework. All other sources
   *          are cached in memory
   * @param noCheckOuterJoin
   */
  public static MapJoinOperator convertMapJoin(
    LinkedHashMap<Operator<? extends OperatorDesc>, OpParseContext> opParseCtxMap,
    JoinOperator op, QBJoinTree joinTree, int mapJoinPos, boolean noCheckOuterJoin)
    throws SemanticException {
    // outer join cannot be performed on a table which is being cached
    JoinDesc desc = op.getConf();
    JoinCondDesc[] condns = desc.getConds();
    Byte[] tagOrder = desc.getTagOrder();

    if (!noCheckOuterJoin) {
      checkMapJoin(mapJoinPos, condns);
    }

    RowResolver oldOutputRS = opParseCtxMap.get(op).getRowResolver();
    RowResolver outputRS = new RowResolver();
    ArrayList<String> outputColumnNames = new ArrayList<String>();
    Map<Byte, List<ExprNodeDesc>> keyExprMap = new HashMap<Byte, List<ExprNodeDesc>>();
    Map<Byte, List<ExprNodeDesc>> valueExprMap = new HashMap<Byte, List<ExprNodeDesc>>();

    // Walk over all the sources (which are guaranteed to be reduce sink
    // operators).
    // The join outputs a concatenation of all the inputs.
    QBJoinTree leftSrc = joinTree.getJoinSrc();

    List<Operator<? extends OperatorDesc>> parentOps = op.getParentOperators();
    List<Operator<? extends OperatorDesc>> newParentOps =
      new ArrayList<Operator<? extends OperatorDesc>>();
    List<Operator<? extends OperatorDesc>> oldReduceSinkParentOps =
       new ArrayList<Operator<? extends OperatorDesc>>();
    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
    HashMap<Byte, HashMap<String, ExprNodeDesc>> columnTransfer =
      new HashMap<Byte, HashMap<String, ExprNodeDesc>>();

    // found a source which is not to be stored in memory
    if (leftSrc != null) {
      // assert mapJoinPos == 0;
      Operator<? extends OperatorDesc> parentOp = parentOps.get(0);
      assert parentOp.getParentOperators().size() == 1;
      Operator<? extends OperatorDesc> grandParentOp =
        parentOp.getParentOperators().get(0);
      oldReduceSinkParentOps.add(parentOp);
      grandParentOp.removeChild(parentOp);
      newParentOps.add(grandParentOp);
    }

    int pos = 0;
    // Remove parent reduce-sink operators
    for (String src : joinTree.getBaseSrc()) {
      if (src != null) {
        Operator<? extends OperatorDesc> parentOp = parentOps.get(pos);
        assert parentOp.getParentOperators().size() == 1;
        Operator<? extends OperatorDesc> grandParentOp =
          parentOp.getParentOperators().get(0);

        grandParentOp.removeChild(parentOp);
        oldReduceSinkParentOps.add(parentOp);
        newParentOps.add(grandParentOp);
      }
      pos++;
    }

    // get the join keys from old parent ReduceSink operators
    for (pos = 0; pos < newParentOps.size(); pos++) {
      ReduceSinkOperator oldPar = (ReduceSinkOperator) oldReduceSinkParentOps.get(pos);
      ReduceSinkDesc rsconf = oldPar.getConf();
      Byte tag = (byte) rsconf.getTag();
      List<ExprNodeDesc> keys = rsconf.getKeyCols();
      keyExprMap.put(tag, keys);

      // set column transfer
      HashMap<String, ExprNodeDesc> map = (HashMap<String, ExprNodeDesc>) oldPar.getColumnExprMap();
      columnTransfer.put(tag, map);
    }

    // create the map-join operator
    for (pos = 0; pos < newParentOps.size(); pos++) {
      RowResolver inputRS = opParseCtxMap.get(newParentOps.get(pos)).getRowResolver();
      List<ExprNodeDesc> values = new ArrayList<ExprNodeDesc>();

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
            ExprNodeDesc colDesc = new ExprNodeColumnDesc(valueInfo.getType(), valueInfo
                .getInternalName(), valueInfo.getTabAlias(), valueInfo.getIsVirtualCol());
            values.add(colDesc);
            outputRS.put(key, field, new ColumnInfo(outputCol, valueInfo.getType(), valueInfo
                .getTabAlias(), valueInfo.getIsVirtualCol(), valueInfo.isHiddenVirtualCol()));
            colExprMap.put(outputCol, colDesc);
          }
        }
      }

      valueExprMap.put(Byte.valueOf((byte) pos), values);
    }

    Map<Byte, List<ExprNodeDesc>> filters = desc.getFilters();
    for (Map.Entry<Byte, List<ExprNodeDesc>> entry : filters.entrySet()) {
      Byte srcAlias = entry.getKey();
      List<ExprNodeDesc> columnDescList = entry.getValue();

      for (ExprNodeDesc nodeExpr : columnDescList) {
        ExprNodeGenericFuncDesc funcDesc = (ExprNodeGenericFuncDesc) nodeExpr;
        for (ExprNodeDesc childDesc : funcDesc.getChildExprs()) {
          if (!(childDesc instanceof ExprNodeColumnDesc)) {
            continue;
          }
          ExprNodeColumnDesc columnDesc = (ExprNodeColumnDesc) childDesc;
          // reset columns
          String column = columnDesc.getColumn();
          String newColumn = null;
          HashMap<String, ExprNodeDesc> map = columnTransfer.get(srcAlias);
          ExprNodeColumnDesc tmpDesc = (ExprNodeColumnDesc) map.get(column);
          if (tmpDesc != null) {
            newColumn = tmpDesc.getColumn();
          }
          if (newColumn == null) {
            throw new SemanticException("No Column name found in parent reduce sink op");
          }
          columnDesc.setColumn(newColumn);
        }
      }
    }

    JoinCondDesc[] joinCondns = op.getConf().getConds();

    Operator[] newPar = new Operator[newParentOps.size()];
    pos = 0;
    for (Operator<? extends OperatorDesc> o : newParentOps) {
      newPar[pos++] = o;
    }

    List<ExprNodeDesc> keyCols = keyExprMap.get(Byte.valueOf((byte) 0));
    StringBuilder keyOrder = new StringBuilder();
    for (int i = 0; i < keyCols.size(); i++) {
      keyOrder.append("+");
    }

    TableDesc keyTableDesc = PlanUtils.getMapJoinKeyTableDesc(PlanUtils
        .getFieldSchemasFromColumnList(keyCols, "mapjoinkey"));

    List<TableDesc> valueTableDescs = new ArrayList<TableDesc>();
    List<TableDesc> valueFiltedTableDescs = new ArrayList<TableDesc>();

    int[][] filterMap = desc.getFilterMap();
    for (pos = 0; pos < newParentOps.size(); pos++) {
      List<ExprNodeDesc> valueCols = valueExprMap.get(Byte.valueOf((byte) pos));
      int length = valueCols.size();
      List<ExprNodeDesc> valueFilteredCols = new ArrayList<ExprNodeDesc>(length);
      // deep copy expr node desc
      for (int i = 0; i < length; i++) {
        valueFilteredCols.add(valueCols.get(i).clone());
      }
      if (filterMap != null && filterMap[pos] != null && pos != mapJoinPos) {
        ExprNodeColumnDesc isFilterDesc = new ExprNodeColumnDesc(TypeInfoFactory
            .getPrimitiveTypeInfo(Constants.TINYINT_TYPE_NAME), "filter", "filter", false);
        valueFilteredCols.add(isFilterDesc);
      }


      keyOrder = new StringBuilder();
      for (int i = 0; i < valueCols.size(); i++) {
        keyOrder.append("+");
      }

      TableDesc valueTableDesc = PlanUtils.getMapJoinValueTableDesc(PlanUtils
          .getFieldSchemasFromColumnList(valueCols, "mapjoinvalue"));
      TableDesc valueFilteredTableDesc = PlanUtils.getMapJoinValueTableDesc(PlanUtils
          .getFieldSchemasFromColumnList(valueFilteredCols, "mapjoinvalue"));

      valueTableDescs.add(valueTableDesc);
      valueFiltedTableDescs.add(valueFilteredTableDesc);
    }
    String dumpFilePrefix = "";
    if( joinTree.getMapAliases() != null ) {
      for(String mapAlias : joinTree.getMapAliases()) {
        dumpFilePrefix = dumpFilePrefix + mapAlias;
      }
      dumpFilePrefix = dumpFilePrefix+"-"+PlanUtils.getCountForMapJoinDumpFilePrefix();
    } else {
      dumpFilePrefix = "mapfile"+PlanUtils.getCountForMapJoinDumpFilePrefix();
    }
    MapJoinDesc mapJoinDescriptor = new MapJoinDesc(keyExprMap, keyTableDesc, valueExprMap,
        valueTableDescs, valueFiltedTableDescs, outputColumnNames, mapJoinPos, joinCondns,
        filters, op.getConf().getNoOuterJoin(), dumpFilePrefix);
    mapJoinDescriptor.setTagOrder(tagOrder);
    mapJoinDescriptor.setNullSafes(desc.getNullSafes());
    mapJoinDescriptor.setFilterMap(desc.getFilterMap());

    MapJoinOperator mapJoinOp = (MapJoinOperator) OperatorFactory.getAndMakeChild(
        mapJoinDescriptor, new RowSchema(outputRS.getColumnInfos()), newPar);

    OpParseContext ctx = new OpParseContext(outputRS);
    opParseCtxMap.put(mapJoinOp, ctx);

    mapJoinOp.getConf().setReversedExprs(op.getConf().getReversedExprs());
    mapJoinOp.setColumnExprMap(colExprMap);

    // change the children of the original join operator to point to the map
    // join operator
    List<Operator<? extends OperatorDesc>> childOps = op.getChildOperators();
    for (Operator<? extends OperatorDesc> childOp : childOps) {
      childOp.replaceParent(op, mapJoinOp);
    }

    mapJoinOp.setChildOperators(childOps);
    mapJoinOp.setParentOperators(newParentOps);
    op.setChildOperators(null);
    op.setParentOperators(null);

    return mapJoinOp;
  }

  public MapJoinOperator generateMapJoinOperator(ParseContext pctx, JoinOperator op,
      QBJoinTree joinTree, int mapJoinPos) throws SemanticException {
    HiveConf hiveConf = pctx.getConf();
    boolean noCheckOuterJoin = HiveConf.getBoolVar(hiveConf,
        HiveConf.ConfVars.HIVEOPTSORTMERGEBUCKETMAPJOIN)
        && HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVEOPTBUCKETMAPJOIN);


    LinkedHashMap<Operator<? extends OperatorDesc>, OpParseContext> opParseCtxMap = pctx
        .getOpParseCtx();
    MapJoinOperator mapJoinOp = convertMapJoin(opParseCtxMap, op, joinTree, mapJoinPos,
        noCheckOuterJoin);
    // create a dummy select to select all columns
    genSelectPlan(pctx, mapJoinOp);
    return mapJoinOp;
  }

  /**
   * Get a list of big table candidates. Only the tables in the returned set can
   * be used as big table in the join operation.
   *
   * The logic here is to scan the join condition array from left to right. If
   * see a inner join, and the bigTableCandidates is empty or the outer join
   * that we last saw is a right outer join, add both side of this inner join to
   * big table candidates only if they are not in bad position. If see a left
   * outer join, set lastSeenRightOuterJoin to false, and the bigTableCandidates
   * is empty, add the left side to it, and if the bigTableCandidates is not
   * empty, do nothing (which means the bigTableCandidates is from left side).
   * If see a right outer join, set lastSeenRightOuterJoin to true, clear the
   * bigTableCandidates, and add right side to the bigTableCandidates, it means
   * the right side of a right outer join always win. If see a full outer join,
   * return null immediately (no one can be the big table, can not do a
   * mapjoin).
   *
   *
   * @param condns
   * @return list of big table candidates
   */
  public static HashSet<Integer> getBigTableCandidates(JoinCondDesc[] condns) {
    HashSet<Integer> bigTableCandidates = new HashSet<Integer>();

    boolean seenOuterJoin = false;
    Set<Integer> seenPostitions = new HashSet<Integer>();
    Set<Integer> leftPosListOfLastRightOuterJoin = new HashSet<Integer>();

    // is the outer join that we saw most recently is a right outer join?
    boolean lastSeenRightOuterJoin = false;
    for (JoinCondDesc condn : condns) {
      int joinType = condn.getType();
      seenPostitions.add(condn.getLeft());
      seenPostitions.add(condn.getRight());

      if (joinType == JoinDesc.FULL_OUTER_JOIN) {
        // setting these 2 parameters here just in case that if the code got
        // changed in future, these 2 are not missing.
        seenOuterJoin = true;
        lastSeenRightOuterJoin = false;
        return null;
      } else if (joinType == JoinDesc.LEFT_OUTER_JOIN
          || joinType == JoinDesc.LEFT_SEMI_JOIN) {
        seenOuterJoin = true;
        if(bigTableCandidates.size() == 0) {
          bigTableCandidates.add(condn.getLeft());
        }

        lastSeenRightOuterJoin = false;
      } else if (joinType == JoinDesc.RIGHT_OUTER_JOIN) {
        seenOuterJoin = true;
        lastSeenRightOuterJoin = true;
        // add all except the right side to the bad positions
        leftPosListOfLastRightOuterJoin.clear();
        leftPosListOfLastRightOuterJoin.addAll(seenPostitions);
        leftPosListOfLastRightOuterJoin.remove(condn.getRight());

        bigTableCandidates.clear();
        bigTableCandidates.add(condn.getRight());
      } else if (joinType == JoinDesc.INNER_JOIN) {
        if (!seenOuterJoin || lastSeenRightOuterJoin) {
          // is the left was at the left side of a right outer join?
          if (!leftPosListOfLastRightOuterJoin.contains(condn.getLeft())) {
            bigTableCandidates.add(condn.getLeft());
          }
          // is the right was at the left side of a right outer join?
          if (!leftPosListOfLastRightOuterJoin.contains(condn.getRight())) {
            bigTableCandidates.add(condn.getRight());
          }
        }
      }
    }

    return bigTableCandidates;
  }

  public static void checkMapJoin(int mapJoinPos, JoinCondDesc[] condns) throws SemanticException {
    HashSet<Integer> bigTableCandidates = MapJoinProcessor.getBigTableCandidates(condns);

    if (bigTableCandidates == null || !bigTableCandidates.contains(mapJoinPos)) {
      throw new SemanticException(ErrorMsg.NO_OUTER_MAPJOIN.getMsg());
    }
    return;
  }

  private void genSelectPlan(ParseContext pctx, MapJoinOperator input) throws SemanticException {
    List<Operator<? extends OperatorDesc>> childOps = input.getChildOperators();
    input.setChildOperators(null);

    // create a dummy select - This select is needed by the walker to split the
    // mapJoin later on
    RowResolver inputRR = pctx.getOpParseCtx().get(input).getRowResolver();

    ArrayList<ExprNodeDesc> exprs = new ArrayList<ExprNodeDesc>();
    ArrayList<String> outputs = new ArrayList<String>();
    List<String> outputCols = input.getConf().getOutputColumnNames();
    RowResolver outputRS = new RowResolver();

    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();

    for (int i = 0; i < outputCols.size(); i++) {
      String internalName = outputCols.get(i);
      String[] nm = inputRR.reverseLookup(internalName);
      ColumnInfo valueInfo = inputRR.get(nm[0], nm[1]);
      ExprNodeDesc colDesc = new ExprNodeColumnDesc(valueInfo.getType(), valueInfo
          .getInternalName(), nm[0], valueInfo.getIsVirtualCol());
      exprs.add(colDesc);
      outputs.add(internalName);
      outputRS.put(nm[0], nm[1], new ColumnInfo(internalName, valueInfo.getType(), nm[0], valueInfo
          .getIsVirtualCol(), valueInfo.isHiddenVirtualCol()));
      colExprMap.put(internalName, colDesc);
    }

    SelectDesc select = new SelectDesc(exprs, outputs, false);

    SelectOperator sel = (SelectOperator) putOpInsertMap(OperatorFactory.getAndMakeChild(select,
        new RowSchema(inputRR.getColumnInfos()), input), inputRR);

    sel.setColumnExprMap(colExprMap);

    // Insert the select operator in between.
    sel.setChildOperators(childOps);
    for (Operator<? extends OperatorDesc> ch : childOps) {
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
   * @return -1 if it cannot be converted to a map-side join, position of the map join node
   *         otherwise
   */
  private int mapSideJoin(JoinOperator op, QBJoinTree joinTree) throws SemanticException {
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
        throw new SemanticException(ErrorMsg.INVALID_MAPJOIN_HINT.getMsg(pGraphContext.getQB()
            .getParseInfo().getHints()));
      }
    }

    return mapJoinPos;
  }

  /**
   * Transform the query tree. For each join, check if it is a map-side join (user specified). If
   * yes, convert it to a map-side join.
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
      Map<MapJoinOperator, QBJoinTree> mapJoinMap = pGraphContext.getMapJoinContext();
      if (mapJoinMap == null) {
        mapJoinMap = new HashMap<MapJoinOperator, QBJoinTree>();
        pGraphContext.setMapJoinContext(mapJoinMap);
      }

      Set<Map.Entry<JoinOperator, QBJoinTree>> joinCtx = pGraphContext.getJoinContext().entrySet();
      Iterator<Map.Entry<JoinOperator, QBJoinTree>> joinCtxIter = joinCtx.iterator();
      while (joinCtxIter.hasNext()) {
        Map.Entry<JoinOperator, QBJoinTree> joinEntry = joinCtxIter.next();
        JoinOperator joinOp = joinEntry.getKey();
        QBJoinTree qbJoin = joinEntry.getValue();
        int mapJoinPos = mapSideJoin(joinOp, qbJoin);
        if (mapJoinPos >= 0) {
          MapJoinOperator mapJoinOp = generateMapJoinOperator(pactx, joinOp, qbJoin, mapJoinPos);
          listMapJoinOps.add(mapJoinOp);
          mapJoinMap.put(mapJoinOp, qbJoin);
        } else {
          joinMap.put(joinOp, qbJoin);
        }
      }

      // store the new joinContext
      pGraphContext.setJoinContext(joinMap);
    }

    // Go over the list and find if a reducer is not needed
    List<AbstractMapJoinOperator<? extends MapJoinDesc>> listMapJoinOpsNoRed = new ArrayList<AbstractMapJoinOperator<? extends MapJoinDesc>>();

    // create a walker which walks the tree in a DFS manner while maintaining
    // the operator stack.
    // The dispatcher generates the plan from the operator tree
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    opRules.put(new RuleRegExp("R0",
      MapJoinOperator.getOperatorName() + "%"),
      getCurrentMapJoin());
    opRules.put(new RuleRegExp("R1",
      MapJoinOperator.getOperatorName() + "%.*" + FileSinkOperator.getOperatorName() + "%"),
      getMapJoinFS());
    opRules.put(new RuleRegExp("R2",
      MapJoinOperator.getOperatorName() + "%.*" + ReduceSinkOperator.getOperatorName() + "%"),
      getMapJoinDefault());
    opRules.put(new RuleRegExp("R4",
      MapJoinOperator.getOperatorName() + "%.*" + UnionOperator.getOperatorName() + "%"),
      getMapJoinDefault());

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(getDefault(), opRules, new MapJoinWalkerCtx(
        listMapJoinOpsNoRed, pGraphContext));

    GraphWalker ogw = new GenMapRedWalker(disp);
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(listMapJoinOps);
    ogw.startWalking(topNodes, null);

    pGraphContext.setListMapJoinOpsNoReducer(listMapJoinOpsNoRed);
    return pGraphContext;
  }

  /**
   * CurrentMapJoin.
   *
   */
  public static class CurrentMapJoin implements NodeProcessor {

    /**
     * Store the current mapjoin in the context.
     */
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      MapJoinWalkerCtx ctx = (MapJoinWalkerCtx) procCtx;
      MapJoinOperator mapJoin = (MapJoinOperator) nd;
      if (ctx.getListRejectedMapJoins() != null && !ctx.getListRejectedMapJoins().contains(mapJoin)) {
        // for rule: MapJoin%.*MapJoin
        // have a child mapjoin. if the the current mapjoin is on a local work,
        // will put the current mapjoin in the rejected list.
        Boolean bigBranch = findGrandChildSubqueryMapjoin(ctx, mapJoin);
        if (bigBranch == null) { // no child map join
          ctx.setCurrMapJoinOp(mapJoin);
          return null;
        }
        if (bigBranch) {
          addNoReducerMapJoinToCtx(ctx, mapJoin);
        } else {
          addRejectMapJoinToCtx(ctx, mapJoin);
        }
      } else {
        ctx.setCurrMapJoinOp(mapJoin);
      }
      return null;
    }

    private Boolean findGrandChildSubqueryMapjoin(MapJoinWalkerCtx ctx, MapJoinOperator mapJoin) {
      Operator<? extends OperatorDesc> parent = mapJoin;
      while (true) {
        if (parent.getChildOperators() == null || parent.getChildOperators().size() != 1) {
          return null;
        }
        Operator<? extends OperatorDesc> ch = parent.getChildOperators().get(0);
        if (ch instanceof MapJoinOperator) {
          if (!nonSubqueryMapJoin(ctx.getpGraphContext(), (MapJoinOperator) ch, mapJoin)) {
            if (ch.getParentOperators().indexOf(parent) == ((MapJoinOperator) ch).getConf()
                .getPosBigTable()) {
              // not come from the local branch
              return true;
            }
          }
          return false; // not from a sub-query.
        }

        if ((ch instanceof JoinOperator) || (ch instanceof UnionOperator)
            || (ch instanceof ReduceSinkOperator) || (ch instanceof LateralViewJoinOperator)
            || (ch instanceof GroupByOperator) || (ch instanceof ScriptOperator)) {
          return null;
        }

        parent = ch;
      }
    }

    private boolean nonSubqueryMapJoin(ParseContext pGraphContext, MapJoinOperator mapJoin,
        MapJoinOperator parentMapJoin) {
      QBJoinTree joinTree = pGraphContext.getMapJoinContext().get(mapJoin);
      QBJoinTree parentJoinTree = pGraphContext.getMapJoinContext().get(parentMapJoin);
      if (joinTree.getJoinSrc() != null && joinTree.getJoinSrc().equals(parentJoinTree)) {
        return true;
      }
      return false;
    }
  }

  private static void addNoReducerMapJoinToCtx(MapJoinWalkerCtx ctx,
      AbstractMapJoinOperator<? extends MapJoinDesc> mapJoin) {
    if (ctx.getListRejectedMapJoins() != null && ctx.getListRejectedMapJoins().contains(mapJoin)) {
      return;
    }
    List<AbstractMapJoinOperator<? extends MapJoinDesc>> listMapJoinsNoRed = ctx
        .getListMapJoinsNoRed();
    if (listMapJoinsNoRed == null) {
      listMapJoinsNoRed = new ArrayList<AbstractMapJoinOperator<? extends MapJoinDesc>>();
    }
    if (!listMapJoinsNoRed.contains(mapJoin)) {
      listMapJoinsNoRed.add(mapJoin);
    }
    ctx.setListMapJoins(listMapJoinsNoRed);
  }

  private static void addRejectMapJoinToCtx(MapJoinWalkerCtx ctx,
      AbstractMapJoinOperator<? extends MapJoinDesc> mapjoin) {
    // current map join is null means it has been handled by CurrentMapJoin
    // process.
    if (mapjoin == null) {
      return;
    }
    List<AbstractMapJoinOperator<? extends MapJoinDesc>> listRejectedMapJoins = ctx
        .getListRejectedMapJoins();
    if (listRejectedMapJoins == null) {
      listRejectedMapJoins = new ArrayList<AbstractMapJoinOperator<? extends MapJoinDesc>>();
    }
    if (!listRejectedMapJoins.contains(mapjoin)) {
      listRejectedMapJoins.add(mapjoin);
    }

    if (ctx.getListMapJoinsNoRed() != null && ctx.getListMapJoinsNoRed().contains(mapjoin)) {
      ctx.getListMapJoinsNoRed().remove(mapjoin);
    }

    ctx.setListRejectedMapJoins(listRejectedMapJoins);
  }

  /**
   * MapJoinFS.
   *
   */
  public static class MapJoinFS implements NodeProcessor {

    /**
     * Store the current mapjoin in a list of mapjoins followed by a filesink.
     */
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      MapJoinWalkerCtx ctx = (MapJoinWalkerCtx) procCtx;
      AbstractMapJoinOperator<? extends MapJoinDesc> mapJoin = ctx.getCurrMapJoinOp();
      List<AbstractMapJoinOperator<? extends MapJoinDesc>> listRejectedMapJoins = ctx
          .getListRejectedMapJoins();

      // the mapjoin has already been handled
      if ((listRejectedMapJoins != null) && (listRejectedMapJoins.contains(mapJoin))) {
        return null;
      }
      addNoReducerMapJoinToCtx(ctx, mapJoin);
      return null;
    }
  }

  /**
   * MapJoinDefault.
   *
   */
  public static class MapJoinDefault implements NodeProcessor {

    /**
     * Store the mapjoin in a rejected list.
     */
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      MapJoinWalkerCtx ctx = (MapJoinWalkerCtx) procCtx;
      AbstractMapJoinOperator<? extends MapJoinDesc> mapJoin = ctx.getCurrMapJoinOp();
      addRejectMapJoinToCtx(ctx, mapJoin);
      return null;
    }
  }

  /**
   * Default.
   *
   */
  public static class Default implements NodeProcessor {

    /**
     * Nothing to do.
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

  /**
   * MapJoinWalkerCtx.
   *
   */
  public static class MapJoinWalkerCtx implements NodeProcessorCtx {

    private ParseContext pGraphContext;
    private List<AbstractMapJoinOperator<? extends MapJoinDesc>> listMapJoinsNoRed;
    private List<AbstractMapJoinOperator<? extends MapJoinDesc>> listRejectedMapJoins;
    private AbstractMapJoinOperator<? extends MapJoinDesc> currMapJoinOp;

    /**
     * @param listMapJoinsNoRed
     * @param pGraphContext
     */
    public MapJoinWalkerCtx(List<AbstractMapJoinOperator<? extends MapJoinDesc>> listMapJoinsNoRed,
        ParseContext pGraphContext) {
      this.listMapJoinsNoRed = listMapJoinsNoRed;
      currMapJoinOp = null;
      listRejectedMapJoins = new ArrayList<AbstractMapJoinOperator<? extends MapJoinDesc>>();
      this.pGraphContext = pGraphContext;
    }

    /**
     * @return the listMapJoins
     */
    public List<AbstractMapJoinOperator<? extends MapJoinDesc>> getListMapJoinsNoRed() {
      return listMapJoinsNoRed;
    }

    /**
     * @param listMapJoinsNoRed
     *          the listMapJoins to set
     */
    public void setListMapJoins(
        List<AbstractMapJoinOperator<? extends MapJoinDesc>> listMapJoinsNoRed) {
      this.listMapJoinsNoRed = listMapJoinsNoRed;
    }

    /**
     * @return the currMapJoinOp
     */
    public AbstractMapJoinOperator<? extends MapJoinDesc> getCurrMapJoinOp() {
      return currMapJoinOp;
    }

    /**
     * @param currMapJoinOp
     *          the currMapJoinOp to set
     */
    public void setCurrMapJoinOp(AbstractMapJoinOperator<? extends MapJoinDesc> currMapJoinOp) {
      this.currMapJoinOp = currMapJoinOp;
    }

    /**
     * @return the listRejectedMapJoins
     */
    public List<AbstractMapJoinOperator<? extends MapJoinDesc>> getListRejectedMapJoins() {
      return listRejectedMapJoins;
    }

    /**
     * @param listRejectedMapJoins
     *          the listRejectedMapJoins to set
     */
    public void setListRejectedMapJoins(
        List<AbstractMapJoinOperator<? extends MapJoinDesc>> listRejectedMapJoins) {
      this.listRejectedMapJoins = listRejectedMapJoins;
    }

    public ParseContext getpGraphContext() {
      return pGraphContext;
    }

    public void setpGraphContext(ParseContext pGraphContext) {
      this.pGraphContext = pGraphContext;
    }

  }
}
