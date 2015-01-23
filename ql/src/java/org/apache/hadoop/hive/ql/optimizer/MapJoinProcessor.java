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
import java.util.Arrays;
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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ObjectPair;
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
import org.apache.hadoop.hive.ql.exec.SMBMapJoinOperator;
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
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.plan.JoinCondDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.MapredLocalWork;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.SMBJoinDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

/**
 * Implementation of one of the rule-based map join optimization. User passes hints to specify
 * map-joins and during this optimization, all user specified map joins are converted to MapJoins -
 * the reduce sink operator above the join are converted to map sink operators. In future, once
 * statistics are implemented, this transformation can also be done based on costs.
 */
public class MapJoinProcessor implements Transform {

  private static final Log LOG = LogFactory.getLog(MapJoinProcessor.class.getName());
  // mapjoin table descriptor contains a key descriptor which needs the field schema
  // (column type + column name). The column name is not really used anywhere, but it
  // needs to be passed. Use the string defined below for that.
  private static final String MAPJOINKEY_FIELDPREFIX = "mapjoinkey";

  public MapJoinProcessor() {
  }

  @SuppressWarnings("nls")
  private static Operator<? extends OperatorDesc> putOpInsertMap (
          ParseContext pGraphContext, Operator<? extends OperatorDesc> op,
          RowResolver rr) {
    OpParseContext ctx = new OpParseContext(rr);
    pGraphContext.getOpParseCtx().put(op, ctx);
    return op;
  }

  /**
   * Generate the MapRed Local Work for the given map-join operator
   *
   * @param newWork
   * @param mapJoinOp
   *          map-join operator for which local work needs to be generated.
   * @param bigTablePos
   * @throws SemanticException
   */
  private static void genMapJoinLocalWork(MapredWork newWork, MapJoinOperator mapJoinOp,
      int bigTablePos) throws SemanticException {
    // keep the small table alias to avoid concurrent modification exception
    ArrayList<String> smallTableAliasList = new ArrayList<String>();

    // create a new  MapredLocalWork
    MapredLocalWork newLocalWork = new MapredLocalWork(
        new LinkedHashMap<String, Operator<? extends OperatorDesc>>(),
        new LinkedHashMap<String, FetchWork>());

    for (Map.Entry<String, Operator<? extends OperatorDesc>> entry :
      newWork.getMapWork().getAliasToWork().entrySet()) {
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
        continue;
      }
      // set alias to work and put into smallTableAliasList
      newLocalWork.getAliasToWork().put(alias, op);
      smallTableAliasList.add(alias);
      // get input path and remove this alias from pathToAlias
      // because this file will be fetched by fetch operator
      LinkedHashMap<String, ArrayList<String>> pathToAliases = newWork.getMapWork().getPathToAliases();

      // keep record all the input path for this alias
      HashSet<String> pathSet = new HashSet<String>();
      HashSet<String> emptyPath = new HashSet<String>();
      for (Map.Entry<String, ArrayList<String>> entry2 : pathToAliases.entrySet()) {
        String path = entry2.getKey();
        ArrayList<String> list = entry2.getValue();
        if (list.contains(alias)) {
          // add to path set
          pathSet.add(path);
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
      List<Path> partDir = new ArrayList<Path>();
      List<PartitionDesc> partDesc = new ArrayList<PartitionDesc>();

      for (String tablePath : pathSet) {
        PartitionDesc partitionDesc = newWork.getMapWork().getPathToPartitionInfo().get(tablePath);
        // create fetchwork for non partitioned table
        if (partitionDesc.getPartSpec() == null || partitionDesc.getPartSpec().size() == 0) {
          fetchWork = new FetchWork(new Path(tablePath), partitionDesc.getTableDesc());
          break;
        }
        // if table is partitioned,add partDir and partitionDesc
        partDir.add(new Path(tablePath));
        partDesc.add(partitionDesc);
      }
      // create fetchwork for partitioned table
      if (fetchWork == null) {
        TableDesc table = newWork.getMapWork().getAliasToPartnInfo().get(alias).getTableDesc();
        fetchWork = new FetchWork(partDir, partDesc, table);
      }
      // set alias to fetch work
      newLocalWork.getAliasToFetchWork().put(alias, fetchWork);
    }
    // remove small table ailias from aliasToWork;Avoid concurrent modification
    for (String alias : smallTableAliasList) {
      newWork.getMapWork().getAliasToWork().remove(alias);
    }

    // set up local work
    newWork.getMapWork().setMapRedLocalWork(newLocalWork);
    // remove reducer
    newWork.setReduceWork(null);
  }

  /**
   * Convert the join to a map-join and also generate any local work needed.
   *
   * @param newWork MapredWork in which the conversion is to happen
   * @param op
   *          The join operator that needs to be converted to map-join
   * @param mapJoinPos
   * @throws SemanticException
   */
  public static void genMapJoinOpAndLocalWork(HiveConf conf, MapredWork newWork,
    JoinOperator op, int mapJoinPos)
      throws SemanticException {
    LinkedHashMap<Operator<? extends OperatorDesc>, OpParseContext> opParseCtxMap =
        newWork.getMapWork().getOpParseCtxMap();
    // generate the map join operator; already checked the map join
    MapJoinOperator newMapJoinOp = new MapJoinProcessor().convertMapJoin(conf, opParseCtxMap, op,
        newWork.getMapWork().isLeftInputJoin(), newWork.getMapWork().getBaseSrc(), newWork.getMapWork().getMapAliases(),
        mapJoinPos, true, false);
    genLocalWorkForMapJoin(newWork, newMapJoinOp, mapJoinPos);
  }

  public static void genLocalWorkForMapJoin(MapredWork newWork, MapJoinOperator newMapJoinOp,
      int mapJoinPos)
      throws SemanticException {
    try {
      // generate the local work for the big table alias
      MapJoinProcessor.genMapJoinLocalWork(newWork, newMapJoinOp, mapJoinPos);
      // clean up the mapred work
      newWork.getMapWork().setOpParseCtxMap(null);
      newWork.getMapWork().setLeftInputJoin(false);
      newWork.getMapWork().setBaseSrc(null);
      newWork.getMapWork().setMapAliases(null);

    } catch (Exception e) {
      e.printStackTrace();
      throw new SemanticException("Failed to generate new mapJoin operator " +
          "by exception : " + e.getMessage());
    }
  }

  private static void checkParentOperatorType(Operator<? extends OperatorDesc> op)
      throws SemanticException {
    if (!op.opAllowedBeforeMapJoin()) {
      throw new SemanticException(ErrorMsg.OPERATOR_NOT_ALLOWED_WITH_MAPJOIN.getMsg());
    }
    if (op.getParentOperators() != null) {
      for (Operator<? extends OperatorDesc> parentOp : op.getParentOperators()) {
        checkParentOperatorType(parentOp);
      }
    }
  }

  private static void checkChildOperatorType(Operator<? extends OperatorDesc> op)
      throws SemanticException {
    if (!op.opAllowedAfterMapJoin()) {
      throw new SemanticException(ErrorMsg.OPERATOR_NOT_ALLOWED_WITH_MAPJOIN.getMsg());
    }
    if (op.getChildOperators() != null) {
      for (Operator<? extends OperatorDesc> childOp : op.getChildOperators()) {
        checkChildOperatorType(childOp);
      }
    }
  }

  private static void validateMapJoinTypes(Operator<? extends OperatorDesc> op)
      throws SemanticException {
    for (Operator<? extends OperatorDesc> parentOp : op.getParentOperators()) {
      checkParentOperatorType(parentOp);
    }

    for (Operator<? extends OperatorDesc> childOp : op.getChildOperators()) {
      checkChildOperatorType(childOp);
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
   * @param validateMapJoinTree
   */
  public MapJoinOperator convertMapJoin(HiveConf conf,
    LinkedHashMap<Operator<? extends OperatorDesc>, OpParseContext> opParseCtxMap,
    JoinOperator op, boolean leftInputJoin, String[] baseSrc, List<String> mapAliases,
    int mapJoinPos, boolean noCheckOuterJoin, boolean validateMapJoinTree) throws SemanticException {

    // outer join cannot be performed on a table which is being cached
    JoinDesc desc = op.getConf();
    JoinCondDesc[] condns = desc.getConds();

    if (!noCheckOuterJoin) {
      if (checkMapJoin(mapJoinPos, condns) < 0) {
        throw new SemanticException(ErrorMsg.NO_OUTER_MAPJOIN.getMsg());
      }
    }

    // Walk over all the sources (which are guaranteed to be reduce sink
    // operators).
    // The join outputs a concatenation of all the inputs.
    List<Operator<? extends OperatorDesc>> parentOps = op.getParentOperators();
    List<Operator<? extends OperatorDesc>> newParentOps =
      new ArrayList<Operator<? extends OperatorDesc>>();
    List<Operator<? extends OperatorDesc>> oldReduceSinkParentOps =
       new ArrayList<Operator<? extends OperatorDesc>>();

    // found a source which is not to be stored in memory
    if (leftInputJoin) {
      // assert mapJoinPos == 0;
      Operator<? extends OperatorDesc> parentOp = parentOps.get(0);
      assert parentOp.getParentOperators().size() == 1;
      Operator<? extends OperatorDesc> grandParentOp =
        parentOp.getParentOperators().get(0);
      oldReduceSinkParentOps.add(parentOp);
      newParentOps.add(grandParentOp);
    }

    byte pos = 0;
    // Remove parent reduce-sink operators
    for (String src : baseSrc) {
      if (src != null) {
        Operator<? extends OperatorDesc> parentOp = parentOps.get(pos);
        assert parentOp.getParentOperators().size() == 1;
        Operator<? extends OperatorDesc> grandParentOp =
          parentOp.getParentOperators().get(0);

        oldReduceSinkParentOps.add(parentOp);
        newParentOps.add(grandParentOp);
      }
      pos++;
    }

    // create the map-join operator
    MapJoinOperator mapJoinOp = convertJoinOpMapJoinOp(conf, opParseCtxMap,
        op, leftInputJoin, baseSrc, mapAliases, mapJoinPos, noCheckOuterJoin);


    // remove old parents
    for (pos = 0; pos < newParentOps.size(); pos++) {
      newParentOps.get(pos).replaceChild(oldReduceSinkParentOps.get(pos), mapJoinOp);
    }

    mapJoinOp.getParentOperators().removeAll(oldReduceSinkParentOps);
    mapJoinOp.setParentOperators(newParentOps);

    // make sure only map-joins can be performed.
    if (validateMapJoinTree) {
      validateMapJoinTypes(mapJoinOp);
    }

    // change the children of the original join operator to point to the map
    // join operator

    return mapJoinOp;
  }

  public static MapJoinOperator convertJoinOpMapJoinOp(HiveConf hconf,
      LinkedHashMap<Operator<? extends OperatorDesc>, OpParseContext> opParseCtxMap,
      JoinOperator op, boolean leftInputJoin, String[] baseSrc, List<String> mapAliases,
      int mapJoinPos, boolean noCheckOuterJoin) throws SemanticException {

    MapJoinDesc mapJoinDescriptor =
        getMapJoinDesc(hconf, opParseCtxMap, op, leftInputJoin, baseSrc, mapAliases,
                mapJoinPos, noCheckOuterJoin);

    // reduce sink row resolver used to generate map join op
    RowResolver outputRS = opParseCtxMap.get(op).getRowResolver();

    MapJoinOperator mapJoinOp = (MapJoinOperator) OperatorFactory.getAndMakeChild(
        mapJoinDescriptor, new RowSchema(outputRS.getColumnInfos()), op.getParentOperators());

    OpParseContext ctx = new OpParseContext(outputRS);
    opParseCtxMap.put(mapJoinOp, ctx);

    mapJoinOp.getConf().setReversedExprs(op.getConf().getReversedExprs());
    Map<String, ExprNodeDesc> colExprMap = op.getColumnExprMap();
    mapJoinOp.setColumnExprMap(colExprMap);

    List<Operator<? extends OperatorDesc>> childOps = op.getChildOperators();
    for (Operator<? extends OperatorDesc> childOp : childOps) {
      childOp.replaceParent(op, mapJoinOp);
    }

    mapJoinOp.setPosToAliasMap(op.getPosToAliasMap());
    mapJoinOp.setChildOperators(childOps);
    op.setChildOperators(null);
    op.setParentOperators(null);

    return mapJoinOp;

  }

  private static boolean needValueIndex(int[] valueIndex) {
    for (int i = 0; i < valueIndex.length; i++) {
      if (valueIndex[i] != -i - 1) {
        return true;
      }
    }
    return false;
  }

  /**
   * convert a sortmerge join to a a map-side join.
   *
   * @param opParseCtxMap
   * @param smbJoinOp
   *          join operator
   * @param joinTree
   *          qb join tree
   * @param bigTablePos
   *          position of the source to be read as part of map-reduce framework. All other sources
   *          are cached in memory
   * @param noCheckOuterJoin
   */
  public static MapJoinOperator convertSMBJoinToMapJoin(HiveConf hconf,
    Map<Operator<? extends OperatorDesc>, OpParseContext> opParseCtxMap,
    SMBMapJoinOperator smbJoinOp, int bigTablePos, boolean noCheckOuterJoin)
    throws SemanticException {
    // Create a new map join operator
    SMBJoinDesc smbJoinDesc = smbJoinOp.getConf();
    List<ExprNodeDesc> keyCols = smbJoinDesc.getKeys().get(Byte.valueOf((byte) 0));
    TableDesc keyTableDesc = PlanUtils.getMapJoinKeyTableDesc(hconf, PlanUtils
        .getFieldSchemasFromColumnList(keyCols, MAPJOINKEY_FIELDPREFIX));
    MapJoinDesc mapJoinDesc = new MapJoinDesc(smbJoinDesc.getKeys(),
        keyTableDesc, smbJoinDesc.getExprs(),
        smbJoinDesc.getValueTblDescs(), smbJoinDesc.getValueTblDescs(),
        smbJoinDesc.getOutputColumnNames(),
        bigTablePos, smbJoinDesc.getConds(),
        smbJoinDesc.getFilters(), smbJoinDesc.isNoOuterJoin(), smbJoinDesc.getDumpFilePrefix());

    mapJoinDesc.setStatistics(smbJoinDesc.getStatistics());

    RowResolver joinRS = opParseCtxMap.get(smbJoinOp).getRowResolver();
    // The mapjoin has the same schema as the join operator
    MapJoinOperator mapJoinOp = (MapJoinOperator) OperatorFactory.getAndMakeChild(
        mapJoinDesc, joinRS.getRowSchema(),
        new ArrayList<Operator<? extends OperatorDesc>>());

    OpParseContext ctx = new OpParseContext(joinRS);
    opParseCtxMap.put(mapJoinOp, ctx);

    // change the children of the original join operator to point to the map
    // join operator
    List<Operator<? extends OperatorDesc>> childOps = smbJoinOp.getChildOperators();
    for (Operator<? extends OperatorDesc> childOp : childOps) {
      childOp.replaceParent(smbJoinOp, mapJoinOp);
    }
    mapJoinOp.setChildOperators(childOps);
    smbJoinOp.setChildOperators(null);

    // change the parent of the original SMBjoin operator to point to the map
    // join operator
    List<Operator<? extends OperatorDesc>> parentOps = smbJoinOp.getParentOperators();
    for (Operator<? extends OperatorDesc> parentOp : parentOps) {
      parentOp.replaceChild(smbJoinOp, mapJoinOp);
    }
    mapJoinOp.setParentOperators(parentOps);
    smbJoinOp.setParentOperators(null);

    return mapJoinOp;
  }

  public MapJoinOperator generateMapJoinOperator(ParseContext pctx, JoinOperator op,
      int mapJoinPos) throws SemanticException {
    HiveConf hiveConf = pctx.getConf();
    boolean noCheckOuterJoin = HiveConf.getBoolVar(hiveConf,
        HiveConf.ConfVars.HIVEOPTSORTMERGEBUCKETMAPJOIN)
        && HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVEOPTBUCKETMAPJOIN);

    LinkedHashMap<Operator<? extends OperatorDesc>, OpParseContext> opParseCtxMap = pctx
        .getOpParseCtx();
    MapJoinOperator mapJoinOp = convertMapJoin(pctx.getConf(), opParseCtxMap, op,
        op.getConf().isLeftInputJoin(), op.getConf().getBaseSrc(), op.getConf().getMapAliases(),
        mapJoinPos, noCheckOuterJoin, true);
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
   * return empty set immediately (no one can be the big table, can not do a
   * mapjoin).
   *
   *
   * @param condns
   * @return set of big table candidates
   */
  public static Set<Integer> getBigTableCandidates(JoinCondDesc[] condns) {
    Set<Integer> bigTableCandidates = new HashSet<Integer>();

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
        // empty set - cannot convert
        return new HashSet<Integer>();
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

  /**
   * @param mapJoinPos the position of big table as determined by either hints or auto conversion.
   * @param condns the join conditions
   * @return if given mapjoin position is a feasible big table position return same else -1.
   * @throws SemanticException if given position is not in the big table candidates.
   */
  public static int checkMapJoin(int mapJoinPos, JoinCondDesc[] condns) {
    Set<Integer> bigTableCandidates = MapJoinProcessor.getBigTableCandidates(condns);

    // bigTableCandidates can never be null
    if (!bigTableCandidates.contains(mapJoinPos)) {
      return -1;
    }
    return mapJoinPos;
  }

  protected void genSelectPlan(ParseContext pctx, MapJoinOperator input) throws SemanticException {
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

    SelectOperator sel = (SelectOperator) putOpInsertMap(pctx, OperatorFactory.getAndMakeChild(select,
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
   * @return -1 if it cannot be converted to a map-side join, position of the map join node
   *         otherwise
   */
  private int mapSideJoin(JoinOperator op) throws SemanticException {
    int mapJoinPos = -1;
    if (op.getConf().isMapSideJoin()) {
      int pos = 0;
      // In a map-side join, exactly one table is not present in memory.
      // The client provides the list of tables which can be cached in memory
      // via a hint.
      if (op.getConf().isLeftInputJoin()) {
        mapJoinPos = pos;
      }
      for (String src : op.getConf().getBaseSrc()) {
        if (src != null) {
          if (!op.getConf().getMapAliases().contains(src)) {
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
        throw new SemanticException(ErrorMsg.INVALID_MAPJOIN_HINT.getMsg(
            Arrays.toString(op.getConf().getBaseSrc())));
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
  @Override
  public ParseContext transform(ParseContext pactx) throws SemanticException {
    List<MapJoinOperator> listMapJoinOps = new ArrayList<MapJoinOperator>();

    // traverse all the joins and convert them if necessary
    if (pactx.getJoinOps() != null) {
      Set<JoinOperator> joinMap = new HashSet<JoinOperator>();
      Set<MapJoinOperator> mapJoinMap = pactx.getMapJoinOps();
      if (mapJoinMap == null) {
        mapJoinMap = new HashSet<MapJoinOperator>();
        pactx.setMapJoinOps(mapJoinMap);
      }

      Iterator<JoinOperator> joinCtxIter = pactx.getJoinOps().iterator();
      while (joinCtxIter.hasNext()) {
        JoinOperator joinOp = joinCtxIter.next();
        int mapJoinPos = mapSideJoin(joinOp);
        if (mapJoinPos >= 0) {
          MapJoinOperator mapJoinOp = generateMapJoinOperator(pactx, joinOp, mapJoinPos);
          listMapJoinOps.add(mapJoinOp);
          mapJoinOp.getConf().setQBJoinTreeProps(joinOp.getConf());
          mapJoinMap.add(mapJoinOp);
        } else {
          joinOp.getConf().setQBJoinTreeProps(joinOp.getConf());
          joinMap.add(joinOp);
        }
      }

      // store the new joinContext
      pactx.setJoinOps(joinMap);
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
        listMapJoinOpsNoRed, pactx));

    GraphWalker ogw = new GenMapRedWalker(disp);
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(listMapJoinOps);
    ogw.startWalking(topNodes, null);

    pactx.setListMapJoinOpsNoReducer(listMapJoinOpsNoRed);
    return pactx;
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
          if (!nonSubqueryMapJoin((MapJoinOperator) ch, mapJoin)) {
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

    private boolean nonSubqueryMapJoin(MapJoinOperator mapJoin, MapJoinOperator parentMapJoin) {
      if (mapJoin.getParentOperators().contains(parentMapJoin)) {
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

  public static ObjectPair<List<ReduceSinkOperator>, Map<Byte, List<ExprNodeDesc>>> getKeys(
          boolean leftInputJoin, String[] baseSrc, JoinOperator op) {

    // Walk over all the sources (which are guaranteed to be reduce sink
    // operators).
    // The join outputs a concatenation of all the inputs.
    List<ReduceSinkOperator> oldReduceSinkParentOps =
        new ArrayList<ReduceSinkOperator>(op.getNumParent());
    if (leftInputJoin) {
      // assert mapJoinPos == 0;
      Operator<? extends OperatorDesc> parentOp = op.getParentOperators().get(0);
      assert parentOp.getParentOperators().size() == 1;
      oldReduceSinkParentOps.add((ReduceSinkOperator) parentOp);
    }

    byte pos = 0;
    for (String src : baseSrc) {
      if (src != null) {
        Operator<? extends OperatorDesc> parentOp = op.getParentOperators().get(pos);
        assert parentOp.getParentOperators().size() == 1;
        oldReduceSinkParentOps.add((ReduceSinkOperator) parentOp);
      }
      pos++;
    }

    // get the join keys from old parent ReduceSink operators
    Map<Byte, List<ExprNodeDesc>> keyExprMap = new HashMap<Byte, List<ExprNodeDesc>>();

    for (pos = 0; pos < op.getParentOperators().size(); pos++) {
      ReduceSinkOperator inputRS = oldReduceSinkParentOps.get(pos);
      List<ExprNodeDesc> keyCols = inputRS.getConf().getKeyCols();
      keyExprMap.put(pos, keyCols);
    }

    return new ObjectPair<List<ReduceSinkOperator>, Map<Byte,List<ExprNodeDesc>>>(
            oldReduceSinkParentOps, keyExprMap);
  }

  public static MapJoinDesc getMapJoinDesc(HiveConf hconf,
      LinkedHashMap<Operator<? extends OperatorDesc>, OpParseContext> opParseCtxMap,
      JoinOperator op, boolean leftInputJoin, String[] baseSrc, List<String> mapAliases,
      int mapJoinPos, boolean noCheckOuterJoin) throws SemanticException {
    JoinDesc desc = op.getConf();
    JoinCondDesc[] condns = desc.getConds();
    Byte[] tagOrder = desc.getTagOrder();

    // outer join cannot be performed on a table which is being cached
    if (!noCheckOuterJoin) {
      if (checkMapJoin(mapJoinPos, condns) < 0) {
        throw new SemanticException(ErrorMsg.NO_OUTER_MAPJOIN.getMsg());
      }
    }

    Map<String, ExprNodeDesc> colExprMap = op.getColumnExprMap();
    List<ColumnInfo> schema = new ArrayList<ColumnInfo>(op.getSchema().getSignature());
    Map<Byte, List<ExprNodeDesc>> valueExprs = op.getConf().getExprs();
    Map<Byte, List<ExprNodeDesc>> newValueExprs = new HashMap<Byte, List<ExprNodeDesc>>();

    ObjectPair<List<ReduceSinkOperator>, Map<Byte,List<ExprNodeDesc>>> pair =
            getKeys(leftInputJoin, baseSrc, op);
    List<ReduceSinkOperator> oldReduceSinkParentOps = pair.getFirst();
    for (Map.Entry<Byte, List<ExprNodeDesc>> entry : valueExprs.entrySet()) {
      byte tag = entry.getKey();
      Operator<?> terminal = oldReduceSinkParentOps.get(tag);

      List<ExprNodeDesc> values = entry.getValue();
      List<ExprNodeDesc> newValues = ExprNodeDescUtils.backtrack(values, op, terminal);
      newValueExprs.put(tag, newValues);
      for (int i = 0; i < schema.size(); i++) {
        ColumnInfo column = schema.get(i);
        if (column == null) {
          continue;
        }
        ExprNodeDesc expr = colExprMap.get(column.getInternalName());
        int index = ExprNodeDescUtils.indexOf(expr, values);
        if (index >= 0) {
          colExprMap.put(column.getInternalName(), newValues.get(index));
          schema.set(i, null);
        }
      }
    }

    // rewrite value index for mapjoin
    Map<Byte, int[]> valueIndices = new HashMap<Byte, int[]>();

    // get the join keys from old parent ReduceSink operators
    Map<Byte, List<ExprNodeDesc>> keyExprMap = pair.getSecond();

    // construct valueTableDescs and valueFilteredTableDescs
    List<TableDesc> valueTableDescs = new ArrayList<TableDesc>();
    List<TableDesc> valueFilteredTableDescs = new ArrayList<TableDesc>();
    int[][] filterMap = desc.getFilterMap();
    for (byte pos = 0; pos < op.getParentOperators().size(); pos++) {
      List<ExprNodeDesc> valueCols = newValueExprs.get(pos);
      if (pos != mapJoinPos) {
        // remove values in key exprs for value table schema
        // value expression for hashsink will be modified in
        // LocalMapJoinProcessor
        int[] valueIndex = new int[valueCols.size()];
        List<ExprNodeDesc> valueColsInValueExpr = new ArrayList<ExprNodeDesc>();
        for (int i = 0; i < valueIndex.length; i++) {
          ExprNodeDesc expr = valueCols.get(i);
          int kindex = ExprNodeDescUtils.indexOf(expr, keyExprMap.get(pos));
          if (kindex >= 0) {
            valueIndex[i] = kindex;
          } else {
            valueIndex[i] = -valueColsInValueExpr.size() - 1;
            valueColsInValueExpr.add(expr);
          }
        }
        if (needValueIndex(valueIndex)) {
          valueIndices.put(pos, valueIndex);
        }
        valueCols = valueColsInValueExpr;
      }
      // deep copy expr node desc
      List<ExprNodeDesc> valueFilteredCols = ExprNodeDescUtils.clone(valueCols);
      if (filterMap != null && filterMap[pos] != null && pos != mapJoinPos) {
        ExprNodeColumnDesc isFilterDesc =
            new ExprNodeColumnDesc(
                TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.SMALLINT_TYPE_NAME), "filter",
                "filter", false);
        valueFilteredCols.add(isFilterDesc);
      }

      TableDesc valueTableDesc =
          PlanUtils.getMapJoinValueTableDesc(PlanUtils.getFieldSchemasFromColumnList(valueCols,
              "mapjoinvalue"));
      TableDesc valueFilteredTableDesc =
          PlanUtils.getMapJoinValueTableDesc(PlanUtils.getFieldSchemasFromColumnList(
              valueFilteredCols, "mapjoinvalue"));

      valueTableDescs.add(valueTableDesc);
      valueFilteredTableDescs.add(valueFilteredTableDesc);
    }

    Map<Byte, List<ExprNodeDesc>> filters = desc.getFilters();
    Map<Byte, List<ExprNodeDesc>> newFilters = new HashMap<Byte, List<ExprNodeDesc>>();
    for (Map.Entry<Byte, List<ExprNodeDesc>> entry : filters.entrySet()) {
      byte srcTag = entry.getKey();
      List<ExprNodeDesc> filter = entry.getValue();

      Operator<?> terminal = oldReduceSinkParentOps.get(srcTag);
      newFilters.put(srcTag, ExprNodeDescUtils.backtrack(filter, op, terminal));
    }
    desc.setFilters(filters = newFilters);

    // create dumpfile prefix needed to create descriptor
    String dumpFilePrefix = "";
    if (mapAliases != null) {
      for (String mapAlias : mapAliases) {
        dumpFilePrefix = dumpFilePrefix + mapAlias;
      }
      dumpFilePrefix = dumpFilePrefix + "-" + PlanUtils.getCountForMapJoinDumpFilePrefix();
    } else {
      dumpFilePrefix = "mapfile" + PlanUtils.getCountForMapJoinDumpFilePrefix();
    }

    List<ExprNodeDesc> keyCols = keyExprMap.get((byte) mapJoinPos);

    List<String> outputColumnNames = op.getConf().getOutputColumnNames();
    TableDesc keyTableDesc =
        PlanUtils.getMapJoinKeyTableDesc(hconf,
            PlanUtils.getFieldSchemasFromColumnList(keyCols, MAPJOINKEY_FIELDPREFIX));
    JoinCondDesc[] joinCondns = op.getConf().getConds();
    MapJoinDesc mapJoinDescriptor =
        new MapJoinDesc(keyExprMap, keyTableDesc, newValueExprs, valueTableDescs,
            valueFilteredTableDescs, outputColumnNames, mapJoinPos, joinCondns, filters, op
                .getConf().getNoOuterJoin(), dumpFilePrefix);
    mapJoinDescriptor.setStatistics(op.getConf().getStatistics());
    mapJoinDescriptor.setTagOrder(tagOrder);
    mapJoinDescriptor.setNullSafes(desc.getNullSafes());
    mapJoinDescriptor.setFilterMap(desc.getFilterMap());
    if (!valueIndices.isEmpty()) {
      mapJoinDescriptor.setValueIndices(valueIndices);
    }

    return mapJoinDescriptor;
  }
}
