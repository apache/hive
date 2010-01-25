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

package org.apache.hadoop.hive.ql.parse;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.unionproc.UnionProcContext;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.LoadFileDesc;
import org.apache.hadoop.hive.ql.plan.LoadTableDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc.sampleDesc;

/**
 * Parse Context: The current parse context. This is passed to the optimizer
 * which then transforms the operator tree using the parse context. All the
 * optimizations are performed sequentially and then the new parse context
 * populated. Note that since the parse context contains the operator tree, it
 * can be easily retrieved by the next optimization step or finally for task
 * generation after the plan has been completely optimized.
 * 
 **/

public class ParseContext {
  private QB qb;
  private ASTNode ast;
  private HashMap<TableScanOperator, ExprNodeDesc> opToPartPruner;
  private HashMap<TableScanOperator, sampleDesc> opToSamplePruner;
  private HashMap<String, Operator<? extends Serializable>> topOps;
  private HashMap<String, Operator<? extends Serializable>> topSelOps;
  private LinkedHashMap<Operator<? extends Serializable>, OpParseContext> opParseCtx;
  private Map<JoinOperator, QBJoinTree> joinContext;
  private HashMap<TableScanOperator, Table> topToTable;
  private List<LoadTableDesc> loadTableWork;
  private List<LoadFileDesc> loadFileWork;
  private Context ctx;
  private HiveConf conf;
  private HashMap<String, String> idToTableNameMap;
  private int destTableId;
  private UnionProcContext uCtx;
  private List<MapJoinOperator> listMapJoinOpsNoReducer; // list of map join
                                                         // operators with no
                                                         // reducer
  private Map<GroupByOperator, Set<String>> groupOpToInputTables;
  private Map<String, PrunedPartitionList> prunedPartitions;

  // is set to true if the expression only contains partitioning columns and not
  // any other column reference.
  // This is used to optimize select * from table where ... scenario, when the
  // where condition only references
  // partitioning columns - the partitions are identified and streamed directly
  // to the client without requiring
  // a map-reduce job
  private boolean hasNonPartCols;

  public ParseContext() {
  }

  /**
   * @param qb
   *          current QB
   * @param ast
   *          current parse tree
   * @param opToPartPruner
   *          map from table scan operator to partition pruner
   * @param topOps
   *          list of operators for the top query
   * @param topSelOps
   *          list of operators for the selects introduced for column pruning
   * @param opParseCtx
   *          operator parse context - contains a mapping from operator to
   *          operator parse state (row resolver etc.)
   * @param joinContext
   *          context needed join processing (map join specifically)
   * @param topToTable
   *          the top tables being processed
   * @param loadTableWork
   *          list of destination tables being loaded
   * @param loadFileWork
   *          list of destination files being loaded
   * @param ctx
   *          parse context
   * @param idToTableNameMap
   * @param destTableId
   * @param uCtx
   * @param listMapJoinOpsNoReducer
   *          list of map join operators with no reducer
   * @param opToSamplePruner
   *          operator to sample pruner map
   */
  public ParseContext(
      HiveConf conf,
      QB qb,
      ASTNode ast,
      HashMap<TableScanOperator, ExprNodeDesc> opToPartPruner,
      HashMap<String, Operator<? extends Serializable>> topOps,
      HashMap<String, Operator<? extends Serializable>> topSelOps,
      LinkedHashMap<Operator<? extends Serializable>, OpParseContext> opParseCtx,
      Map<JoinOperator, QBJoinTree> joinContext,
      HashMap<TableScanOperator, Table> topToTable,
      List<LoadTableDesc> loadTableWork, List<LoadFileDesc> loadFileWork,
      Context ctx, HashMap<String, String> idToTableNameMap, int destTableId,
      UnionProcContext uCtx, List<MapJoinOperator> listMapJoinOpsNoReducer,
      Map<GroupByOperator, Set<String>> groupOpToInputTables,
      Map<String, PrunedPartitionList> prunedPartitions,
      HashMap<TableScanOperator, sampleDesc> opToSamplePruner) {
    this.conf = conf;
    this.qb = qb;
    this.ast = ast;
    this.opToPartPruner = opToPartPruner;
    this.joinContext = joinContext;
    this.topToTable = topToTable;
    this.loadFileWork = loadFileWork;
    this.loadTableWork = loadTableWork;
    this.opParseCtx = opParseCtx;
    this.topOps = topOps;
    this.topSelOps = topSelOps;
    this.ctx = ctx;
    this.idToTableNameMap = idToTableNameMap;
    this.destTableId = destTableId;
    this.uCtx = uCtx;
    this.listMapJoinOpsNoReducer = listMapJoinOpsNoReducer;
    hasNonPartCols = false;
    this.groupOpToInputTables = new HashMap<GroupByOperator, Set<String>>();
    this.groupOpToInputTables = groupOpToInputTables;
    this.prunedPartitions = prunedPartitions;
    this.opToSamplePruner = opToSamplePruner;
  }

  /**
   * @return the qb
   */
  public QB getQB() {
    return qb;
  }

  /**
   * @param qb
   *          the qb to set
   */
  public void setQB(QB qb) {
    this.qb = qb;
  }

  /**
   * @return the context
   */
  public Context getContext() {
    return ctx;
  }

  /**
   * @param ctx
   *          the context to set
   */
  public void setContext(Context ctx) {
    this.ctx = ctx;
  }

  /**
   * @return the hive conf
   */
  public HiveConf getConf() {
    return conf;
  }

  /**
   * @param conf
   *          the conf to set
   */
  public void setConf(HiveConf conf) {
    this.conf = conf;
  }

  /**
   * @return the ast
   */
  public ASTNode getParseTree() {
    return ast;
  }

  /**
   * @param ast
   *          the parsetree to set
   */
  public void setParseTree(ASTNode ast) {
    this.ast = ast;
  }

  /**
   * @return the opToPartPruner
   */
  public HashMap<TableScanOperator, ExprNodeDesc> getOpToPartPruner() {
    return opToPartPruner;
  }

  /**
   * @param opToPartPruner
   *          the opToPartPruner to set
   */
  public void setOpToPartPruner(
      HashMap<TableScanOperator, ExprNodeDesc> opToPartPruner) {
    this.opToPartPruner = opToPartPruner;
  }

  /**
   * @return the topToTable
   */
  public HashMap<TableScanOperator, Table> getTopToTable() {
    return topToTable;
  }

  /**
   * @param topToTable
   *          the topToTable to set
   */
  public void setTopToTable(HashMap<TableScanOperator, Table> topToTable) {
    this.topToTable = topToTable;
  }

  /**
   * @return the topOps
   */
  public HashMap<String, Operator<? extends Serializable>> getTopOps() {
    return topOps;
  }

  /**
   * @param topOps
   *          the topOps to set
   */
  public void setTopOps(HashMap<String, Operator<? extends Serializable>> topOps) {
    this.topOps = topOps;
  }

  /**
   * @return the topSelOps
   */
  public HashMap<String, Operator<? extends Serializable>> getTopSelOps() {
    return topSelOps;
  }

  /**
   * @param topSelOps
   *          the topSelOps to set
   */
  public void setTopSelOps(
      HashMap<String, Operator<? extends Serializable>> topSelOps) {
    this.topSelOps = topSelOps;
  }

  /**
   * @return the opParseCtx
   */
  public LinkedHashMap<Operator<? extends Serializable>, OpParseContext> getOpParseCtx() {
    return opParseCtx;
  }

  /**
   * @param opParseCtx
   *          the opParseCtx to set
   */
  public void setOpParseCtx(
      LinkedHashMap<Operator<? extends Serializable>, OpParseContext> opParseCtx) {
    this.opParseCtx = opParseCtx;
  }

  /**
   * @return the loadTableWork
   */
  public List<LoadTableDesc> getLoadTableWork() {
    return loadTableWork;
  }

  /**
   * @param loadTableWork
   *          the loadTableWork to set
   */
  public void setLoadTableWork(List<LoadTableDesc> loadTableWork) {
    this.loadTableWork = loadTableWork;
  }

  /**
   * @return the loadFileWork
   */
  public List<LoadFileDesc> getLoadFileWork() {
    return loadFileWork;
  }

  /**
   * @param loadFileWork
   *          the loadFileWork to set
   */
  public void setLoadFileWork(List<LoadFileDesc> loadFileWork) {
    this.loadFileWork = loadFileWork;
  }

  public HashMap<String, String> getIdToTableNameMap() {
    return idToTableNameMap;
  }

  public void setIdToTableNameMap(HashMap<String, String> idToTableNameMap) {
    this.idToTableNameMap = idToTableNameMap;
  }

  public int getDestTableId() {
    return destTableId;
  }

  public void setDestTableId(int destTableId) {
    this.destTableId = destTableId;
  }

  public UnionProcContext getUCtx() {
    return uCtx;
  }

  public void setUCtx(UnionProcContext uCtx) {
    this.uCtx = uCtx;
  }

  /**
   * @return the joinContext
   */
  public Map<JoinOperator, QBJoinTree> getJoinContext() {
    return joinContext;
  }

  /**
   * @param joinContext
   *          the joinContext to set
   */
  public void setJoinContext(Map<JoinOperator, QBJoinTree> joinContext) {
    this.joinContext = joinContext;
  }

  /**
   * @return the listMapJoinOpsNoReducer
   */
  public List<MapJoinOperator> getListMapJoinOpsNoReducer() {
    return listMapJoinOpsNoReducer;
  }

  /**
   * @param listMapJoinOpsNoReducer
   *          the listMapJoinOpsNoReducer to set
   */
  public void setListMapJoinOpsNoReducer(
      List<MapJoinOperator> listMapJoinOpsNoReducer) {
    this.listMapJoinOpsNoReducer = listMapJoinOpsNoReducer;
  }

  /**
   * Sets the hasNonPartCols flag
   * 
   * @param val
   */
  public void setHasNonPartCols(boolean val) {
    hasNonPartCols = val;
  }

  /**
   * Gets the value of the hasNonPartCols flag
   */
  public boolean getHasNonPartCols() {
    return hasNonPartCols;
  }

  /**
   * @return the opToSamplePruner
   */
  public HashMap<TableScanOperator, sampleDesc> getOpToSamplePruner() {
    return opToSamplePruner;
  }

  /**
   * @param opToSamplePruner
   *          the opToSamplePruner to set
   */
  public void setOpToSamplePruner(
      HashMap<TableScanOperator, sampleDesc> opToSamplePruner) {
    this.opToSamplePruner = opToSamplePruner;
  }

  /**
   * @return the groupOpToInputTables
   */
  public Map<GroupByOperator, Set<String>> getGroupOpToInputTables() {
    return groupOpToInputTables;
  }

  /**
   * @param groupOpToInputTables
   */
  public void setGroupOpToInputTables(
      Map<GroupByOperator, Set<String>> groupOpToInputTables) {
    this.groupOpToInputTables = groupOpToInputTables;
  }

  /**
   * @return pruned partition map
   */
  public Map<String, PrunedPartitionList> getPrunedPartitions() {
    return prunedPartitions;
  }

  /**
   * @param prunedPartitions
   */
  public void setPrunedPartitions(
      Map<String, PrunedPartitionList> prunedPartitions) {
    this.prunedPartitions = prunedPartitions;
  }
}
