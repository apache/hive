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
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.plan.loadFileDesc;
import org.apache.hadoop.hive.ql.plan.loadTableDesc;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.optimizer.unionproc.UnionProcContext;

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
  private HashMap<String, PartitionPruner> aliasToPruner;
  private HashMap<String, SamplePruner> aliasToSamplePruner;
  private HashMap<String, Operator<? extends Serializable>> topOps;
  private HashMap<String, Operator<? extends Serializable>> topSelOps;
  private HashMap<Operator<? extends Serializable>, OpParseContext> opParseCtx;
  private Map<JoinOperator, QBJoinTree> joinContext;
  private List<loadTableDesc> loadTableWork;
  private List<loadFileDesc> loadFileWork;
  private Context ctx;
  private HiveConf conf;
  private HashMap<String, String> idToTableNameMap;
  private int destTableId;
  private UnionProcContext uCtx;
  private List<MapJoinOperator> listMapJoinOpsNoReducer;  // list of map join operators with no reducer
  
  /**
   * @param qb
   *          current QB
   * @param ast
   *          current parse tree
   * @param aliasToPruner
   *          partition pruner list
   * @param aliasToSamplePruner
   *          sample pruner list
   * @param loadFileWork
   *          list of destination files being loaded
   * @param loadTableWork
   *          list of destination tables being loaded
   * @param opParseCtx
   *          operator parse context - contains a mapping from operator to
   *          operator parse state (row resolver etc.)
   * @param topOps
   *          list of operators for the top query
   * @param topSelOps
   *          list of operators for the selects introduced for column pruning
   * @param listMapJoinOpsNoReducer
   *          list of map join operators with no reducer         
   */
  public ParseContext(HiveConf conf, QB qb, ASTNode ast,
      HashMap<String, PartitionPruner> aliasToPruner,
      HashMap<String, SamplePruner> aliasToSamplePruner,
      HashMap<String, Operator<? extends Serializable>> topOps,
      HashMap<String, Operator<? extends Serializable>> topSelOps,
      HashMap<Operator<? extends Serializable>, OpParseContext> opParseCtx,
      Map<JoinOperator, QBJoinTree> joinContext,
      List<loadTableDesc> loadTableWork, List<loadFileDesc> loadFileWork,
      Context ctx, HashMap<String, String> idToTableNameMap, int destTableId, UnionProcContext uCtx,
      List<MapJoinOperator> listMapJoinOpsNoReducer) {
    this.conf = conf;
    this.qb = qb;
    this.ast = ast;
    this.aliasToPruner = aliasToPruner;
    this.aliasToSamplePruner = aliasToSamplePruner;
    this.joinContext = joinContext;
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
   * @return the aliasToPruner
   */
  public HashMap<String, PartitionPruner> getAliasToPruner() {
    return aliasToPruner;
  }

  /**
   * @param aliasToPruner
   *          the aliasToPruner to set
   */
  public void setAliasToPruner(HashMap<String, PartitionPruner> aliasToPruner) {
    this.aliasToPruner = aliasToPruner;
  }

  /**
   * @return the aliasToSamplePruner
   */
  public HashMap<String, SamplePruner> getAliasToSamplePruner() {
    return aliasToSamplePruner;
  }

  /**
   * @param aliasToSamplePruner
   *          the aliasToSamplePruner to set
   */
  public void setAliasToSamplePruner(
      HashMap<String, SamplePruner> aliasToSamplePruner) {
    this.aliasToSamplePruner = aliasToSamplePruner;
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
  public HashMap<Operator<? extends Serializable>, OpParseContext> getOpParseCtx() {
    return opParseCtx;
  }

  /**
   * @param opParseCtx
   *          the opParseCtx to set
   */
  public void setOpParseCtx(
      HashMap<Operator<? extends Serializable>, OpParseContext> opParseCtx) {
    this.opParseCtx = opParseCtx;
  }

  /**
   * @return the loadTableWork
   */
  public List<loadTableDesc> getLoadTableWork() {
    return loadTableWork;
  }

  /**
   * @param loadTableWork
   *          the loadTableWork to set
   */
  public void setLoadTableWork(List<loadTableDesc> loadTableWork) {
    this.loadTableWork = loadTableWork;
  }

  /**
   * @return the loadFileWork
   */
  public List<loadFileDesc> getLoadFileWork() {
    return loadFileWork;
  }

  /**
   * @param loadFileWork
   *          the loadFileWork to set
   */
  public void setLoadFileWork(List<loadFileDesc> loadFileWork) {
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
   * @param joinContext the joinContext to set
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
   * @param listMapJoinOpsNoReducer the listMapJoinOpsNoReducer to set
   */
  public void setListMapJoinOpsNoReducer(
      List<MapJoinOperator> listMapJoinOpsNoReducer) {
    this.listMapJoinOpsNoReducer = listMapJoinOpsNoReducer;
  }
}
