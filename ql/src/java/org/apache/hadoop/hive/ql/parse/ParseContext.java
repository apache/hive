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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.QueryProperties;
import org.apache.hadoop.hive.ql.exec.AbstractMapJoinOperator;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.ListSinkOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.SMBMapJoinOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.hooks.LineageInfo;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.ppr.PartitionPruner;
import org.apache.hadoop.hive.ql.optimizer.unionproc.UnionProcContext;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc.sampleDesc;
import org.apache.hadoop.hive.ql.plan.LoadFileDesc;
import org.apache.hadoop.hive.ql.plan.LoadTableDesc;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;

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
  private HashMap<TableScanOperator, PrunedPartitionList> opToPartList;
  private HashMap<TableScanOperator, sampleDesc> opToSamplePruner;
  private Map<TableScanOperator, Map<String, ExprNodeDesc>> opToPartToSkewedPruner;
  private HashMap<String, Operator<? extends OperatorDesc>> topOps;
  private HashMap<String, Operator<? extends OperatorDesc>> topSelOps;
  private LinkedHashMap<Operator<? extends OperatorDesc>, OpParseContext> opParseCtx;
  private Map<JoinOperator, QBJoinTree> joinContext;
  private Map<MapJoinOperator, QBJoinTree> mapJoinContext;
  private Map<SMBMapJoinOperator, QBJoinTree> smbMapJoinContext;
  private HashMap<TableScanOperator, Table> topToTable;
  private Map<FileSinkOperator, Table> fsopToTable;
  private List<ReduceSinkOperator> reduceSinkOperatorsAddedByEnforceBucketingSorting;
  private HashMap<TableScanOperator, Map<String, String>> topToProps;
  private HashMap<String, SplitSample> nameToSplitSample;
  private List<LoadTableDesc> loadTableWork;
  private List<LoadFileDesc> loadFileWork;
  private Context ctx;
  private HiveConf conf;
  private HashMap<String, String> idToTableNameMap;
  private int destTableId;
  private UnionProcContext uCtx;
  private List<AbstractMapJoinOperator<? extends MapJoinDesc>> listMapJoinOpsNoReducer; // list of map join
  // operators with no
  // reducer
  private Map<GroupByOperator, Set<String>> groupOpToInputTables;
  private Map<String, PrunedPartitionList> prunedPartitions;
  private Map<String, ReadEntity> viewAliasToInput;

  /**
   * The lineage information.
   */
  private LineageInfo lInfo;

  private GlobalLimitCtx globalLimitCtx;

  private HashSet<ReadEntity> semanticInputs;
  private List<Task<? extends Serializable>> rootTasks;

  private FetchTask fetchTask;
  private QueryProperties queryProperties;

  private TableDesc fetchTabledesc;
  private Operator<?> fetchSource;
  private ListSinkOperator fetchSink;

  public ParseContext() {
  }

  /**
   * @param conf
   * @param qb
   *          current QB
   * @param ast
   *          current parse tree
   * @param opToPartPruner
   *          map from table scan operator to partition pruner
   * @param opToPartList
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
   * @param uCtx
   * @param destTableId
   * @param listMapJoinOpsNoReducer
   *          list of map join operators with no reducer
   * @param groupOpToInputTables
   * @param prunedPartitions
   * @param opToSamplePruner
   *          operator to sample pruner map
   * @param globalLimitCtx
   * @param nameToSplitSample
   * @param rootTasks
   */
  public ParseContext(
      HiveConf conf,
      QB qb,
      ASTNode ast,
      HashMap<TableScanOperator, ExprNodeDesc> opToPartPruner,
      HashMap<TableScanOperator, PrunedPartitionList> opToPartList,
      HashMap<String, Operator<? extends OperatorDesc>> topOps,
      HashMap<String, Operator<? extends OperatorDesc>> topSelOps,
      LinkedHashMap<Operator<? extends OperatorDesc>, OpParseContext> opParseCtx,
      Map<JoinOperator, QBJoinTree> joinContext,
      Map<SMBMapJoinOperator, QBJoinTree> smbMapJoinContext,
      HashMap<TableScanOperator, Table> topToTable,
      HashMap<TableScanOperator, Map<String, String>> topToProps,
      Map<FileSinkOperator, Table> fsopToTable,
      List<LoadTableDesc> loadTableWork, List<LoadFileDesc> loadFileWork,
      Context ctx, HashMap<String, String> idToTableNameMap, int destTableId,
      UnionProcContext uCtx, List<AbstractMapJoinOperator<? extends MapJoinDesc>> listMapJoinOpsNoReducer,
      Map<GroupByOperator, Set<String>> groupOpToInputTables,
      Map<String, PrunedPartitionList> prunedPartitions,
      HashMap<TableScanOperator, sampleDesc> opToSamplePruner,
      GlobalLimitCtx globalLimitCtx,
      HashMap<String, SplitSample> nameToSplitSample,
      HashSet<ReadEntity> semanticInputs, List<Task<? extends Serializable>> rootTasks,
      Map<TableScanOperator, Map<String, ExprNodeDesc>> opToPartToSkewedPruner,
      Map<String, ReadEntity> viewAliasToInput,
      List<ReduceSinkOperator> reduceSinkOperatorsAddedByEnforceBucketingSorting,
      QueryProperties queryProperties) {
    this.conf = conf;
    this.qb = qb;
    this.ast = ast;
    this.opToPartPruner = opToPartPruner;
    this.opToPartList = opToPartList;
    this.joinContext = joinContext;
    this.smbMapJoinContext = smbMapJoinContext;
    this.topToTable = topToTable;
    this.fsopToTable = fsopToTable;
    this.topToProps = topToProps;
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
    this.groupOpToInputTables = groupOpToInputTables;
    this.prunedPartitions = prunedPartitions;
    this.opToSamplePruner = opToSamplePruner;
    this.nameToSplitSample = nameToSplitSample;
    this.globalLimitCtx = globalLimitCtx;
    this.semanticInputs = semanticInputs;
    this.rootTasks = rootTasks;
    this.opToPartToSkewedPruner = opToPartToSkewedPruner;
    this.viewAliasToInput = viewAliasToInput;
    this.reduceSinkOperatorsAddedByEnforceBucketingSorting =
        reduceSinkOperatorsAddedByEnforceBucketingSorting;
    this.queryProperties = queryProperties;
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

  public HashMap<TableScanOperator, PrunedPartitionList> getOpToPartList() {
    return opToPartList;
  }

  public void setOpToPartList(HashMap<TableScanOperator, PrunedPartitionList> opToPartList) {
    this.opToPartList = opToPartList;
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

  public Map<FileSinkOperator, Table> getFsopToTable() {
    return fsopToTable;
  }

  public void setFsopToTable(Map<FileSinkOperator, Table> fsopToTable) {
    this.fsopToTable = fsopToTable;
  }

  public List<ReduceSinkOperator> getReduceSinkOperatorsAddedByEnforceBucketingSorting() {
    return reduceSinkOperatorsAddedByEnforceBucketingSorting;
  }

  public void setReduceSinkOperatorsAddedByEnforceBucketingSorting(
      List<ReduceSinkOperator> reduceSinkOperatorsAddedByEnforceBucketingSorting) {
    this.reduceSinkOperatorsAddedByEnforceBucketingSorting =
        reduceSinkOperatorsAddedByEnforceBucketingSorting;
  }

  /**
   * @return the topToProps
   */
  public HashMap<TableScanOperator, Map<String, String>> getTopToProps() {
    return topToProps;
  }

  /**
   * @param topToProps
   *          the topToProps to set
   */
  public void setTopToProps(HashMap<TableScanOperator, Map<String, String>> topToProps) {
    this.topToProps = topToProps;
  }

  /**
   * @return the topOps
   */
  public HashMap<String, Operator<? extends OperatorDesc>> getTopOps() {
    return topOps;
  }

  /**
   * @param topOps
   *          the topOps to set
   */
  public void setTopOps(HashMap<String, Operator<? extends OperatorDesc>> topOps) {
    this.topOps = topOps;
  }

  /**
   * @return the topSelOps
   */
  public HashMap<String, Operator<? extends OperatorDesc>> getTopSelOps() {
    return topSelOps;
  }

  /**
   * @param topSelOps
   *          the topSelOps to set
   */
  public void setTopSelOps(
      HashMap<String, Operator<? extends OperatorDesc>> topSelOps) {
    this.topSelOps = topSelOps;
  }

  /**
   * @return the opParseCtx
   */
  public LinkedHashMap<Operator<? extends OperatorDesc>, OpParseContext> getOpParseCtx() {
    return opParseCtx;
  }

  /**
   * Remove the OpParseContext of a specific operator op
   * @param op
   * @return
   */
  public OpParseContext removeOpParseCtx(Operator<? extends OperatorDesc> op) {
    return opParseCtx.remove(op);
  }

  /**
   * Update the OpParseContext of operator op to newOpParseContext.
   * If op is not in opParseCtx, a new entry will be added into opParseCtx.
   * The key is op, and the value is newOpParseContext.
   * @param op
   * @param newOpParseContext
   */
  public void updateOpParseCtx(Operator<? extends OperatorDesc> op,
      OpParseContext newOpParseContext) {
    opParseCtx.put(op, newOpParseContext);
  }

  /**
   * @param opParseCtx
   *          the opParseCtx to set
   */
  public void setOpParseCtx(
      LinkedHashMap<Operator<? extends OperatorDesc>, OpParseContext> opParseCtx) {
    this.opParseCtx = opParseCtx;
  }

  public HashMap<String, SplitSample> getNameToSplitSample() {
    return nameToSplitSample;
  }

  public void setNameToSplitSample(HashMap<String, SplitSample> nameToSplitSample) {
    this.nameToSplitSample = nameToSplitSample;
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
  public List<AbstractMapJoinOperator<? extends MapJoinDesc>> getListMapJoinOpsNoReducer() {
    return listMapJoinOpsNoReducer;
  }

  /**
   * @param listMapJoinOpsNoReducer
   *          the listMapJoinOpsNoReducer to set
   */
  public void setListMapJoinOpsNoReducer(
      List<AbstractMapJoinOperator<? extends MapJoinDesc>> listMapJoinOpsNoReducer) {
    this.listMapJoinOpsNoReducer = listMapJoinOpsNoReducer;
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

  /**
   * Sets the lineage information.
   *
   * @param lInfo The lineage information.
   */
  public void setLineageInfo(LineageInfo lInfo) {
    this.lInfo = lInfo;
  }

  /**
   * Gets the associated lineage information.
   *
   * @return LineageInfo
   */
  public LineageInfo getLineageInfo() {
    return lInfo;
  }

  public Map<MapJoinOperator, QBJoinTree> getMapJoinContext() {
    return mapJoinContext;
  }

  public void setMapJoinContext(Map<MapJoinOperator, QBJoinTree> mapJoinContext) {
    this.mapJoinContext = mapJoinContext;
  }

  public Map<SMBMapJoinOperator, QBJoinTree> getSmbMapJoinContext() {
    return smbMapJoinContext;
  }

  public void setSmbMapJoinContext(Map<SMBMapJoinOperator, QBJoinTree> smbMapJoinContext) {
    this.smbMapJoinContext = smbMapJoinContext;
  }

  public GlobalLimitCtx getGlobalLimitCtx() {
    return globalLimitCtx;
  }

  public void setGlobalLimitCtx(GlobalLimitCtx globalLimitCtx) {
    this.globalLimitCtx = globalLimitCtx;
  }

  public HashSet<ReadEntity> getSemanticInputs() {
    return semanticInputs;
  }

  public void replaceRootTask(Task<? extends Serializable> rootTask,
                              List<? extends Task<? extends Serializable>> tasks) {
    this.rootTasks.remove(rootTask);
    this.rootTasks.addAll(tasks);
  }

  public FetchTask getFetchTask() {
    return fetchTask;
  }

  public void setFetchTask(FetchTask fetchTask) {
    this.fetchTask = fetchTask;
  }

  public PrunedPartitionList getPrunedPartitions(String alias, TableScanOperator ts)
      throws HiveException {
    PrunedPartitionList partsList = opToPartList.get(ts);
    if (partsList == null) {
      partsList = PartitionPruner.prune(ts, this, alias);
      opToPartList.put(ts, partsList);
    }
    return partsList;
  }

  /**
   * @return the opToPartToSkewedPruner
   */
  public Map<TableScanOperator, Map<String, ExprNodeDesc>> getOpToPartToSkewedPruner() {
    return opToPartToSkewedPruner;
  }

  /**
   * @param opToPartToSkewedPruner
   *          the opToSkewedPruner to set
   */
  public void setOpPartToSkewedPruner(
      HashMap<TableScanOperator, Map<String, ExprNodeDesc>> opToPartToSkewedPruner) {
    this.opToPartToSkewedPruner = opToPartToSkewedPruner;
  }

  public Map<String, ReadEntity> getViewAliasToInput() {
    return viewAliasToInput;
  }

  public QueryProperties getQueryProperties() {
    return queryProperties;
  }

  public void setQueryProperties(QueryProperties queryProperties) {
    this.queryProperties = queryProperties;
  }

  public TableDesc getFetchTabledesc() {
    return fetchTabledesc;
  }

  public void setFetchTabledesc(TableDesc fetchTabledesc) {
    this.fetchTabledesc = fetchTabledesc;
  }

  public Operator<?> getFetchSource() {
    return fetchSource;
  }

  public void setFetchSource(Operator<?> fetchSource) {
    this.fetchSource = fetchSource;
  }

  public ListSinkOperator getFetchSink() {
    return fetchSink;
  }

  public void setFetchSink(ListSinkOperator fetchSink) {
    this.fetchSink = fetchSink;
  }
}
