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

package org.apache.hadoop.hive.ql.parse;

import com.google.common.collect.Multimap;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.QueryProperties;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.table.create.CreateTableDesc;
import org.apache.hadoop.hive.ql.ddl.view.create.CreateMaterializedViewDesc;
import org.apache.hadoop.hive.ql.ddl.view.materialized.update.MaterializedViewUpdateDesc;
import org.apache.hadoop.hive.ql.exec.AbstractMapJoinOperator;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.ListSinkOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.SMBMapJoinOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.TerminalOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.hooks.LineageInfo;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.ppr.PartitionPruner;
import org.apache.hadoop.hive.ql.optimizer.unionproc.UnionProcContext;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.AnalyzeRewriteContext;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc.SampleDesc;
import org.apache.hadoop.hive.ql.plan.LoadFileDesc;
import org.apache.hadoop.hive.ql.plan.LoadTableDesc;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

  private Map<TableScanOperator, ExprNodeDesc> opToPartPruner;
  private Map<TableScanOperator, PrunedPartitionList> opToPartList;
  private Map<TableScanOperator, SampleDesc> opToSamplePruner;
  private Map<TableScanOperator, Map<String, ExprNodeDesc>> opToPartToSkewedPruner;
  private Map<String, TableScanOperator> topOps;
  private Set<JoinOperator> joinOps;
  private Set<MapJoinOperator> mapJoinOps;
  private Set<SMBMapJoinOperator> smbMapJoinOps;
  private List<ReduceSinkOperator> reduceSinkOperatorsAddedByEnforceBucketingSorting;
  private Map<String, SplitSample> nameToSplitSample;
  private List<LoadTableDesc> loadTableWork;
  private List<LoadFileDesc> loadFileWork;
  private List<ColumnStatsAutoGatherContext> columnStatsAutoGatherContexts;
  private Context ctx;
  private QueryState queryState;
  private HiveConf conf;
  private Map<String, String> idToTableNameMap;
  private int destTableId;
  private UnionProcContext uCtx;
  private List<AbstractMapJoinOperator<? extends MapJoinDesc>> listMapJoinOpsNoReducer; // list of map join
  // operators with no
  // reducer
  private Map<String, PrunedPartitionList> prunedPartitions;
  private Map<String, ReadEntity> viewAliasToInput;
  private QueryTables tabNameToTabObject;

  /**
   * The lineage information.
   */
  private LineageInfo lInfo;

  private GlobalLimitCtx globalLimitCtx;

  private Set<ReadEntity> semanticInputs;
  private List<Task<?>> rootTasks;

  private FetchTask fetchTask;
  private QueryProperties queryProperties;

  private TableDesc fetchTableDesc;
  private Operator<?> fetchSource;
  private ListSinkOperator fetchSink;

  private AnalyzeRewriteContext analyzeRewrite;
  private CreateTableDesc createTableDesc;
  private CreateMaterializedViewDesc createViewDesc;
  private MaterializedViewUpdateDesc materializedViewUpdateDesc;
  private boolean reduceSinkAddedBySortedDynPartition;

  private Map<SelectOperator, Table> viewProjectToViewSchema;
  private ColumnAccessInfo columnAccessInfo;
  private boolean needViewColumnAuthorization;

  private Map<ReduceSinkOperator, RuntimeValuesInfo> rsToRuntimeValuesInfo =
          new LinkedHashMap<ReduceSinkOperator, RuntimeValuesInfo>();
  /**
   * Mapping holding information about semijoins.
   *
   * In various places we need to iterate over the entries of the map so having a predictable iterator is important for
   * keeping the plans stable. It is possible to use an alternative implementation that does not provide specific
   * iteration guarantees but this might introduce small changes in the plans (e.g., different operator ids) from one
   * execution to the other.
   */
  private LinkedHashMap<ReduceSinkOperator, SemiJoinBranchInfo> rsToSemiJoinBranchInfo = new LinkedHashMap<>();
  private Map<ExprNodeDesc, GroupByOperator> colExprToGBMap = new HashMap<>();

  private Map<String, List<SemiJoinHint>> semiJoinHints;
  private boolean disableMapJoin;
  private Multimap<TerminalOperator<?>, ReduceSinkOperator> terminalOpToRSMap;

  public ParseContext() {
  }

  /**
   * @param opToPartPruner
   *          map from table scan operator to partition pruner
   * @param opToPartList
   * @param topOps
   *          list of operators for the top query
   * @param joinOps
   *          context needed join processing (map join specifically)
   * @param smbMapJoinOps
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
   * @param prunedPartitions
   * @param opToSamplePruner
   *          operator to sample pruner map
   * @param globalLimitCtx
   * @param nameToSplitSample
   * @param rootTasks
   * @param opToPartToSkewedPruner
   * @param viewAliasToInput
   * @param reduceSinkOperatorsAddedByEnforceBucketingSorting
   * @param analyzeRewrite
   * @param createTableDesc
   * @param createViewDesc
   * @param queryProperties
   */
  public ParseContext(
      QueryState queryState,
      Map<TableScanOperator, ExprNodeDesc> opToPartPruner,
      Map<TableScanOperator, PrunedPartitionList> opToPartList,
      Map<String, TableScanOperator> topOps,
      Set<JoinOperator> joinOps,
      Set<SMBMapJoinOperator> smbMapJoinOps,
      List<LoadTableDesc> loadTableWork, List<LoadFileDesc> loadFileWork,
      List<ColumnStatsAutoGatherContext> columnStatsAutoGatherContexts,
      Context ctx, Map<String, String> idToTableNameMap, int destTableId,
      UnionProcContext uCtx, List<AbstractMapJoinOperator<? extends MapJoinDesc>> listMapJoinOpsNoReducer,
      Map<String, PrunedPartitionList> prunedPartitions,
      QueryTables tabNameToTabObject,
      Map<TableScanOperator, SampleDesc> opToSamplePruner,
      GlobalLimitCtx globalLimitCtx,
      Map<String, SplitSample> nameToSplitSample,
      Set<ReadEntity> semanticInputs, List<Task<?>> rootTasks,
      Map<TableScanOperator, Map<String, ExprNodeDesc>> opToPartToSkewedPruner,
      Map<String, ReadEntity> viewAliasToInput,
      List<ReduceSinkOperator> reduceSinkOperatorsAddedByEnforceBucketingSorting,
      AnalyzeRewriteContext analyzeRewrite, CreateTableDesc createTableDesc,
      CreateMaterializedViewDesc createViewDesc, MaterializedViewUpdateDesc materializedViewUpdateDesc,
      QueryProperties queryProperties,
      Map<SelectOperator, Table> viewProjectToTableSchema) {
    this.queryState = queryState;
    this.conf = queryState.getConf();
    this.opToPartPruner = opToPartPruner;
    this.opToPartList = opToPartList;
    this.joinOps = joinOps;
    this.smbMapJoinOps = smbMapJoinOps;
    this.loadFileWork = loadFileWork;
    this.loadTableWork = loadTableWork;
    this.columnStatsAutoGatherContexts = columnStatsAutoGatherContexts;
    this.topOps = topOps;
    this.ctx = ctx;
    this.idToTableNameMap = idToTableNameMap;
    this.destTableId = destTableId;
    this.uCtx = uCtx;
    this.listMapJoinOpsNoReducer = listMapJoinOpsNoReducer;
    this.prunedPartitions = prunedPartitions;
    this.tabNameToTabObject = tabNameToTabObject;
    this.opToSamplePruner = opToSamplePruner;
    this.nameToSplitSample = nameToSplitSample;
    this.globalLimitCtx = globalLimitCtx;
    this.semanticInputs = semanticInputs;
    this.rootTasks = rootTasks;
    this.opToPartToSkewedPruner = opToPartToSkewedPruner;
    this.viewAliasToInput = viewAliasToInput;
    this.reduceSinkOperatorsAddedByEnforceBucketingSorting =
        reduceSinkOperatorsAddedByEnforceBucketingSorting;
    this.analyzeRewrite = analyzeRewrite;
    this.createTableDesc = createTableDesc;
    this.createViewDesc = createViewDesc;
    this.materializedViewUpdateDesc = materializedViewUpdateDesc;
    this.queryProperties = queryProperties;
    this.viewProjectToViewSchema = viewProjectToTableSchema;
    this.needViewColumnAuthorization = viewProjectToTableSchema != null
        && !viewProjectToTableSchema.isEmpty();
    if (this.needViewColumnAuthorization) {
      // this will trigger the column pruner to collect view column
      // authorization info.
      this.columnAccessInfo = new ColumnAccessInfo();
    }
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
   * @return the hive conf
   */
  public QueryState getQueryState() {
    return queryState;
  }

  /**
   * @return the opToPartPruner
   */
  public Map<TableScanOperator, ExprNodeDesc> getOpToPartPruner() {
    return opToPartPruner;
  }

  /**
   * @param opToPartPruner
   *          the opToPartPruner to set
   */
  public void setOpToPartPruner(Map<TableScanOperator, ExprNodeDesc> opToPartPruner) {
    this.opToPartPruner = opToPartPruner;
  }

  public Map<TableScanOperator, PrunedPartitionList> getOpToPartList() {
    return opToPartList;
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
   * @return the topOps
   */
  public Map<String, TableScanOperator> getTopOps() {
    return topOps;
  }

  public Set<TableName> getTablesUsed() {
    Set<TableName> tablesUsed = new HashSet<>();
    for (TableScanOperator topOp : topOps.values()) {
      Table table = topOp.getConf().getTableMetadata();
      if (!table.isMaterializedTable() && !table.isView()) {
        tablesUsed.add(table.getFullTableName());
      }
    }
    return tablesUsed;
  }

  /**
   * @param topOps
   *          the topOps to set
   */
  public void setTopOps(Map<String, TableScanOperator> topOps) {
    this.topOps = topOps;
  }

  public Map<String, SplitSample> getNameToSplitSample() {
    return nameToSplitSample;
  }

  public void setNameToSplitSample(Map<String, SplitSample> nameToSplitSample) {
    this.nameToSplitSample = nameToSplitSample;
  }

  /**
   * @return the loadTableWork
   */
  public List<LoadTableDesc> getLoadTableWork() {
    return loadTableWork;
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

  public Map<String, String> getIdToTableNameMap() {
    return idToTableNameMap;
  }

  public void setIdToTableNameMap(Map<String, String> idToTableNameMap) {
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
   * @return the joinOps
   */
  public Set<JoinOperator> getJoinOps() {
    return joinOps;
  }

  /**
   * @param joinOps
   *          the joinOps to set
   */
  public void setJoinOps(Set<JoinOperator> joinOps) {
    this.joinOps = joinOps;
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
  public Map<TableScanOperator, SampleDesc> getOpToSamplePruner() {
    return opToSamplePruner;
  }

  /**
   * @param opToSamplePruner
   *          the opToSamplePruner to set
   */
  public void setOpToSamplePruner(
      Map<TableScanOperator, SampleDesc> opToSamplePruner) {
    this.opToSamplePruner = opToSamplePruner;
  }

  /**
   * @return col stats
   */
  public Map<String, ColumnStatsList> getColStatsCache() {
    return ctx.getOpContext().getColStatsCache();
  }

  /**
   * @param partList
   * @return col stats
   */
  public ColumnStatsList getColStatsCached(PrunedPartitionList partList) {
    return ctx.getOpContext().getColStatsCache().get(partList.getKey().orElse(null));
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

  public Set<MapJoinOperator> getMapJoinOps() {
    return mapJoinOps;
  }

  public void setMapJoinOps(Set<MapJoinOperator> mapJoinOps) {
    this.mapJoinOps = mapJoinOps;
  }

  public Set<SMBMapJoinOperator> getSmbMapJoinOps() {
    return smbMapJoinOps;
  }

  public void setSmbMapJoinOps(Set<SMBMapJoinOperator> smbMapJoinOps) {
    this.smbMapJoinOps = smbMapJoinOps;
  }

  public GlobalLimitCtx getGlobalLimitCtx() {
    return globalLimitCtx;
  }

  public void setGlobalLimitCtx(GlobalLimitCtx globalLimitCtx) {
    this.globalLimitCtx = globalLimitCtx;
  }

  public Set<ReadEntity> getSemanticInputs() {
    return semanticInputs;
  }

  public void replaceRootTask(Task<?> rootTask,
                              List<? extends Task<?>> tasks) {
    this.rootTasks.remove(rootTask);
    this.rootTasks.addAll(tasks);
  }

  public FetchTask getFetchTask() {
    return fetchTask;
  }

  public void setFetchTask(FetchTask fetchTask) {
    this.fetchTask = fetchTask;
  }

  public PrunedPartitionList getPrunedPartitions(TableScanOperator ts)
      throws SemanticException {
    return getPrunedPartitions(ts.getConf().getAlias(), ts);
  }

  public PrunedPartitionList getPrunedPartitions(String alias, TableScanOperator ts)
      throws SemanticException {
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
  public void setOpPartToSkewedPruner(Map<TableScanOperator, Map<String, ExprNodeDesc>> opToPartToSkewedPruner) {
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

  public TableDesc getFetchTableDesc() {
    return fetchTableDesc;
  }

  public void setFetchTabledesc(TableDesc fetchTableDesc) {
    this.fetchTableDesc = fetchTableDesc;
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

  public AnalyzeRewriteContext getAnalyzeRewrite() {
    return this.analyzeRewrite;
  }

  public void setAnalyzeRewrite(AnalyzeRewriteContext analyzeRewrite) {
    this.analyzeRewrite = analyzeRewrite;
  }

  public CreateTableDesc getCreateTable() {
    return this.createTableDesc;
  }

  public void setCreateTable(CreateTableDesc createTableDesc) {
    this.createTableDesc = createTableDesc;
  }

  public CreateMaterializedViewDesc getCreateViewDesc() {
    return createViewDesc;
  }

  public MaterializedViewUpdateDesc getMaterializedViewUpdateDesc() {
    return materializedViewUpdateDesc;
  }

  public void setReduceSinkAddedBySortedDynPartition(
      final boolean reduceSinkAddedBySortedDynPartition) {
    this.reduceSinkAddedBySortedDynPartition = reduceSinkAddedBySortedDynPartition;
  }

  public boolean isReduceSinkAddedBySortedDynPartition() {
    return reduceSinkAddedBySortedDynPartition;
  }

  public Map<SelectOperator, Table> getViewProjectToTableSchema() {
    return viewProjectToViewSchema;
  }

  public ColumnAccessInfo getColumnAccessInfo() {
    return columnAccessInfo;
  }

  public void setColumnAccessInfo(ColumnAccessInfo columnAccessInfo) {
    this.columnAccessInfo = columnAccessInfo;
  }

  public boolean isNeedViewColumnAuthorization() {
    return needViewColumnAuthorization;
  }

  public void setNeedViewColumnAuthorization(boolean needViewColumnAuthorization) {
    this.needViewColumnAuthorization = needViewColumnAuthorization;
  }

  public QueryTables getTabNameToTabObject() {
    return tabNameToTabObject;
  }

  public List<ColumnStatsAutoGatherContext> getColumnStatsAutoGatherContexts() {
    return columnStatsAutoGatherContexts;
  }

  public void setColumnStatsAutoGatherContexts(
      List<ColumnStatsAutoGatherContext> columnStatsAutoGatherContexts) {
    this.columnStatsAutoGatherContexts = columnStatsAutoGatherContexts;
  }

  public Collection<Operator> getAllOps() {
    List<Operator> ops = new ArrayList<>();
    Set<Operator> visited = new HashSet<Operator>();
    for (Operator<?> op : getTopOps().values()) {
      getAllOps(ops, visited, op);
    }
    return ops;
  }

  private static void getAllOps(List<Operator> builder, Set<Operator> visited, Operator<?> op) {
    boolean added = visited.add(op);
    builder.add(op);
    if (!added) return;
    if (op.getNumChild() > 0) {
      List<Operator<?>> children = op.getChildOperators();
      for (int i = 0; i < children.size(); i++) {
        getAllOps(builder, visited, children.get(i));
      }
    }
  }

  public void setRsToRuntimeValuesInfoMap(Map<ReduceSinkOperator, RuntimeValuesInfo> rsToRuntimeValuesInfo) {
    this.rsToRuntimeValuesInfo = rsToRuntimeValuesInfo;
  }

  public Map<ReduceSinkOperator, RuntimeValuesInfo> getRsToRuntimeValuesInfoMap() {
    return rsToRuntimeValuesInfo;
  }

  public void setRsToSemiJoinBranchInfo(LinkedHashMap<ReduceSinkOperator, SemiJoinBranchInfo> rsToSemiJoinBranchInfo) {
    this.rsToSemiJoinBranchInfo = rsToSemiJoinBranchInfo;
  }

  public LinkedHashMap<ReduceSinkOperator, SemiJoinBranchInfo> getRsToSemiJoinBranchInfo() {
    return rsToSemiJoinBranchInfo;
  }

  public void setColExprToGBMap(Map<ExprNodeDesc, GroupByOperator> colExprToGBMap) {
    this.colExprToGBMap = colExprToGBMap;
  }

  public Map<ExprNodeDesc, GroupByOperator> getColExprToGBMap() {
    return colExprToGBMap;
  }

  public void setSemiJoinHints(Map<String, List<SemiJoinHint>> hints) {
    this.semiJoinHints = hints;
  }

  public Map<String, List<SemiJoinHint>> getSemiJoinHints() {
    return semiJoinHints;
  }

  public void setDisableMapJoin(boolean disableMapJoin) {
    this.disableMapJoin = disableMapJoin;
  }

  public boolean getDisableMapJoin() {
    return disableMapJoin;
  }

  public void setTerminalOpToRSMap(Multimap<TerminalOperator<?>, ReduceSinkOperator> terminalOpToRSMap) {
    this.terminalOpToRSMap = terminalOpToRSMap;
  }

  public Multimap<TerminalOperator<?>, ReduceSinkOperator> getTerminalOpToRSMap() {
    return terminalOpToRSMap;
  }
}
