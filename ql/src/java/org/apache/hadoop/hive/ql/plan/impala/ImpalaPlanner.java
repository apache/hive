/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.plan.impala;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf.ImpalaResultMethod;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.parse.QB;
import org.apache.hadoop.hive.ql.parse.QBMetaData;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.Pair;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.planner.DataPartition;
import org.apache.impala.planner.DistributedPlanner;
import org.apache.impala.planner.HdfsTableSink;
import org.apache.impala.planner.ParallelPlanner;
import org.apache.impala.planner.PlanFragment;
import org.apache.impala.planner.PlanNode;
import org.apache.impala.planner.PlanRootSink;
import org.apache.impala.planner.Planner;
import org.apache.impala.planner.RuntimeFilterGenerator;
import org.apache.impala.planner.SingleNodePlanner;
import org.apache.impala.planner.TableSink;
import org.apache.impala.service.Frontend;
import org.apache.impala.thrift.TColumn;
import org.apache.impala.thrift.TExecRequest;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TPlanExecInfo;
import org.apache.impala.thrift.TPlanFragment;
import org.apache.impala.thrift.TQueryCtx;
import org.apache.impala.thrift.TQueryExecRequest;
import org.apache.impala.thrift.TResultSetMetadata;
import org.apache.impala.thrift.TRuntimeFilterMode;
import org.apache.impala.thrift.TSortingOrder;
import org.apache.impala.thrift.TStmtType;
import org.apache.impala.util.EventSequence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The ImpalaPlanner encapsulates selected functionality from Impala's Frontend and
 * Planner classes. It takes as input the Hive generated single node plan
 * and calls Impala's distributed planner to create plan fragments for a distributed
 * plan. It then creates a TExecRequest thrift structure that represents the plan
 * that can be sent to backend for execution.
 */
public class ImpalaPlanner {

  public final Hive db_;
  private final QB qb_;
  private TStmtType stmtType_;
  private TStmtType resultStmtType_;
  private ImpalaPlannerContext ctx_;
  private FileSinkDesc fileSinkDesc_;
  private static final Logger LOG = LoggerFactory.getLogger(ImpalaPlanner.class);

  public ImpalaPlanner(ImpalaQueryContext queryContext, FileSinkDesc fileSinkDesc, Hive db, QB qb,
      TStmtType stmtType, TStmtType resultStmtType, EventSequence timeline) throws HiveException {
    db_ = db;
    qb_ = qb;
    stmtType_ = stmtType;
    resultStmtType_ = resultStmtType;
    fileSinkDesc_ = fileSinkDesc;
    ctx_ = new ImpalaPlannerContext(queryContext, timeline);
  }

  private void markEvent(String event) {
    ctx_.getTimeline().markEvent(event);
  }

  public ImpalaPlannerContext getPlannerContext() {
      return ctx_;
  }


  List<PlanFragment> createPlans(PlanNode planNodeRoot, Path destination, boolean isOverwrite,
      long writeId) throws HiveException {
    try {
      // Create the values transfer graph in the Analyzer. Note that FENG plans
      // don't register equijoin predicates in the Analyzer's GlobalState since
      // Hive/Calcite should have already done the predicate inferencing analysis.
      // Hence, the GlobalState's registeredValueTransfers will be empty. It is
      // still necessary to instantiate the graph because otherwise
      // RuntimeFilterGenerator tries to de-reference it and encounters NPE.
      // TODO: CDPD-9689 tracks if we are missing any runtime filters compared
      // to Impala
      ctx_.getRootAnalyzer().computeValueTransferGraph();
      Planner.checkForSmallQueryOptimization(planNodeRoot, ctx_);

      // Although the Hive CBO plan creates the relative order among different
      // joins, currently it does not swap left and right inputs if the right
      // input has higher estimated cardinality. Do this through Impala's method
      // since we are using Impala's cardinality estimates in the physical planning.
      Planner.invertJoins(planNodeRoot, ctx_.isSingleNodeExec(), ctx_.getRootAnalyzer());
      Planner.checkParallelPlanEligibility(ctx_);
      SingleNodePlanner.validatePlan(ctx_, planNodeRoot);

      List<PlanFragment> fragments = createPlanFragments(planNodeRoot, destination,
          isOverwrite, writeId);
      PlanFragment planFragmentRoot = fragments.get(0);
      List<PlanFragment> rootFragments;
      if (Planner.useParallelPlan(ctx_)) {
        ParallelPlanner parallelPlanner = new ParallelPlanner(ctx_);
        // The rootFragmentList contains the 'root' fragments of each of the parallel plans
        rootFragments = parallelPlanner.createPlans(planFragmentRoot);
        ctx_.getTimeline().markEvent("Parallel plans created");
      } else {
        rootFragments = new ArrayList(Arrays.asList(planFragmentRoot));
      }
      return rootFragments;

    } catch (ImpalaException e) {
      // Catch and wrap Impala exception types
      throw new HiveException("Failed creating plan", e);
    }
  }

  /**
   * Create an exec request for Impala to execute based on the supplied plan.
   */
  public TExecRequest createExecRequest(PlanNode planNodeRoot, boolean isExplain, Path destination,
      boolean isOverwrite, long writeId) throws HiveException {
    LOG.info("Creating Impala execution request: isExplain: {} destination: {} isOverwrite: {} writeId: {}",
        isExplain, destination, isOverwrite, writeId);
    List<PlanFragment> fragments = createPlans(planNodeRoot, destination, isOverwrite, writeId);
    PlanFragment planFragmentRoot = fragments.get(0);

    TQueryExecRequest queryExecRequest = new TQueryExecRequest();
    TExecRequest result = createExecRequest(ctx_.getQueryCtx(), planFragmentRoot,
        queryExecRequest);
    queryExecRequest.setHost_list(ctx_.getHostLocations());

    // compute resource requirements of the final plan
    boolean isQuery = getResultStmtType() == TStmtType.QUERY;
    Planner.computeResourceReqs(fragments, ctx_.getQueryCtx(), queryExecRequest,
        ctx_, isQuery);

    // create the plan's exec-info and assign fragment idx
    int idx = 0;
    for (PlanFragment planRoot : fragments) {
      TPlanExecInfo tPlanExecInfo = Frontend.createPlanExecInfo(planRoot, ctx_.getQueryCtx());
      queryExecRequest.addToPlan_exec_info(tPlanExecInfo);
      for (TPlanFragment fragment : tPlanExecInfo.fragments) {
        fragment.setIdx(idx++);
      }
    }

    // create EXPLAIN output after setting everything else
    queryExecRequest.setQuery_ctx(ctx_.getQueryCtx()); // needed by getExplainString()

    List<PlanFragment> allFragments = planFragmentRoot.getNodesPreOrder();
    // to mimic Impala's behavior, use EXTENDED mode explain except for EXPLAIN statements
    TExplainLevel explainLevel = isExplain ? ctx_.getQueryOptions().getExplain_level() :
        TExplainLevel.EXTENDED;
    queryExecRequest.setQuery_plan(getExplainString(allFragments, explainLevel));


    try {
      ctx_.getQueryCtx().setDesc_tbl_serialized(ctx_.getRootAnalyzer().getDescTbl().toSerializedThrift());
    } catch (ImpalaException e) {
      throw new HiveException(e);
    }

    markEvent("Execution request created");
    result.setTimeline(ctx_.getTimeline().toThrift());
    return result;
  }

  // TODO: CDPD-8176: Refactor and share Impala's getExplainString()
  private String getExplainString(List<PlanFragment> fragments, TExplainLevel explainLevel) {
    if (explainLevel.ordinal() < TExplainLevel.VERBOSE.ordinal()) {
      // Print the non-fragmented parallel plan.
      return fragments.get(0).getExplainString(ctx_.getQueryOptions(), explainLevel);
    }

    StringBuffer sb = new StringBuffer();
    // Print the fragmented parallel plan.
    for (int i = 0; i < fragments.size(); ++i) {
      PlanFragment fragment = fragments.get(i);
      sb.append(fragment.getExplainString(ctx_.getQueryOptions(), explainLevel));
      if (i < fragments.size() - 1) {
        sb.append("\n");
      }
    }
    return sb.toString();
  }

  void initTargetTable() throws HiveException {
    if (resultStmtType_ == TStmtType.DML) {
      ctx_.initTxnId();
      // Use FileSinkDesc to determine expected location of query results
      org.apache.hadoop.hive.ql.metadata.Table tab = fileSinkDesc_.getTable();
      org.apache.hadoop.hive.ql.metadata.Partition part =  fileSinkDesc_.getPartition();
      if (tab == null && part != null) {
        // static partition case
        tab = part.getTable();
      }

      HdfsTable hdfsTable = null;
      org.apache.hadoop.hive.metastore.api.Table msTbl = tab.getTTable();

      if (qb_.isCTAS()) {
        // Create a dummy target for a CTAS table (the HMS object is created after execution)
        try {
          org.apache.hadoop.hive.metastore.api.Database msDb = db_.getDatabase(tab.getDbName());
          if (msTbl.getSd().getLocation() == null || msTbl.getSd().getLocation().isEmpty()) {
            msTbl.getSd().setLocation(MetastoreShim.getPathForNewTable(msDb, msTbl));
          }
          hdfsTable = HdfsTable.createCtasTarget(new org.apache.impala.catalog.Db(msTbl.getDbName(), msDb),  msTbl);
        } catch (Exception e) {
          throw new HiveException("Failed to create CTAS target", e);
        }
      } else {
        // Load the target table
        hdfsTable = ctx_.getTableLoader().loadHdfsTable(db_, ctx_.getQueryContext().getConf(), msTbl);
      }

      if (hdfsTable != null) {
        ctx_.setTargetTable(hdfsTable);
      }
      if (part != null) {
        ctx_.setTargetPartition(part);
      }
    }
  }

  /**
   * Create one or more plan fragments corresponding to the supplied single node physical plan.
   * This function calls Impala's DistributedPlanner to create the plan fragments and does
   * some post-processing.  It is loosely based on Impala's Planner.createPlan() function.
   * @param planNodeRoot root node of the Impala physical plan
   * @return list of plan fragments in the order [root fragment, child of root ... leaf fragment]
   * @throws ImpalaException
   */
  private List<PlanFragment> createPlanFragments(PlanNode planNodeRoot, Path destination,
      boolean isOverwrite, long writeId)
    throws ImpalaException, HiveException {

    DistributedPlanner distributedPlanner = new DistributedPlanner(ctx_);
    List<PlanFragment> fragments;

    if (ctx_.isSingleNodeExec()) {
      // create one fragment containing the entire single-node plan tree
      fragments = Lists.newArrayList(new PlanFragment(
          ctx_.getNextFragmentId(), planNodeRoot, DataPartition.UNPARTITIONED));
    } else {
      fragments = new ArrayList<>();
      // create distributed plan
      // for queries, isPartitioned is false; in the future, make this conditional
      // on whether it is an insert/CTAS etc.
      final boolean isPartitioned = false;
      distributedPlanner.createPlanFragments(planNodeRoot, isPartitioned, fragments);
    }

    PlanFragment rootFragment = fragments.get(fragments.size() - 1);

    // Create runtime filters.
    if (ctx_.getQueryOptions().getRuntime_filter_mode() != TRuntimeFilterMode.OFF) {
      RuntimeFilterGenerator.generateRuntimeFilters(ctx_, rootFragment.getPlanRoot());
      markEvent("Runtime filters computed");
    }

    rootFragment.verifyTree();

    int numStaticColumns = 0;
    if (ctx_.getTargetTable() != null) {
      List<Expr> partitionKeyExprs = new ArrayList<>(); // List order must match table order
      List<Integer> referencedColumns = new ArrayList<>(); // Kudu only position mapping
      boolean inputIsClustered = false; // !hasNoClusteredHint_ || !sortExprs_.isEmpty();
      boolean isUpsert = false; // Kudu only upsert
      List<Integer> sortColumns = new ArrayList<>(); // Sort column positions
      TSortingOrder sortingOrder = TSortingOrder.LEXICAL;
      Pair<List<Integer>, TSortingOrder> sortProperties = new Pair<>(sortColumns, sortingOrder);
      FeTable targetTable = ctx_.getTargetTable();
      if (targetTable instanceof HdfsTable && ((HdfsTable)targetTable).isPartitioned()) {
        Partition part = ctx_.getTargetPartition();
        if (part != null) { // Static partition case
          // Iterate over the partition columns and values
          List<FieldSchema> part_field_schema = part.getTable().getPartCols();
          List<String> part_values = part.getValues();
          numStaticColumns = part_field_schema.size();
          for (int i = 0; i < numStaticColumns; i++) {
            FieldSchema fs = part_field_schema.get(i);
            String value = part_values.get(i);
            partitionKeyExprs.add(LiteralExpr.createFromUnescapedStr(value, targetTable.getColumn(fs.getName()).getType()));
          }
        } else {
          String dest = getQB().getParseInfo().getClauseNames().iterator().next();
          Map<String, String> partSpec = getQB().getMetaData().getDPCtx(dest).getPartSpec();
          int numPartitionColumns = partSpec.size();
          for (Map.Entry<String,String> partEntry : partSpec.entrySet()) {
            String value = partEntry.getValue();
            String columnName = partEntry.getKey();
            if (value == null) {
              // We've hit the first dynamic partition
              break;
            }
            partitionKeyExprs.add(LiteralExpr.createFromUnescapedStr(value, targetTable.getColumn(columnName).getType()));
            numStaticColumns++;
          }
          int numResultExprs = ctx_.getResultExprs().size();
          int numDynamicColumns = numPartitionColumns - numStaticColumns;
          // partition columns are at the end of resultExprs
          partitionKeyExprs.addAll(ctx_.getResultExprs().subList(numResultExprs - numDynamicColumns, numResultExprs));
        }
      }

      // This is a safety mechanism, in that Hive treats invalid/no writeId as -1 and 0. Impala only considers -1 and
      // will hit preconditions when writeId is set to 0.
      if (writeId <= 0) {
        writeId = -1;
      }
      TableSink sink = TableSink.create(ctx_.getTargetTable(),
            isUpsert ? TableSink.Op.UPSERT : TableSink.Op.INSERT,
            partitionKeyExprs, ctx_.getResultExprs(), referencedColumns,
            isOverwrite, inputIsClustered, sortProperties, writeId);
      Preconditions.checkState(sink instanceof HdfsTableSink, "Currently only HDFS table sinks are supported");
      Preconditions.checkNotNull(destination, "Invalid destination for Impala sink");
      HdfsTableSink s = (HdfsTableSink) sink;
      s.setExternalStagingDir(destination.toUri().toString());
      // This is how deep into a partition that FENG has precreated in destination.
      // Table Partitioning - (year, month, day)
      // I.E. hdfs://localhost/warehouse/test.db/test_table/year=2020/month=2
      // The destinationPartitionDepth is 2 due to the fact the year and month partitions are precreated.
      // (This ends up acting as a hint for Impala TableSink not to create the same partition directories in
      // the staging directory we setup). HS2 seems to always create the static portion of partition specs for
      // DML.
      s.setExternalStagingPartitionDepth(numStaticColumns);
      rootFragment.setSink(sink);
    } else {
      // create the data sink
      boolean isFileBased = SessionState.get().getConf().getImpalaResultMethod() == ImpalaResultMethod.FILE;
      if (isFileBased) { // File based query results
        // This is the location Hive expects to find QUERY results.
        // This is typically a staging directory on a DFS, sometimes it is within a table directory.
        // The location is determined based on various factors, such as if encryption zones are enabled.
        Path resultPath = fileSinkDesc_.getDirName();
        String fileFormatClass = fileSinkDesc_.getTableInfo().getInputFileFormatClassName();
        List<Expr> resultExprs = ctx_.getResultExprs();
        ImpalaResultLocation resultLocation = new ImpalaResultLocation(resultExprs,
            resultPath.toUri().toString(), fileFormatClass);
        ctx_.getRootAnalyzer().getDescTbl().setTargetTable(resultLocation);
        List<Integer> referencedColumns =  new ArrayList<>();
        // Creates a table sink that uses ImpalaResultLocation that is used to specify the
        // desired location of query results
        TableSink sink = TableSink.create(resultLocation, TableSink.Op.INSERT,
            ImmutableList.<Expr>of(), resultExprs, referencedColumns, false, false,
            new Pair<>(ImmutableList.<Integer> of(), TSortingOrder.LEXICAL), -1, true);
        rootFragment.setSink(sink);
      } else { // Streaming query results
        rootFragment.setSink(new PlanRootSink(ctx_.getResultExprs()));
      }
    }

    Planner.checkForDisableCodegen(rootFragment.getPlanRoot(), ctx_);
    // finalize exchanges: this ensures that for hash partitioned joins, the partitioning
    // keys on both sides of the join have compatible data types
    for (PlanFragment fragment: fragments) {
      fragment.finalizeExchanges(ctx_.getRootAnalyzer());
    }

    Collections.reverse(fragments);
    markEvent("Distributed plan created");
    return fragments;
  }

  /**
   * Add the metadata for the result set
   */
  private TResultSetMetadata createQueryResultSetMetadata(List<Expr> outputExprs) {
    TResultSetMetadata metadata = new TResultSetMetadata();
    int colCnt = outputExprs.size();
    for (int i = 0; i < colCnt; ++i) {
      TColumn colDesc = new TColumn(outputExprs.get(i).toString(),
          outputExprs.get(i).getType().toThrift());
      metadata.addToColumns(colDesc);
    }
    return metadata;
  }

  public QB getQB() {
    return qb_;
  }

  private TStmtType getStmtType() {
    return stmtType_;
  }

  private TStmtType getResultStmtType() {
    return resultStmtType_;
  }

  private TExecRequest createExecRequest(TQueryCtx queryCtx, PlanFragment planFragmentRoot,
      TQueryExecRequest queryExecRequest) {
    TExecRequest result = new TExecRequest();
    // NOTE: the below 4 are mandatory fields
    result.setQuery_options(queryCtx.getClient_request().getQuery_options());

    // TODO: see CDPD-8107 for populating the following 3 fields
    result.setAccess_events(Lists.newArrayList());
    result.setAnalysis_warnings(Lists.newArrayList());
    result.setUser_has_profile_access(true);

    result.setQuery_exec_request(queryExecRequest);

    result.setStmt_type(getStmtType());
    result.getQuery_exec_request().setStmt_type(getResultStmtType());

    // fill in the metadata using the root fragment's PlanRootSink
    Preconditions.checkState(planFragmentRoot.hasSink());
    List<Expr> outputExprs = new ArrayList<>();

    planFragmentRoot.getSink().collectExprs(outputExprs);
    result.setResult_set_metadata(createQueryResultSetMetadata(outputExprs));

    return result;
  }
}
