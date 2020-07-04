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
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.fs.Path;
import org.apache.impala.analysis.Analyzer;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.StmtMetadataLoader;
import org.apache.impala.authorization.AuthorizationFactory;
import org.apache.impala.authorization.NoopAuthorizationFactory;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.Pair;
import org.apache.impala.planner.DataPartition;
import org.apache.impala.planner.DistributedPlanner;
import org.apache.impala.planner.ParallelPlanner;
import org.apache.impala.planner.PlanFragment;
import org.apache.impala.planner.PlanNode;
import org.apache.impala.planner.PlanRootSink;
import org.apache.impala.planner.Planner;
import org.apache.impala.planner.RuntimeFilterGenerator;
import org.apache.impala.planner.SingleNodePlanner;
import org.apache.impala.planner.TableSink;
import org.apache.impala.service.Frontend;
import org.apache.impala.thrift.TClientRequest;
import org.apache.impala.thrift.TColumn;
import org.apache.impala.thrift.TExecRequest;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.thrift.TPlanExecInfo;
import org.apache.impala.thrift.TPlanFragment;
import org.apache.impala.thrift.TQueryCtx;
import org.apache.impala.thrift.TQueryExecRequest;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.thrift.TResultSetMetadata;
import org.apache.impala.thrift.TRuntimeFilterMode;
import org.apache.impala.thrift.TSessionState;
import org.apache.impala.thrift.TSessionType;
import org.apache.impala.thrift.TSortingOrder;
import org.apache.impala.thrift.TStmtType;
import org.apache.impala.thrift.TUniqueId;
import org.apache.impala.util.EventSequence;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

/**
 * The ImpalaPlanner encapsulates selected functionality from Impala's Frontend and
 * Planner classes. It takes as input the Hive generated single node plan
 * and calls Impala's distributed planner to create plan fragments for a distributed
 * plan. It then creates a TExecRequest thrift structure that represents the plan
 * that can be sent to backend for execution.
 */
public class ImpalaPlanner {

  private ImpalaPlannerContext ctx_;
  private List<TNetworkAddress> hostLocations = new ArrayList<>();
  // Path where result files are written when hive.impala.execution.mode is file.
  private final Path resultPath;

  public ImpalaPlanner(String dbname, String username, TQueryOptions options, Path resultPath) throws HiveException {
    // TODO: replace hostname and port with configured parameter settings
    hostLocations.add(new TNetworkAddress("127.0.0.1", 22000));

    this.resultPath = resultPath;
    TQueryCtx queryCtx = createQueryContext(dbname, username, options, hostLocations.get(0));

    AuthorizationFactory authFactory = new NoopAuthorizationFactory();
    StmtMetadataLoader.StmtTableCache stmtTableCache =
        new StmtMetadataLoader.StmtTableCache(new DummyCatalog(), Sets.newHashSet(), Maps.newHashMap());
    ImpalaBasicAnalyzer analyzer = new ImpalaBasicAnalyzer(stmtTableCache, queryCtx,
        authFactory, hostLocations);
    EventSequence timeline = new EventSequence("Starting conversion of Hive plan to Impala plan.");
    ctx_ = new ImpalaPlannerContext(queryCtx, timeline, analyzer);
  }

  public ImpalaPlannerContext getPlannerContext() {return ctx_;}

  /**
   * Create an exec request for Impala to execute based on the supplied plan
   * @param planNodeRoot root node of the Impala physical plan
   * @return TExecRequest thrift structure for backend to execute
   * @throws ImpalaException
   */
  public TExecRequest createExecRequest(PlanNode planNodeRoot, boolean isExplain) throws ImpalaException {
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

    List<PlanFragment> fragments = createPlanFragments(planNodeRoot);
    Preconditions.checkArgument(fragments.size() > 0);
    PlanFragment planFragmentRoot = fragments.get(0);
    List<PlanFragment> rootFragmentList = new ArrayList<>();

    if (Planner.useParallelPlan(ctx_)) {
      ParallelPlanner parallelPlanner = new ParallelPlanner(ctx_);
      List<PlanFragment> parallelPlans = parallelPlanner.createPlans(planFragmentRoot);
      ctx_.getTimeline().markEvent("Parallel plans created");

      // The rootFragmentList contains the 'root' fragments of each of the parallel plans
      rootFragmentList.addAll(parallelPlans);
    } else {
      rootFragmentList.add(planFragmentRoot);
    }

    TQueryExecRequest queryExecRequest = new TQueryExecRequest();
    TExecRequest result = createExecRequest(ctx_.getQueryCtx(), planFragmentRoot,
        queryExecRequest);
    queryExecRequest.setHost_list(hostLocations);

    boolean isQuery = getStmtType() == TStmtType.QUERY;

    // compute resource requirements of the final plan
    Planner.computeResourceReqs(rootFragmentList, ctx_.getQueryCtx(), queryExecRequest,
        ctx_, isQuery);

    // create the plan's exec-info
    for (PlanFragment planRoot : rootFragmentList) {
      TPlanExecInfo tPlanExecInfo = Frontend.createPlanExecInfo(planRoot, ctx_.getQueryCtx());

      queryExecRequest.addToPlan_exec_info(tPlanExecInfo);
    }

    // assign fragment idx
    int idx = 0;
    for (TPlanExecInfo tPlanExecInfo : queryExecRequest.getPlan_exec_info()) {
      for (TPlanFragment fragment : tPlanExecInfo.fragments) {
        fragment.setIdx(idx++);
      }
    }

    // create EXPLAIN output after setting everything else
    queryExecRequest.setQuery_ctx(ctx_.getQueryCtx()); // needed by getExplainString()
    List<PlanFragment> allFragments = rootFragmentList.get(0).getNodesPreOrder();

    // to mimic Impala's behavior, use EXTENDED mode explain except for EXPLAIN statements
    TExplainLevel explainLevel = isExplain ? ctx_.getQueryOptions().getExplain_level() :
        TExplainLevel.EXTENDED;
    String explainStr = getExplainString(allFragments, explainLevel);
    queryExecRequest.setQuery_plan(explainStr);

    ctx_.getQueryCtx().setDesc_tbl_serialized(ctx_.getRootAnalyzer().getDescTbl().toSerializedThrift());

    return result;
  }

  // TODO: CDPD-8176: Refactor and share Impala's getExplainString()
  private String getExplainString(List<PlanFragment> fragments,
      TExplainLevel explainLevel) {
    StringBuilder str = new StringBuilder();
    if (explainLevel.ordinal() < TExplainLevel.VERBOSE.ordinal()) {
      // Print the non-fragmented parallel plan.
      str.append(fragments.get(0).getExplainString(ctx_.getQueryOptions(), explainLevel));
    } else {
      // Print the fragmented parallel plan.
      for (int i = 0; i < fragments.size(); ++i) {
        PlanFragment fragment = fragments.get(i);
        str.append(fragment.getExplainString(ctx_.getQueryOptions(), explainLevel));
        if (i < fragments.size() - 1)
          str.append("\n");
      }
    }
    return str.toString();
  }

  /**
   * Create one or more plan fragments corresponding to the supplied single node physical plan.
   * This function calls Impala's DistributedPlanner to create the plan fragments and does
   * some post-processing.  It is loosely based on Impala's Planner.createPlan() function.
   * @param planNodeRoot root node of the Impala physical plan
   * @return list of plan fragments in the order [root fragment, child of root ... leaf fragment]
   * @throws ImpalaException
   */
  private List<PlanFragment> createPlanFragments(PlanNode planNodeRoot) throws ImpalaException {

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
      boolean isPartitioned = false;
      distributedPlanner.createPlanFragments(planNodeRoot, isPartitioned, fragments);
    }

    PlanFragment rootFragment = fragments.get(fragments.size() - 1);

    // Create runtime filters.
    if (ctx_.getQueryOptions().getRuntime_filter_mode() != TRuntimeFilterMode.OFF) {
      RuntimeFilterGenerator.generateRuntimeFilters(ctx_, rootFragment.getPlanRoot());
      ctx_.getTimeline().markEvent("Runtime filters computed");
    }

    rootFragment.verifyTree();

    // create the data sink
    List<Expr> resultExprs = ctx_.getResultExprs();
    if (resultPath != null) {
      String resultSinkPath = resultPath.toUri().toString();
      ImpalaResultLocation resultLocation = new ImpalaResultLocation(resultExprs, resultSinkPath);
      ctx_.getRootAnalyzer().getDescTbl().setTargetTable(resultLocation);
      List<Integer> referencedColumns =  new ArrayList<>();
      // Creates a table sink that uses ImpalaResultLocation that is used to specify the
      // desired location of query results
      TableSink sink = TableSink.create(resultLocation, TableSink.Op.INSERT,
          ImmutableList.<Expr>of(), resultExprs, referencedColumns, false, false,
          new Pair<>(ImmutableList.<Integer> of(), TSortingOrder.LEXICAL), -1, true);
      rootFragment.setSink(sink);
    } else {
      rootFragment.setSink(new PlanRootSink(resultExprs));
    }

    Planner.checkForDisableCodegen(rootFragment.getPlanRoot(), ctx_);
    // finalize exchanges: this ensures that for hash partitioned joins, the partitioning
    // keys on both sides of the join have compatible data types
    for (PlanFragment fragment: fragments) {
      fragment.finalizeExchanges(ctx_.getRootAnalyzer());
    }


    Collections.reverse(fragments);
    ctx_.getTimeline().markEvent("Distributed plan created");

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

  private TStmtType getStmtType() {
    // TODO: retrieve the statement type from Hive
    return TStmtType.QUERY;
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
    result.getQuery_exec_request().setStmt_type(getStmtType());

    // fill in the metadata using the root fragment's PlanRootSink
    Preconditions.checkState(planFragmentRoot.hasSink());
    List<Expr> outputExprs = new ArrayList<>();

    planFragmentRoot.getSink().collectExprs(outputExprs);
    result.setResult_set_metadata(createQueryResultSetMetadata(outputExprs));
    return result;
  }

  public TQueryCtx createQueryContext(String defaultDb,
      String user, TQueryOptions options, TNetworkAddress hostLocation) {
    TQueryCtx queryCtx = new TQueryCtx();
    queryCtx.setClient_request(new TClientRequest("Submitting Hive generate plan", options));
    queryCtx.setQuery_id(new TUniqueId());
    queryCtx.setSession(new TSessionState(new TUniqueId(), TSessionType.HIVESERVER2,
        defaultDb, user, hostLocation));

    // TODO: following fields need to be configured appropriately
    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSSSSS");
    Date now = Calendar.getInstance().getTime();
    queryCtx.setNow_string(formatter.format(now));
    formatter.setTimeZone(TimeZone.getTimeZone("GMT"));
    queryCtx.setUtc_timestamp_string(formatter.format(now));
    queryCtx.setLocal_time_zone("UTC");
    queryCtx.setStart_unix_millis(System.currentTimeMillis());
    queryCtx.setPid(1000);
    String requestPool = SessionState.get().getConf()
        .getVar(HiveConf.ConfVars.HIVE_IMPALA_REQUEST_POOL);
    queryCtx.setRequest_pool(requestPool); // for admission control
    queryCtx.setCoord_address(hostLocation);

    TNetworkAddress krpcCordAddr = new TNetworkAddress();
    krpcCordAddr.setHostname("127.0.0.1");
    krpcCordAddr.setPort(27000);
    queryCtx.setCoord_krpc_address(krpcCordAddr);

    return queryCtx;
  }

}
