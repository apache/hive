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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.StmtMetadataLoader;
import org.apache.impala.authorization.AuthorizationFactory;
import org.apache.impala.authorization.NoopAuthorizationFactory;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.planner.DistributedPlanner;
import org.apache.impala.planner.PlanFragment;
import org.apache.impala.planner.PlanNode;
import org.apache.impala.planner.PlanRootSink;
import org.apache.impala.planner.Planner;
import org.apache.impala.planner.RuntimeFilterGenerator;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.service.FeSupport;
import org.apache.impala.service.Frontend;
import org.apache.impala.thrift.TBackendGflags;
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
import org.apache.impala.thrift.TReservedWordsVersion;
import org.apache.impala.thrift.TResultSetMetadata;
import org.apache.impala.thrift.TRuntimeFilterMode;
import org.apache.impala.thrift.TSessionState;
import org.apache.impala.thrift.TSessionType;
import org.apache.impala.thrift.TStmtType;
import org.apache.impala.thrift.TUniqueId;
import org.apache.impala.util.EventSequence;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private static final Logger LOG = LoggerFactory.getLogger(ImpalaPlanner.class);

  private ImpalaPlannerContext ctx_;
  private List<TNetworkAddress> hostLocations = new ArrayList<>();

  public ImpalaPlanner(String dbname, String username) {

    HiveConf conf = SessionState.get().getConf();
    TQueryOptions options = createDefaultQueryOptions(conf);

    initBackendConfig();;

    // TODO: replace hostname and port with configured parameter settings
    hostLocations.add(new TNetworkAddress("127.0.0.1", 22000));

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
  public TExecRequest createExecRequest(PlanNode planNodeRoot) throws ImpalaException {
    // Create the values transfer graph in the Analyzer. Note that FENG plans
    // don't register equijoin predicates in the Analyzer's GlobalState since 
    // Hive/Calcite should have already done the predicate inferencing analysis.
    // Hence, the GlobalState's registeredValueTransfers will be empty. It is
    // still necessary to instantiate the graph because otherwise
    // RuntimeFilterGenerator tries to de-reference it and encounters NPE.
    // TODO: CDPD-9689 tracks if we are missing any runtime filters compared
    // to Impala
    ctx_.getRootAnalyzer().computeValueTransferGraph();
    List<PlanFragment> fragments = createPlanFragments(planNodeRoot);
    Preconditions.checkArgument(fragments.size() > 0);
    PlanFragment planFragmentRoot = fragments.get(0);

    TQueryExecRequest queryExecRequest = new TQueryExecRequest();
    TExecRequest result = createExecRequest(ctx_.getQueryCtx(), planFragmentRoot,
        queryExecRequest);
    queryExecRequest.setHost_list(hostLocations);

    List<PlanFragment> rootFragmentList = new ArrayList<>();
    rootFragmentList.add(planFragmentRoot);

    boolean isQuery = getStmtType() == TStmtType.QUERY;

    // compute resource requirements of the final plan
    Planner.computeResourceReqs(rootFragmentList, ctx_.getQueryCtx(), queryExecRequest,
        ctx_, isQuery);

    // create the plan's exec-info
    TPlanExecInfo tPlanExecInfo =
        Frontend.createPlanExecInfo(planFragmentRoot, ctx_.getQueryCtx());

    queryExecRequest.addToPlan_exec_info(tPlanExecInfo);

    // assign fragment idx
    int idx = 0;
    for (TPlanFragment fragment : tPlanExecInfo.fragments) {
      fragment.setIdx(idx++);
    }

    // create EXPLAIN output after setting everything else
    queryExecRequest.setQuery_ctx(ctx_.getQueryCtx()); // needed by getExplainString()
    List<PlanFragment> allFragments = planFragmentRoot.getNodesPreOrder();
    String explainStr = getExplainString(allFragments, TExplainLevel.VERBOSE);
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
    List<PlanFragment> fragments = new ArrayList<>();
    // for queries, isPartitioned is false; in the future, make this conditional
    // on whether it is an insert/CTAS etc.
    boolean isPartitioned = false;
    distributedPlanner.createPlanFragments(planNodeRoot, isPartitioned, fragments);

    PlanFragment rootFragment = fragments.get(fragments.size() - 1);

    // Create runtime filters.
    if (ctx_.getQueryOptions().getRuntime_filter_mode() != TRuntimeFilterMode.OFF) {
      RuntimeFilterGenerator.generateRuntimeFilters(ctx_, rootFragment.getPlanRoot());
      ctx_.getTimeline().markEvent("Runtime filters computed");
    }

    rootFragment.verifyTree();

    // create the data sink
    rootFragment.setSink(new PlanRootSink(ctx_.getResultExprs()));

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

  private TQueryOptions createDefaultQueryOptions(HiveConf conf) {
    TQueryOptions options = new TQueryOptions();
    // TODO: Fill in session options
    options.setNum_nodes(2);
    options.setParquet_dictionary_filtering(
            conf.getBoolVar(HiveConf.ConfVars.HIVE_IMPALA_PARQUET_DICTIONARY_FILTERING));
    return options;
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

  private void initBackendConfig() {
    if (BackendConfig.INSTANCE == null) {
      // TODO: CDPD-7427: Retrieve BackendConfig from Impalad
      final TBackendGflags cfg = new TBackendGflags();
      cfg.setSentry_config("");
      cfg.setLoad_auth_to_local_rules(false);
      cfg.setNon_impala_java_vlog(3);
      cfg.setImpala_log_lvl(2);
      cfg.setInc_stats_size_limit_bytes(209715200);
      cfg.setLineage_event_log_dir("");
      cfg.setLoad_catalog_in_background(false);
      cfg.setNum_metadata_loading_threads(16);
      cfg.setPrincipal("");
      cfg.setServer_name("");
      cfg.setAuthorization_policy_provider_class("org.apache.sentry.provider.common.HadoopGroupResourceAuthorizationProvider");
      cfg.setKudu_master_hosts("localhost");
      cfg.setLocal_library_path("/tmp");
      cfg.setRead_size(8388608);
      cfg.setKudu_operation_timeout_ms(180000);
      cfg.setInitial_hms_cnxn_timeout_s(120);
      cfg.setEnable_stats_extrapolation(false);
      cfg.setSentry_catalog_polling_frequency_s(60);
      cfg.setMax_hdfs_partitions_parallel_load(5);
      cfg.setMax_nonhdfs_partitions_parallel_load(20);
      cfg.setReserved_words_version(TReservedWordsVersion.IMPALA_3_0);
      cfg.setMax_filter_error_rate(0.75);
      cfg.setMin_buffer_size(8192);
      cfg.setEnable_orc_scanner(true);
      cfg.setAuthorized_proxy_group_config("");
      cfg.setUse_local_catalog(false);
      cfg.setDisable_catalog_data_ops_debug_only(false);
      cfg.setLocal_catalog_cache_mb(-1);
      cfg.setLocal_catalog_cache_expiration_s(3600);
      cfg.setCatalog_topic_mode("full");
      cfg.setInvalidate_tables_timeout_s(0);
      cfg.setInvalidate_tables_on_memory_pressure(false);
      cfg.setInvalidate_tables_gc_old_gen_full_threshold(0.6);
      cfg.setInvalidate_tables_fraction_on_memory_pressure(0.1);
      cfg.setLocal_catalog_max_fetch_retries(40);
      cfg.setKudu_scanner_thread_estimated_bytes_per_column(393216);
      cfg.setKudu_scanner_thread_max_estimated_bytes(33554432);
      cfg.setCatalog_max_parallel_partial_fetch_rpc(32);
      cfg.setCatalog_partial_fetch_rpc_queue_timeout_s(Long.MAX_VALUE);
      cfg.setExchg_node_buffer_size_bytes(10485760);
      cfg.setKudu_mutation_buffer_size(10485760);
      cfg.setKudu_error_buffer_size(10485760);
      cfg.setHms_event_polling_interval_s(0);
      cfg.setImpala_build_version("3.4.0-SNAPSHOT");
      cfg.setAuthorization_factory_class("");
      cfg.setUnlock_mt_dop(false);
      cfg.setRanger_service_type("hive");
      cfg.setRanger_app_id("");
      cfg.setAuthorization_provider("ranger");
      cfg.setRecursively_list_partitions(true);
      cfg.setQuery_event_hook_classes("");
      cfg.setQuery_event_hook_nthreads(1);
      cfg.setIs_executor(true);
      cfg.setUse_dedicated_coordinator_estimates(true);
      cfg.setBlacklisted_dbs("sys,information_schema");
      cfg.setBlacklisted_tables("");
      cfg.setUnlock_zorder_sort(false);
      // TODO: Determine the appropriate default value
      cfg.setMin_privilege_set_for_show_stmts("");
      cfg.setMt_dop_auto_fallback(false);

      try {
        FeSupport.loadLibrary(false);
      } catch (RuntimeException e) {
        LOG.warn("initBackendConfig", e);
      }
      BackendConfig.create(cfg,
          false /* don't initialize SqlScanner or AuthToLocal */);
    }
  }

}

