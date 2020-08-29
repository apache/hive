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

package org.apache.hadoop.hive.ql.plan.impala;

import com.google.common.base.Preconditions;

import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.exec.impala.ImpalaSession;
import org.apache.hadoop.hive.ql.exec.impala.ImpalaSessionManager;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveImpalaRules;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveImpalaWindowingFixRule;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.type.FunctionHelper;
import org.apache.hadoop.hive.ql.plan.impala.funcmapper.ImpalaFunctionHelper;
import org.apache.hadoop.hive.ql.plan.impala.node.ImpalaPlanRel;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.parse.CalcitePlanner;
import org.apache.hadoop.hive.ql.parse.QB;
import org.apache.impala.catalog.BuiltinsDb;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.InternalException;
import org.apache.impala.planner.PlanNode;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.service.FeSupport;
import org.apache.impala.thrift.TBackendGflags;
import org.apache.impala.thrift.TExecRequest;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.thrift.TReservedWordsVersion;
import org.apache.impala.thrift.TRuntimeFilterMode;
import org.apache.impala.thrift.TStmtType;
import org.apache.impala.util.EventSequence;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

public class ImpalaHelper {

  private final ImpalaQueryContext queryContext;

  private static final Logger LOG = LoggerFactory.getLogger(ImpalaHelper.class);
  private final EventSequence timeline = new EventSequence("Frontend Timeline");

  static {
    // ensure that the instance is created with the "true" parameter.
    // If we don't call it here, it could be called from within impala-frontend with
    // the "false" parameter.
    BuiltinsDb.getInstance(true);
  }

  public ImpalaHelper(HiveConf conf, String dbname, String username)
      throws SemanticException {
    try {
      queryContext = new ImpalaQueryContext(conf, dbname, username, createQueryOptions(conf));
    } catch (HiveException e) {
      throw new SemanticException(e);
    }
  }

  public ImpalaQueryContext getQueryContext() {
    return queryContext;
  }

  public EventSequence getTimeline() {
    return timeline;
  }

  public HepProgram getHepProgram(Hive db) {
    HepProgramBuilder programBuilder = new HepProgramBuilder();
    programBuilder.addMatchOrder(HepMatchOrder.DEPTH_FIRST);
    programBuilder.addRuleInstance(
        HiveImpalaWindowingFixRule.INSTANCE);
    programBuilder.addRuleInstance(
        new HiveImpalaRules.ImpalaProjectProjectRule(HiveRelFactories.HIVE_BUILDER));
    programBuilder.addRuleInstance(
        new HiveImpalaRules.ImpalaFilterSortRule(HiveRelFactories.HIVE_BUILDER));
    programBuilder.addRuleInstance(
        new HiveImpalaRules.ImpalaFilterScanRule(HiveRelFactories.HIVE_BUILDER, db));
    programBuilder.addRuleInstance(
        new HiveImpalaRules.ImpalaFilterAggRule(HiveRelFactories.HIVE_BUILDER));
    programBuilder.addRuleInstance(
        new HiveImpalaRules.ImpalaFilterProjectRule(HiveRelFactories.HIVE_BUILDER));
    programBuilder.addRuleInstance(
        new HiveImpalaRules.ImpalaFilterJoinRule(HiveRelFactories.HIVE_BUILDER));
    programBuilder.addRuleInstance(
        new HiveImpalaRules.ImpalaSortRule(HiveRelFactories.HIVE_BUILDER));
    programBuilder.addRuleInstance(
        new HiveImpalaRules.ImpalaScanRule(HiveRelFactories.HIVE_BUILDER, db));
    programBuilder.addRuleInstance(
        new HiveImpalaRules.ImpalaTableFunctionRule(HiveRelFactories.HIVE_BUILDER, db));
    programBuilder.addRuleInstance(
        new HiveImpalaRules.ImpalaProjectRule(HiveRelFactories.HIVE_BUILDER));
    programBuilder.addRuleInstance(
        new HiveImpalaRules.ImpalaAggRule(HiveRelFactories.HIVE_BUILDER));
    programBuilder.addRuleInstance(
        new HiveImpalaRules.ImpalaJoinRule(HiveRelFactories.HIVE_BUILDER));
    programBuilder.addRuleInstance(
        new HiveImpalaRules.ImpalaSemiJoinRule(HiveRelFactories.HIVE_BUILDER));
    programBuilder.addRuleInstance(
        new HiveImpalaRules.ImpalaUnionRule(HiveRelFactories.HIVE_BUILDER));
    return programBuilder.build();
  }

  public ImpalaCompiledPlan compilePlan(Hive db, RelNode rootRelNode, Path resultPath,
      boolean isExplain, QB qb, CalcitePlanner.PreCboCtx.Type stmtType, long writeId) throws HiveException {
    try {
      Preconditions.checkState(rootRelNode instanceof ImpalaPlanRel, "Plan contains operators not supported by Impala");
      ImpalaPlanRel impalaRelNode = (ImpalaPlanRel) rootRelNode;
      ImpalaPlanner impalaPlanner = new ImpalaPlanner(queryContext, resultPath, db, qb,
          getImpalaStmtType(stmtType), getImpalaResultStmtType(stmtType), timeline, writeId);
      ImpalaPlannerContext planCtx = impalaPlanner.getPlannerContext();
      impalaPlanner.initTargetTable();
      planCtx.getTableLoader().loadTablesAndPartitions(db);

      PlanNode rootImpalaNode = impalaRelNode.getRootPlanNode(planCtx);
      timeline.markEvent("Single node plan created");
      TExecRequest execRequest = impalaPlanner.createExecRequest(rootImpalaNode, isExplain);
      LOG.debug("Impala request is {}", execRequest);
      return new ImpalaCompiledPlan(execRequest, timeline);
    } catch (ImpalaException | MetaException e) {
      throw new HiveException(e);
    }
  }

  /**
   * Top-level statement type being executed. There may be multiple ExecRequests
   * with different statement types submitted under this top-level statement.
   */
  private TStmtType getImpalaStmtType(CalcitePlanner.PreCboCtx.Type stmtType) {
    switch (stmtType) {
      case NONE:
        return TStmtType.QUERY;
      case INSERT:
        return TStmtType.DML;
      case CTAS:
      case VIEW:
        return TStmtType.DDL;
      default:
        throw new RuntimeException("Unsupported statement type "+stmtType.toString());
    }
  }

  /**
   * Statement type for the current request being submitted to Impala.
   */
  private TStmtType getImpalaResultStmtType(CalcitePlanner.PreCboCtx.Type stmtType) {
    switch (stmtType) {
      case NONE:
        return TStmtType.QUERY;
      case INSERT:
        return TStmtType.DML;
      case CTAS:
        return TStmtType.DML;
      case VIEW:
        return TStmtType.DDL;
      default:
        throw new RuntimeException("Unsupported statement type "+stmtType.toString());
    }
  }

  public FunctionHelper createFunctionHelper(RexBuilder rexBuilder) {
    return new ImpalaFunctionHelper(queryContext, rexBuilder);
  }

  private TQueryOptions createQueryOptions(HiveConf conf) throws HiveException {
    TQueryOptions options = conf.getBoolVar(ConfVars.HIVE_IN_TEST) ?
        createTestQueryOptions(conf) : createDefaultQueryOptions(conf);

    // If unset, set MT_DOP to 0 to simplify the rest of the code (this mimics Impala FE)
    if (!options.isSetMt_dop()) {
      options.setMt_dop(0);
    }
    return options;
  }

  private TQueryOptions createTestQueryOptions(HiveConf conf) {
    // Note: For the FENG test suite, we currently create the options from HiveConf
    // since we do not establish a session with Impala's coordinator.
    Map<String, String> queryOptions = conf.subtree("impala").entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey,Map.Entry::getValue));
    BackendConfig.create(getBackendConfig(queryOptions), false);
    return parseQueryOptions(queryOptions);
  }

  private TQueryOptions createDefaultQueryOptions(HiveConf conf) throws HiveException {
    ImpalaSession session = ImpalaSessionManager.getInstance().getSession(conf);

    // Collect the option settings that are returned in the HS2 session
    // config and generate a comma separated string apply using FeSupport
    // http_addr is added by HS2 and will cause an error if not removed
    String csvQueryOptions = session.getSessionConfig().entrySet().stream()
        .filter(e -> !e.getKey().equals("http_addr"))
        .map(e -> e.getKey() + "=" + e.getValue())
        .collect(Collectors.joining(","));

    // Overlay defaults from Impala backend option settings
    TQueryOptions options;
    try {
      options = FeSupport.ParseQueryOptions(csvQueryOptions,
          new TQueryOptions());
    } catch (InternalException e) {
      throw new HiveException(e);
    }
    return options;
  }

  /**
   * This method is only used for test purposes.
   */
  private TBackendGflags getBackendConfig(Map<String, String> options) {
    final TBackendGflags cfg = new TBackendGflags();
    if (options.size() == 0) {
      return cfg;
    }
    for (Entry<String, String> kv: options.entrySet()) {
      if (kv.getKey().length() == 0) {
        continue;
      }
      TBackendGflags._Fields field = TBackendGflags._Fields.findByName(
          kv.getKey().toLowerCase());
      if (field == null) {
        continue;
      }
      switch (field) {
      case SENTRY_CONFIG:
        cfg.setSentry_config(kv.getValue());
        break;
      case LOAD_AUTH_TO_LOCAL_RULES:
        cfg.setLoad_auth_to_local_rules(
            Boolean.parseBoolean(kv.getValue()));
        break;
      case NON_IMPALA_JAVA_VLOG:
        cfg.setNon_impala_java_vlog(
            Integer.parseInt(kv.getValue()));
        break;
      case IMPALA_LOG_LVL:
        cfg.setImpala_log_lvl(
            Integer.parseInt(kv.getValue()));
        break;
      case INC_STATS_SIZE_LIMIT_BYTES:
        cfg.setInc_stats_size_limit_bytes(
            Long.parseLong(kv.getValue()));
        break;
      case LINEAGE_EVENT_LOG_DIR:
        cfg.setLineage_event_log_dir(kv.getValue());
        break;
      case LOAD_CATALOG_IN_BACKGROUND:
        cfg.setLoad_catalog_in_background(
            Boolean.parseBoolean(kv.getValue()));
        break;
      case NUM_METADATA_LOADING_THREADS:
        cfg.setNum_metadata_loading_threads(
            Integer.parseInt(kv.getValue()));
        break;
      case PRINCIPAL:
        cfg.setPrincipal(kv.getValue());
        break;
      case SERVER_NAME:
        cfg.setServer_name(kv.getValue());
        break;
      case AUTHORIZATION_POLICY_PROVIDER_CLASS:
        cfg.setAuthorization_policy_provider_class(kv.getValue());
        break;
      case KUDU_MASTER_HOSTS:
        cfg.setKudu_master_hosts(kv.getValue());
        break;
      case LOCAL_LIBRARY_PATH:
        cfg.setLocal_library_path(kv.getValue());
        break;
      case READ_SIZE:
        cfg.setRead_size(
            Integer.parseInt(kv.getValue()));
        break;
      case KUDU_OPERATION_TIMEOUT_MS:
        cfg.setKudu_operation_timeout_ms(
            Integer.parseInt(kv.getValue()));
        break;
      case INITIAL_HMS_CNXN_TIMEOUT_S:
        cfg.setInitial_hms_cnxn_timeout_s(
            Integer.parseInt(kv.getValue()));
        break;
      case ENABLE_STATS_EXTRAPOLATION:
        cfg.setEnable_stats_extrapolation(
            Boolean.parseBoolean(kv.getValue()));
        break;
      case SENTRY_CATALOG_POLLING_FREQUENCY_S:
        cfg.setSentry_catalog_polling_frequency_s(
            Long.parseLong(kv.getValue()));
        break;
      case MAX_HDFS_PARTITIONS_PARALLEL_LOAD:
        cfg.setMax_hdfs_partitions_parallel_load(
            Integer.parseInt(kv.getValue()));
        break;
      case MAX_NONHDFS_PARTITIONS_PARALLEL_LOAD:
        cfg.setMax_nonhdfs_partitions_parallel_load(
            Integer.parseInt(kv.getValue()));
        break;
      case RESERVED_WORDS_VERSION:
        cfg.setReserved_words_version(
            TReservedWordsVersion.valueOf(kv.getValue().toUpperCase()));
        break;
      case MAX_FILTER_ERROR_RATE:
        cfg.setMax_filter_error_rate(
            Double.parseDouble(kv.getValue()));
        break;
      case MIN_BUFFER_SIZE:
        cfg.setMin_buffer_size(
            Long.parseLong(kv.getValue()));
        break;
      case ENABLE_ORC_SCANNER:
        cfg.setEnable_orc_scanner(
            Boolean.parseBoolean(kv.getValue()));
        break;
      case AUTHORIZED_PROXY_GROUP_CONFIG:
        cfg.setAuthorized_proxy_group_config(kv.getValue());
        break;
      case USE_LOCAL_CATALOG:
        cfg.setUse_local_catalog(
            Boolean.parseBoolean(kv.getValue()));
        break;
      case DISABLE_CATALOG_DATA_OPS_DEBUG_ONLY:
        cfg.setDisable_catalog_data_ops_debug_only(
            Boolean.parseBoolean(kv.getValue()));
        break;
      case LOCAL_CATALOG_CACHE_MB:
        cfg.setLocal_catalog_cache_mb(
            Integer.parseInt(kv.getValue()));
        break;
      case LOCAL_CATALOG_CACHE_EXPIRATION_S:
        cfg.setLocal_catalog_cache_expiration_s(
            Integer.parseInt(kv.getValue()));
        break;
      case CATALOG_TOPIC_MODE:
        cfg.setCatalog_topic_mode(kv.getValue());
        break;
      case INVALIDATE_TABLES_TIMEOUT_S:
        cfg.setInvalidate_tables_timeout_s(
            Integer.parseInt(kv.getValue()));
        break;
      case INVALIDATE_TABLES_ON_MEMORY_PRESSURE:
        cfg.setInvalidate_tables_on_memory_pressure(
            Boolean.parseBoolean(kv.getValue()));
        break;
      case INVALIDATE_TABLES_GC_OLD_GEN_FULL_THRESHOLD:
        cfg.setInvalidate_tables_gc_old_gen_full_threshold(
            Double.parseDouble(kv.getValue()));
        break;
      case INVALIDATE_TABLES_FRACTION_ON_MEMORY_PRESSURE:
        cfg.setInvalidate_tables_fraction_on_memory_pressure(
            Double.parseDouble(kv.getValue()));
        break;
      case LOCAL_CATALOG_MAX_FETCH_RETRIES:
        cfg.setLocal_catalog_max_fetch_retries(
            Integer.parseInt(kv.getValue()));
        break;
      case KUDU_SCANNER_THREAD_ESTIMATED_BYTES_PER_COLUMN:
        cfg.setKudu_scanner_thread_estimated_bytes_per_column(
            Long.parseLong(kv.getValue()));
        break;
      case KUDU_SCANNER_THREAD_MAX_ESTIMATED_BYTES:
        cfg.setKudu_scanner_thread_max_estimated_bytes(
            Long.parseLong(kv.getValue()));
        break;
      case CATALOG_MAX_PARALLEL_PARTIAL_FETCH_RPC:
        cfg.setCatalog_max_parallel_partial_fetch_rpc(
            Integer.parseInt(kv.getValue()));
        break;
      case CATALOG_PARTIAL_FETCH_RPC_QUEUE_TIMEOUT_S:
        cfg.setCatalog_partial_fetch_rpc_queue_timeout_s(
            Long.parseLong(kv.getValue()));
        break;
      case EXCHG_NODE_BUFFER_SIZE_BYTES:
        cfg.setExchg_node_buffer_size_bytes(
            Long.parseLong(kv.getValue()));
        break;
      case KUDU_MUTATION_BUFFER_SIZE:
        cfg.setKudu_mutation_buffer_size(
            Integer.parseInt(kv.getValue()));
        break;
      case KUDU_ERROR_BUFFER_SIZE:
        cfg.setKudu_error_buffer_size(
            Integer.parseInt(kv.getValue()));
        break;
      case HMS_EVENT_POLLING_INTERVAL_S:
        cfg.setHms_event_polling_interval_s(
            Integer.parseInt(kv.getValue()));
        break;
      case IMPALA_BUILD_VERSION:
        cfg.setImpala_build_version(kv.getValue());
        break;
      case AUTHORIZATION_FACTORY_CLASS:
        cfg.setAuthorization_factory_class(kv.getValue());
        break;
      case UNLOCK_MT_DOP:
        cfg.setUnlock_mt_dop(
            Boolean.parseBoolean(kv.getValue()));
        break;
      case RANGER_SERVICE_TYPE:
        cfg.setRanger_service_type(kv.getValue());
        break;
      case RANGER_APP_ID:
        cfg.setRanger_app_id(kv.getValue());
        break;
      case AUTHORIZATION_PROVIDER:
        cfg.setAuthorization_provider(kv.getValue());
        break;
      case RECURSIVELY_LIST_PARTITIONS:
        cfg.setRecursively_list_partitions(
            Boolean.parseBoolean(kv.getValue()));
        break;
      case QUERY_EVENT_HOOK_CLASSES:
        cfg.setQuery_event_hook_classes(kv.getValue());
        break;
      case QUERY_EVENT_HOOK_NTHREADS:
        cfg.setQuery_event_hook_nthreads(
            Integer.parseInt(kv.getValue()));
        break;
      case IS_EXECUTOR:
        cfg.setIs_executor(
            Boolean.parseBoolean(kv.getValue()));
        break;
      case USE_DEDICATED_COORDINATOR_ESTIMATES:
        cfg.setUse_dedicated_coordinator_estimates(
            Boolean.parseBoolean(kv.getValue()));
        break;
      case BLACKLISTED_DBS:
        cfg.setBlacklisted_dbs(kv.getValue());
        break;
      case BLACKLISTED_TABLES:
        cfg.setBlacklisted_tables(kv.getValue());
        break;
      case UNLOCK_ZORDER_SORT:
        cfg.setUnlock_zorder_sort(
            Boolean.parseBoolean(kv.getValue()));
        break;
      case MIN_PRIVILEGE_SET_FOR_SHOW_STMTS:
        cfg.setMin_privilege_set_for_show_stmts(kv.getValue());
        break;
      case MT_DOP_AUTO_FALLBACK:
        cfg.setMt_dop_auto_fallback(
            Boolean.parseBoolean(kv.getValue()));
        break;
      default:
        throw new RuntimeException("Backend config option '" + field + "' not supported for yet. "
            + "Please add handling to parse options");
      }
    }
    return cfg;
  }

  /**
   * This method is only used for test purposes.
   */
  private TQueryOptions parseQueryOptions(Map<String, String> options) {
    final TQueryOptions queryOptions = new TQueryOptions();
    if (options.size() == 0) {
      return queryOptions;
    }
    for (Entry<String, String> kv: options.entrySet()) {
      if (kv.getKey().length() == 0) {
        continue;
      }
      TQueryOptions._Fields field = TQueryOptions._Fields.findByName(
          kv.getKey().toLowerCase());
      if (field == null) {
        continue;
      }
      switch (field) {
      case NUM_NODES:
        queryOptions.setNum_nodes(
            Integer.parseInt(kv.getValue()));
        break;
      case PARQUET_DICTIONARY_FILTERING:
        queryOptions.setParquet_dictionary_filtering(
            Boolean.parseBoolean(kv.getValue()));
        break;
      case RUNTIME_FILTER_MODE:
        queryOptions.setRuntime_filter_mode(
            TRuntimeFilterMode.valueOf(kv.getValue().toUpperCase()));
        break;
      case EXPLAIN_LEVEL:
        queryOptions.setExplain_level(
            TExplainLevel.valueOf(kv.getValue().toUpperCase()));
        break;
      default:
        throw new RuntimeException("Query option '" + field + "' not supported for yet. "
            + "Please add handling to parse options");
      }
    }
    return queryOptions;
  }
}
