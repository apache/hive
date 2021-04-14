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

package org.apache.hadoop.hive.impala.plan;

import com.google.common.base.Preconditions;

import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.hadoop.hive.common.ValidTxnWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.impala.calcite.rules.HiveImpalaRules;
import org.apache.hadoop.hive.impala.calcite.rules.HiveImpalaWindowingFixRule;
import org.apache.hadoop.hive.impala.exec.ImpalaSessionManager;
import org.apache.hadoop.hive.impala.funcmapper.ImpalaFunctionHelper;
import org.apache.hadoop.hive.impala.node.ImpalaPlanRel;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.engine.EngineEventSequence;
import org.apache.hadoop.hive.ql.engine.EngineQueryHelper;
import org.apache.hadoop.hive.ql.engine.EngineSession;
import org.apache.hadoop.hive.ql.lockmgr.HiveTxnManager;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.parse.CalcitePlanner;
import org.apache.hadoop.hive.ql.parse.QB;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.type.FunctionHelper;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.InternalException;
import org.apache.impala.planner.PlanNode;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.service.FeSupport;
import org.apache.impala.thrift.TBackendGflags;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.thrift.TReservedWordsVersion;
import org.apache.impala.thrift.TRuntimeFilterMode;
import org.apache.impala.thrift.TStmtType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.stream.Collectors;

public class ImpalaQueryHelperImpl implements EngineQueryHelper {

  private final ImpalaQueryContext queryContext;

  private static final Logger LOG = LoggerFactory.getLogger(ImpalaQueryHelperImpl.class);
  private final EngineEventSequence timeline;
  private final QueryState queryState;

  public ImpalaQueryHelperImpl(HiveConf conf, String dbname, String username, HiveTxnManager txnMgr,
      Context ctx, QueryState queryState) throws SemanticException {
    timeline = ctx.getTimeline();
    Preconditions.checkNotNull(timeline);
    this.queryState = queryState;
    try {
      this.queryContext =
          new ImpalaQueryContext(conf, dbname, username, createQueryOptions(conf), txnMgr, ctx);
    } catch (HiveException e) {
      throw new SemanticException(e);
    }
  }

  public ImpalaQueryHelperImpl(ImpalaQueryHelperImpl queryHelper) throws SemanticException {
    Preconditions.checkNotNull(queryHelper.timeline);
    this.timeline = queryHelper.timeline;
    this.queryState = queryHelper.queryState;
    try {
      this.queryContext = new ImpalaQueryContext(queryHelper.queryContext);
    } catch (HiveException e) {
      throw new SemanticException(e);
    }
  }

  public ImpalaQueryContext getQueryContext() {
    return queryContext;
  }

  public EngineEventSequence getTimeline() {
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

  public FileSinkDesc compilePlan(Hive db, RelNode rootRelNode,
      FileSinkDesc fileSinkDesc, boolean isExplain, QB qb, CalcitePlanner.PreCboCtx.Type stmtType,
      ValidTxnWriteIdList txnWriteIdList) throws HiveException {
    try {
      Preconditions.checkState(rootRelNode instanceof ImpalaPlanRel, "Plan contains operators not supported by Impala");
      ImpalaPlanRel impalaRelNode = (ImpalaPlanRel) rootRelNode;
      ImpalaPlanner impalaPlanner = new ImpalaPlanner(queryContext, fileSinkDesc, db, qb,
          getImpalaStmtType(stmtType, qb), timeline);
      ImpalaPlannerContext planCtx = impalaPlanner.getPlannerContext();
      planCtx.getTableLoader().loadTablesAndPartitions(db, txnWriteIdList);
      impalaPlanner.initTargetTable();

      PlanNode rootImpalaNode = impalaRelNode.getRootPlanNode(planCtx);
      timeline.markEvent("Single node plan created");
      ImpalaCompiledPlan compiledPlan =
          new ImpalaCompiledPlan(impalaPlanner, rootImpalaNode, timeline, isExplain);
      return new ImpalaQueryDesc(compiledPlan);
    } catch (ImpalaException | MetaException e) {
      throw new HiveException(e);
    }
  }

  /**
   * Statement type for the current request being submitted to Impala.
   */
  private TStmtType getImpalaStmtType(CalcitePlanner.PreCboCtx.Type stmtType,
      QB qb) {
    switch (stmtType) {
      case NONE:
        return TStmtType.QUERY;
      case CTAS:
      case INSERT:
        return TStmtType.DML;
      case VIEW:
        // Views are treated as DDL. Materialized Views are treated as DML since Impala will
        // execute the initial build.
        return qb.isMaterializedView() ? TStmtType.DML : TStmtType.DDL;
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

    // CDPD-20969: Stopgap fix.  We don't support abort_on_error=false.  When this
    // gets supported, this statement needs to go away.
    options.setAbort_on_error(true);
    return options;
  }

  private TQueryOptions createTestQueryOptions(HiveConf conf) {
    // Note: Currently in the non-testing environment, i.e.,
    // conf.getBoolVar(ConfVars.HIVE_IN_TEST) evaluates to false, we use 'conf' to
    // initialize those fields in an ImpalaSession that can be set up via Hive-related
    // configurations, e.g., 'HiveConf.ConfVars.HIVE_IMPALA_ADDRESS'. Since we do not
    // establish a session with Impala's coordinator in the testing environment, 'conf'
    // is not really used in this method.

    // If there is a change to an Impala-related query option not prefixed by Impala's
    // namespace, we also need to update the value corresponding to the option prefixed
    // by Impala's namespace. Therefore, we need to modify the corresponding property
    // in SessionState.get().getConf() so that a subsequent SET statement for the updated
    // query option would reflect the change. Refer to SetProcessor#getVariable() for
    // further details. Simply updating the corresponding property in 'conf' cannot make
    // the change to an option prefixed by Impala's namespace persistent after a query
    // is executed. Similarly, to remove an unsupported query option prefixed by Impala's
    // namespace, we need to update the corresponding property in
    // SessionState.get().getConf().
    HiveConf sessionConf = SessionState.get().getConf();
    filterUnsupportedImpalaQueryOptions(sessionConf);
    updateImpalaQueryOptions(sessionConf);
    Map<String, String> queryOptions = sessionConf.subtree("impala").entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey,Map.Entry::getValue));
    BackendConfig.create(getBackendConfig(queryOptions), false);
    return parseQueryOptions(queryOptions);
  }

  private TQueryOptions createDefaultQueryOptions(HiveConf conf) throws HiveException {
    // Update Impala's query options if applicable. Both 'conf' and 'sessionConf' contain
    // the same set of Impala's query options without Impala's namespace. We choose to
    // update 'sessionConf' because 1) currently to prepare a TOpenSessionReq for the
    // Impala backend, we retrieve the query options in 'sessionConf', and 2) the query
    // option prefixed by Impala's namespace would be updated accordingly. Refer to
    // ImpalaSession#open() for the first reason and SetProcessor#getVariable() for the
    // second reason.
    HiveConf sessionConf = SessionState.get().getConf();
    filterUnsupportedImpalaQueryOptions(sessionConf);
    updateImpalaQueryOptions(sessionConf);

    ImpalaSessionManager mgr = ImpalaSessionManager.getInstance();
    EngineSession session = mgr.getSession(conf);
    // make sure cluster membership snapshot is not stale
    mgr.ensureCurrentMembership(session);
    timeline.markEvent("Set current executor membership");

    Map<String, String> configurations = session.getSessionConfig();
    if (queryState.getConfOverlay() != null && !queryState.getConfOverlay().isEmpty()) {
      configurations = new LinkedHashMap<>(session.getSessionConfig());
      // The conf overlay contains overrides for the session config to be used
      // at execution time.
      configurations.putAll(queryState.getConfOverlay());
    }

    // Collect the option settings that are returned in the HS2 session
    // config and generate a comma separated string apply using FeSupport
    // http_addr is added by HS2 and will cause an error if not removed
    String csvQueryOptions = configurations.entrySet().stream()
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

  private void filterUnsupportedImpalaQueryOptions(HiveConf conf) {
    Properties props = conf.getAllProperties();

    for (Map.Entry<Object, Object> e : props.entrySet()) {
      String key = (String) e.getKey();
      if (key.startsWith("impala")) {
        if (key.length() > "impala.".length()) {
          String keyWithoutNameSpace = key.substring("impala.".length()).toLowerCase();
          TQueryOptions._Fields field = TQueryOptions._Fields.findByName(
              keyWithoutNameSpace);
          if (field == null) {
            conf.unset(key);
            // Recall that in HiveConf#verifyAndSet() we called HiveConf#set() on a key
            // prefixed by "impala." so we have to call unset() here if the key is
            // actually unsupported by Impala.
            conf.unset(keyWithoutNameSpace);
          }
        } else {
          conf.unset(key);
        }
      }
    }
  }

  private void updateImpalaQueryOptions(HiveConf conf) {
    Properties origProps = conf.getAllProperties();
    Properties impalaProps = new Properties();
    List<String> origImpalaProps = new ArrayList<>();

    for (Map.Entry<Object, Object> e : origProps.entrySet()) {
      String key = (String) e.getKey();
      TQueryOptions._Fields field = TQueryOptions._Fields.findByName(
              key.toLowerCase());
      if (field != null) {
        impalaProps.put("impala.".concat((String) e.getKey()), e.getValue());
        origImpalaProps.add((String) e.getKey());
      }
    }

    // We need to update 'isImpalaConfigUpdated' since we were not able to determine this
    // in HiveConf#verifyAndSet(). Specifically, if there is an existing ImpalaSession
    // and we do not call setImpalaConfigUpdated(true) when an Impala-related query
    // option is updated, the change will not be reflected in the TOpenSessionReq we send
    // to the Impala backend in the next query. Refer to
    // ImpalaSessionManager#getSession() for further details.
    if (!origImpalaProps.isEmpty()) conf.setImpalaConfigUpdated(true);

    for (Map.Entry<Object, Object> e : impalaProps.entrySet()) {
      conf.set((String) e.getKey(), (String) e.getValue());
    }

    // We do not call unset() for each query option 'name' in 'origImpalaProps' so that a
    // user is able to get the value for the query option via "SET 'name'" instead of
    // "SET impala.'name'";
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
      cfg.setFieldValue(field, kv.getValue());
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
