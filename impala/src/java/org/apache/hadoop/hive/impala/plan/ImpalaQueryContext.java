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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidTxnWriteIdList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.impala.exec.ImpalaSessionImpl;
import org.apache.hadoop.hive.impala.exec.ImpalaSessionManager;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.engine.EngineSession;
import org.apache.hadoop.hive.ql.lockmgr.HiveTxnManager;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.impala.prune.ImpalaBasicHdfsTable;
import org.apache.hadoop.hive.impala.prune.ImpalaBasicHdfsTable.TableWithPartitionNames;
import org.apache.hadoop.hive.impala.prune.ImpalaBasicPartition;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.StmtMetadataLoader;
import org.apache.impala.authorization.AuthorizationFactory;
import org.apache.impala.authorization.NoopAuthorizationFactory;
import org.apache.impala.thrift.TClientRequest;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.thrift.TQueryCtx;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.thrift.TSessionState;
import org.apache.impala.thrift.TSessionType;
import org.apache.impala.thrift.TUniqueId;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

public class ImpalaQueryContext {

  private final HiveConf conf;
  private final ImpalaBasicAnalyzer analyzer;
  private final ImpalaBasicAnalyzer pruneAnalyzer;
  private final List<TNetworkAddress> hostLocations = Lists.newArrayList();
  private final TQueryCtx queryCtx;
  private final Map<String, TableWithPartitionNames> cachedTables = Maps.newHashMap();
  private final Map<String, Database> cachedDbs = Maps.newHashMap();
  private final AuthorizationFactory authFactory;
  private final HiveTxnManager txnMgr;
  private final Context calcitePlannerCtx;

  public ImpalaQueryContext(HiveConf conf, String dbname, String username,
      TQueryOptions options, HiveTxnManager txnMgr, Context ctx) throws HiveException {
    this.conf = conf;
    // TODO: replace hostname and port with configured parameter settings
    hostLocations.add(new TNetworkAddress("127.0.0.1", 22000));

    queryCtx = createQueryContext(conf, dbname, username, options, hostLocations.get(0));

    authFactory = new NoopAuthorizationFactory();
    StmtMetadataLoader.StmtTableCache stmtTableCache =
        new StmtMetadataLoader.StmtTableCache(new DummyCatalog(), Sets.newHashSet(),
            Maps.newHashMap());
    analyzer = new ImpalaBasicAnalyzer(stmtTableCache, queryCtx, authFactory, hostLocations);
    // Extra analyzer and cache needed for "basic" tables.  This will hold additional metadata
    // such as the partition names. As an enhancement, we should try to limit the lifetime
    // of this object.
    StmtMetadataLoader.StmtTableCache pruneStmtTableCache =
        new StmtMetadataLoader.StmtTableCache(new DummyCatalog(), Sets.newHashSet(),
            Maps.newHashMap());
    pruneAnalyzer =
        new ImpalaBasicAnalyzer(pruneStmtTableCache, queryCtx, authFactory, hostLocations);
    this.txnMgr = txnMgr;
    this.calcitePlannerCtx = ctx;
  }

  public Analyzer getAnalyzer() {
    return analyzer;
  }

  public ImpalaQueryContext(ImpalaQueryContext queryContext) throws HiveException {
    this(queryContext.conf,
        queryContext.queryCtx.getSession().getDatabase(),
        queryContext.queryCtx.getSession().getConnected_user(),
        queryContext.queryCtx.getClient_request().getQuery_options(),
        queryContext.txnMgr,
        queryContext.calcitePlannerCtx);
  }

  public HiveConf getConf() {
    return conf;
  }

  /**
   * Creates a temporary analyzer. To be used if an analyzer is needed but it is not desired
   * to pollute the main query analyzer state.
   */
  public ImpalaBasicAnalyzer getPruneAnalyzer() {
    return pruneAnalyzer;
  }

  public TQueryCtx getTQueryCtx() {
    return queryCtx;
  }

  public List<TNetworkAddress> getHostLocations() {
    return hostLocations;
  }

  private TQueryCtx createQueryContext(HiveConf conf, String defaultDb,
      String user, TQueryOptions options, TNetworkAddress hostLocation) throws HiveException {
    TQueryCtx queryCtx;
    if (conf.getBoolVar(ConfVars.HIVE_IN_TEST)) {
      queryCtx = createTestQueryContext(conf, defaultDb, user, options, hostLocation);
    } else {
      ImpalaSessionImpl sessionImpl = ImpalaSessionManager.getInstance().getSession(getConf());
      queryCtx = sessionImpl.getQueryContext();
    }

    queryCtx.setClient_request(new TClientRequest("Submitting Hive generate plan", options));
    queryCtx.setSession(new TSessionState(new TUniqueId(), TSessionType.HIVESERVER2,
        defaultDb, user, hostLocation));

    String requestPool = conf.getVar(HiveConf.ConfVars.HIVE_IMPALA_REQUEST_POOL);
    queryCtx.setRequest_pool(requestPool); // for admission control

    return queryCtx;
  }

  public ImpalaBasicHdfsTable getBasicTable(Table msTbl) {
    String tableName = getTableName(msTbl);
    return cachedTables.containsKey(tableName)
        ? cachedTables.get(tableName).getTable()
        : null;
  }

  public void cacheBasicTable(Table msTbl, ImpalaBasicHdfsTable basicTable) {
    String tableName = getTableName(msTbl);
    // if cache already contains table, just return.  Table will always be the same.
    if (cachedTables.containsKey(tableName)) {
      return;
    }
    cachedTables.put(tableName, new TableWithPartitionNames(basicTable));
  }

  public void addBasicTableNewNames(Table msTbl, ImpalaBasicHdfsTable basicTable,
      Collection<ImpalaBasicPartition> partitions) {
    String tableName = getTableName(msTbl);
    Preconditions.checkState(cachedTables.containsKey(tableName));
    cachedTables.get(tableName).addPartitionNames(partitions);
  }

  public List<TableWithPartitionNames> getBasicTables() {
    return ImmutableList.copyOf(cachedTables.values());
  }

  public Database getDb(RelOptHiveTable table) throws HiveException {
    Table msTbl = table.getHiveTableMD().getTTable();
    Database msDb = cachedDbs.get(msTbl.getDbName());
    if (msDb == null) {
      // cache the Database object to avoid rpc to the metastore for future requests
      msDb = table.getDatabase();
      cachedDbs.put(msTbl.getDbName(), msDb);
    }
    return msDb;
  }

  public boolean isLoadingMaterializedViews() {
    return calcitePlannerCtx.isLoadingMaterializedView();
  }

  public void initTxnId() {
    txnMgr.getCurrentTxnId();
  }

  public ValidWriteIdList getValidWriteIdList(HiveConf conf, String tableName
      ) throws HiveException {
    String txnString = conf.get(ValidTxnList.VALID_TXNS_KEY);
    // This makes a call out to HMS.
    ValidTxnWriteIdList validTxnWriteIdList = txnMgr != null
        ? txnMgr.getValidWriteIds(Lists.newArrayList(tableName), txnString)
        : null;
    return (validTxnWriteIdList != null)
        ? validTxnWriteIdList.getTableValidWriteIdList(tableName)
        : null;
  }

  private String getTableName(Table msTbl) {
    return msTbl.getDbName() + "." + msTbl.getTableName();
  }

  private TQueryCtx createTestQueryContext(HiveConf conf, String defaultDb,
      String user, TQueryOptions options, TNetworkAddress hostLocation) {
    // Note: Currently in the testing environment, i.e., conf.getBoolVar(ConfVars.HIVE_IN_TEST)
    // evaluates to false, we create a mock TQueryCtx since we do not establish a session with
    // Impala's coordinator.
    TQueryCtx queryCtx = new TQueryCtx();
    queryCtx.setClient_request(new TClientRequest("Submitting Hive generate plan", options));
    queryCtx.setQuery_id(new TUniqueId());
    queryCtx.setSession(new TSessionState(new TUniqueId(), TSessionType.HIVESERVER2,
        defaultDb, user, hostLocation));

    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSSSSS");
    Date now = Calendar.getInstance().getTime();
    queryCtx.setNow_string(formatter.format(now));
    formatter.setTimeZone(TimeZone.getTimeZone("GMT"));
    queryCtx.setUtc_timestamp_string(formatter.format(now));
    queryCtx.setLocal_time_zone("UTC");
    queryCtx.setStart_unix_millis(System.currentTimeMillis());
    queryCtx.setPid(1000);
    String requestPool = conf.getVar(HiveConf.ConfVars.HIVE_IMPALA_REQUEST_POOL);
    queryCtx.setRequest_pool(requestPool); // for admission control
    queryCtx.setCoord_hostname("127.0.0.1");
    queryCtx.setStatus_report_interval_ms(5000);

    TNetworkAddress krpcCordAddr = new TNetworkAddress();
    krpcCordAddr.setHostname("127.0.0.1");
    krpcCordAddr.setPort(27000);
    queryCtx.setCoord_ip_address(krpcCordAddr);

    return queryCtx;
  }
}
