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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.hadoop.hive.common.ValidTxnWriteIdList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.plan.impala.catalog.ImpalaHdfsTable;
import org.apache.hadoop.hive.ql.plan.impala.prune.ImpalaBasicHdfsTable;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.StmtMetadataLoader;
import org.apache.impala.analysis.TableName;
import org.apache.impala.authorization.AuthorizationFactory;
import org.apache.impala.authorization.NoopAuthorizationFactory;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.thrift.TClientRequest;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.thrift.TQueryCtx;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.thrift.TSessionState;
import org.apache.impala.thrift.TSessionType;
import org.apache.impala.thrift.TUniqueId;
import org.apache.thrift.TException;

import java.text.SimpleDateFormat;
import java.util.Calendar;
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
  private final Map<String, ImpalaBasicHdfsTable> cachedTables = Maps.newHashMap();
  private final Map<String, Database> cachedDbs = Maps.newHashMap();
  private final AuthorizationFactory authFactory;

  public ImpalaQueryContext(HiveConf conf, String dbname, String username,
      TQueryOptions options) {
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
  }

  public Analyzer getAnalyzer() {
    return analyzer;
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
    String requestPool = conf.getVar(HiveConf.ConfVars.HIVE_IMPALA_REQUEST_POOL);
    queryCtx.setRequest_pool(requestPool); // for admission control
    queryCtx.setCoord_address(hostLocation);

    TNetworkAddress krpcCordAddr = new TNetworkAddress();
    krpcCordAddr.setHostname("127.0.0.1");
    krpcCordAddr.setPort(27000);
    queryCtx.setCoord_krpc_address(krpcCordAddr);

    return queryCtx;
  }

  /**
   * Get the HdfsTable. This HdfsTable differs slightly from the main HdfsTable class
   * as existing in Impala, see ImpalaBasicHdfsTable class for details.
   */
  public ImpalaBasicHdfsTable getBasicTableInstance(IMetaStoreClient client, RelOptHiveTable table,
      ValidTxnWriteIdList validTxnWriteIdList) throws HiveException {
    Table msTbl = table.getHiveTableMD().getTTable();
    Database msDb = cachedDbs.get(msTbl.getDbName());
    if (msDb == null) {
      // cache the Database object to avoid rpc to the metastore for future requests
      msDb = table.getDatabase();
      cachedDbs.put(msTbl.getDbName(), msDb);
    }
    String tableName = msTbl.getDbName() + "." + msTbl.getTableName();
    // Only store one copy for the query, so check the cache if it exists.
    ImpalaBasicHdfsTable cachedTable = cachedTables.get(tableName);
    if (cachedTable == null) {
      cachedTable = createBasicHdfsTable(client, msTbl, msDb, validTxnWriteIdList);
      cachedTables.put(tableName, cachedTable);
    }
    return cachedTable;
  }

  private ImpalaBasicHdfsTable createBasicHdfsTable(IMetaStoreClient client, Table msTbl,
      Database msDb, ValidTxnWriteIdList validTxnWriteIdList) throws HiveException {
    String tableName = msTbl.getDbName() + "." + msTbl.getTableName();

    ValidWriteIdList validWriteIdList = null;
    if (validTxnWriteIdList != null) {
      // Lets get this specific table's write id list
      validWriteIdList =
          validTxnWriteIdList.getTableValidWriteIdList(tableName);
    }

    ImpalaBasicHdfsTable cachedTable =
        new ImpalaBasicHdfsTable(conf, client, msTbl, msDb, validWriteIdList);

    cachedTables.put(tableName, cachedTable);
    // The :Prune" HdfsTable holds an HdfsTable that contains all the metadata information
    // needed for the query. This is the table that needs to be set in the analyzer.
    TableName impalaTableName = TableName.parse(tableName);

    return cachedTable;
  }

  public List<ImpalaBasicHdfsTable> getBasicTables() {
    return ImmutableList.copyOf(cachedTables.values());
  }

  public void initTxnId() {
    queryCtx.setTransaction_id(SessionState.get().getTxnMgr().getCurrentTxnId());
  }
}
