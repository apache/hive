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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.parse.type.FunctionHelper;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.StmtMetadataLoader;
import org.apache.impala.authorization.AuthorizationFactory;
import org.apache.impala.authorization.NoopAuthorizationFactory;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.planner.PlannerContext;
import org.apache.impala.thrift.TClientRequest;
import org.apache.impala.thrift.TExecRequest;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.thrift.TQueryCtx;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.thrift.TSessionState;
import org.apache.impala.thrift.TSessionType;
import org.apache.impala.thrift.TUniqueId;
import org.apache.impala.util.EventSequence;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

public class ImpalaQueryContext {

  private final ImpalaBasicAnalyzer analyzer;
  private final List<TNetworkAddress> hostLocations = Lists.newArrayList();
  private final TQueryCtx queryCtx;

  public ImpalaQueryContext(HiveConf conf, String dbname, String username, TQueryOptions options) {
    // TODO: replace hostname and port with configured parameter settings
    hostLocations.add(new TNetworkAddress("127.0.0.1", 22000));

    queryCtx = createQueryContext(conf, dbname, username, options, hostLocations.get(0));

    AuthorizationFactory authFactory = new NoopAuthorizationFactory();
    StmtMetadataLoader.StmtTableCache stmtTableCache =
        new StmtMetadataLoader.StmtTableCache(new DummyCatalog(), Sets.newHashSet(), Maps.newHashMap());
    analyzer = new ImpalaBasicAnalyzer(stmtTableCache, queryCtx, authFactory, hostLocations);
  }

  public Analyzer getAnalyzer() {
    return analyzer;
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
}

