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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.ql.metadata.events;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.AllTableConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.SQLAllTableConstraints;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveMetaStoreClientWithLocalCache;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * The metastore client cache holding a non-transactional table across queries by its version.
 */
public class TestMetadataCacheByVersion {
  private static int queryCount;

  private final TableVersionRegistry registry = TableVersionRegistry.get();
  private IMetaStoreClient delegate;

  @Before
  public void setUp() throws Exception {
    HiveConf conf = new HiveConf();
    HiveMetaStoreClientWithLocalCache.init(conf);
    registry.enable(conf);

    Table table = new Table();
    table.setId(42L);
    table.setCatName("hive");
    table.setDbName("db");
    table.setTableName("t");
    table.setTableType(TableType.EXTERNAL_TABLE.name());
    table.setParameters(new HashMap<>());
    SQLAllTableConstraints constraints = new SQLAllTableConstraints();
    constraints.setPrimaryKeys(new ArrayList<>(List.of(
        new SQLPrimaryKey("db", "t", "id", 1, "pk", false, false, false))));
    delegate = mock(IMetaStoreClient.class);
    when(delegate.getTable(any(GetTableRequest.class))).thenReturn(table);
    when(delegate.getAllTableConstraints(any())).thenReturn(constraints);
    when(delegate.listPartitionNames(anyString(), anyString(), anyString(), anyInt()))
        .thenReturn(List.of("p=1", "p=2"));
  }

  @After
  public void tearDown() {
    registry.disable();
  }

  /** Starts a query as SemanticAnalyzer.startAnalysis does, and returns a client for it. */
  private IMetaStoreClient startQuery(boolean optIn) throws Exception {
    String queryId = "test_metadata_cache_by_version_" + queryCount++;
    HiveConf conf = new HiveConf();
    conf.setVar(HiveConf.ConfVars.HIVE_QUERY_ID, queryId);
    conf.setVar(HiveConf.ConfVars.HIVE_METADATA_CACHE_NONTRANSACTIONAL_MODE, optIn ? "bounded" : "off");
    SessionState ss = new SessionState(conf);
    SessionState.setCurrentSessionState(ss);
    QueryState queryState = new QueryState.Builder().withHiveConf(conf).withGenerateNewQueryId(false).build();
    // which also decides, as for every query, whether it may use the cache by versions
    queryState.createHMSCache();
    ss.addQueryState(queryId, queryState);
    Hive.get(conf);
    return HiveMetaStoreClientWithLocalCache.newClient(conf, delegate);
  }

  private static GetTableRequest getTableRequest() {
    GetTableRequest req = new GetTableRequest("db", "t");
    req.setCatName("hive");
    req.setGetColumnStats(false);
    req.setEngine(Constants.HIVE_ENGINE);
    return req;
  }

  private void compileQuery(boolean optIn) throws Exception {
    IMetaStoreClient client = startQuery(optIn);
    Table first = client.getTable(getTableRequest());
    Table second = client.getTable(getTableRequest());
    if (optIn) {
      assertNotSame("each caller gets its own copy", first, second);
    }
    client.listPartitionNames("hive", "db", "t", -1);
    SQLAllTableConstraints constraints = client.getAllTableConstraints(new AllTableConstraintsRequest("db", "t", "hive"));
    // Hive.getTableConstraints changes the object it gets
    constraints.getPrimaryKeys().clear();
  }

  @Test
  public void testCachedAcrossQueriesUntilInvalidated() throws Exception {
    compileQuery(true);
    compileQuery(true);
    verify(delegate, times(1)).getTable(any(GetTableRequest.class));
    verify(delegate, times(1)).listPartitionNames(anyString(), anyString(), anyString(), anyInt());
    verify(delegate, times(1)).getAllTableConstraints(any());
    assertEquals(1, startQuery(true).getAllTableConstraints(new AllTableConstraintsRequest("db", "t", "hive"))
        .getPrimaryKeys().size());

    // an event on the table
    registry.invalidateTable("hive", "db", "t");
    compileQuery(true);
    verify(delegate, times(2)).getTable(any(GetTableRequest.class));
    verify(delegate, times(2)).listPartitionNames(anyString(), anyString(), anyString(), anyInt());
    verify(delegate, times(2)).getAllTableConstraints(any());
  }

  @Test
  public void testNotCachedWithoutOptIn() throws Exception {
    compileQuery(false);
    compileQuery(false);
    verify(delegate, atLeast(4)).getTable(any(GetTableRequest.class));
    verify(delegate, times(2)).listPartitionNames(anyString(), anyString(), anyString(), anyInt());
    verify(delegate, times(2)).getAllTableConstraints(any());
  }
}
