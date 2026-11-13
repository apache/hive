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

package org.apache.hadoop.hive.ql.metadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AllTableConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.CheckConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.CreateTableRequest;
import org.apache.hadoop.hive.metastore.api.DefaultConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.SQLAllTableConstraints;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.Test;

public class TestSessionHiveMetaStoreClientQueryCache {

  /** Registers a query with an active query-level HMS cache, as SemanticAnalyzer.startAnalysis does. */
  private static HiveConf startQuery(String queryId) throws Exception {
    HiveConf conf = new HiveConf();
    conf.setVar(HiveConf.ConfVars.HIVE_QUERY_ID, queryId);
    SessionState ss = new SessionState(conf);
    SessionState.setCurrentSessionState(ss);
    QueryState queryState = new QueryState.Builder().withHiveConf(conf).withGenerateNewQueryId(false).build();
    queryState.createHMSCache();
    ss.addQueryState(queryId, queryState);
    Hive.get(conf);
    return conf;
  }

  /** The request Hive.getTable(db, tbl) sends. */
  private static GetTableRequest hiveGetTableRequest() {
    GetTableRequest req = new GetTableRequest("db", "t");
    req.setCatName("hive");
    req.setGetColumnStats(false);
    req.setEngine(Constants.HIVE_ENGINE);
    return req;
  }

  private static IMetaStoreClient tableDelegate() throws Exception {
    Table table = new Table();
    table.setId(42L);
    table.setDbName("db");
    table.setTableName("t");
    table.setParameters(new HashMap<>());
    IMetaStoreClient delegate = mock(IMetaStoreClient.class);
    when(delegate.getTable(any(GetTableRequest.class))).thenReturn(table);
    return delegate;
  }

  @Test
  public void testAllTableConstraintsFetchedOncePerQuery() throws Exception {
    HiveConf conf = startQuery("test_all_table_constraints_query_cache");

    SQLAllTableConstraints constraints = new SQLAllTableConstraints();
    constraints.setPrimaryKeys(new ArrayList<>(List.of(
        new SQLPrimaryKey("db", "t", "id", 1, "pk", false, false, false))));
    IMetaStoreClient delegate = mock(IMetaStoreClient.class);
    when(delegate.getAllTableConstraints(any())).thenReturn(constraints);
    SessionHiveMetaStoreClient client = SessionHiveMetaStoreClient.newClient(conf, delegate);

    SQLAllTableConstraints first = client.getAllTableConstraints(new AllTableConstraintsRequest("db", "t", "hive"));
    // Hive.getTableConstraints keeps only the RELY constraints in the object it gets
    first.getPrimaryKeys().clear();
    SQLAllTableConstraints second = client.getAllTableConstraints(new AllTableConstraintsRequest("db", "t", "hive"));

    verify(delegate, times(1)).getAllTableConstraints(any());
    assertEquals(1, second.getPrimaryKeys().size());
  }

  @Test
  public void testTableCachedAcrossRequestMutation() throws Exception {
    HiveConf conf = startQuery("test_table_request_mutation");
    IMetaStoreClient delegate = tableDelegate();
    SessionHiveMetaStoreClient client = SessionHiveMetaStoreClient.newClient(conf, delegate);

    GetTableRequest first = hiveGetTableRequest();
    client.getTable(first);
    // the caller owns the request; changing it afterwards must not affect the cached entry
    first.setTblName("other");
    client.getTable(hiveGetTableRequest());

    verify(delegate, times(1)).getTable(any(GetTableRequest.class));
  }

  @Test
  public void testValidWriteIdListLookupSharesTableEntry() throws Exception {
    HiveConf conf = startQuery("test_valid_write_id_list_table_entry");
    // a valid txn list makes getValidWriteIdList look the table up to check whether it is transactional
    Hive.get().getConf().set(ValidTxnList.VALID_TXNS_KEY, "1:" + Long.MAX_VALUE + "::");
    IMetaStoreClient delegate = tableDelegate();
    SessionHiveMetaStoreClient client = SessionHiveMetaStoreClient.newClient(conf, delegate);

    client.getTable(hiveGetTableRequest());
    client.getPartitionColumnStatistics("hive", "db", "t", List.of("p=1"), List.of("c"), Constants.HIVE_ENGINE, null);

    verify(delegate, times(1)).getTable(any(GetTableRequest.class));
  }

  @Test
  public void testCheckAndDefaultConstraintsFetchedOncePerQuery() throws Exception {
    HiveConf conf = startQuery("test_check_default_constraints_query_cache");
    IMetaStoreClient delegate = mock(IMetaStoreClient.class);
    when(delegate.getCheckConstraints(any())).thenReturn(new ArrayList<>());
    when(delegate.getDefaultConstraints(any())).thenReturn(new ArrayList<>());
    SessionHiveMetaStoreClient client = SessionHiveMetaStoreClient.newClient(conf, delegate);

    for (int i = 0; i < 2; i++) {
      client.getCheckConstraints(new CheckConstraintsRequest("hive", "db", "t"));
      client.getDefaultConstraints(new DefaultConstraintsRequest("hive", "db", "t"));
    }

    verify(delegate, times(1)).getCheckConstraints(any());
    verify(delegate, times(1)).getDefaultConstraints(any());
  }

  @Test
  public void testPartitionWithAuthInfoFetchedOncePerQuery() throws Exception {
    HiveConf conf = startQuery("test_partition_with_auth_info_query_cache");
    org.apache.hadoop.hive.metastore.api.Partition partition = new org.apache.hadoop.hive.metastore.api.Partition();
    partition.setValues(new ArrayList<>(List.of("x")));
    partition.setParameters(new HashMap<>());
    IMetaStoreClient delegate = mock(IMetaStoreClient.class);
    when(delegate.getPartitionWithAuthInfo(any(), any(), any(), any(), any(), any())).thenReturn(partition);
    SessionHiveMetaStoreClient client = SessionHiveMetaStoreClient.newClient(conf, delegate);

    org.apache.hadoop.hive.metastore.api.Partition first =
        client.getPartitionWithAuthInfo("hive", "db", "t", List.of("x"), "user", List.of("group"));
    // the partition handed out is the caller's to change
    first.getParameters().put("k", "v");
    org.apache.hadoop.hive.metastore.api.Partition second =
        client.getPartitionWithAuthInfo("hive", "db", "t", List.of("x"), "user", List.of("group"));

    verify(delegate, times(1)).getPartitionWithAuthInfo(any(), any(), any(), any(), any(), any());
    assertEquals(0, second.getParameters().size());
  }

  @Test
  public void testMissingTableLookedUpOncePerQuery() throws Exception {
    HiveConf conf = startQuery("test_missing_table_query_cache");
    IMetaStoreClient delegate = mock(IMetaStoreClient.class);
    when(delegate.getTable(any(GetTableRequest.class))).thenThrow(new NoSuchObjectException("db.t not found"));
    SessionHiveMetaStoreClient client = SessionHiveMetaStoreClient.newClient(conf, delegate);

    for (int i = 0; i < 2; i++) {
      try {
        client.getTable(hiveGetTableRequest());
        fail("table should not be found");
      } catch (NoSuchObjectException e) {
        assertEquals("db.t not found", e.getMessage());
      }
    }
    verify(delegate, times(1)).getTable(any(GetTableRequest.class));

    // a table created in the meantime is looked up again
    Table created = new Table();
    created.setCatName("hive");
    created.setDbName("db");
    created.setTableName("t");
    client.createTable(new CreateTableRequest(created));
    try {
      client.getTable(hiveGetTableRequest());
    } catch (NoSuchObjectException e) {
      // the mock still has no such table
    }
    verify(delegate, times(2)).getTable(any(GetTableRequest.class));
  }

  @Test
  public void testLocalCacheWatermarkReusesQueryLevelTable() throws Exception {
    HiveConf conf = startQuery("test_local_cache_watermark_table");
    Hive.get().getConf().set(ValidTxnList.VALID_TXNS_KEY, "1:" + Long.MAX_VALUE + "::");
    HiveMetaStoreClientWithLocalCache.init(conf);
    IMetaStoreClient delegate = tableDelegate();
    when(delegate.getTableColumnStatistics(any(), any(), any(), any(), any(), any())).thenReturn(new ArrayList<>());
    // the production layering: query-level cache over the HS2-level cache over the Thrift client
    SessionHiveMetaStoreClient client =
        SessionHiveMetaStoreClient.newClient(conf, HiveMetaStoreClientWithLocalCache.newClient(conf, delegate));

    client.getTable(hiveGetTableRequest());
    // columns without statistics are fetched again on every call; the table behind them is not
    for (int i = 0; i < 2; i++) {
      client.getTableColumnStatistics("hive", "db", "t", List.of("c"), Constants.HIVE_ENGINE, null);
    }

    verify(delegate, times(2)).getTableColumnStatistics(any(), any(), any(), any(), any(), any());
    verify(delegate, times(1)).getTable(any(GetTableRequest.class));
  }
}
