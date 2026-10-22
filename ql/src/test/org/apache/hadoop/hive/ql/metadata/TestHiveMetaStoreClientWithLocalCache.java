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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.Test;

/**
 * The cache is keyed by a {@link org.apache.hadoop.hive.ql.metadata.client.MetaStoreClientCacheUtils.TableWatermark},
 * so it can only hold a response for a transactional table. These tests cover what that means for the table id the
 * watermark needs: a non-transactional table must not be charged a metastore call to fetch it.
 */
public class TestHiveMetaStoreClientWithLocalCache {

  private static final List<String> COLS = List.of("c");
  private static final List<String> PARTS = List.of("p=1");

  /** A client whose cache is enabled: the cache is used only inside a query, and only once initialized. */
  private static IMetaStoreClient enabledClient(IMetaStoreClient delegate, String queryId) throws Exception {
    HiveConf conf = new HiveConf();
    conf.setVar(HiveConf.ConfVars.HIVE_QUERY_ID, queryId);
    SessionState ss = new SessionState(conf);
    SessionState.setCurrentSessionState(ss);
    QueryState queryState = new QueryState.Builder().withHiveConf(conf).withGenerateNewQueryId(false).build();
    queryState.createHMSCache();
    ss.addQueryState(queryId, queryState);
    // the cache looks its query up through Hive.get(), which keeps the conf of the first caller on this thread
    Hive.get(conf);
    HiveMetaStoreClientWithLocalCache.init(conf);
    return HiveMetaStoreClientWithLocalCache.newClient(conf, delegate);
  }

  private static IMetaStoreClient delegateReturning() throws Exception {
    IMetaStoreClient delegate = mock(IMetaStoreClient.class);
    Table table = new Table();
    table.setId(42);
    when(delegate.getTable(any(GetTableRequest.class))).thenReturn(table);
    when(delegate.getAggrColStatsFor(any(), any(), any(), anyList(), anyList(), any(), any()))
        .thenReturn(new AggrStats());
    return delegate;
  }

  @Test
  public void testNonTransactionalTableIsNotChargedAWatermarkTableFetch() throws Exception {
    IMetaStoreClient delegate = delegateReturning();
    IMetaStoreClient client = enabledClient(delegate, "test_watermark_non_transactional");

    client.getAggrColStatsFor("hive", "db", "nonacid", COLS, PARTS, "hive", null);

    // no write-id list means the watermark cannot be valid, so fetching the table id it needs buys nothing
    verify(delegate, never()).getTable(any(GetTableRequest.class));
    verify(delegate, times(1)).getAggrColStatsFor("hive", "db", "nonacid", COLS, PARTS, "hive", null);
  }

  /**
   * The positive control: it fails if the fixture leaves the cache disabled, which would make the test above pass
   * for the wrong reason.
   */
  @Test
  public void testTransactionalTableStillCaches() throws Exception {
    IMetaStoreClient delegate = delegateReturning();
    IMetaStoreClient client = enabledClient(delegate, "test_watermark_transactional");
    String writeIds = "db.acid:1:9223372036854775807::";

    client.getAggrColStatsFor("hive", "db", "acid", COLS, PARTS, "hive", writeIds);
    client.getAggrColStatsFor("hive", "db", "acid", COLS, PARTS, "hive", writeIds);

    verify(delegate, times(1)).getAggrColStatsFor("hive", "db", "acid", COLS, PARTS, "hive", writeIds);
    // the watermark needs the table id on each call, and that lookup carries no write-id list of its own, so it is
    // not served from the cache either -- unchanged by the skip, which only applies when there is no write-id list
    verify(delegate, times(2)).getTable(any(GetTableRequest.class));
  }
}
