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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AllTableConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.SQLAllTableConstraints;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.Test;

public class TestSessionHiveMetaStoreClientQueryCache {

  @Test
  public void testAllTableConstraintsFetchedOncePerQuery() throws Exception {
    HiveConf conf = new HiveConf();
    String queryId = "test_all_table_constraints_query_cache";
    conf.setVar(HiveConf.ConfVars.HIVE_QUERY_ID, queryId);
    SessionState ss = new SessionState(conf);
    SessionState.setCurrentSessionState(ss);
    QueryState queryState = new QueryState.Builder().withHiveConf(conf).withGenerateNewQueryId(false).build();
    queryState.createHMSCache();
    ss.addQueryState(queryId, queryState);
    Hive.get(conf);

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
}
