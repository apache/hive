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

package org.apache.hadoop.hive.metastore.handler;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DatabaseType;
import org.apache.hadoop.hive.metastore.api.GetTablesRequest;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;

/**
 * Unit tests for HIVE-29690: verifies that when both tblNames and tablesPattern are set,
 * GetTableHandler takes the batched code path and correctly passes the pattern to each
 * batch call, rather than the non-batched (pattern-only) path that ignores the name list.
 */
public class TestGetTableHandlerBatching {

  /**
   * Verifies the batched path for Case 3 (tableNames != null, pattern != null).
   *
   * Strategy: set BATCH_RETRIEVE_MAX=1 so that 2 table names require 2 separate
   * calls to RawStore.getTableObjectsByName.  Then use Mockito.verify to assert
   * that the method was called exactly twice, each time with:
   *   - a non-null sublist of size 1  (proves the name list was respected, not ignored)
   *   - the pattern argument          (proves the pattern was forwarded to every batch)
   *
   * If the code took the non-batched (pattern-only) path instead, the method would be
   * called exactly once with tblNames=null, and verify(times(2)) would fail.
   */
  @Test
  public void testTableNamesAndPatternTakesBatchedPath() throws Exception {
    // --- configuration ---
    Configuration conf = MetastoreConf.newMetastoreConf();
    // One name per batch so that 2 names → 2 calls.
    MetastoreConf.setLongVar(conf, MetastoreConf.ConfVars.BATCH_RETRIEVE_MAX, 1);
    // Disable HIVE_IN_TEST so the capability check inside getTableObjectsInternal is skipped.
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.HIVE_IN_TEST, false);

    // --- mocks ---
    RawStore ms = Mockito.mock(RawStore.class);
    Mockito.when(ms.getTableObjectsByName(
            Mockito.anyString(), Mockito.anyString(),
            Mockito.anyList(), Mockito.any(), Mockito.anyString()))
        .thenReturn(Collections.emptyList());

    Database localDb = new Database("testdb", null, null, null);
    localDb.setType(DatabaseType.NATIVE);

    IHMSHandler handler = Mockito.mock(IHMSHandler.class);
    Mockito.when(handler.getConf()).thenReturn(conf);
    Mockito.when(handler.getMS()).thenReturn(ms);
    Mockito.when(handler.getMetadataTransformer()).thenReturn(null);
    Mockito.when(handler.getMetaFilterHook()).thenReturn(null);
    Mockito.when(handler.get_database_core(Mockito.anyString(), Mockito.anyString()))
        .thenReturn(localDb);

    // --- request: two names + a pattern ---
    GetTablesRequest req = new GetTablesRequest("testdb");
    req.setCatName("hive");
    req.setTblNames(Arrays.asList("test_table_1", "test_table_2"));
    req.setTablesPattern("test_*");

    GetTableHandler.getTables(null, handler, req, null);

    // --- verify ---
    // With BATCH_RETRIEVE_MAX=1, the while-loop must fire once per name.
    // Each call must receive a non-null sublist (size 1) and the pattern.
    // A non-batched (pattern-only) call would use tblNames=null and fire only once —
    // both of which would cause the verification below to fail.
    Mockito.verify(ms, Mockito.times(2))
        .getTableObjectsByName(
            Mockito.anyString(),                                   // catName
            Mockito.anyString(),                                   // dbName
            Mockito.argThat(list -> list != null && list.size() == 1), // one name per batch
            Mockito.any(),                                         // projectionSpec
            Mockito.eq("test_*"));                                 // pattern forwarded to every batch
  }
}
