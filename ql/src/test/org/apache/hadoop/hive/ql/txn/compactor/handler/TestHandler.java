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
package org.apache.hadoop.hive.ql.txn.compactor.handler;

import org.apache.hadoop.hive.metastore.api.ShowCompactResponseElement;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.ShowCompactRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.txn.CompactionInfo;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.ql.txn.compactor.CacheContainer;
import org.apache.hadoop.hive.ql.txn.compactor.Cleaner;
import org.apache.hadoop.hive.ql.txn.compactor.CleaningRequest;
import org.apache.hadoop.hive.ql.txn.compactor.TestCleaner;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_COMPACTOR_DELAYED_CLEANUP_ENABLED;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;

public class TestHandler extends TestCleaner {

  @Test
  public void testCompactionHandlerForSuccessfulCompaction() throws Exception {
    Table t = newTable("default", "handler_success_table", true);
    Partition p = newPartition(t, "today");
    addBaseFile(t, p, 20L, 20);
    addDeltaFile(t, p, 21L, 22L, 2);
    addDeltaFile(t, p, 23L, 24L, 2);
    addBaseFile(t, p, 25L, 25);

    burnThroughTransactions(t.getDbName(), t.getTableName(), 25);

    CompactionRequest rqst = new CompactionRequest(t.getDbName(), t.getTableName(), CompactionType.MAJOR);
    rqst.setPartitionname("ds=today");
    compactInTxn(rqst);
    CacheContainer cacheContainer = Mockito.spy(CacheContainer.class);

    CleaningRequestHandler cleaningRequestHandler = new CompactionCleaningRequestHandler(conf, txnHandler, cacheContainer, false);

    // Fetch the compaction request using the cleaningRequestHandler
    List<CleaningRequest> cleaningRequests = cleaningRequestHandler.findReadyToClean();
    Assert.assertEquals(1, cleaningRequests.size());
    CleaningRequest cr = cleaningRequests.get(0);
    Assert.assertEquals(t.getDbName(), cr.getDbName());
    Assert.assertEquals(t.getTableName(), cr.getTableName());
    Assert.assertEquals("ds=today", cr.getPartitionName());
    Assert.assertEquals(CleaningRequest.RequestType.COMPACTION, cr.getType());

    // Check whether appropriate cleaningRequestHandler utility methods are called exactly once in a successful compaction scenario.
    CleaningRequestHandler mockedCleaningRequestHandler = Mockito.spy(cleaningRequestHandler);
    AtomicBoolean stop = new AtomicBoolean(true);
    Cleaner cleaner = new Cleaner();
    cleaner.setConf(conf);
    cleaner.setCleaningRequestHandlers(Arrays.asList(mockedCleaningRequestHandler));
    cleaner.init(stop);
    cleaner.run();

    Mockito.verify(mockedCleaningRequestHandler, Mockito.times(1)).findReadyToClean();
    Mockito.verify(mockedCleaningRequestHandler, Mockito.times(1)).beforeExecutingCleaningRequest(any(CleaningRequest.class));
    Mockito.verify(mockedCleaningRequestHandler, Mockito.times(1)).afterExecutingCleaningRequest(any(CleaningRequest.class), any(List.class));
    Mockito.verify(mockedCleaningRequestHandler, Mockito.times(0)).failureExecutingCleaningRequest(any(CleaningRequest.class), any(Exception.class));
  }

  @Test
  public void testCompactionHandlerForFailureCompaction() throws Exception {
    Table t = newTable("default", "handler_failure_table", true);
    Partition p = newPartition(t, "today");
    addBaseFile(t, p, 20L, 20);
    addDeltaFile(t, p, 21L, 22L, 2);
    addDeltaFile(t, p, 23L, 24L, 2);
    addBaseFile(t, p, 25L, 25);

    burnThroughTransactions(t.getDbName(), t.getTableName(), 25);

    CompactionRequest rqst = new CompactionRequest(t.getDbName(), t.getTableName(), CompactionType.MAJOR);
    rqst.setPartitionname("ds=today");
    compactInTxn(rqst);

    // Check whether appropriate handler utility methods are called exactly once in a failure compaction scenario.
    TxnStore mockedTxnHandler = Mockito.spy(txnHandler);
    doThrow(new RuntimeException()).when(mockedTxnHandler).markCleanerStart(any());
    CacheContainer cacheContainer = Mockito.spy(CacheContainer.class);
    CleaningRequestHandler mockedCleaningRequestHandler = Mockito.spy(new CompactionCleaningRequestHandler(conf, mockedTxnHandler, cacheContainer, false));
    AtomicBoolean stop = new AtomicBoolean(true);
    Cleaner cleaner = new Cleaner();
    cleaner.setConf(conf);
    cleaner.setCleaningRequestHandlers(Arrays.asList(mockedCleaningRequestHandler));
    cleaner.init(stop);
    cleaner.run();

    Mockito.verify(mockedCleaningRequestHandler, Mockito.times(1)).findReadyToClean();
    Mockito.verify(mockedCleaningRequestHandler, Mockito.times(1)).beforeExecutingCleaningRequest(any(CleaningRequest.class));
    Mockito.verify(mockedCleaningRequestHandler, Mockito.times(0)).afterExecutingCleaningRequest(any(CleaningRequest.class), any(List.class));
    Mockito.verify(mockedCleaningRequestHandler, Mockito.times(1)).failureExecutingCleaningRequest(any(CleaningRequest.class), any(Exception.class));
  }

  @Test
  public void testMetaCache() throws Exception {
    conf.setBoolVar(HIVE_COMPACTOR_DELAYED_CLEANUP_ENABLED, false);

    Table t = newTable("default", "retry_test", false);

    addBaseFile(t, null, 20L, 20);
    addDeltaFile(t, null, 21L, 22L, 2);
    addDeltaFile(t, null, 23L, 24L, 2);
    burnThroughTransactions("default", "retry_test", 25);

    CompactionRequest rqst = new CompactionRequest("default", "retry_test", CompactionType.MAJOR);
    long compactTxn = compactInTxn(rqst);
    addBaseFile(t, null, 25L, 25, compactTxn);

    //Prevent cleaner from marking the compaction as cleaned
    CacheContainer mockedCacheContainer = Mockito.spy(CacheContainer.class);
    mockedCacheContainer.initializeCache(true);
    TxnStore mockedHandler = spy(txnHandler);
    doThrow(new RuntimeException()).when(mockedHandler).markCleaned(nullable(CompactionInfo.class));
    CleaningRequestHandler cleaningRequestHandler = Mockito.spy(new CompactionCleaningRequestHandler(conf, mockedHandler, mockedCacheContainer, false));
    Cleaner cleaner = new Cleaner();
    cleaner.setConf(conf);
    cleaner.setCleaningRequestHandlers(Arrays.asList(cleaningRequestHandler));
    cleaner.init(new AtomicBoolean(true));
    cleaner.run();
    cleaner.run();

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    Assert.assertEquals(1, compacts.size());
    Mockito.verify(mockedCacheContainer, times(3)).computeIfAbsent(Mockito.any(),Mockito.any());
    Mockito.verify(cleaningRequestHandler, times(1)).resolveTable(Mockito.any(), Mockito.any());
  }
}
