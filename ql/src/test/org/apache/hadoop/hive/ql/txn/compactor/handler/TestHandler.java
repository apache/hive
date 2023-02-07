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

import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.ql.txn.compactor.Cleaner;
import org.apache.hadoop.hive.ql.txn.compactor.CleaningRequest;
import org.apache.hadoop.hive.ql.txn.compactor.TestCleaner;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;

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

    Handler handler = new CompactionHandler(conf, txnHandler, false);

    // Fetch the compaction request using the handler
    List<CleaningRequest> cleaningRequests = handler.findReadyToClean();
    Assert.assertEquals(1, cleaningRequests.size());
    CleaningRequest cr = cleaningRequests.get(0);
    Assert.assertEquals(cr.getDbName(), t.getDbName());
    Assert.assertEquals(cr.getTableName(), t.getTableName());
    Assert.assertEquals(cr.getPartitionName(), "ds=today");
    Assert.assertEquals(cr.getType(), CleaningRequest.RequestType.COMPACTION);

    // Check whether appropriate handler utility methods are called exactly once in a successful compaction scenario.
    Handler mockedHandler = Mockito.spy(handler);
    AtomicBoolean stop = new AtomicBoolean(true);
    Cleaner cleaner = new Cleaner();
    cleaner.setConf(conf);
    cleaner.setHandlers(Arrays.asList(mockedHandler));
    cleaner.init(stop);
    cleaner.run();

    Mockito.verify(mockedHandler, Mockito.times(1)).findReadyToClean();
    Mockito.verify(mockedHandler, Mockito.times(1)).beforeExecutingCleaningRequest(any(CleaningRequest.class));
    Mockito.verify(mockedHandler, Mockito.times(1)).afterExecutingCleaningRequest(any(CleaningRequest.class), any(List.class));
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
    doThrow(new RuntimeException()).when(mockedTxnHandler).markCleaned(any());
    Handler mockedHandler = Mockito.spy(new CompactionHandler(conf, mockedTxnHandler, false));
    AtomicBoolean stop = new AtomicBoolean(true);
    Cleaner cleaner = new Cleaner();
    cleaner.setConf(conf);
    cleaner.setHandlers(Arrays.asList(mockedHandler));
    cleaner.init(stop);
    cleaner.run();

    Mockito.verify(mockedHandler, Mockito.times(1)).findReadyToClean();
    Mockito.verify(mockedHandler, Mockito.times(1)).beforeExecutingCleaningRequest(any(CleaningRequest.class));
    Mockito.verify(mockedHandler, Mockito.times(1)).failureExecutingCleaningRequest(any(CleaningRequest.class), any(Exception.class));
  }
}
