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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.ReplChangeManager;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponseElement;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.ShowCompactRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.ql.txn.compactor.Cleaner;
import org.apache.hadoop.hive.ql.txn.compactor.CleanupRequest;
import org.apache.hadoop.hive.ql.txn.compactor.CompactorUtil;
import org.apache.hadoop.hive.ql.txn.compactor.FSRemover;
import org.apache.hadoop.hive.ql.txn.compactor.MetadataCache;
import org.apache.hadoop.hive.ql.txn.compactor.TestCleaner;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.hive.conf.Constants.COMPACTOR_CLEANER_THREAD_NAME_FORMAT;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_COMPACTOR_DELAYED_CLEANUP_ENABLED;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;

public class TestHandler extends TestCleaner {

  @Test
  public void testCompactionHandlerAndFsRemover() throws Exception {
    Table t = newTable("default", "handler_test", true);
    Partition p = newPartition(t, "today");
    addBaseFile(t, p, 20L, 20);
    addDeltaFile(t, p, 21L, 22L, 2);
    addDeltaFile(t, p, 23L, 24L, 2);
    addBaseFile(t, p, 25L, 25);

    burnThroughTransactions(t.getDbName(), t.getTableName(), 25);

    CompactionRequest rqst = new CompactionRequest(t.getDbName(), t.getTableName(), CompactionType.MAJOR);
    rqst.setPartitionname("ds=today");
    compactInTxn(rqst);
    MetadataCache metadataCache = new MetadataCache();
    FSRemover mockedFSRemover = Mockito.spy(new FSRemover(conf, ReplChangeManager.getInstance(conf), metadataCache));
    ExecutorService cleanerExecutor = CompactorUtil.createExecutorWithThreadFactory(
            conf.getIntVar(HiveConf.ConfVars.HIVE_COMPACTOR_CLEANER_THREADS_NUM),
            COMPACTOR_CLEANER_THREAD_NAME_FORMAT);

    RequestHandler mockedRequestHandler = Mockito.spy(new CompactionCleanHandler(conf, txnHandler, metadataCache,
            false, mockedFSRemover, cleanerExecutor));
    AtomicBoolean stop = new AtomicBoolean(true);
    Cleaner cleaner = new Cleaner();
    cleaner.setConf(conf);
    cleaner.setRequestHandlers(Arrays.asList(mockedRequestHandler));
    cleaner.init(stop);
    cleaner.run();

    Mockito.verify(mockedFSRemover, Mockito.times(1)).clean(any(CleanupRequest.class));
    Mockito.verify(mockedRequestHandler, Mockito.times(1)).process();
    Mockito.verify(mockedRequestHandler, Mockito.times(1)).fetchCleanTasks();
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
    MetadataCache mockedMetadataCache = Mockito.spy(MetadataCache.class);
    mockedMetadataCache.initializeCache(true);
    TxnStore mockedTxnHandler = spy(txnHandler);
    FSRemover fsRemover = new FSRemover(conf, ReplChangeManager.getInstance(conf), mockedMetadataCache);
    ExecutorService cleanerExecutor = CompactorUtil.createExecutorWithThreadFactory(
            conf.getIntVar(HiveConf.ConfVars.HIVE_COMPACTOR_CLEANER_THREADS_NUM),
            COMPACTOR_CLEANER_THREAD_NAME_FORMAT);
    RequestHandler mockedRequestHandler = Mockito.spy(new CompactionCleanHandler(conf, mockedTxnHandler, mockedMetadataCache,
            false, fsRemover, cleanerExecutor));
    Cleaner cleaner = new Cleaner();
    cleaner.setConf(conf);
    cleaner.setRequestHandlers(Arrays.asList(mockedRequestHandler));
    cleaner.init(new AtomicBoolean(true));
    cleaner.run();

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    Assert.assertEquals(1, compacts.size());
    Mockito.verify(mockedMetadataCache, times(3)).computeIfAbsent(any(), any());
    Mockito.verify(mockedRequestHandler, times(1)).resolveTable(any(), any());
  }
}
