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

package org.apache.hadoop.hive.ql.exec;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConfForTest;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.PartitionIterable;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.*;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.*;

public class TestGetPartitionsWithSpecsInBatches {
  private final String catName = "hive";
  private final String dbName = "default";
  private final String tableName = "test_partition_batch";
  private static HiveConf hiveConf;
  private static HiveMetaStoreClient msc;
  private static Hive hive;
  private Table table;
  private static final int NUM_PARTITIONS = 30;

  @BeforeClass
  public static void setupClass() throws HiveException {
    hiveConf = new HiveConfForTest(TestGetPartitionsWithSpecsInBatches.class);
    hiveConf.set("hive.security.authorization.manager","org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider");
    SessionState ss = SessionState.start(hiveConf);
    hive = ss.getHiveDb();
    try {
      msc = new HiveMetaStoreClient(hiveConf);
    } catch (MetaException e) {
      throw new HiveException(e);
    }
  }

  @Before
  public void before() throws Exception {
    PartitionUtil.createPartitionedTable(msc, catName, dbName, tableName);
    table = msc.getTable(catName, dbName, tableName);
    PartitionUtil.addPartitions(msc, dbName, tableName, table.getSd().getLocation(), hiveConf, NUM_PARTITIONS);
  }

  @After
  public void after() {
    PartitionUtil.cleanUpTableQuietly(msc, catName, dbName, tableName);
  }

  @Test
  public void testNumberOfGetPartitionCalls() throws Exception {
    HiveMetaStoreClient spyMSC = spy(msc);
    hive.setMSC(spyMSC);
    // test with a batch size of 10 and decaying factor of 2
    hive.getAllPartitionsWithSpecsInBatches(hive.getTable(dbName, tableName),10, 2, 0, new GetPartitionsRequest(dbName, tableName, new GetProjectionsSpec(), new GetPartitionsFilterSpec()));
    ArgumentCaptor<GetPartitionsRequest> req = ArgumentCaptor.forClass(GetPartitionsRequest.class);
    // there should be 3 calls to get partitions
    verify(spyMSC, times(3)).getPartitionsWithSpecs(req.capture());
    Assert.assertEquals(10, req.getValue().getFilterSpec().getFiltersSize());
  }

  @Test
  public void testUnevenNumberOfGetPartitionCalls() throws Exception {
    HiveMetaStoreClient spyMSC = spy(msc);
    hive.setMSC(spyMSC);

    List<GetPartitionsRequest> capturedRequests = new ArrayList<>();
    doAnswer((Answer<GetPartitionsResponse>) invocation -> {
      GetPartitionsRequest request = invocation.getArgument(0);
      capturedRequests.add(new GetPartitionsRequest(request));
      return (GetPartitionsResponse) invocation.callRealMethod();
    }).when(spyMSC).getPartitionsWithSpecs(any(GetPartitionsRequest.class));

    // there should be 2 calls to get partitions with batch sizes of 19, 11
    hive.getAllPartitionsWithSpecsInBatches(hive.getTable(dbName, tableName),19, 2, 0, new GetPartitionsRequest(dbName, tableName, new GetProjectionsSpec(), new GetPartitionsFilterSpec()));
    ArgumentCaptor<GetPartitionsRequest> req = ArgumentCaptor.forClass(GetPartitionsRequest.class);
    // there should be 2 calls to get partitions
    verify(spyMSC, times(2)).getPartitionsWithSpecs(req.capture());
    // confirm the batch sizes were 19, 11 in the two calls to get partitions
    Assert.assertEquals(19, capturedRequests.get(0).getFilterSpec().getFiltersSize());
    Assert.assertEquals(11, capturedRequests.get(1).getFilterSpec().getFiltersSize());
  }

  @Test
  public void testSmallNumberOfPartitions() throws Exception {
    HiveMetaStoreClient spyMSC = spy(msc);
    hive.setMSC(spyMSC);
    hive.getAllPartitionsWithSpecsInBatches(hive.getTable(dbName, tableName),100, 2, 0, new GetPartitionsRequest(dbName, tableName, new GetProjectionsSpec(), new GetPartitionsFilterSpec()));
    ArgumentCaptor<GetPartitionsRequest> req = ArgumentCaptor.forClass(GetPartitionsRequest.class);
    // there should be 1 call to get partitions
    verify(spyMSC, times(1)).getPartitionsWithSpecs(req.capture());
    Assert.assertEquals(30, req.getValue().getFilterSpec().getFiltersSize());
  }

  @Test
  public void testRetriesExhaustedBatchSize() throws Exception {
    HiveMetaStoreClient spyMSC = spy(msc);
    hive.setMSC(spyMSC);

    List<GetPartitionsRequest> capturedRequests = new ArrayList<>();
    doAnswer((Answer<Void>) invocation -> {
      GetPartitionsRequest request = invocation.getArgument(0);
      capturedRequests.add(new GetPartitionsRequest(request));
      throw new MetaException("MetaException to test retries");
    }).when(spyMSC).getPartitionsWithSpecs(any(GetPartitionsRequest.class));

    try {
      hive.getAllPartitionsWithSpecsInBatches(hive.getTable(dbName, tableName), 30, 2, 0, new GetPartitionsRequest(dbName, tableName, new GetProjectionsSpec(), new GetPartitionsFilterSpec()));
    } catch (Exception ignored) {}
    ArgumentCaptor<GetPartitionsRequest> req = ArgumentCaptor.forClass(GetPartitionsRequest.class);
    // there should be 5 call to get partitions with batch sizes as 30, 15, 7, 3, 1
    verify(spyMSC, times(5)).getPartitionsWithSpecs(req.capture());

    Assert.assertEquals(5, capturedRequests.size());

    Assert.assertEquals(30, capturedRequests.get(0).getFilterSpec().getFiltersSize());
    Assert.assertEquals(15, capturedRequests.get(1).getFilterSpec().getFiltersSize());
    Assert.assertEquals(7, capturedRequests.get(2).getFilterSpec().getFiltersSize());
    Assert.assertEquals(3, capturedRequests.get(3).getFilterSpec().getFiltersSize());
    Assert.assertEquals(1, capturedRequests.get(4).getFilterSpec().getFiltersSize());
  }

  @Test
  public void testMaxRetriesReached() throws Exception {
    HiveMetaStoreClient spyMSC = spy(msc);
    hive.setMSC(spyMSC);

    List<GetPartitionsRequest> capturedRequests = new ArrayList<>();
    doAnswer((Answer<Void>) invocation -> {
      GetPartitionsRequest request = invocation.getArgument(0);
      capturedRequests.add(new GetPartitionsRequest(request));
      throw new MetaException("MetaException to test retries");
    }).when(spyMSC).getPartitionsWithSpecs(any(GetPartitionsRequest.class));

    try {
      hive.getAllPartitionsWithSpecsInBatches(hive.getTable(dbName, tableName), 30, 2, 2, new GetPartitionsRequest(dbName, tableName, new GetProjectionsSpec(), new GetPartitionsFilterSpec()));
    } catch (Exception ignored) {}
    ArgumentCaptor<GetPartitionsRequest> req = ArgumentCaptor.forClass(GetPartitionsRequest.class);
    // there should be 2 call to get partitions with batch sizes as 30, 15
    verify(spyMSC, times(2)).getPartitionsWithSpecs(req.capture());

    Assert.assertEquals(2, capturedRequests.size());

    Assert.assertEquals(30, capturedRequests.get(0).getFilterSpec().getFiltersSize());
    Assert.assertEquals(15, capturedRequests.get(1).getFilterSpec().getFiltersSize());
  }

  @Test
  public void testBatchingWhenException() throws Exception {
    HiveMetaStoreClient spyMSC = spy(msc);
    hive.setMSC(spyMSC);

    List<GetPartitionsRequest> capturedRequests = new ArrayList<>();
    AtomicInteger invocationCount = new AtomicInteger();
    // This will throw exception only the first time.
    doAnswer((Answer<GetPartitionsResponse>) invocation -> {
      invocationCount.getAndIncrement();
      GetPartitionsRequest request = invocation.getArgument(0);
      capturedRequests.add(new GetPartitionsRequest(request));

      if (invocationCount.get() == 1) {
        throw new MetaException();
      } else {
        return (GetPartitionsResponse) invocation.callRealMethod();
      }
    }).when(spyMSC).getPartitionsWithSpecs(any(GetPartitionsRequest.class));

    hive.getAllPartitionsWithSpecsInBatches(hive.getTable(dbName, tableName), 30, 2, 5, new GetPartitionsRequest(dbName, tableName, new GetProjectionsSpec(), new GetPartitionsFilterSpec()));
    ArgumentCaptor<GetPartitionsRequest> req = ArgumentCaptor.forClass(GetPartitionsRequest.class);
    // The first call with batch size of 30 will fail, the rest two call will be of size 15 each. Total 3 calls
    verify(spyMSC, times(3)).getPartitionsWithSpecs(req.capture());

    Assert.assertEquals(3, capturedRequests.size());

    Assert.assertEquals(30, capturedRequests.get(0).getFilterSpec().getFiltersSize());
    Assert.assertEquals(15, capturedRequests.get(1).getFilterSpec().getFiltersSize());
    Assert.assertEquals(15, capturedRequests.get(2).getFilterSpec().getFiltersSize());

    Set<String> partNames = new HashSet<>(capturedRequests.get(1).getFilterSpec().getFilters());
    partNames.addAll(capturedRequests.get(2).getFilterSpec().getFilters());
    assert(partNames.size() == 30);

    List<String> partitionNames = hive.getPartitionNames(table.getDbName(),table.getTableName(), (short) -1);
    assert(partitionNames.size() == 30);
    partitionNames.forEach(partNames::remove);
    assert(partitionNames.size() == 30);
    // In case any duplicate/incomplete list is given by hive.getAllPartitionsWithSpecsInBatches, the below assertion will fail
    assert(partNames.size() == 0);
  }

  @Test
  public void testBatchingWhenBatchSizeIsZero() throws MetaException {
    HiveMetaStoreClient spyMSC = spy(msc);
    hive.setMSC(spyMSC);
    int batchSize = 0;
    try {
      org.apache.hadoop.hive.ql.metadata.Table t = hive.getTable(dbName, tableName);
      new PartitionIterable(hive,
          new GetPartitionsRequest(t.getDbName(), t.getTableName(), new GetProjectionsSpec(), new GetPartitionsFilterSpec())
          ,batchSize);
    } catch (HiveException e) {
      Assert.assertTrue(e.getMessage().contains("Invalid batch size for partition iterable." +
          " Please use a batch size greater than 0"));
    }
    try {
      new org.apache.hadoop.hive.metastore.PartitionIterable(msc, table, batchSize).withProjectSpec(new GetPartitionsRequest(dbName, tableName, new GetProjectionsSpec(), new GetPartitionsFilterSpec()));
    } catch (MetastoreException e) {
      Assert.assertTrue(e.getMessage().contains("Invalid batch size for partition iterable." +
          " Please use a batch size greater than 0"));
    }
  }
}
