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

package org.apache.hadoop.hive.metastore;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionsRequest;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.PartitionBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Ignore("HIVE-28659")
@Category(MetastoreUnitTest.class)
public class TestMetaStoreDeadlock {
  private Configuration conf;
  private static int POOL_SIZE = 3;
  private static CountDownLatch LATCH1 = new CountDownLatch(POOL_SIZE + 1);
  private static CountDownLatch LATCH2 = new CountDownLatch(POOL_SIZE);

  @Before
  public void setUp() throws Exception {
    conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setLongVar(conf, MetastoreConf.ConfVars.CONNECTION_POOLING_MAX_CONNECTIONS, POOL_SIZE);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_METADATA_TRANSFORMER_CLASS, SleepOnGetPartitions.class.getName());
    MetastoreConf.setTimeVar(conf, MetastoreConf.ConfVars.CLIENT_SOCKET_TIMEOUT, 1, TimeUnit.HOURS);
    conf.setLong("hikaricp.connectionTimeout", 3600000);
    MetaStoreTestUtils.startMetaStoreWithRetry(HadoopThriftAuthBridge.getBridge(), conf);
  }

  @Test(timeout = 60000)
  public void testLockContention() throws Exception {
    String dbName = "_test_deadlock_";
    String tableName1 = "tbl1";
    try (HiveMetaStoreClient msc = new HiveMetaStoreClient(conf)) {
      new DatabaseBuilder().setName(dbName).create(msc, conf);
      new TableBuilder().setDbName(dbName).setTableName(tableName1).addCol("a", "string").addPartCol("dt", "string")
          .create(msc, conf);
      Table table1 = msc.getTable(dbName, tableName1);
      new PartitionBuilder().inTable(table1).addValue("2024-09-29").addToTable(msc, conf);
    }
    GetPartitionsByNamesRequest request = new GetPartitionsByNamesRequest(dbName, tableName1);
    request.setNames(Arrays.asList("dt=2024-09-28"));
    request.setProcessorCapabilities(Arrays.asList("HIVEFULLACIDWRITE", "HIVEFULLACIDREAD", "HIVEMANAGEDINSERTWRITE"));
    Thread[] holdConnThreads = new Thread[POOL_SIZE];
    for (int i = 0; i < POOL_SIZE; i++) {
      holdConnThreads[i] = new Thread(() -> {
        try (HiveMetaStoreClient client = new HiveMetaStoreClient(conf)) {
          LATCH1.countDown();
          client.getPartitionsByNames(request);
        } catch (TException e) {
          throw new RuntimeException(e);
        }
      });
      holdConnThreads[i].start();
    }
    LATCH2.await();
    Thread holdMS = new Thread(() -> {
      try (HiveMetaStoreClient client = new HiveMetaStoreClient(conf)) {
        client.getPartitionsRequest(new PartitionsRequest(dbName, tableName1)).getPartitions();
      } catch (TException e) {
        throw new RuntimeException(e);
      }
    });
    holdMS.start();
    Thread.sleep(5000);
    LATCH1.countDown();
    Assert.assertEquals(0, LATCH1.getCount());
    // this thread will be hanging on as there is no available connection until connection timeout
    holdMS.join();
  }

  public static class SleepOnGetPartitions extends MetastoreDefaultTransformer {

    public SleepOnGetPartitions(IHMSHandler handler) throws HiveMetaException {
      super(handler);
    }
    @Override
    public List<Partition> transformPartitions(List<Partition> objects,
        Table table, List<String> processorCapabilities, String processorId) throws MetaException {
      try {
        LATCH2.countDown();
        LATCH1.await();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      return super.transformPartitions(objects, table, processorCapabilities, processorId);
    }
  }
}
