/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.txn.compactor;

import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponseElement;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hive.streaming.HiveStreamingConnection;
import org.apache.hive.streaming.StrictDelimitedInputWriter;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@SuppressWarnings("CallToThreadRun")
public class TestCompactionPools extends TestCompactorBase {

  private static final String DB_NAME = "default";
  private static final String TABLE_NAME = "compaction_test";

  public void setup() throws Exception {
    super.setup();

    executeStatementOnDriver("drop table if exists " + TABLE_NAME, driver);
    executeStatementOnDriver("CREATE TABLE " + TABLE_NAME + "(a INT, b STRING) " +
        " CLUSTERED BY(a) INTO 4 BUCKETS" + //currently ACID requires table to be bucketed
        " STORED AS ORC TBLPROPERTIES ('transactional'='true')", driver);

    StrictDelimitedInputWriter writer = StrictDelimitedInputWriter.newBuilder()
        .withFieldDelimiter(',')
        .build();
    HiveStreamingConnection connection = HiveStreamingConnection.newBuilder()
        .withDatabase(DB_NAME)
        .withTable(TABLE_NAME)
        .withAgentInfo("UT_" + Thread.currentThread().getName())
        .withHiveConf(conf)
        .withRecordWriter(writer)
        .connect();
    connection.beginTransaction();
    connection.write("55, 'London'".getBytes());
    connection.commitTransaction();
    connection.beginTransaction();
    connection.write("56, 'Paris'".getBytes());
    connection.commitTransaction();
    connection.close();

    executeStatementOnDriver("INSERT INTO TABLE " + TABLE_NAME + " values(57, 'Budapest')", driver);
    executeStatementOnDriver("INSERT INTO TABLE " + TABLE_NAME + " values(58, 'Milano')", driver);
    execSelectAndDumpData("select * from " + TABLE_NAME, driver, "Dumping data for " +
        TABLE_NAME + " after load:");
  }

  @Test
  public void testWorkerPicksUpFromRightPool() throws Exception {
    TxnStore txnHandler = TxnUtils.getTxnStore(conf);

    //Do a major compaction
    CompactionRequest rqst = new CompactionRequest(DB_NAME, TABLE_NAME, CompactionType.MAJOR);
    rqst.setPoolName("pool1");
    txnHandler.compact(rqst);

    Worker worker =new Worker();
    worker.setConf(conf);
    worker.setPoolName("pool2");
    worker.init(new AtomicBoolean(true));

    worker.run();

    ShowCompactResponse resp = msClient.showCompactions();
    Assert.assertEquals(1, resp.getCompacts().size());
    ShowCompactResponseElement e = resp.getCompacts().get(0);
    Assert.assertEquals("initiated", e.getState());
    Assert.assertEquals("pool1", e.getPoolName());

    worker.setPoolName("pool1");
    worker.run();

    resp = msClient.showCompactions();
    Assert.assertEquals(1, resp.getCompacts().size());
    e = resp.getCompacts().get(0);
    Assert.assertEquals("ready for cleaning", e.getState());
    Assert.assertEquals("pool1", e.getPoolName());
  }

  @Test
  public void testDefaultWorkerPicksUpOnlyNonLabeled() throws Exception {
    TxnStore txnHandler = TxnUtils.getTxnStore(conf);

    //Do a major compaction
    CompactionRequest rqst = new CompactionRequest(DB_NAME, TABLE_NAME, CompactionType.MAJOR);
    rqst.setPoolName("pool1");
    txnHandler.compact(rqst);

    rqst = new CompactionRequest(DB_NAME, TABLE_NAME + "2", CompactionType.MINOR);
    txnHandler.compact(rqst);

    Worker worker = new Worker();
    worker.setConf(conf);
    worker.init(new AtomicBoolean(true));

    worker.run();

    ShowCompactResponse resp = msClient.showCompactions();
    Assert.assertEquals(2, resp.getCompacts().size());
    for(ShowCompactResponseElement e : resp.getCompacts()) {
      if (e.getPoolName().equals("default")) {
        Assert.assertEquals("failed", e.getState());
      } else {
        Assert.assertEquals("initiated", e.getState());
      }
    }
  }

  @Test
  public void testDefaultWorkerPicksUpIfTimedOut() throws Exception {
    MetastoreConf.setTimeVar(conf, MetastoreConf.ConfVars.COMPACTOR_WORKER_POOL_TIMEOUT, 2, TimeUnit.SECONDS);

    TxnStore txnHandler = TxnUtils.getTxnStore(conf);

    //Do a major compaction
    CompactionRequest rqst = new CompactionRequest(DB_NAME, TABLE_NAME, CompactionType.MAJOR);
    rqst.setPoolName("pool1");
    txnHandler.compact(rqst);

    Worker worker = new Worker();
    worker.setConf(conf);
    worker.init(new AtomicBoolean(true));

    worker.run();

    ShowCompactResponse resp = msClient.showCompactions();
    Assert.assertEquals(1, resp.getCompacts().size());
    ShowCompactResponseElement e = resp.getCompacts().get(0);
    Assert.assertEquals("initiated", e.getState());

    Thread.sleep(3000);
    worker.run();

    resp = msClient.showCompactions();
    Assert.assertEquals(1, resp.getCompacts().size());
    e = resp.getCompacts().get(0);
    Assert.assertEquals("ready for cleaning", e.getState());
  }

}
