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
package org.apache.hadoop.hive.ql.parse;

import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.messaging.json.gzip.GzipJSONMessageEncoder;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hive.streaming.HiveStreamingConnection;
import org.apache.hive.streaming.StrictDelimitedInputWriter;
import org.apache.hive.streaming.StreamingConnection;

import org.junit.rules.TestName;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.hive.common.repl.ReplConst.SOURCE_OF_REPLICATION;

/**
 * TestReplicationOfHiveStreaming - test replication for streaming ingest on ACID tables.
 */
public class TestReplicationOfHiveStreaming {

  @Rule
  public final TestName testName = new TestName();

  protected static final Logger LOG = LoggerFactory.getLogger(TestReplicationOfHiveStreaming.class);
  private static  WarehouseInstance primary;
  private static WarehouseInstance replica;
  private static String primaryDbName;
  private static String replicatedDbName;

  @BeforeClass
  public static void classLevelSetup() throws Exception {
    Map<String, String> overrides = new HashMap<>();
    overrides.put(MetastoreConf.ConfVars.EVENT_MESSAGE_FACTORY.getHiveName(),
        GzipJSONMessageEncoder.class.getCanonicalName());

    internalBeforeClassSetup(overrides, TestReplicationOfHiveStreaming.class);
  }

  static void internalBeforeClassSetup(Map<String, String> overrides,
      Class clazz) throws Exception {

    HiveConf conf = new HiveConf(clazz);
    conf.set("dfs.client.use.datanode.hostname", "true");
    conf.set("hadoop.proxyuser." + Utils.getUGI().getShortUserName() + ".hosts", "*");
    MiniDFSCluster miniDFSCluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(2).format(true).build();
    Map<String, String> acidEnableConf = new HashMap<String, String>() {{
        put("fs.defaultFS", miniDFSCluster.getFileSystem().getUri().toString());
        put("hive.support.concurrency", "true");
        put("hive.txn.manager", "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager");
        put("hive.metastore.client.capability.check", "false");
        put("hive.repl.bootstrap.dump.open.txn.timeout", "1s");
        put("hive.strict.checks.bucketing", "false");
        put("hive.mapred.mode", "nonstrict");
        put("mapred.input.dir.recursive", "true");
        put("hive.metastore.disallow.incompatible.col.type.changes", "false");
        put("hive.strict.managed.tables", "true");
        put("hive.in.repl.test", "true");
      }};

    acidEnableConf.putAll(overrides);

    primary = new WarehouseInstance(LOG, miniDFSCluster, acidEnableConf);
    acidEnableConf.put(MetastoreConf.ConfVars.REPLDIR.getHiveName(), primary.repldDir);
    replica = new WarehouseInstance(LOG, miniDFSCluster, acidEnableConf);
  }

  @AfterClass
  public static void classLevelTearDown() throws IOException {
    primary.close();
    replica.close();
  }

  @Before
  public void setup() throws Throwable {
    primaryDbName = testName.getMethodName() + "_" + +System.currentTimeMillis();
    replicatedDbName = "replicated_" + primaryDbName;
    primary.run("create database " + primaryDbName + " WITH DBPROPERTIES ( '" +
            SOURCE_OF_REPLICATION + "' = '1,2,3')");
  }

  @After
  public void tearDown() throws Throwable {
    primary.run("drop database if exists " + primaryDbName + " cascade");
    replica.run("drop database if exists " + replicatedDbName + " cascade");
  }

  @Test
  public void testHiveStreamingUnpartitionedWithTxnBatchSizeAsOne() throws Throwable {
    primary.dump(primaryDbName);
    replica.loadWithoutExplain(replicatedDbName, primaryDbName);

    // Create an ACID table.
    String tblName = "alerts";
    primary.run("use " + primaryDbName)
            .run("create table " + tblName + "( id int , msg string ) " +
                    "clustered by (id) into 5 buckets " +
                    "stored as orc tblproperties(\"transactional\"=\"true\")");

    // Create delimited record writer whose schema exactly matches table schema
    StrictDelimitedInputWriter writer = StrictDelimitedInputWriter.newBuilder()
            .withFieldDelimiter(',')
            .build();

    // Create and open streaming connection (default.src table has to exist already)
    // By default, txn batch size is 1.
    StreamingConnection connection = HiveStreamingConnection.newBuilder()
            .withDatabase(primaryDbName)
            .withTable(tblName)
            .withAgentInfo("example-agent-1")
            .withRecordWriter(writer)
            .withHiveConf(primary.getConf())
            .connect();

    // Begin a transaction, write records and commit 1st transaction
    connection.beginTransaction();
    connection.write("1,val1".getBytes());
    connection.write("2,val2".getBytes());
    connection.commitTransaction();

    // Replicate the committed data which should be visible.
    primary.dump(primaryDbName);
    replica.loadWithoutExplain(replicatedDbName, primaryDbName)
            .run("use " + replicatedDbName)
            .run("select msg from " + tblName + " order by msg")
            .verifyResults((new String[] {"val1", "val2"}));

    // Begin another transaction, write more records and commit 2nd transaction after REPL LOAD.
    connection.beginTransaction();
    connection.write("3,val3".getBytes());
    connection.write("4,val4".getBytes());

    // Replicate events before committing txn. The uncommitted data shouldn't be seen.
    primary.dump(primaryDbName);
    replica.loadWithoutExplain(replicatedDbName, primaryDbName)
            .run("use " + replicatedDbName)
            .run("select msg from " + tblName + " order by msg")
            .verifyResults((new String[] {"val1", "val2"}));

    connection.commitTransaction();

    // After commit, the data should be replicated and visible.
    primary.dump(primaryDbName);
    replica.loadWithoutExplain(replicatedDbName, primaryDbName)
            .run("use " + replicatedDbName)
            .run("select msg from " + tblName + " order by msg")
            .verifyResults((new String[] {"val1", "val2", "val3", "val4"}));

    // Begin another transaction, write more records and abort 3rd transaction
    connection.beginTransaction();
    connection.write("5,val5".getBytes());
    connection.write("6,val6".getBytes());
    connection.abortTransaction();

    // Aborted data shouldn't be visible.
    primary.dump(primaryDbName);
    replica.loadWithoutExplain(replicatedDbName, primaryDbName)
            .run("use " + replicatedDbName)
            .run("select msg from " + tblName + " order by msg")
            .verifyResults((new String[] {"val1", "val2", "val3", "val4"}));

    // Close the streaming connection
    connection.close();
  }

  @Test
  public void testHiveStreamingStaticPartitionWithTxnBatchSizeAsOne() throws Throwable {
    primary.dump(primaryDbName);
    replica.loadWithoutExplain(replicatedDbName, primaryDbName);

    // Create an ACID table.
    String tblName = "alerts";
    primary.run("use " + primaryDbName)
            .run("create table " + tblName + "( id int , msg string ) " +
                    "partitioned by (continent string, country string) " +
                    "clustered by (id) into 5 buckets " +
                    "stored as orc tblproperties(\"transactional\"=\"true\")");

    // Static partition values
    ArrayList<String> partitionVals = new ArrayList<String>(2);
    partitionVals.add("Asia");
    partitionVals.add("India");

    // Create delimited record writer whose schema exactly matches table schema
    StrictDelimitedInputWriter writer = StrictDelimitedInputWriter.newBuilder()
            .withFieldDelimiter(',')
            .build();

    // Create and open streaming connection (default.src table has to exist already)
    // By default, txn batch size is 1.
    StreamingConnection connection = HiveStreamingConnection.newBuilder()
            .withDatabase(primaryDbName)
            .withTable(tblName)
            .withStaticPartitionValues(partitionVals)
            .withAgentInfo("example-agent-1")
            .withRecordWriter(writer)
            .withHiveConf(primary.getConf())
            .connect();

    // Begin a transaction, write records and commit 1st transaction
    connection.beginTransaction();
    connection.write("1,val1".getBytes());
    connection.write("2,val2".getBytes());
    connection.commitTransaction();

    // Replicate the committed data which should be visible.
    primary.dump(primaryDbName);
    replica.loadWithoutExplain(replicatedDbName, primaryDbName)
            .run("use " + replicatedDbName)
            .run("select msg from " + tblName + " where continent='Asia' and country='India' order by msg")
            .verifyResults((new String[] {"val1", "val2"}));

    // Begin another transaction, write more records and commit 2nd transaction after REPL LOAD.
    connection.beginTransaction();
    connection.write("3,val3".getBytes());
    connection.write("4,val4".getBytes());

    // Replicate events before committing txn. The uncommitted data shouldn't be seen.
    primary.dump(primaryDbName);
    replica.loadWithoutExplain(replicatedDbName, primaryDbName)
            .run("use " + replicatedDbName)
            .run("select msg from " + tblName + " where continent='Asia' and country='India' order by msg")
            .verifyResults((new String[] {"val1", "val2"}));

    connection.commitTransaction();

    // After commit, the data should be replicated and visible.
    primary.dump(primaryDbName);
    replica.loadWithoutExplain(replicatedDbName, primaryDbName)
            .run("use " + replicatedDbName)
            .run("select msg from " + tblName + " where continent='Asia' and country='India' order by msg")
            .verifyResults((new String[] {"val1", "val2", "val3", "val4"}));

    // Begin another transaction, write more records and abort 3rd transaction
    connection.beginTransaction();
    connection.write("5,val5".getBytes());
    connection.write("6,val6".getBytes());
    connection.abortTransaction();

    // Aborted data shouldn't be visible.
    primary.dump(primaryDbName);
    replica.loadWithoutExplain(replicatedDbName, primaryDbName)
            .run("use " + replicatedDbName)
            .run("select msg from " + tblName + " where continent='Asia' and country='India' order by msg")
            .verifyResults((new String[] {"val1", "val2", "val3", "val4"}));

    // Close the streaming connection
    connection.close();
  }

  @Test
  public void testHiveStreamingDynamicPartitionWithTxnBatchSizeAsOne() throws Throwable {
    primary.dump(primaryDbName);
    replica.loadWithoutExplain(replicatedDbName, primaryDbName);

    // Create an ACID table.
    String tblName = "alerts";
    primary.run("use " + primaryDbName)
            .run("create table " + tblName + "( id int , msg string ) " +
                    "partitioned by (continent string, country string) " +
                    "clustered by (id) into 5 buckets " +
                    "stored as orc tblproperties(\"transactional\"=\"true\")");

    // Dynamic partitioning
    // Create delimited record writer whose schema exactly matches table schema
    StrictDelimitedInputWriter writer = StrictDelimitedInputWriter.newBuilder()
            .withFieldDelimiter(',')
            .build();

    // Create and open streaming connection (default.src table has to exist already)
    // By default, txn batch size is 1.
    StreamingConnection connection = HiveStreamingConnection.newBuilder()
            .withDatabase(primaryDbName)
            .withTable(tblName)
            .withAgentInfo("example-agent-1")
            .withRecordWriter(writer)
            .withHiveConf(primary.getConf())
            .connect();

    // Begin a transaction, write records and commit 1st transaction
    connection.beginTransaction();

    // Dynamic partition mode where last 2 columns are partition values
    connection.write("11,val11,Asia,China".getBytes());
    connection.write("12,val12,Asia,India".getBytes());
    connection.commitTransaction();

    // Replicate the committed data which should be visible.
    primary.dump(primaryDbName);
    replica.loadWithoutExplain(replicatedDbName, primaryDbName)
            .run("use " + replicatedDbName)
            .run("select msg from " + tblName + " where continent='Asia' and country='China' order by msg")
            .verifyResults((new String[] {"val11"}))
            .run("select msg from " + tblName + " where continent='Asia' and country='India' order by msg")
            .verifyResults((new String[] {"val12"}));

    // Begin another transaction, write more records and commit 2nd transaction after REPL LOAD.
    connection.beginTransaction();
    connection.write("13,val13,Europe,Germany".getBytes());
    connection.write("14,val14,Asia,India".getBytes());

    // Replicate events before committing txn. The uncommitted data shouldn't be seen.
    primary.dump(primaryDbName);
    replica.loadWithoutExplain(replicatedDbName, primaryDbName)
            .run("use " + replicatedDbName)
            .run("select msg from " + tblName + " where continent='Asia' and country='India' order by msg")
            .verifyResults((new String[] {"val12"}));

    connection.commitTransaction();

    // After committing the txn, the data should be visible.
    primary.dump(primaryDbName);
    replica.loadWithoutExplain(replicatedDbName, primaryDbName)
            .run("use " + replicatedDbName)
            .run("select msg from " + tblName + " where continent='Asia' and country='India' order by msg")
            .verifyResults((new String[] {"val12", "val14"}))
            .run("select msg from " + tblName + " where continent='Europe' and country='Germany' order by msg")
            .verifyResults((new String[] {"val13"}));

    // Begin a transaction, write records and abort 3rd transaction
    connection.beginTransaction();
    connection.write("15,val15,Asia,China".getBytes());
    connection.write("16,val16,Asia,India".getBytes());
    connection.abortTransaction();

    // Aborted data should not be visible.
    primary.dump(primaryDbName);
    replica.loadWithoutExplain(replicatedDbName, primaryDbName)
            .run("use " + replicatedDbName)
            .run("select msg from " + tblName + " where continent='Asia' and country='India' order by msg")
            .verifyResults((new String[] {"val12", "val14"}))
            .run("select msg from " + tblName + " where continent='Asia' and country='China' order by msg")
            .verifyResults((new String[] {"val11"}));

    // Close the streaming connection
    connection.close();
  }
}
