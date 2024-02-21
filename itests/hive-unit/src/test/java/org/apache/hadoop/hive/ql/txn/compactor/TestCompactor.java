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

import com.google.common.collect.Lists;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.ReplChangeManager;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.ShowCompactRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponseElement;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionInfo;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.utils.TestTxnDbUtil;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.txn.compactor.handler.TaskHandler;
import org.apache.hadoop.hive.ql.txn.compactor.handler.TaskHandlerFactory;
import org.apache.hive.streaming.HiveStreamingConnection;
import org.apache.hive.streaming.StreamingConnection;
import org.apache.hive.streaming.StreamingException;
import org.apache.hive.streaming.StrictDelimitedInputWriter;
import org.apache.orc.OrcConf;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.FieldSetter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.hive.common.AcidConstants.VISIBILITY_PATTERN;
import static org.apache.hadoop.hive.ql.TestTxnCommands2.runCleaner;
import static org.apache.hadoop.hive.ql.TestTxnCommands2.runInitiator;
import static org.apache.hadoop.hive.ql.TestTxnCommands2.runWorker;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;


/**
 * Compaction related unit tests.
 */
@SuppressWarnings({ "deprecation", "CallToThreadRun", "resource", "ForLoopReplaceableByForEach" })
public class TestCompactor extends TestCompactorBase {

  @Test
  public void testHeartbeatShutdownOnFailedCompaction() throws Exception {
    String dbName = "default";
    String tblName = "compaction_test";
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("CREATE TABLE " + tblName + "(a INT, b STRING) " +
            " PARTITIONED BY(bkt INT)" +
            " CLUSTERED BY(a) INTO 4 BUCKETS" + //currently ACID requires table to be bucketed
            " STORED AS ORC  TBLPROPERTIES ('transactional'='true')", driver);

    StrictDelimitedInputWriter writer = StrictDelimitedInputWriter.newBuilder()
            .withFieldDelimiter(',')
            .build();
    HiveStreamingConnection connection = HiveStreamingConnection.newBuilder()
            .withDatabase(dbName)
            .withTable(tblName)
            .withStaticPartitionValues(Collections.singletonList("0"))
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

    executeStatementOnDriver("INSERT INTO TABLE " + tblName + " PARTITION(bkt=1)" +
            " values(57, 'Budapest')", driver);
    executeStatementOnDriver("INSERT INTO TABLE " + tblName + " PARTITION(bkt=1)" +
            " values(58, 'Milano')", driver);
    execSelectAndDumpData("select * from " + tblName, driver, "Dumping data for " +
            tblName + " after load:");

    TxnStore txnHandler = TxnUtils.getTxnStore(conf);

    // Commit will throw an exception
    IMetaStoreClient mockedClient = Mockito.spy(new HiveMetaStoreClient(conf));
    doThrow(new RuntimeException("Simulating RuntimeException from CompactionTxn.commit")).when(mockedClient).commitTxn(Mockito.anyLong());
    doAnswer(invocation -> {
      Object o = invocation.callRealMethod();
      //Check if the heartbeating is running
      Assert.assertTrue(Thread.getAllStackTraces().keySet()
              .stream().anyMatch(k -> k.getName().contains("CompactionTxn Heartbeater")));
      return o;
    }).when(mockedClient).openTxn(any(), any());

    //Do a major compaction
    CompactionRequest rqst = new CompactionRequest(dbName, tblName, CompactionType.MAJOR);
    rqst.setPartitionname("bkt=0");
    txnHandler.compact(rqst);

    Worker worker = Mockito.spy(new Worker());
    worker.setConf(conf);
    worker.init(new AtomicBoolean(true));
    FieldSetter.setField(worker, RemoteCompactorThread.class.getDeclaredField("msc"), mockedClient);

    worker.run();

    //Check if the transaction was opened
    verify(mockedClient, times(1)).openTxn(any(), any());
    //Check if the heartbeating is properly terminated
    Assert.assertTrue(Thread.getAllStackTraces().keySet()
            .stream().noneMatch(k -> k.getName().contains("CompactionTxn Heartbeater")));
  }

  /**
   * Simple schema evolution add columns with partitioning.
   *
   * @throws Exception
   */
  @Test
  public void schemaEvolutionAddColDynamicPartitioningInsert() throws Exception {
    String tblName = "dpct";
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("CREATE TABLE " + tblName + "(a INT, b STRING) " +
      " PARTITIONED BY(ds string)" +
      " CLUSTERED BY(a) INTO 2 BUCKETS" + //currently ACID requires table to be bucketed
      " STORED AS ORC TBLPROPERTIES ('transactional'='true')", driver);

    // First INSERT round.
    executeStatementOnDriver("insert into " + tblName + " partition (ds) values (1, 'fred', " +
      "'today'), (2, 'wilma', 'yesterday')", driver);

    // ALTER TABLE ... ADD COLUMNS
    executeStatementOnDriver("ALTER TABLE " + tblName + " ADD COLUMNS(c int)", driver);

    // Validate there is an added NULL for column c.
    executeStatementOnDriver("SELECT * FROM " + tblName + " ORDER BY a", driver);
    ArrayList<String> valuesReadFromHiveDriver = new ArrayList<>();
    driver.getResults(valuesReadFromHiveDriver);
    Assert.assertEquals(2, valuesReadFromHiveDriver.size());
    Assert.assertEquals("1\tfred\tNULL\ttoday", valuesReadFromHiveDriver.get(0));
    Assert.assertEquals("2\twilma\tNULL\tyesterday", valuesReadFromHiveDriver.get(1));

    // Second INSERT round with new inserts into previously existing partition 'yesterday'.
    executeStatementOnDriver("insert into " + tblName + " partition (ds) values " +
        "(3, 'mark', 1900, 'soon'), (4, 'douglas', 1901, 'last_century'), " +
        "(5, 'doc', 1902, 'yesterday')",
      driver);

    // Validate there the new insertions for column c.
    executeStatementOnDriver("SELECT * FROM " + tblName + " ORDER BY a", driver);
    valuesReadFromHiveDriver = new ArrayList<>();
    driver.getResults(valuesReadFromHiveDriver);
    Assert.assertEquals(5, valuesReadFromHiveDriver.size());
    Assert.assertEquals("1\tfred\tNULL\ttoday", valuesReadFromHiveDriver.get(0));
    Assert.assertEquals("2\twilma\tNULL\tyesterday", valuesReadFromHiveDriver.get(1));
    Assert.assertEquals("3\tmark\t1900\tsoon", valuesReadFromHiveDriver.get(2));
    Assert.assertEquals("4\tdouglas\t1901\tlast_century", valuesReadFromHiveDriver.get(3));
    Assert.assertEquals("5\tdoc\t1902\tyesterday", valuesReadFromHiveDriver.get(4));
    conf.setIntVar(HiveConf.ConfVars.HIVE_COMPACTOR_DELTA_NUM_THRESHOLD, 0);
    runInitiator(conf);

    TxnStore txnHandler = TxnUtils.getTxnStore(conf);
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    Assert.assertEquals(4, compacts.size());
    SortedSet<String> partNames = new TreeSet<>();
    verifyCompactions(compacts, partNames, tblName);
    List<String> names = new ArrayList<>(partNames);
    Assert.assertEquals("ds=last_century", names.get(0));
    Assert.assertEquals("ds=soon", names.get(1));
    Assert.assertEquals("ds=today", names.get(2));
    Assert.assertEquals("ds=yesterday", names.get(3));

    // Validate after compaction.
    executeStatementOnDriver("SELECT * FROM " + tblName + " ORDER BY a", driver);
    valuesReadFromHiveDriver = new ArrayList<>();
    driver.getResults(valuesReadFromHiveDriver);
    Assert.assertEquals(5, valuesReadFromHiveDriver.size());
    Assert.assertEquals("1\tfred\tNULL\ttoday", valuesReadFromHiveDriver.get(0));
    Assert.assertEquals("2\twilma\tNULL\tyesterday", valuesReadFromHiveDriver.get(1));
    Assert.assertEquals("3\tmark\t1900\tsoon", valuesReadFromHiveDriver.get(2));
    Assert.assertEquals("4\tdouglas\t1901\tlast_century", valuesReadFromHiveDriver.get(3));
    Assert.assertEquals("5\tdoc\t1902\tyesterday", valuesReadFromHiveDriver.get(4));

  }

  @Test
  public void schemaEvolutionAddColDynamicPartitioningUpdate() throws Exception {
    String tblName = "udpct";
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("CREATE TABLE " + tblName + "(a INT, b STRING) " +
      " PARTITIONED BY(ds string)" +
      " CLUSTERED BY(a) INTO 2 BUCKETS" + //currently ACID requires table to be bucketed
      " STORED AS ORC TBLPROPERTIES ('transactional'='true')", driver);
    executeStatementOnDriver("insert into " + tblName + " partition (ds) values (1, 'fred', " +
      "'today'), (2, 'wilma', 'yesterday')", driver);

    executeStatementOnDriver("update " + tblName + " set b = 'barney'", driver);

    // Validate the update.
    executeStatementOnDriver("SELECT * FROM " + tblName + " ORDER BY a", driver);
    ArrayList<String> valuesReadFromHiveDriver = new ArrayList<>();
    driver.getResults(valuesReadFromHiveDriver);
    Assert.assertEquals(2, valuesReadFromHiveDriver.size());
    Assert.assertEquals("1\tbarney\ttoday", valuesReadFromHiveDriver.get(0));
    Assert.assertEquals("2\tbarney\tyesterday", valuesReadFromHiveDriver.get(1));

    // ALTER TABLE ... ADD COLUMNS
    executeStatementOnDriver("ALTER TABLE " + tblName + " ADD COLUMNS(c int)", driver);

    // Validate there is an added NULL for column c.
    executeStatementOnDriver("SELECT * FROM " + tblName + " ORDER BY a", driver);
    valuesReadFromHiveDriver = new ArrayList<>();
    driver.getResults(valuesReadFromHiveDriver);
    Assert.assertEquals(2, valuesReadFromHiveDriver.size());
    Assert.assertEquals("1\tbarney\tNULL\ttoday", valuesReadFromHiveDriver.get(0));
    Assert.assertEquals("2\tbarney\tNULL\tyesterday", valuesReadFromHiveDriver.get(1));

    // Second INSERT round with new inserts into previously existing partition 'yesterday'.
    executeStatementOnDriver("insert into " + tblName + " partition (ds) values " +
        "(3, 'mark', 1900, 'soon'), (4, 'douglas', 1901, 'last_century'), " +
        "(5, 'doc', 1902, 'yesterday')",
      driver);

    // Validate there the new insertions for column c.
    executeStatementOnDriver("SELECT * FROM " + tblName + " ORDER BY a", driver);
    valuesReadFromHiveDriver = new ArrayList<>();
    driver.getResults(valuesReadFromHiveDriver);
    Assert.assertEquals(5, valuesReadFromHiveDriver.size());
    Assert.assertEquals("1\tbarney\tNULL\ttoday", valuesReadFromHiveDriver.get(0));
    Assert.assertEquals("2\tbarney\tNULL\tyesterday", valuesReadFromHiveDriver.get(1));
    Assert.assertEquals("3\tmark\t1900\tsoon", valuesReadFromHiveDriver.get(2));
    Assert.assertEquals("4\tdouglas\t1901\tlast_century", valuesReadFromHiveDriver.get(3));
    Assert.assertEquals("5\tdoc\t1902\tyesterday", valuesReadFromHiveDriver.get(4));

    executeStatementOnDriver("update " + tblName + " set c = 2000", driver);

    // Validate the update of new column c, even in old rows.
    executeStatementOnDriver("SELECT * FROM " + tblName + " ORDER BY a", driver);
    valuesReadFromHiveDriver = new ArrayList<>();
    driver.getResults(valuesReadFromHiveDriver);
    Assert.assertEquals(5, valuesReadFromHiveDriver.size());
    Assert.assertEquals("1\tbarney\t2000\ttoday", valuesReadFromHiveDriver.get(0));
    Assert.assertEquals("2\tbarney\t2000\tyesterday", valuesReadFromHiveDriver.get(1));
    Assert.assertEquals("3\tmark\t2000\tsoon", valuesReadFromHiveDriver.get(2));
    Assert.assertEquals("4\tdouglas\t2000\tlast_century", valuesReadFromHiveDriver.get(3));
    Assert.assertEquals("5\tdoc\t2000\tyesterday", valuesReadFromHiveDriver.get(4));

    // Set to 1 so insert doesn't set it off but update does
    conf.setIntVar(HiveConf.ConfVars.HIVE_COMPACTOR_DELTA_NUM_THRESHOLD, 1);
    runInitiator(conf);

    TxnStore txnHandler = TxnUtils.getTxnStore(conf);
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    Assert.assertEquals(4, compacts.size());
    SortedSet<String> partNames = new TreeSet<>();
    verifyCompactions(compacts, partNames, tblName);
    List<String> names = new ArrayList<>(partNames);
    Assert.assertEquals("ds=last_century", names.get(0));
    Assert.assertEquals("ds=soon", names.get(1));
    Assert.assertEquals("ds=today", names.get(2));
    Assert.assertEquals("ds=yesterday", names.get(3));

    // Validate after compaction.
    executeStatementOnDriver("SELECT * FROM " + tblName + " ORDER BY a", driver);
    valuesReadFromHiveDriver = new ArrayList<>();
    driver.getResults(valuesReadFromHiveDriver);
    Assert.assertEquals(5, valuesReadFromHiveDriver.size());
    Assert.assertEquals("1\tbarney\t2000\ttoday", valuesReadFromHiveDriver.get(0));
    Assert.assertEquals("2\tbarney\t2000\tyesterday", valuesReadFromHiveDriver.get(1));
    Assert.assertEquals("3\tmark\t2000\tsoon", valuesReadFromHiveDriver.get(2));
    Assert.assertEquals("4\tdouglas\t2000\tlast_century", valuesReadFromHiveDriver.get(3));
    Assert.assertEquals("5\tdoc\t2000\tyesterday", valuesReadFromHiveDriver.get(4));
  }

  /**
   * After each major compaction, stats need to be updated on the table
   * 1. create a partitioned ORC backed table (Orc is currently required by ACID)
   * 2. populate with data
   * 3. compute stats
   * 4. Trigger major compaction on one of the partitions (which should update stats)
   * 5. check that stats have been updated for that partition only
   *
   * @throws Exception todo:
   *                   4. add a test with sorted table?
   */
  @Test
  public void testStatsAfterCompactionPartTbl() throws Exception {
    //as of (8/27/2014) Hive 0.14, ACID/Orc requires HiveInputFormat
    String dbName = "default";
    String tblName = "compaction_test";
    IMetaStoreClient msClient = new HiveMetaStoreClient(conf);
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("CREATE TABLE " + tblName + "(a INT, b STRING) " +
      " PARTITIONED BY(bkt INT)" +
      " CLUSTERED BY(a) INTO 4 BUCKETS" + //currently ACID requires table to be bucketed
      " STORED AS ORC  TBLPROPERTIES ('transactional'='true')", driver);

    StrictDelimitedInputWriter writer = StrictDelimitedInputWriter.newBuilder()
            .withFieldDelimiter(',')
            .build();
    HiveStreamingConnection connection = HiveStreamingConnection.newBuilder()
            .withDatabase(dbName)
            .withTable(tblName)
            .withStaticPartitionValues(Collections.singletonList("0"))
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

    executeStatementOnDriver("INSERT INTO TABLE " + tblName + " PARTITION(bkt=1)" +
      " values(57, 'Budapest')", driver);
    executeStatementOnDriver("INSERT INTO TABLE " + tblName + " PARTITION(bkt=1)" +
            " values(58, 'Milano')", driver);
    execSelectAndDumpData("select * from " + tblName, driver, "Dumping data for " +
      tblName + " after load:");

    TxnStore txnHandler = TxnUtils.getTxnStore(conf);
    Table table = msClient.getTable(dbName, tblName);

    //compute stats before compaction
    CompactionInfo ci = new CompactionInfo(dbName, tblName, "bkt=0", CompactionType.MAJOR);
    statsUpdater.gatherStats(ci, conf, System.getProperty("user.name"),
            CompactorUtil.getCompactorJobQueueName(conf, ci, table), msClient);
    ci = new CompactionInfo(dbName, tblName, "bkt=1", CompactionType.MAJOR);
    statsUpdater.gatherStats(ci, conf, System.getProperty("user.name"),
            CompactorUtil.getCompactorJobQueueName(conf, ci, table), msClient);

    //Check basic stats are collected
    org.apache.hadoop.hive.ql.metadata.Table hiveTable = Hive.get().getTable(tblName);
    List<org.apache.hadoop.hive.ql.metadata.Partition> partitions = Hive.get().getPartitions(hiveTable);
    Map<String, String> parameters = partitions
            .stream()
            .filter(p -> p.getName().equals("bkt=0"))
            .findFirst()
            .orElseThrow(() -> new RuntimeException("Could not get Partition"))
            .getParameters();
    Assert.assertEquals("The number of files is differing from the expected", "2", parameters.get("numFiles"));
    Assert.assertEquals("The number of rows is differing from the expected", "2", parameters.get("numRows"));

    parameters = partitions
            .stream()
            .filter(p -> p.getName().equals("bkt=1"))
            .findFirst()
            .orElseThrow(() -> new RuntimeException("Could not get Partition"))
            .getParameters();
    Assert.assertEquals("The number of files is differing from the expected", "2", parameters.get("numFiles"));
    Assert.assertEquals("The number of rows is differing from the expected", "2", parameters.get("numRows"));

    //Do a major compaction
    CompactionRequest rqst = new CompactionRequest(dbName, tblName, CompactionType.MAJOR);
    rqst.setPartitionname("bkt=0");
    txnHandler.compact(rqst);
    runWorker(conf);

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    if (1 != compacts.size()) {
      Assert.fail("Expecting 1 file and found " + compacts.size() + " files " + compacts);
    }
    Assert.assertEquals("ready for cleaning", compacts.get(0).getState());

    //Check basic stats are updated for partition bkt=0, but not updated for partition bkt=1
    partitions = Hive.get().getPartitions(hiveTable);
    parameters = partitions
            .stream()
            .filter(p -> p.getName().equals("bkt=0"))
            .findFirst()
            .orElseThrow(() -> new RuntimeException("Could not get Partition"))
            .getParameters();
    Assert.assertEquals("The number of files is differing from the expected", "1", parameters.get("numFiles"));
    Assert.assertEquals("The number of rows is differing from the expected", "2", parameters.get("numRows"));

    parameters = partitions
            .stream()
            .filter(p -> p.getName().equals("bkt=1"))
            .findFirst()
            .orElseThrow(() -> new RuntimeException("Could not get Partition"))
            .getParameters();
    Assert.assertEquals("The number of files is differing from the expected", "2", parameters.get("numFiles"));
    Assert.assertEquals("The number of rows is differing from the expected", "2", parameters.get("numRows"));
  }

  /**
   * After each major compaction, stats need to be updated on the table
   * 1. create an ORC backed table (Orc is currently required by ACID)
   * 2. populate with data
   * 3. compute stats
   * 4. Trigger major compaction (which should update stats)
   * 5. check that stats have been updated
   *
   * @throws Exception todo:
   *                   4. add a test with sorted table?
   */
  @Test
  public void testStatsAfterCompactionTbl() throws Exception {
    //as of (8/27/2014) Hive 0.14, ACID/Orc requires HiveInputFormat
    String dbName = "default";
    String tblName = "compaction_test";
    IMetaStoreClient msClient = new HiveMetaStoreClient(conf);
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("CREATE TABLE " + tblName + "(a INT, b STRING) " +
            " CLUSTERED BY(a) INTO 4 BUCKETS" + //currently ACID requires table to be bucketed
            " STORED AS ORC  TBLPROPERTIES ('transactional'='true')", driver);
    executeStatementOnDriver("INSERT INTO TABLE " + tblName +
            " values(55, 'London')", driver);
    executeStatementOnDriver("INSERT INTO TABLE " + tblName +
            " values(56, 'Paris')", driver);
    execSelectAndDumpData("select * from " + tblName, driver, "Dumping data for " +
            tblName + " after load:");

    TxnStore txnHandler = TxnUtils.getTxnStore(conf);
    Table table = msClient.getTable(dbName, tblName);

    //compute stats before compaction
    CompactionInfo ci = new CompactionInfo(dbName, tblName, null, CompactionType.MAJOR);
    statsUpdater.gatherStats(ci, conf, System.getProperty("user.name"),
            CompactorUtil.getCompactorJobQueueName(conf, ci, table), msClient);

    //Check basic stats are collected
    Map<String, String> parameters = Hive.get().getTable(tblName).getParameters();
    Assert.assertEquals("The number of files is differing from the expected", "2", parameters.get("numFiles"));
    Assert.assertEquals("The number of rows is differing from the expected", "2", parameters.get("numRows"));

    //Do a major compaction
    CompactionRequest rqst = new CompactionRequest(dbName, tblName, CompactionType.MAJOR);
    txnHandler.compact(rqst);
    runWorker(conf);

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    if (1 != compacts.size()) {
      Assert.fail("Expecting 1 file and found " + compacts.size() + " files " + compacts);
    }
    Assert.assertEquals("ready for cleaning", compacts.get(0).getState());

    //Check basic stats are updated
    parameters = Hive.get().getTable(tblName).getParameters();
    Assert.assertEquals("The number of files is differing from the expected", "1", parameters.get("numFiles"));
    Assert.assertEquals("The number of rows is differing from the expected", "2", parameters.get("numRows"));
  }

  @Test
  public void dynamicPartitioningInsert() throws Exception {
    String tblName = "dpct";
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("CREATE TABLE " + tblName + "(a INT, b STRING) " +
      " PARTITIONED BY(ds string)" +
      " CLUSTERED BY(a) INTO 2 BUCKETS" + //currently ACID requires table to be bucketed
      " STORED AS ORC TBLPROPERTIES ('transactional'='true')", driver);
    executeStatementOnDriver("insert into " + tblName + " partition (ds) values (1, 'fred', " +
      "'today'), (2, 'wilma', 'yesterday')", driver);

    conf.setIntVar(HiveConf.ConfVars.HIVE_COMPACTOR_DELTA_NUM_THRESHOLD, 0);
    runInitiator(conf);

    TxnStore txnHandler = TxnUtils.getTxnStore(conf);
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    Assert.assertEquals(2, compacts.size());
    SortedSet<String> partNames = new TreeSet<>();
    verifyCompactions(compacts, partNames, tblName);
    List<String> names = new ArrayList<>(partNames);
    Assert.assertEquals("ds=today", names.get(0));
    Assert.assertEquals("ds=yesterday", names.get(1));
  }

  @Test
  public void dynamicPartitioningUpdate() throws Exception {
    String tblName = "udpct";
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("CREATE TABLE " + tblName + "(a INT, b STRING) " +
      " PARTITIONED BY(ds string)" +
      " CLUSTERED BY(a) INTO 2 BUCKETS" + //currently ACID requires table to be bucketed
      " STORED AS ORC TBLPROPERTIES ('transactional'='true')", driver);
    executeStatementOnDriver("insert into " + tblName + " partition (ds) values (1, 'fred', " +
      "'today'), (2, 'wilma', 'yesterday')", driver);

    executeStatementOnDriver("update " + tblName + " set b = 'barney'", driver);

    // Set to 1 so insert doesn't set it off but update does
    conf.setIntVar(HiveConf.ConfVars.HIVE_COMPACTOR_DELTA_NUM_THRESHOLD, 1);
    runInitiator(conf);

    TxnStore txnHandler = TxnUtils.getTxnStore(conf);
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    Assert.assertEquals(2, compacts.size());
    SortedSet<String> partNames = new TreeSet<>();
    verifyCompactions(compacts, partNames, tblName);
    List<String> names = new ArrayList<>(partNames);
    Assert.assertEquals("ds=today", names.get(0));
    Assert.assertEquals("ds=yesterday", names.get(1));
  }

  @Test
  public void dynamicPartitioningDelete() throws Exception {
    String tblName = "ddpct";
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("CREATE TABLE " + tblName + "(a INT, b STRING) " +
      " PARTITIONED BY(ds string)" +
      " CLUSTERED BY(a) INTO 2 BUCKETS" + //currently ACID requires table to be bucketed
      " STORED AS ORC TBLPROPERTIES ('transactional'='true')", driver);
    executeStatementOnDriver("insert into " + tblName + " partition (ds) values (1, 'fred', " +
      "'today'), (2, 'wilma', 'yesterday')", driver);

    executeStatementOnDriver("update " + tblName + " set b = 'fred' where a = 1", driver);

    executeStatementOnDriver("delete from " + tblName + " where b = 'fred'", driver);

    // Set to 2 so insert and update don't set it off but delete does
    conf.setIntVar(HiveConf.ConfVars.HIVE_COMPACTOR_DELTA_NUM_THRESHOLD, 2);
    runInitiator(conf);

    TxnStore txnHandler = TxnUtils.getTxnStore(conf);
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    Assert.assertEquals(1, compacts.size());
    SortedSet<String> partNames = new TreeSet<>();
    verifyCompactions(compacts, partNames, tblName);
    List<String> names = new ArrayList<>(partNames);
    Assert.assertEquals("ds=today", names.get(0));
  }

  @Test
  public void minorCompactWhileStreaming() throws Exception {
    String dbName = "default";
    String tblName = "cws";
    String columnNamesProperty = "a,b";
    String columnTypesProperty = "int:string";
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("CREATE TABLE " + tblName + "(a INT, b STRING) " +
      " CLUSTERED BY(a) INTO 1 BUCKETS" + //currently ACID requires table to be bucketed
      " STORED AS ORC  TBLPROPERTIES ('transactional'='true')", driver);

    StreamingConnection connection = null;
    try {
      // Write a couple of batches
      for (int i = 0; i < 2; i++) {
        CompactorTestUtil.writeBatch(conf, dbName, tblName, false, false);
      }

      // Start a third batch, but don't close it.
      connection = CompactorTestUtil.writeBatch(conf, dbName, tblName, false, true);

      // Now, compact
      TxnStore txnHandler = TxnUtils.getTxnStore(conf);
      txnHandler.compact(new CompactionRequest(dbName, tblName, CompactionType.MINOR));
      runWorker(conf);

      // Find the location of the table
      IMetaStoreClient msClient = new HiveMetaStoreClient(conf);
      Table table = msClient.getTable(dbName, tblName);
      FileSystem fs = FileSystem.get(conf);
      FileStatus[] stat =
        fs.listStatus(new Path(table.getSd().getLocation()), AcidUtils.deltaFileFilter);
      String[] names = new String[stat.length];
      Path resultFile = null;
      for (int i = 0; i < names.length; i++) {
        names[i] = stat[i].getPath().getName();
        if (names[i].equals("delta_0000001_0000004_v0000009")) {
          resultFile = stat[i].getPath();
        }
      }
      Arrays.sort(names);
      String[] expected = new String[]{"delta_0000001_0000002",
        "delta_0000001_0000004_v0000009", "delta_0000003_0000004", "delta_0000005_0000006"};
      if (!Arrays.deepEquals(expected, names)) {
        Assert.fail("Expected: " + Arrays.toString(expected) + ", found: " + Arrays.toString(names) + ",stat="
            + CompactorTestUtil.printFileStatus(stat));
      }
      CompactorTestUtil
          .checkExpectedTxnsPresent(null, new Path[] {resultFile}, columnNamesProperty, columnTypesProperty, 0, 1L,
              4L, null, 1);

    } finally {
      if (connection != null) {
        connection.close();
      }
    }
  }

  @Test
  public void majorCompactWhileStreaming() throws Exception {
    String dbName = "default";
    String tblName = "cws";
    String columnNamesProperty = "a,b";
    String columnTypesProperty = "int:string";
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("CREATE TABLE " + tblName + "(a INT, b STRING) " +
      " CLUSTERED BY(a) INTO 1 BUCKETS" + //currently ACID requires table to be bucketed
      " STORED AS ORC  TBLPROPERTIES ('transactional'='true') ", driver);

    StreamingConnection connection = null;
    try {
      // Write a couple of batches
      for (int i = 0; i < 2; i++) {
        CompactorTestUtil.writeBatch(conf, dbName, tblName, false, false);
      }

      // Start a third batch, but don't close it.  this delta will be ignored by compaction since
      // it has an open txn in it
      connection = CompactorTestUtil.writeBatch(conf, dbName, tblName, false, true);

      runMajorCompaction(dbName, tblName);

      // Find the location of the table
      IMetaStoreClient msClient = new HiveMetaStoreClient(conf);
      Table table = msClient.getTable(dbName, tblName);
      FileSystem fs = FileSystem.get(conf);
      FileStatus[] stat =
        fs.listStatus(new Path(table.getSd().getLocation()), AcidUtils.baseFileFilter);
      if (1 != stat.length) {
        Assert.fail("Expecting 1 file \"base_0000004\" and found " + stat.length + " files " + Arrays.toString(stat));
      }
      String name = stat[0].getPath().getName();
      Assert.assertEquals("base_0000004_v0000009", name);
      CompactorTestUtil
          .checkExpectedTxnsPresent(stat[0].getPath(), null, columnNamesProperty, columnTypesProperty, 0, 1L, 4L, null,
              1);
    } finally {
      if (connection != null) {
        connection.close();
      }
    }
  }

  @Test
  public void autoCompactOnStreamingIngestWithDynamicPartition() throws Exception {
    String dbName = "default";
    String tblName = "cws";
    String columnNamesProperty = "a,b";
    String columnTypesProperty = "string:int";
    String agentInfo = "UT_" + Thread.currentThread().getName();

    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("CREATE TABLE " + tblName + "(a STRING) " +
        " PARTITIONED BY (b INT)" + //currently ACID requires table to be bucketed
        " STORED AS ORC  TBLPROPERTIES ('transactional'='true')", driver);

    StrictDelimitedInputWriter writer1 = StrictDelimitedInputWriter.newBuilder()
        .withFieldDelimiter(',')
        .build();
    StrictDelimitedInputWriter writer2 = StrictDelimitedInputWriter.newBuilder()
        .withFieldDelimiter(',')
        .build();

    StreamingConnection connection1 = HiveStreamingConnection.newBuilder()
        .withDatabase(dbName)
        .withTable(tblName)
        .withAgentInfo(agentInfo)
        .withHiveConf(conf)
        .withRecordWriter(writer1)
        .withStreamingOptimizations(true)
        // Transaction size has to be one or exception should happen.
        .withTransactionBatchSize(1)
        .connect();

    StreamingConnection connection2 = HiveStreamingConnection.newBuilder()
        .withDatabase(dbName)
        .withTable(tblName)
        .withAgentInfo(agentInfo)
        .withHiveConf(conf)
        .withRecordWriter(writer2)
        .withStreamingOptimizations(true)
        // Transaction size has to be one or exception should happen.
        .withTransactionBatchSize(1)
        .connect();

    try {
      connection1.beginTransaction();
      connection1.write("1,1".getBytes());
      connection1.commitTransaction();

      connection1.beginTransaction();
      connection1.write("1,1".getBytes());
      connection1.commitTransaction();
      connection1.close();

      conf.setIntVar(HiveConf.ConfVars.HIVE_COMPACTOR_DELTA_NUM_THRESHOLD, 1);
      runInitiator(conf);

      TxnStore txnHandler = TxnUtils.getTxnStore(conf);
      ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());

      List<ShowCompactResponseElement> compacts1 = rsp.getCompacts();
      Assert.assertEquals(1, compacts1.size());
      SortedSet<String> partNames1 = new TreeSet<>();
      verifyCompactions(compacts1, partNames1, tblName);
      List<String> names1 = new ArrayList<>(partNames1);
      Assert.assertEquals("b=1", names1.get(0));

      runWorker(conf);
      runCleaner(conf);

      connection2.beginTransaction();
      connection2.write("1,1".getBytes());
      connection2.commitTransaction();

      connection2.beginTransaction();
      connection2.write("1,1".getBytes());
      connection2.commitTransaction();
      connection2.close();

      runInitiator(conf);

      List<ShowCompactResponseElement> compacts2 = rsp.getCompacts();
      Assert.assertEquals(1, compacts2.size());
      SortedSet<String> partNames2 = new TreeSet<>();
      verifyCompactions(compacts2, partNames2, tblName);
      List<String> names2 = new ArrayList<>(partNames2);
      Assert.assertEquals("b=1", names2.get(0));

      runWorker(conf);
      runCleaner(conf);

      // Find the location of the table
      IMetaStoreClient msClient = new HiveMetaStoreClient(conf);
      Table table = msClient.getTable(dbName, tblName);
      String tablePath = table.getSd().getLocation();
      String partName = "b=1";
      Path partPath = new Path(tablePath, partName);
      FileSystem fs = FileSystem.get(conf);
      FileStatus[] stat = fs.listStatus(partPath, AcidUtils.baseFileFilter);
      if (1 != stat.length) {
        Assert.fail("Expecting 1 file \"base_0000004\" and found " + stat.length + " files " + Arrays.toString(stat));
      }
      String name = stat[0].getPath().getName();
      Assert.assertEquals("base_0000004_v0000009", name);
      CompactorTestUtil
          .checkExpectedTxnsPresent(stat[0].getPath(), null, columnNamesProperty, columnTypesProperty, 0, 1L, 4L, null,
              1);
    } finally {
      if (connection1 != null) {
        connection1.close();
      }
      if (connection2 != null) {
        connection2.close();
      }
    }
  }

  @Test
  public void testNoDataLossWhenMaxNumDeltaIsUsed() throws Exception {
    String dbName = "default";
    String tblName = "cws";
    executeStatementOnDriver("drop table if exists " + tblName, driver);

    executeStatementOnDriver("CREATE TABLE " + tblName + "(a INT, b STRING) " +
      " STORED AS ORC  TBLPROPERTIES ('transactional'='true')", driver);
    executeStatementOnDriver("insert into " + tblName + " values (1, 'a')", driver);
    executeStatementOnDriver("insert into " + tblName + " values (3, 'b')", driver);

    runMajorCompaction(dbName, tblName);
    runCleaner(conf);

    for (int i = 0; i < 3; i++) {
      executeStatementOnDriver("MERGE INTO " + tblName + " AS T USING (" +
        "select * from " + tblName + " union all select a+1, b from " + tblName + ") AS S " +
        "ON T.a=s.a " +
        "WHEN MATCHED THEN DELETE " +
        "WHEN not MATCHED THEN INSERT values (s.a, s.b)", driver);
    }

    driver.run("select a from " + tblName);
    List<String> res = new ArrayList<>();
    driver.getFetchTask().fetch(res);
    Assert.assertEquals(res, Arrays.asList("4", "6"));

    conf.setIntVar(HiveConf.ConfVars.COMPACTOR_MAX_NUM_DELTA, 5);
    runMajorCompaction(dbName, tblName);

    List<String> matchesNotFound = new ArrayList<>(5);
    matchesNotFound.add(AcidUtils.deleteDeltaSubdir(3, 4) + VISIBILITY_PATTERN);
    matchesNotFound.add(AcidUtils.deltaSubdir(3, 4) + VISIBILITY_PATTERN);
    matchesNotFound.add(AcidUtils.deleteDeltaSubdir(5, 5, 0));
    matchesNotFound.add(AcidUtils.deltaSubdir(5, 5, 1));
    matchesNotFound.add(AcidUtils.baseDir(5) + VISIBILITY_PATTERN);

    IMetaStoreClient msClient = new HiveMetaStoreClient(conf);
    Table table = msClient.getTable(dbName, tblName);
    msClient.close();

    FileSystem fs = FileSystem.get(conf);
    FileStatus[] stat = fs.listStatus(new Path(table.getSd().getLocation()));

    for (FileStatus f : stat) {
      for (int j = 0; j < matchesNotFound.size(); j++) {
        if (f.getPath().getName().matches(matchesNotFound.get(j))) {
          matchesNotFound.remove(j);
          break;
        }
      }
    }
    Assert.assertEquals("Matches Not Found: " + matchesNotFound, 0, matchesNotFound.size());
    runCleaner(conf);

    driver.run("select a from " + tblName);
    res = new ArrayList<>();
    driver.getFetchTask().fetch(res);
    Assert.assertEquals(res, Arrays.asList("4", "6"));
  }

  @Test
  public void minorCompactAfterAbort() throws Exception {
    String dbName = "default";
    String tblName = "cws";
    String columnNamesProperty = "a,b";
    String columnTypesProperty = "int:string";
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("CREATE TABLE " + tblName + "(a INT, b STRING) " +
      " CLUSTERED BY(a) INTO 1 BUCKETS" + //currently ACID requires table to be bucketed
      " STORED AS ORC  TBLPROPERTIES ('transactional'='true')", driver);

    processStreamingAPI(dbName, tblName);

    // Now, compact
    TxnStore txnHandler = TxnUtils.getTxnStore(conf);
    txnHandler.compact(new CompactionRequest(dbName, tblName, CompactionType.MINOR));
    runWorker(conf);

    // Find the location of the table
    IMetaStoreClient msClient = new HiveMetaStoreClient(conf);
    Table table = msClient.getTable(dbName, tblName);
    FileSystem fs = FileSystem.get(conf);
    FileStatus[] stat =
      fs.listStatus(new Path(table.getSd().getLocation()), AcidUtils.deltaFileFilter);
    String[] names = new String[stat.length];
    Path resultDelta = null;
    for (int i = 0; i < names.length; i++) {
      names[i] = stat[i].getPath().getName();
      if (names[i].equals("delta_0000001_0000004_v0000009")) {
        resultDelta = stat[i].getPath();
      }
    }
    Arrays.sort(names);
    String[] expected = new String[]{"delta_0000001_0000002",
      "delta_0000001_0000004_v0000009", "delta_0000003_0000004"};
    if (!Arrays.deepEquals(expected, names)) {
      Assert.fail("Expected: " + Arrays.toString(expected) + ", found: " + Arrays.toString(names));
    }
    CompactorTestUtil
        .checkExpectedTxnsPresent(null, new Path[] {resultDelta}, columnNamesProperty, columnTypesProperty, 0, 1L, 4L,
            Lists.newArrayList(5, 6), 1);
  }

  @Test
  public void majorCompactAfterAbort() throws Exception {
    String dbName = "default";
    String tblName = "cws";
    String columnNamesProperty = "a,b";
    String columnTypesProperty = "int:string";
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("CREATE TABLE " + tblName + "(a INT, b STRING) " +
      " CLUSTERED BY(a) INTO 1 BUCKETS" + //currently ACID requires table to be bucketed
      " STORED AS ORC  TBLPROPERTIES ('transactional'='true')", driver);

    processStreamingAPI(dbName, tblName);
    runMajorCompaction(dbName, tblName);

    // Find the location of the table
    IMetaStoreClient msClient = new HiveMetaStoreClient(conf);
    Table table = msClient.getTable(dbName, tblName);
    FileSystem fs = FileSystem.get(conf);
    FileStatus[] stat =
      fs.listStatus(new Path(table.getSd().getLocation()), AcidUtils.baseFileFilter);
    if (1 != stat.length) {
      Assert.fail("Expecting 1 file \"base_0000004\" and found " + stat.length + " files " + Arrays.toString(stat));
    }
    String name = stat[0].getPath().getName();
    if (!name.equals("base_0000004_v0000009")) {
      Assert.fail("majorCompactAfterAbort name " + name + " not equals to base_0000004");
    }
    CompactorTestUtil
        .checkExpectedTxnsPresent(stat[0].getPath(), null, columnNamesProperty, columnTypesProperty, 0, 1L, 4L,
            Lists.newArrayList(5, 6), 1);
  }

  @Test
  public void majorCompactDuringFetchTaskConvertedRead() throws Exception {
    driver.close();
    driver = DriverFactory.newDriver(conf);
    String dbName = "default";
    String tblName = "cws";
    executeStatementOnDriver("drop table if exists " + tblName, driver);

    executeStatementOnDriver(
        "CREATE TABLE " + tblName + "(a INT, b STRING) " + " STORED AS ORC  TBLPROPERTIES ('transactional'='true')",
        driver);
    executeStatementOnDriver("insert into " + tblName + " values (1, 'a')", driver);
    executeStatementOnDriver("insert into " + tblName + " values (3, 'b')", driver);
    executeStatementOnDriver("insert into " + tblName + " values (4, 'a')", driver);
    executeStatementOnDriver("insert into " + tblName + " values (5, 'b')", driver);

    CommandProcessorResponse resp = driver.run("select * from " + tblName + " LIMIT 5");
    FetchTask ft = driver.getFetchTask();
    ft.setMaxRows(1);
    List res = new ArrayList();
    ft.fetch(res);
    assertEquals(1, res.size());

    runMajorCompaction(dbName, tblName);
    runCleaner(conf);

    ft.fetch(res);
    assertEquals(2, res.size());
  }

  @Test
  public void testCleanAbortCompactAfter2ndCommitAbort() throws Exception {
    String dbName = "default";
    String tblName = "cws";

    HiveStreamingConnection connection = prepareTableAndConnection(dbName, tblName, 2);

    connection.beginTransaction();
    connection.write("1,1".getBytes());
    connection.write("2,2".getBytes());
    connection.commitTransaction();

    connection.beginTransaction();
    connection.write("3,2".getBytes());
    connection.write("3,3".getBytes());
    connection.abortTransaction();

    assertAndCompactCleanAbort(dbName, tblName, true, true);
    connection.close();
  }

  @Test
  public void testCleanAbortCompactAfter1stCommitAbort() throws Exception {
    String dbName = "default";
    String tblName = "cws";

    HiveStreamingConnection connection = prepareTableAndConnection(dbName, tblName, 2);

    connection.beginTransaction();
    connection.write("1,1".getBytes());
    connection.write("2,2".getBytes());
    connection.abortTransaction();

    connection.beginTransaction();
    connection.write("3,2".getBytes());
    connection.write("3,3".getBytes());
    connection.commitTransaction();

    assertAndCompactCleanAbort(dbName, tblName, true, true);
    connection.close();
  }

  @Test
  public void testCleanAbortCompactAfterAbortTwoPartitions() throws Exception {
    String dbName = "default";
    String tblName = "cws";

    HiveStreamingConnection connection1 = prepareTableTwoPartitionsAndConnection(dbName, tblName, 1);
    HiveStreamingConnection connection2 = prepareTableTwoPartitionsAndConnection(dbName, tblName, 1);

    // to skip optimization introduced in HIVE-22122
    executeStatementOnDriver("truncate " + tblName, driver);

    connection1.beginTransaction();
    connection1.write("1,1".getBytes());
    connection1.write("2,2".getBytes());
    connection1.abortTransaction();

    connection2.beginTransaction();
    connection2.write("1,3".getBytes());
    connection2.write("2,3".getBytes());
    connection2.write("3,3".getBytes());
    connection2.abortTransaction();

    assertAndCompactCleanAbort(dbName, tblName, false, false);

    connection1.close();
    connection2.close();
  }

  @Test
  public void testCleanAbortCompactAfterAbort() throws Exception {
    String dbName = "default";
    String tblName = "cws";

    // Create three folders with two different transactions
    HiveStreamingConnection connection1 = prepareTableAndConnection(dbName, tblName, 1);
    HiveStreamingConnection connection2 = prepareTableAndConnection(dbName, tblName, 1);

    // to skip optimization introduced in HIVE-22122
    executeStatementOnDriver("truncate " + tblName, driver);

    connection1.beginTransaction();
    connection1.write("1,1".getBytes());
    connection1.write("2,2".getBytes());
    connection1.abortTransaction();

    connection2.beginTransaction();
    connection2.write("1,3".getBytes());
    connection2.write("2,3".getBytes());
    connection2.write("3,3".getBytes());
    connection2.abortTransaction();

    assertAndCompactCleanAbort(dbName, tblName, false, false);

    connection1.close();
    connection2.close();
  }

  @Test
  public void testAbortAfterMarkCleaned() throws Exception {
    assumeTrue(MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.COMPACTOR_CLEAN_ABORTS_USING_CLEANER));
    String dbName = "default";
    String tableName = "cws";

    String agentInfo = "UT_" + Thread.currentThread().getName();

    executeStatementOnDriver("drop table if exists " + tableName, driver);
    executeStatementOnDriver("CREATE TABLE " + tableName + "(a STRING, b STRING) " + //currently ACID requires table to be bucketed
            " STORED AS ORC  TBLPROPERTIES ('transactional'='true')", driver);
    executeStatementOnDriver("insert into table " + tableName + " values ('1', '2'), ('3', '4') ", driver);
    executeStatementOnDriver("insert into table " + tableName + " values ('1', '2'), ('3', '4') ", driver);


    StrictDelimitedInputWriter writer = StrictDelimitedInputWriter.newBuilder()
            .withFieldDelimiter(',')
            .build();

    // Create three folders with two different transactions
    HiveStreamingConnection connection1 = HiveStreamingConnection.newBuilder()
            .withDatabase(dbName)
            .withTable(tableName)
            .withAgentInfo(agentInfo)
            .withHiveConf(conf)
            .withRecordWriter(writer)
            .withStreamingOptimizations(true)
            .withTransactionBatchSize(1)
            .connect();

    HiveStreamingConnection connection2 = HiveStreamingConnection.newBuilder()
            .withDatabase(dbName)
            .withTable(tableName)
            .withAgentInfo(agentInfo)
            .withHiveConf(conf)
            .withRecordWriter(writer)
            .withStreamingOptimizations(true)
            .withTransactionBatchSize(1)
            .connect();

    // Abort a transaction which writes data.
    connection1.beginTransaction();
    connection1.write("1,1".getBytes());
    connection1.write("2,1".getBytes());
    connection1.abortTransaction();

    // Open a txn which is opened and long running.
    connection2.beginTransaction();
    connection2.write("3,1".getBytes());

    Cleaner cleaner = new Cleaner();
    TxnStore mockedTxnHandler = Mockito.spy(TxnUtils.getTxnStore(conf));
    doAnswer(invocationOnMock -> {
      connection2.abortTransaction();
      return invocationOnMock.callRealMethod();
    }).when(mockedTxnHandler).markCleaned(any());

    MetadataCache metadataCache = new MetadataCache(false);
    FSRemover fsRemover = new FSRemover(conf, ReplChangeManager.getInstance(conf), metadataCache);
    cleaner.setConf(conf);
    List<TaskHandler> cleanupHandlers = TaskHandlerFactory.getInstance()
            .getHandlers(conf, mockedTxnHandler, metadataCache, false, fsRemover);
    cleaner.init(new AtomicBoolean(true));
    cleaner.setCleanupHandlers(cleanupHandlers);
    cleaner.run();

    int count = TestTxnDbUtil.countQueryAgent(conf, "select count(*) from TXN_COMPONENTS");
    Assert.assertEquals(TestTxnDbUtil.queryToString(conf, "select * from TXN_COMPONENTS"), 1, count);
  }

  private void assertAndCompactCleanAbort(String dbName, String tblName, boolean partialAbort, boolean singleSession) throws Exception {
    boolean useCleanerForAbortCleanup = MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.COMPACTOR_CLEAN_ABORTS_USING_CLEANER);
    IMetaStoreClient msClient = new HiveMetaStoreClient(conf);
    TxnStore txnHandler = TxnUtils.getTxnStore(conf);
    Table table = msClient.getTable(dbName, tblName);
    FileSystem fs = FileSystem.get(conf);
    FileStatus[] stat =
        fs.listStatus(new Path(table.getSd().getLocation()));
    if (3 != stat.length) {
      Assert.fail("Expecting three directories corresponding to three partitions, FileStatus[] stat " + Arrays.toString(stat));
    }

    int count = TestTxnDbUtil.countQueryAgent(conf, "select count(*) from TXN_COMPONENTS where TC_OPERATION_TYPE='i'");
    // We should have two rows corresponding to the two aborted transactions
    Assert.assertEquals(TestTxnDbUtil.queryToString(conf, "select * from TXN_COMPONENTS"), partialAbort ? 1 : 2, count);

    runInitiator(conf);
    count = TestTxnDbUtil.countQueryAgent(conf, "select count(*) from COMPACTION_QUEUE");
    // Only one job is added to the queue per table. This job corresponds to all the entries for a particular table
    // with rows in TXN_COMPONENTS
    Assert.assertEquals(TestTxnDbUtil.queryToString(conf, "select * from COMPACTION_QUEUE"),
            useCleanerForAbortCleanup ? 0 : 1, count);
    runWorker(conf);

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(useCleanerForAbortCleanup ? 0 : 1, rsp.getCompacts().size());
    if (!useCleanerForAbortCleanup) {
      Assert.assertEquals(TxnStore.CLEANING_RESPONSE, rsp.getCompacts().get(0).getState());
      Assert.assertEquals("cws", rsp.getCompacts().get(0).getTablename());
      Assert.assertEquals(CompactionType.MINOR, rsp.getCompacts().get(0).getType());
    }

    runCleaner(conf);

    // After the cleaner runs TXN_COMPONENTS and COMPACTION_QUEUE should have zero rows, also the folders should have been deleted.
    count = TestTxnDbUtil.countQueryAgent(conf, "select count(*) from TXN_COMPONENTS");
    Assert.assertEquals(TestTxnDbUtil.queryToString(conf, "select * from TXN_COMPONENTS"), (singleSession && partialAbort) ? 1 : 0, count);

    count = TestTxnDbUtil.countQueryAgent(conf, "select count(*) from COMPACTION_QUEUE");
    Assert.assertEquals(TestTxnDbUtil.queryToString(conf, "select * from COMPACTION_QUEUE"), 0, count);

    RemoteIterator<LocatedFileStatus> it =
        fs.listFiles(new Path(table.getSd().getLocation()), true);
    if (it.hasNext() && !partialAbort) {
      Assert.fail("Expected cleaner to drop aborted delta & base directories, FileStatus[] stat " + Arrays.toString(stat));
    }

    rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(useCleanerForAbortCleanup ? 0 : 1, rsp.getCompacts().size());
    if (!useCleanerForAbortCleanup) {
      Assert.assertEquals(TxnStore.SUCCEEDED_RESPONSE, rsp.getCompacts().get(0).getState());
      Assert.assertEquals("cws", rsp.getCompacts().get(0).getTablename());
      Assert.assertEquals(CompactionType.MINOR, rsp.getCompacts().get(0).getType());
    }
  }

  @Test
  public void testCleanAbortCompactSeveralTables() throws Exception {
    String dbName = "default";
    String tblName1 = "cws1";
    String tblName2 = "cws2";
    boolean useCleanerForAbortCleanup = MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.COMPACTOR_CLEAN_ABORTS_USING_CLEANER);

    HiveStreamingConnection connection1 = prepareTableAndConnection(dbName, tblName1, 1);
    HiveStreamingConnection connection2 = prepareTableAndConnection(dbName, tblName2, 1);

    connection1.beginTransaction();
    connection1.write("1,1".getBytes());
    connection1.write("2,2".getBytes());
    connection1.abortTransaction();

    connection2.beginTransaction();
    connection2.write("1,1".getBytes());
    connection2.write("2,2".getBytes());
    connection2.abortTransaction();

    IMetaStoreClient msClient = new HiveMetaStoreClient(conf);
    FileSystem fs = FileSystem.get(conf);
    Table table1 = msClient.getTable(dbName, tblName1);
    FileStatus[] stat =
        fs.listStatus(new Path(table1.getSd().getLocation()));
    if (2 != stat.length) {
      Assert.fail("Expecting two directories corresponding to two partitions, FileStatus[] stat " + Arrays.toString(stat));
    }
    Table table2 = msClient.getTable(dbName, tblName2);
    stat = fs.listStatus(new Path(table2.getSd().getLocation()));
    if (2 != stat.length) {
      Assert.fail("Expecting two directories corresponding to two partitions, FileStatus[] stat " + Arrays.toString(stat));
    }

    int count = TestTxnDbUtil.countQueryAgent(conf, "select count(*) from TXN_COMPONENTS where TC_OPERATION_TYPE='i'");
    // We should have two rows corresponding to the two aborted transactions
    Assert.assertEquals(TestTxnDbUtil.queryToString(conf, "select * from TXN_COMPONENTS"), 2, count);

    runInitiator(conf);
    count = TestTxnDbUtil.countQueryAgent(conf, "select count(*) from COMPACTION_QUEUE");
    // Only one job is added to the queue per table. This job corresponds to all the entries for a particular table
    // with rows in TXN_COMPONENTS
    Assert.assertEquals(TestTxnDbUtil.queryToString(conf, "select * from COMPACTION_QUEUE"),
            useCleanerForAbortCleanup ? 0 : 2, count);

    runWorker(conf);
    runWorker(conf);

    runCleaner(conf);
    // After the cleaner runs TXN_COMPONENTS and COMPACTION_QUEUE should have zero rows, also the folders should have been deleted.
    count = TestTxnDbUtil.countQueryAgent(conf, "select count(*) from TXN_COMPONENTS");
    Assert.assertEquals(TestTxnDbUtil.queryToString(conf, "select * from TXN_COMPONENTS"), 0, count);

    count = TestTxnDbUtil.countQueryAgent(conf, "select count(*) from COMPACTION_QUEUE");
    Assert.assertEquals(TestTxnDbUtil.queryToString(conf, "select * from COMPACTION_QUEUE"), 0, count);

    RemoteIterator<LocatedFileStatus> it =
        fs.listFiles(new Path(table1.getSd().getLocation()), true);
    if (it.hasNext()) {
      Assert.fail("Expected cleaner to drop aborted delta & base directories, FileStatus[] stat " + Arrays.toString(stat));
    }

    connection1.close();
    connection2.close();
  }

  @Test
  public void testCleanAbortCorrectlyCleaned() throws Exception {
    // Test that at commit the tables are cleaned properly
    String dbName = "default";
    String tblName = "cws";
    HiveStreamingConnection connection = prepareTableAndConnection(dbName, tblName, 1);
    connection.beginTransaction();
    connection.write("1,1".getBytes());
    connection.write("2,2".getBytes());

    int count = TestTxnDbUtil.countQueryAgent(conf, "select count(*) from TXN_COMPONENTS where TC_OPERATION_TYPE='i'");
    // We should have 1 row corresponding to the aborted transaction
    Assert.assertEquals(TestTxnDbUtil.queryToString(conf, "select * from TXN_COMPONENTS"), 1, count);

    connection.commitTransaction();

    // After commit the row should have been deleted
    count = TestTxnDbUtil.countQueryAgent(conf, "select count(*) from TXN_COMPONENTS where TC_OPERATION_TYPE='i'");
    Assert.assertEquals(TestTxnDbUtil.queryToString(conf, "select * from TXN_COMPONENTS"), 0, count);
  }



  @Test
  public void testCleanDynPartAbortNoDataLoss() throws Exception {
    boolean useCleanerForAbortCleanup = MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.COMPACTOR_CLEAN_ABORTS_USING_CLEANER);
    String dbName = "default";
    String tblName = "cws";

    HiveStreamingConnection connection = prepareTableAndConnection(dbName, tblName, 1);

    executeStatementOnDriver("insert into " + tblName + " partition (a) values (1, '1')", driver);
    executeStatementOnDriver("update " + tblName + " set b='2' where a=1", driver);

    executeStatementOnDriver("insert into " + tblName + " partition (a) values (2, '2')", driver);
    executeStatementOnDriver("update " + tblName + " set b='3' where a=2", driver);

    connection.beginTransaction();
    connection.write("1,1".getBytes());
    connection.write("2,2".getBytes());
    connection.abortTransaction();

    executeStatementOnDriver("insert into " + tblName + " partition (a) values (3, '3')", driver);
    executeStatementOnDriver("update " + tblName + " set b='4' where a=3", driver);

    conf.setIntVar(HiveConf.ConfVars.HIVE_COMPACTOR_DELTA_NUM_THRESHOLD, 0);
    runInitiator(conf);

    int count = TestTxnDbUtil.countQueryAgent(conf, "select count(*) from COMPACTION_QUEUE");
    Assert.assertEquals(TestTxnDbUtil.queryToString(conf, "select * from COMPACTION_QUEUE"),
            useCleanerForAbortCleanup ? 3 : 4, count);

    runWorker(conf);
    runWorker(conf);
    runWorker(conf);
    runWorker(conf);

    // Cleaning should happen in threads concurrently for the minor compaction and the clean abort one.
    runCleaner(conf);

    count = TestTxnDbUtil.countQueryAgent(conf, "select count(*) from TXN_COMPONENTS");
    Assert.assertEquals(TestTxnDbUtil.queryToString(conf, "select * from TXN_COMPONENTS"), 0, count);

    IMetaStoreClient msClient = new HiveMetaStoreClient(conf);
    Partition p1 = msClient.getPartition(dbName, tblName, "a=1"),
      p2 = msClient.getPartition(dbName, tblName, "a=2"),
      p3 = msClient.getPartition(dbName, tblName, "a=3");
    msClient.close();

    FileSystem fs = FileSystem.get(conf);
    verifyDeltaCount(p1.getSd(), fs, 0);
    verifyHasBase(p1.getSd(), fs, "base_0000002_v0000010");
    verifyDeltaCount(p2.getSd(), fs, 0);
    verifyHasBase(p2.getSd(), fs, "base_0000004_v0000012");
    verifyDeltaCount(p3.getSd(), fs, 0);
    verifyHasBase(p3.getSd(), fs, "base_0000007_v0000014");
  }

  @Test
  public void testCleanAbortAndMinorCompact() throws Exception {
    String dbName = "default";
    String tblName = "cws";
    boolean useCleanerForAbortCleanup = MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.COMPACTOR_CLEAN_ABORTS_USING_CLEANER);

    HiveStreamingConnection connection = prepareTableAndConnection(dbName, tblName, 1);

    connection.beginTransaction();
    connection.write("1,1".getBytes());
    connection.write("2,2".getBytes());
    connection.abortTransaction();

    executeStatementOnDriver("insert into " + tblName + " partition (a) values (1, '1')", driver);
    executeStatementOnDriver("delete from " + tblName + " where b=1", driver);

    conf.setIntVar(HiveConf.ConfVars.HIVE_COMPACTOR_DELTA_NUM_THRESHOLD, 0);
    runInitiator(conf);

    int count = TestTxnDbUtil.countQueryAgent(conf, "select count(*) from COMPACTION_QUEUE");
    Assert.assertEquals(TestTxnDbUtil.queryToString(conf, "select * from COMPACTION_QUEUE"),
            useCleanerForAbortCleanup ? 1 : 2, count);

    runWorker(conf);
    runWorker(conf);

    // Cleaning should happen in threads concurrently for the minor compaction and the clean abort one.
    runCleaner(conf);

    count = TestTxnDbUtil.countQueryAgent(conf, "select count(*) from COMPACTION_QUEUE");
    Assert.assertEquals(TestTxnDbUtil.queryToString(conf, "select * from COMPACTION_QUEUE"), 0, count);

    count = TestTxnDbUtil.countQueryAgent(conf, "select count(*) from TXN_COMPONENTS");
    Assert.assertEquals(TestTxnDbUtil.queryToString(conf, "select * from TXN_COMPONENTS"), 0, count);

  }

  private HiveStreamingConnection prepareTableAndConnection(String dbName, String tblName, int batchSize) throws Exception {
    String agentInfo = "UT_" + Thread.currentThread().getName();

    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("CREATE TABLE " + tblName + "(b STRING) " +
        " PARTITIONED BY (a INT)" + //currently ACID requires table to be bucketed
        " STORED AS ORC  TBLPROPERTIES ('transactional'='true')", driver);

    StrictDelimitedInputWriter writer = StrictDelimitedInputWriter.newBuilder()
        .withFieldDelimiter(',')
        .build();

    // Create three folders with two different transactions
    return HiveStreamingConnection.newBuilder()
        .withDatabase(dbName)
        .withTable(tblName)
        .withAgentInfo(agentInfo)
        .withHiveConf(conf)
        .withRecordWriter(writer)
        .withStreamingOptimizations(true)
        // Transaction size has to be one or exception should happen.
        .withTransactionBatchSize(batchSize)
        .connect();
  }

  private HiveStreamingConnection prepareTableTwoPartitionsAndConnection(String dbName, String tblName, int batchSize) throws Exception {
    String agentInfo = "UT_" + Thread.currentThread().getName();

    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("CREATE TABLE " + tblName + "(c STRING) " +
        " PARTITIONED BY (a INT, b INT)" + //currently ACID requires table to be bucketed
        " STORED AS ORC  TBLPROPERTIES ('transactional'='true')", driver);

    StrictDelimitedInputWriter writer = StrictDelimitedInputWriter.newBuilder()
        .withFieldDelimiter(',')
        .build();

    // Create three folders with two different transactions
    return HiveStreamingConnection.newBuilder()
        .withDatabase(dbName)
        .withTable(tblName)
        .withAgentInfo(agentInfo)
        .withHiveConf(conf)
        .withRecordWriter(writer)
        .withStreamingOptimizations(true)
        // Transaction size has to be one or exception should happen.
        .withTransactionBatchSize(batchSize)
        .connect();
  }

  /**
   * There is a special case handled in Compaction Worker that will skip compaction
   * if there is only one valid delta. But this compaction will be still cleaned up, if there are aborted directories.
   * @see Worker.isEnoughToCompact
   * However if no compaction was done, deltas containing mixed aborted / committed writes from streaming can not be cleaned
   * and the metadata belonging to those aborted transactions can not be removed.
   * @throws Exception ex
   */
  @Test
  public void testSkippedCompactionCleanerKeepsAborted() throws Exception {
    String dbName = "default";
    String tblName = "cws";

    String agentInfo = "UT_" + Thread.currentThread().getName();
    TxnStore txnHandler = TxnUtils.getTxnStore(conf);

    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("CREATE TABLE " + tblName + "(b STRING) " +
        " PARTITIONED BY (a INT) STORED AS ORC  TBLPROPERTIES ('transactional'='true')", driver);
    executeStatementOnDriver("alter table " + tblName + " add partition(a=1)", driver);

    StrictDelimitedInputWriter writer = StrictDelimitedInputWriter.newBuilder()
        .withFieldDelimiter(',')
        .build();

    // Create initial aborted txn
    HiveStreamingConnection connection = HiveStreamingConnection.newBuilder()
        .withDatabase(dbName)
        .withTable(tblName)
        .withStaticPartitionValues(Collections.singletonList("1"))
        .withAgentInfo(agentInfo)
        .withHiveConf(conf)
        .withRecordWriter(writer)
        .withStreamingOptimizations(true)
        .withTransactionBatchSize(1)
        .connect();

    connection.beginTransaction();
    connection.write("3,1".getBytes());
    connection.write("4,1".getBytes());
    connection.abortTransaction();

    connection.close();

    // Create a sequence of commit, abort, commit to the same delta folder
    connection = HiveStreamingConnection.newBuilder()
        .withDatabase(dbName)
        .withTable(tblName)
        .withStaticPartitionValues(Collections.singletonList("1"))
        .withAgentInfo(agentInfo)
        .withHiveConf(conf)
        .withRecordWriter(writer)
        .withStreamingOptimizations(true)
        .withTransactionBatchSize(3)
        .connect();

    connection.beginTransaction();
    connection.write("1,1".getBytes());
    connection.write("2,1".getBytes());
    connection.commitTransaction();

    connection.beginTransaction();
    connection.write("3,1".getBytes());
    connection.write("4,1".getBytes());
    connection.abortTransaction();

    connection.beginTransaction();
    connection.write("5,1".getBytes());
    connection.write("6,1".getBytes());
    connection.commitTransaction();

    connection.close();

    // Check that aborted are not read back
    driver.run("select * from cws");
    List<?> res = new ArrayList<>();
    driver.getFetchTask().fetch(res);
    Assert.assertEquals(4, res.size());

    int count = TestTxnDbUtil.countQueryAgent(conf, "select count(*) from TXN_COMPONENTS");
    Assert.assertEquals("There should be 2 record for two aborted transaction", 2, count);

    // Start a compaction, that will be skipped, because only one valid delta is there
    driver.run("alter table cws partition(a='1') compact 'minor'");
    runWorker(conf);
    // Cleaner should not delete info about aborted txn 2
    runCleaner(conf);
    txnHandler.cleanEmptyAbortedAndCommittedTxns();
    count = TestTxnDbUtil.countQueryAgent(conf, "select count(*) from TXN_COMPONENTS");
    Assert.assertEquals("There should be 1 record for the second aborted transaction", 1, count);

    driver.run("select * from cws");
    res.clear();
    driver.getFetchTask().fetch(res);
    Assert.assertEquals(4, res.size());

  }

  @Test
  public void mmTable() throws Exception {
    String dbName = "default";
    String tblName = "mm_nonpart";
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("CREATE TABLE " + tblName + "(a INT, b STRING) STORED AS ORC" +
        " TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only')",
        driver);
    IMetaStoreClient msClient = new HiveMetaStoreClient(conf);
    Table table = msClient.getTable(dbName, tblName);
    msClient.close();

    executeStatementOnDriver("INSERT INTO " + tblName +"(a,b) VALUES(1, 'foo')", driver);
    executeStatementOnDriver("INSERT INTO " + tblName +"(a,b) VALUES(2, 'bar')", driver);

    verifyFooBarResult(tblName, 1);

    // Check that we have two deltas.
    FileSystem fs = FileSystem.get(conf);
    verifyDeltaCount(table.getSd(), fs, 2);

    runMajorCompaction(dbName, tblName);
    verifyFooBarResult(tblName, 1);
    verifyHasBase(table.getSd(), fs, "base_0000002_v0000006");

    // Make sure we don't compact if we don't need to compact.
    runMajorCompaction(dbName, tblName);
    verifyFooBarResult(tblName, 1);
    verifyHasBase(table.getSd(), fs, "base_0000002_v0000006");
  }

  @Test public void mmTableOriginalsMajorOrc() throws Exception {
    mmTableOriginalsMajor("orc", true);
  }

  @Test public void mmTableOriginalsMajorText() throws Exception {
    mmTableOriginalsMajor("textfile", false);
  }

  /**
   * Major compact an mm table that contains original files.
   */
  private void mmTableOriginalsMajor(String format, boolean allowOriginals) throws Exception {
    driver.getConf().setBoolVar(ConfVars.HIVE_MM_ALLOW_ORIGINALS, allowOriginals);
    String dbName = "default";
    String tblName = "mm_nonpart";
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("CREATE TABLE " + tblName + "(a INT, b STRING) STORED AS " + format
        + " TBLPROPERTIES ('transactional'='false')", driver);
    Table table = msClient.getTable(dbName, tblName);

    executeStatementOnDriver("INSERT INTO " + tblName + "(a,b) VALUES(1, 'foo')", driver);
    executeStatementOnDriver("INSERT INTO " + tblName + " (a,b) VALUES(2, 'bar')", driver);
    executeStatementOnDriver("INSERT INTO " + tblName + "(a,b) SELECT a,b FROM " + tblName
        + " UNION ALL SELECT a,b FROM " + tblName, driver);

    verifyFooBarResult(tblName, 3);

    FileSystem fs = FileSystem.get(conf);
    executeStatementOnDriver("ALTER TABLE " + tblName + " SET TBLPROPERTIES "
        + "('transactional'='true', 'transactional_properties'='insert_only')", driver);

    if (allowOriginals) {
      verifyDeltaCount(table.getSd(), fs, 0);
      verifyFooBarResult(tblName, 3);

      runMajorCompaction(dbName, tblName);
      verifyFooBarResult(tblName, 3);
      verifyHasBase(table.getSd(), fs, "base_0000001_v0000009");
    } else {
      verifyDeltaCount(table.getSd(), fs, 1);
      // 1 delta dir won't be compacted. Skip testing major compaction.
    }
  }

  @Test public void mmMajorOriginalsDeltasOrc() throws Exception {
    mmMajorOriginalsDeltas("orc", true);
  }

  @Test public void mmMajorOriginalsDeltasText() throws Exception {
    mmMajorOriginalsDeltas("textfile", false);
  }

  /**
   * Major compact an mm table with both original and delta files.
   */
  private void mmMajorOriginalsDeltas(String format, boolean allowOriginals) throws Exception {
    driver.getConf().setBoolVar(ConfVars.HIVE_MM_ALLOW_ORIGINALS, allowOriginals);
    String dbName = "default";
    String tblName = "mm_nonpart";
    FileSystem fs = FileSystem.get(conf);
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("CREATE TABLE " + tblName + "(a INT, b STRING) STORED AS " + format
        + " TBLPROPERTIES ('transactional'='false')", driver);
    Table table = msClient.getTable(dbName, tblName);

    executeStatementOnDriver("INSERT INTO " + tblName + "(a,b) VALUES(1, 'foo')", driver);
    executeStatementOnDriver("INSERT INTO " + tblName + " (a,b) VALUES(2, 'bar')", driver);
    executeStatementOnDriver("INSERT INTO " + tblName + "(a,b) SELECT a,b FROM " + tblName
        + " UNION ALL SELECT a,b FROM " + tblName, driver);
    verifyFooBarResult(tblName, 3);

    executeStatementOnDriver("ALTER TABLE " + tblName + " SET TBLPROPERTIES "
        + "('transactional'='true', 'transactional_properties'='insert_only')", driver);
    executeStatementOnDriver("INSERT INTO " + tblName + "(a,b) SELECT a,b FROM " + tblName
        + " UNION ALL SELECT a,b FROM " + tblName, driver);

    verifyFooBarResult(tblName, 9);

    runMajorCompaction(dbName, tblName);
    verifyFooBarResult(tblName, 9);
    verifyHasBase(table.getSd(), fs, "base_0000002_v0000010");
  }

  @Test public void mmMajorOriginalsBaseOrc() throws Exception {
    mmMajorOriginalsBase("orc", true);
  }

  @Test public void mmMajorOriginalsBaseText() throws Exception {
    mmMajorOriginalsBase("textfile", false);
  }

  /**
   * Insert overwrite and major compact an mm table with only original files.
   *
   * @param format file format for table
   * @throws Exception
   */
  private void mmMajorOriginalsBase(String format, boolean allowOriginals) throws Exception {
    driver.getConf().setBoolVar(ConfVars.HIVE_MM_ALLOW_ORIGINALS, allowOriginals);
    // Try with an extra base.
    String dbName = "default";
    String tblName = "mm_nonpart";
    FileSystem fs = FileSystem.get(conf);
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("CREATE TABLE " + tblName + "(a INT, b STRING) STORED AS " + format
        + " TBLPROPERTIES ('transactional'='false')", driver);
    Table table = msClient.getTable(dbName, tblName);

    executeStatementOnDriver("INSERT INTO " + tblName + "(a,b) VALUES(1, 'foo')", driver);
    executeStatementOnDriver("INSERT INTO " + tblName + " (a,b) VALUES(2, 'bar')", driver);
    executeStatementOnDriver("INSERT INTO " + tblName + "(a,b) SELECT a,b FROM " + tblName
        + " UNION ALL SELECT a,b FROM " + tblName, driver);
    verifyFooBarResult(tblName, 3);

    executeStatementOnDriver("ALTER TABLE " + tblName + " SET TBLPROPERTIES "
        + "('transactional'='true', 'transactional_properties'='insert_only')", driver);
    executeStatementOnDriver("INSERT OVERWRITE TABLE " + tblName + " SELECT a,b FROM " + tblName
        + " UNION ALL SELECT a,b FROM " + tblName, driver);
    verifyFooBarResult(tblName, 6);

    runMajorCompaction(dbName, tblName);
    verifyFooBarResult(tblName, 6);
    verifyHasBase(table.getSd(), fs, "base_0000002");
  }

  @Test
  public void mmTableBucketed() throws Exception {
    String dbName = "default";
    String tblName = "mm_nonpart";
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("CREATE TABLE " + tblName + "(a INT, b STRING) CLUSTERED BY (a) " +
        "INTO 64 BUCKETS STORED AS ORC TBLPROPERTIES ('transactional'='true', " +
        "'transactional_properties'='insert_only')", driver);
    IMetaStoreClient msClient = new HiveMetaStoreClient(conf);
    Table table = msClient.getTable(dbName, tblName);
    msClient.close();

    executeStatementOnDriver("INSERT INTO " + tblName +"(a,b) VALUES(1, 'foo')", driver);
    executeStatementOnDriver("INSERT INTO " + tblName +"(a,b) VALUES(2, 'bar')", driver);

    verifyFooBarResult(tblName, 1);

    // Check that we have two deltas.
    FileSystem fs = FileSystem.get(conf);
    verifyDeltaCount(table.getSd(), fs, 2);

    runMajorCompaction(dbName, tblName);
    verifyFooBarResult(tblName, 1);
    String baseDir = "base_0000002_v0000006";
    verifyHasBase(table.getSd(), fs, baseDir);

    FileStatus[] files = fs.listStatus(new Path(table.getSd().getLocation(), baseDir),
        FileUtils.HIDDEN_FILES_PATH_FILTER);
    Assert.assertEquals(Lists.newArrayList(files).toString(), 64, files.length);
  }

  @Test
  public void mmTableOpenWriteId() throws Exception {
    String dbName = "default";
    String tblName = "mm_nonpart";
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("CREATE TABLE " + tblName + "(a INT, b STRING) STORED AS TEXTFILE" +
        " TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only')",
        driver);
    IMetaStoreClient msClient = new HiveMetaStoreClient(conf);
    Table table = msClient.getTable(dbName, tblName);
    msClient.close();

    executeStatementOnDriver("INSERT INTO " + tblName +"(a,b) VALUES(1, 'foo')", driver);
    executeStatementOnDriver("INSERT INTO " + tblName +"(a,b) VALUES(2, 'bar')", driver);

    verifyFooBarResult(tblName, 1);

    long openTxnId = msClient.openTxn("test");
    long openWriteId = msClient.allocateTableWriteId(openTxnId, dbName, tblName);
    Assert.assertEquals(3, openWriteId); // Just check to make sure base_5 below is not new.

    executeStatementOnDriver("INSERT INTO " + tblName +"(a,b) VALUES(1, 'foo')", driver);
    executeStatementOnDriver("INSERT INTO " + tblName +"(a,b) VALUES(2, 'bar')", driver);

    verifyFooBarResult(tblName, 2);

    runMajorCompaction(dbName, tblName); // Don't compact 4 and 5; 3 is opened.
    FileSystem fs = FileSystem.get(conf);
    verifyHasBase(table.getSd(), fs, "base_0000002_v0000010");
    verifyDirCount(table.getSd(), fs, 1, AcidUtils.baseFileFilter);
    verifyFooBarResult(tblName, 2);

    runCleaner(conf);
    verifyHasDir(table.getSd(), fs, "delta_0000004_0000004_0000", AcidUtils.deltaFileFilter);
    verifyHasDir(table.getSd(), fs, "delta_0000005_0000005_0000", AcidUtils.deltaFileFilter);
    verifyFooBarResult(tblName, 2);

    msClient.abortTxns(Lists.newArrayList(openTxnId)); // Now abort 3.
    runMajorCompaction(dbName, tblName); // Compact 4 and 5.
    verifyFooBarResult(tblName, 2);
    verifyHasBase(table.getSd(), fs, "base_0000005_v0000017");
    runCleaner(conf);
    // in case when we have # of accumulated entries for the same table/partition - we need to process them one-by-one in ASC order of write_id's,
    // however, to support multi-threaded processing in the Cleaner, we have to move entries from the same group to the next Cleaner cycle,
    // so that they are not processed by multiple threads concurrently.
    runCleaner(conf);
    verifyDeltaCount(table.getSd(), fs, 0);
  }

  private void verifyHasBase(
      StorageDescriptor sd, FileSystem fs, String baseName) throws Exception {
    verifyHasDir(sd, fs, baseName, AcidUtils.baseFileFilter);
  }

  private void verifyHasDir(
      StorageDescriptor sd, FileSystem fs, String name, PathFilter filter) throws Exception {
    FileStatus[] stat = fs.listStatus(new Path(sd.getLocation()), filter);
    for (FileStatus file : stat) {
      if (name.equals(file.getPath().getName())) {
        return;
      }
    }
    Assert.fail("Cannot find " + name + ": " + Arrays.toString(stat));
  }

  private void verifyDeltaCount(
      StorageDescriptor sd, FileSystem fs, int count) throws Exception {
    verifyDirCount(sd, fs, count, AcidUtils.deltaFileFilter);
  }

  private void verifyDirCount(
      StorageDescriptor sd, FileSystem fs, int count, PathFilter filter) throws Exception {
    FileStatus[] stat = fs.listStatus(new Path(sd.getLocation()), filter);
    Assert.assertEquals(Arrays.toString(stat), count, stat.length);
  }

  @Test
  public void mmTablePartitioned() throws Exception {
    String dbName = "default";
    String tblName = "mm_part";
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("CREATE TABLE " + tblName + "(a INT, b STRING) " +
        " PARTITIONED BY(ds int) STORED AS TEXTFILE" +
        " TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only')",
        driver);

    executeStatementOnDriver("INSERT INTO " + tblName + " partition (ds) VALUES(1, 'foo', 1)", driver);
    executeStatementOnDriver("INSERT INTO " + tblName + " partition (ds) VALUES(1, 'foo', 1)", driver);
    executeStatementOnDriver("INSERT INTO " + tblName + " partition (ds) VALUES(2, 'bar', 1)", driver);
    executeStatementOnDriver("INSERT INTO " + tblName + " partition (ds) VALUES(1, 'foo', 2)", driver);
    executeStatementOnDriver("INSERT INTO " + tblName + " partition (ds) VALUES(2, 'bar', 2)", driver);
    executeStatementOnDriver("INSERT INTO " + tblName + " partition (ds) VALUES(2, 'bar', 3)", driver);

    verifyFooBarResult(tblName, 3);

    IMetaStoreClient msClient = new HiveMetaStoreClient(conf);
    Partition p1 = msClient.getPartition(dbName, tblName, "ds=1"),
        p2 = msClient.getPartition(dbName, tblName, "ds=2"),
        p3 = msClient.getPartition(dbName, tblName, "ds=3");
    msClient.close();

    FileSystem fs = FileSystem.get(conf);
    verifyDeltaCount(p1.getSd(), fs, 3);
    verifyDeltaCount(p2.getSd(), fs, 2);
    verifyDeltaCount(p3.getSd(), fs, 1);

    runMajorCompaction(dbName, tblName, "ds=1", "ds=2", "ds=3");

    verifyFooBarResult(tblName, 3);
    verifyDeltaCount(p3.getSd(), fs, 1);
    verifyHasBase(p1.getSd(), fs, "base_0000006_v0000010");
    verifyHasBase(p2.getSd(), fs, "base_0000006_v0000015");

    executeStatementOnDriver("INSERT INTO " + tblName + " partition (ds) VALUES(1, 'foo', 2)", driver);
    executeStatementOnDriver("INSERT INTO " + tblName + " partition (ds) VALUES(2, 'bar', 2)", driver);

    runMajorCompaction(dbName, tblName, "ds=1", "ds=2", "ds=3");

    // Make sure we don't compact if we don't need to compact; but do if we do.
    verifyFooBarResult(tblName, 4);
    verifyDeltaCount(p3.getSd(), fs, 1);
    verifyHasBase(p1.getSd(), fs, "base_0000006_v0000010");
    verifyHasBase(p2.getSd(), fs, "base_0000006_v0000015");

  }

  private void verifyFooBarResult(String tblName, int count) throws Exception {
    List<String> valuesReadFromHiveDriver = new ArrayList<>();
    executeStatementOnDriver("SELECT a,b FROM " + tblName, driver);
    driver.getResults(valuesReadFromHiveDriver);
    Assert.assertEquals(2 * count, valuesReadFromHiveDriver.size());
    int fooCount = 0, barCount = 0;
    for (String s : valuesReadFromHiveDriver) {
      if ("1\tfoo".equals(s)) {
        ++fooCount;
      } else if ("2\tbar".equals(s)) {
        ++barCount;
      } else {
        Assert.fail("Unexpected " + s);
      }
    }
    Assert.assertEquals(fooCount, count);
    Assert.assertEquals(barCount, count);
  }

  private void runMajorCompaction(
      String dbName, String tblName, String... partNames) throws Exception {
    TxnStore txnHandler = TxnUtils.getTxnStore(conf);
    Worker t = new Worker();
    t.setConf(conf);
    t.init(new AtomicBoolean(true));
    if (partNames.length == 0) {
      txnHandler.compact(new CompactionRequest(dbName, tblName, CompactionType.MAJOR));
      t.run();
    } else {
      for (String partName : partNames) {
        CompactionRequest cr = new CompactionRequest(dbName, tblName, CompactionType.MAJOR);
        cr.setPartitionname(partName);
        txnHandler.compact(cr);
        t.run();
      }
    }
  }

  @Test
  public void majorCompactWhileStreamingForSplitUpdate() throws Exception {
    String dbName = "default";
    String tblName = "cws";
    String columnNamesProperty = "a,b";
    String columnTypesProperty = "int:string";
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("CREATE TABLE " + tblName + "(a INT, b STRING) " +
      " CLUSTERED BY(a) INTO 2 BUCKETS" + //currently ACID requires table to be bucketed
      " STORED AS ORC  TBLPROPERTIES ('transactional'='true', "
      + "'transactional_properties'='default') ", driver); // this turns on split-update U=D+I

    // Write a couple of batches
    for (int i = 0; i < 2; i++) {
      CompactorTestUtil.writeBatch(conf, dbName, tblName, false, false);
    }

    // Start a third batch, but don't close it.
    StreamingConnection connection1 = CompactorTestUtil.writeBatch(conf, dbName, tblName, false, true);

    runMajorCompaction(dbName, tblName);

    // Find the location of the table
    IMetaStoreClient msClient = new HiveMetaStoreClient(conf);
    Table table = msClient.getTable(dbName, tblName);
    FileSystem fs = FileSystem.get(conf);
    FileStatus[] stat =
      fs.listStatus(new Path(table.getSd().getLocation()), AcidUtils.baseFileFilter);
    if (1 != stat.length) {
      Assert.fail("Expecting 1 file \"base_0000004\" and found " + stat.length + " files " + Arrays.toString(stat));
    }
    String name = stat[0].getPath().getName();
    Assert.assertEquals("base_0000004_v0000009", name);
    CompactorTestUtil
        .checkExpectedTxnsPresent(stat[0].getPath(), null, columnNamesProperty, columnTypesProperty, 1, 1L, 4L, null,
            2);
    if (connection1 != null) {
      connection1.close();
    }
  }

  @Test
  public void testMinorCompactionForSplitUpdateWithInsertsAndDeletes() throws Exception {
    String dbName = "default";
    String tblName = "cws";
    String columnNamesProperty = "a,b";
    String columnTypesProperty = "int:string";
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("CREATE TABLE " + tblName + "(a INT, b STRING) " +
      " CLUSTERED BY(a) INTO 1 BUCKETS" + //currently ACID requires table to be bucketed
      " STORED AS ORC  TBLPROPERTIES ('transactional'='true',"
      + "'transactional_properties'='default')", driver);

    // Insert some data -> this will generate only insert deltas and no delete deltas: delta_3_3
    executeStatementOnDriver("INSERT INTO " + tblName + "(a,b) VALUES(1, 'foo')", driver);

    // Insert some data -> this will again generate only insert deltas and no delete deltas: delta_4_4
    executeStatementOnDriver("INSERT INTO " + tblName + "(a,b) VALUES(2, 'bar')", driver);

    // Delete some data -> this will generate only delete deltas and no insert deltas: delete_delta_5_5
    executeStatementOnDriver("DELETE FROM " + tblName + " WHERE a = 2", driver);

    // Now, compact -> Compaction produces a single range for both delta and delete delta
    // That is, both delta and delete_deltas would be compacted into delta_3_5 and delete_delta_3_5
    // even though there are only two delta_3_3, delta_4_4 and one delete_delta_5_5.
    TxnStore txnHandler = TxnUtils.getTxnStore(conf);
    txnHandler.compact(new CompactionRequest(dbName, tblName, CompactionType.MINOR));
    runWorker(conf);

    // Find the location of the table
    IMetaStoreClient msClient = new HiveMetaStoreClient(conf);
    Table table = msClient.getTable(dbName, tblName);
    FileSystem fs = FileSystem.get(conf);

    // Verify that we have got correct set of deltas.
    FileStatus[] stat =
      fs.listStatus(new Path(table.getSd().getLocation()), AcidUtils.deltaFileFilter);
    String[] deltas = new String[stat.length];
    Path minorCompactedDelta = null;
    for (int i = 0; i < deltas.length; i++) {
      deltas[i] = stat[i].getPath().getName();
      if (deltas[i].equals("delta_0000001_0000003_v0000006")) {
        minorCompactedDelta = stat[i].getPath();
      }
    }
    Arrays.sort(deltas);
    String[] expectedDeltas = new String[]{"delta_0000001_0000001_0000", "delta_0000001_0000003_v0000006",
      "delta_0000002_0000002_0000"};
    if (!Arrays.deepEquals(expectedDeltas, deltas)) {
      Assert.fail("Expected: " + Arrays.toString(expectedDeltas) + ", found: " + Arrays.toString(deltas));
    }
    CompactorTestUtil
        .checkExpectedTxnsPresent(null, new Path[] {minorCompactedDelta}, columnNamesProperty, columnTypesProperty, 0,
            1L, 2L, null, 1);

    // Verify that we have got correct set of delete_deltas.
    FileStatus[] deleteDeltaStat =
      fs.listStatus(new Path(table.getSd().getLocation()), AcidUtils.deleteEventDeltaDirFilter);
    String[] deleteDeltas = new String[deleteDeltaStat.length];
    Path minorCompactedDeleteDelta = null;
    for (int i = 0; i < deleteDeltas.length; i++) {
      deleteDeltas[i] = deleteDeltaStat[i].getPath().getName();
      if (deleteDeltas[i].equals("delete_delta_0000001_0000003_v0000006")) {
        minorCompactedDeleteDelta = deleteDeltaStat[i].getPath();
      }
    }
    Arrays.sort(deleteDeltas);
    String[] expectedDeleteDeltas = new String[]{"delete_delta_0000001_0000003_v0000006", "delete_delta_0000003_0000003_0000"};
    if (!Arrays.deepEquals(expectedDeleteDeltas, deleteDeltas)) {
      Assert.fail("Expected: " + Arrays.toString(expectedDeleteDeltas) + ", found: " + Arrays.toString(deleteDeltas));
    }
    CompactorTestUtil.checkExpectedTxnsPresent(null, new Path[] {minorCompactedDeleteDelta}, columnNamesProperty,
        columnTypesProperty, 0, 2L, 2L, null, 1);
  }

  @Test
  public void testMinorCompactionForSplitUpdateWithOnlyInserts() throws Exception {
    String dbName = "default";
    String tblName = "cws";
    String columnNamesProperty = "a,b";
    String columnTypesProperty = "int:string";
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("CREATE TABLE " + tblName + "(a INT, b STRING) " +
      " CLUSTERED BY(a) INTO 1 BUCKETS" + //currently ACID requires table to be bucketed
      " STORED AS ORC  TBLPROPERTIES ('transactional'='true',"
      + "'transactional_properties'='default')", driver);

    // Insert some data -> this will generate only insert deltas and no delete deltas: delta_1_1
    executeStatementOnDriver("INSERT INTO " + tblName + "(a,b) VALUES(1, 'foo')", driver);

    // Insert some data -> this will again generate only insert deltas and no delete deltas: delta_2_2
    executeStatementOnDriver("INSERT INTO " + tblName + "(a,b) VALUES(2, 'bar')", driver);

    // Now, compact
    // One important thing to note in this test is that minor compaction always produces
    // delta_x_y and a counterpart delete_delta_x_y, even when there are no delete_delta events.
    // Such a choice has been made to simplify processing of AcidUtils.getAcidState().

    TxnStore txnHandler = TxnUtils.getTxnStore(conf);
    txnHandler.compact(new CompactionRequest(dbName, tblName, CompactionType.MINOR));
    runWorker(conf);

    // Find the location of the table
    IMetaStoreClient msClient = new HiveMetaStoreClient(conf);
    Table table = msClient.getTable(dbName, tblName);
    FileSystem fs = FileSystem.get(conf);

    // Verify that we have got correct set of deltas.
    FileStatus[] stat =
      fs.listStatus(new Path(table.getSd().getLocation()), AcidUtils.deltaFileFilter);
    String[] deltas = new String[stat.length];
    Path minorCompactedDelta = null;
    for (int i = 0; i < deltas.length; i++) {
      deltas[i] = stat[i].getPath().getName();
      if (deltas[i].equals("delta_0000001_0000002_v0000005")) {
        minorCompactedDelta = stat[i].getPath();
      }
    }
    Arrays.sort(deltas);
    String[] expectedDeltas = new String[]{"delta_0000001_0000001_0000", "delta_0000001_0000002_v0000005",
      "delta_0000002_0000002_0000"};
    if (!Arrays.deepEquals(expectedDeltas, deltas)) {
      Assert.fail("Expected: " + Arrays.toString(expectedDeltas) + ", found: " + Arrays.toString(deltas));
    }
    CompactorTestUtil
        .checkExpectedTxnsPresent(null, new Path[] {minorCompactedDelta}, columnNamesProperty, columnTypesProperty, 0,
            1L, 2L, null, 1);

    //Assert that we have no delete deltas if there are no input delete events.
    FileStatus[] deleteDeltaStat =
      fs.listStatus(new Path(table.getSd().getLocation()), AcidUtils.deleteEventDeltaDirFilter);
    assertEquals(0, deleteDeltaStat.length);
  }

  @Test
  public void testCompactionForFileInSratchDir() throws Exception {
    String dbName = "default";
    String tblName = "cfs";
    String createQuery = "CREATE TABLE " + tblName + "(a INT, b STRING) " + "STORED AS ORC  TBLPROPERTIES ('transactional'='true',"
            + "'transactional_properties'='default')";
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver(createQuery, driver);



    // Insert some data -> this will generate only insert deltas
    executeStatementOnDriver("INSERT INTO " + tblName + "(a,b) VALUES(1, 'foo')", driver);

    // Insert some data -> this will again generate only insert deltas
    executeStatementOnDriver("INSERT INTO " + tblName + "(a,b) VALUES(2, 'bar')", driver);

    // Find the location of the table
    IMetaStoreClient msClient = new HiveMetaStoreClient(conf);
    Table table = msClient.getTable(dbName, tblName);
    FileSystem fs = FileSystem.get(conf);

    Map<String, String> tblProperties = new HashMap<>();
    tblProperties.put("compactor.hive.compactor.input.tmp.dir",table.getSd().getLocation() + "/" + "_tmp");

    //Create empty file in ScratchDir under table location
    String scratchDirPath = table.getSd().getLocation() + "/" + "_tmp";
    Path dir = new Path(scratchDirPath + "/base_0000002_v0000005");
    fs.mkdirs(dir);
    Path emptyFile = AcidUtils.createBucketFile(dir, 0);
    fs.create(emptyFile);

    //Run MajorCompaction
    TxnStore txnHandler = TxnUtils.getTxnStore(conf);
    Worker t = new Worker();
    t.setConf(conf);
    t.init(new AtomicBoolean(true));
    CompactionRequest Cr = new CompactionRequest(dbName, tblName, CompactionType.MAJOR);
    Cr.setProperties(tblProperties);
    txnHandler.compact(Cr);
    t.run();

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(1, rsp.getCompacts().size());
    Assert.assertEquals(TxnStore.CLEANING_RESPONSE, rsp.getCompacts().get(0).getState());

  }

  @Test
  public void minorCompactWhileStreamingWithSplitUpdate() throws Exception {
    String dbName = "default";
    String tblName = "cws";
    String columnNamesProperty = "a,b";
    String columnTypesProperty = "int:string";
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("CREATE TABLE " + tblName + "(a INT, b STRING) " +
      " CLUSTERED BY(a) INTO 1 BUCKETS" + //currently ACID requires table to be bucketed
      " STORED AS ORC  TBLPROPERTIES ('transactional'='true',"
      + "'transactional_properties'='default')", driver);

    // Write a couple of batches
    for (int i = 0; i < 2; i++) {
      CompactorTestUtil.writeBatch(conf, dbName, tblName, false, false);
    }

    // Start a third batch, but don't close it.
    StreamingConnection connection1 = CompactorTestUtil.writeBatch(conf, dbName, tblName, false, true);
    // Now, compact
    TxnStore txnHandler = TxnUtils.getTxnStore(conf);
    txnHandler.compact(new CompactionRequest(dbName, tblName, CompactionType.MINOR));
    runWorker(conf);

    // Find the location of the table
    IMetaStoreClient msClient = new HiveMetaStoreClient(conf);
    Table table = msClient.getTable(dbName, tblName);
    FileSystem fs = FileSystem.get(conf);
    FileStatus[] stat =
      fs.listStatus(new Path(table.getSd().getLocation()), AcidUtils.deltaFileFilter);
    String[] names = new String[stat.length];
    Path resultFile = null;
    for (int i = 0; i < names.length; i++) {
      names[i] = stat[i].getPath().getName();
      if (names[i].equals("delta_0000001_0000004_v0000009")) {
        resultFile = stat[i].getPath();
      }
    }
    Arrays.sort(names);
    String[] expected = new String[]{"delta_0000001_0000002",
      "delta_0000001_0000004_v0000009", "delta_0000003_0000004", "delta_0000005_0000006"};
    if (!Arrays.deepEquals(expected, names)) {
      Assert.fail("Expected: " + Arrays.toString(expected) + ", found: " + Arrays.toString(names));
    }
    CompactorTestUtil
        .checkExpectedTxnsPresent(null, new Path[] {resultFile}, columnNamesProperty, columnTypesProperty, 0, 1L, 4L,
            null, 1);

    //Assert that we have no delete deltas if there are no input delete events.
    FileStatus[] deleteDeltaStat =
      fs.listStatus(new Path(table.getSd().getLocation()), AcidUtils.deleteEventDeltaDirFilter);
    assertEquals(0, deleteDeltaStat.length);

    if (connection1 != null) {
      connection1.close();
    }
  }

  @Test
  public void testCompactorGatherStats() throws Exception {
    String dbName = "default";
    String tableName = "stats_comp_test";
    List<String> colNames = Collections.singletonList("a");
    executeStatementOnDriver("drop table if exists " + dbName + "." + tableName, driver);
    executeStatementOnDriver("create table " + dbName + "." + tableName +
        " (a INT) STORED AS ORC TBLPROPERTIES ('transactional'='true')", driver);
    executeStatementOnDriver("insert into " + dbName + "." + tableName + " values(1)", driver);
    executeStatementOnDriver("insert into " + dbName + "." + tableName + " values(1)", driver);

    TxnStore txnHandler = TxnUtils.getTxnStore(conf);
    txnHandler.compact(new CompactionRequest(dbName, tableName, CompactionType.MAJOR));
    runWorker(conf);

    // Make sure we do not have statistics for this table yet
    // Compaction generates stats only if there is any
    List<ColumnStatisticsObj> colStats = msClient.getTableColumnStatistics(dbName,
        tableName, colNames, Constants.HIVE_ENGINE);
    assertEquals("No stats should be there for the table", 0, colStats.size());

    executeStatementOnDriver("analyze table " + dbName + "." + tableName + " compute statistics for columns", driver);
    executeStatementOnDriver("insert into " + dbName + "." + tableName + " values(2)", driver);

    // Make sure we have old statistics for the table
    colStats = msClient.getTableColumnStatistics(dbName, tableName, colNames, Constants.HIVE_ENGINE);
    assertEquals("Stats should be there", 1, colStats.size());
    assertEquals("Value should contain old data", 1, colStats.get(0).getStatsData().getLongStats().getHighValue());
    assertEquals("Value should contain old data", 1, colStats.get(0).getStatsData().getLongStats().getLowValue());

    txnHandler.compact(new CompactionRequest(dbName, tableName, CompactionType.MAJOR));
    runWorker(conf);
    // Make sure the statistics is updated for the table
    colStats = msClient.getTableColumnStatistics(dbName, tableName, colNames, Constants.HIVE_ENGINE);
    assertEquals("Stats should be there", 1, colStats.size());
    assertEquals("Value should contain new data", 2, colStats.get(0).getStatsData().getLongStats().getHighValue());
    assertEquals("Value should contain new data", 1, colStats.get(0).getStatsData().getLongStats().getLowValue());
  }

  /**
   * Users have the choice of specifying compaction related tblproperties either in CREATE TABLE
   * statement or in ALTER TABLE .. COMPACT statement. This tests both cases.
   */
  @Test
  public void testTableProperties() throws Exception {
    conf.setVar(HiveConf.ConfVars.COMPACTOR_JOB_QUEUE, "root.user1");
    String tblName1 = "ttp1"; // plain acid table
    String tblName2 = "ttp2"; // acid table with customized tblproperties
    executeStatementOnDriver("drop table if exists " + tblName1, driver);
    executeStatementOnDriver("drop table if exists " + tblName2, driver);
    executeStatementOnDriver("CREATE TABLE " + tblName1 + "(a INT, b STRING) " +
      " CLUSTERED BY(a) INTO 2 BUCKETS STORED AS ORC" +
      " TBLPROPERTIES ('transactional'='true', 'orc.compress.size'='2700')", driver);
    executeStatementOnDriver("CREATE TABLE " + tblName2 + "(a INT, b STRING) " +
      " CLUSTERED BY(a) INTO 2 BUCKETS STORED AS ORC TBLPROPERTIES (" +
      "'transactional'='true'," +
      "'compactor.mapreduce.map.memory.mb'='2048'," + // 2048 MB memory for compaction map job
      "'compactorthreshold.hive.compactor.delta.num.threshold'='4'," +  // minor compaction if more than 4 delta dirs
      "'compactorthreshold.hive.compactor.delta.pct.threshold'='0.47'," + // major compaction if more than 47%
      "'compactor.hive.compactor.job.queue'='root.user2'" + // Override the system wide compactor queue for this table
      ")", driver);

    // Insert 5 rows to both tables
    executeStatementOnDriver("insert into " + tblName1 + " values (1, 'a')", driver);
    executeStatementOnDriver("insert into " + tblName1 + " values (2, 'b')", driver);
    executeStatementOnDriver("insert into " + tblName1 + " values (3, 'c')", driver);
    executeStatementOnDriver("insert into " + tblName1 + " values (4, 'd')", driver);
    executeStatementOnDriver("insert into " + tblName1 + " values (5, 'e')", driver);

    executeStatementOnDriver("insert into " + tblName2 + " values (1, 'a')", driver);
    executeStatementOnDriver("insert into " + tblName2 + " values (2, 'b')", driver);
    executeStatementOnDriver("insert into " + tblName2 + " values (3, 'c')", driver);
    executeStatementOnDriver("insert into " + tblName2 + " values (4, 'd')", driver);
    executeStatementOnDriver("insert into " + tblName2 + " values (5, 'e')", driver);

    runInitiator(conf);

    // Compactor should only schedule compaction for ttp2 (delta.num.threshold=4), not ttp1
    TxnStore txnHandler = TxnUtils.getTxnStore(conf);
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(1, rsp.getCompacts().size());
    Assert.assertEquals(TxnStore.INITIATED_RESPONSE, rsp.getCompacts().get(0).getState());
    Assert.assertEquals("ttp2", rsp.getCompacts().get(0).getTablename());
    Assert.assertEquals(CompactionType.MAJOR,
      rsp.getCompacts().get(0).getType()); // type is MAJOR since there's no base yet

    // Finish the scheduled compaction for ttp2, and manually compact ttp1, to make them comparable again
    executeStatementOnDriver("alter table " + tblName1 + " compact 'major'", driver);
    rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(2, rsp.getCompacts().size());
    Assert.assertEquals("ttp2", rsp.getCompacts().get(0).getTablename());
    Assert.assertEquals(TxnStore.INITIATED_RESPONSE, rsp.getCompacts().get(0).getState());
    Assert.assertEquals("ttp1", rsp.getCompacts().get(1).getTablename());
    Assert.assertEquals(TxnStore.INITIATED_RESPONSE, rsp.getCompacts().get(1).getState());
    // compact ttp2, by running the Worker explicitly, in order to get the reference to the compactor MR job
    runWorker(conf);
    // Compact ttp1
    runWorker(conf);
    // Clean up
    runCleaner(conf);
    rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(2, rsp.getCompacts().size());
    for(ShowCompactResponseElement scre : rsp.getCompacts()) {
      Assert.assertEquals(TxnStore.SUCCEEDED_RESPONSE, scre.getState());
      if ((!"ttp1".equals(scre.getTablename()) && (!"ttp2".equals(scre.getTablename())))) {
        Assert.fail("Unexpected table in compaction requetss:" + scre.getTablename());
      }
    }

    /*
     * we just did a major compaction on ttp1.  Open any file produced by it and check buffer size.
     * It should be the default.
     */
    List<String> rs = execSelectAndDumpData("select distinct INPUT__FILE__NAME from "
      + tblName1, driver, "Find Orc File bufer default");
    Assert.assertTrue("empty rs?", rs.size() > 0);
    Path p = new Path(rs.get(0));
    try (Reader orcReader = OrcFile.createReader(p.getFileSystem(conf), p)) {
      Assert.assertEquals("Expected default compression size",
          2700, orcReader.getCompressionSize());
    }
    //make sure 2700 is not the default so that we are testing if tblproperties indeed propagate
    Assert.assertNotEquals("Unexpected default compression size", 2700,
      OrcConf.BUFFER_SIZE.getDefaultValue());

    // Insert one more row - this should trigger hive.compactor.delta.pct.threshold to be reached for ttp2
    executeStatementOnDriver("insert into " + tblName1 + " values (6, 'f')", driver);
    executeStatementOnDriver("insert into " + tblName2 + " values (6, 'f')", driver);

    // Intentionally set this high so that it will not trigger major compaction for ttp1.
    // Only trigger major compaction for ttp2 (delta.pct.threshold=0.5) because of the newly inserted row (actual pct: 0.66)
    conf.setFloatVar(HiveConf.ConfVars.HIVE_COMPACTOR_DELTA_PCT_THRESHOLD, 0.8f);
    runInitiator(conf);
    rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(3, rsp.getCompacts().size());
    Assert.assertEquals("ttp2", rsp.getCompacts().get(0).getTablename());
    Assert.assertEquals(TxnStore.INITIATED_RESPONSE, rsp.getCompacts().get(0).getState());

    // Finish the scheduled compaction for ttp2
    runWorker(conf);
    runCleaner(conf);
    rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(3, rsp.getCompacts().size());
    Assert.assertEquals("ttp2", rsp.getCompacts().get(0).getTablename());
    Assert.assertEquals(TxnStore.SUCCEEDED_RESPONSE, rsp.getCompacts().get(0).getState());

    // Now test tblproperties specified on ALTER TABLE .. COMPACT .. statement
    executeStatementOnDriver("insert into " + tblName2 + " values (7, 'g')", driver);
    executeStatementOnDriver("alter table " + tblName2 + " compact 'major'" +
      " with overwrite tblproperties (" +
      "'compactor.mapreduce.map.memory.mb'='3072'," +
      "'tblprops.orc.compress.size'='3141'," +
      "'compactor.hive.compactor.job.queue'='root.user2')", driver);

    rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(4, rsp.getCompacts().size());
    Assert.assertEquals("ttp2", rsp.getCompacts().get(0).getTablename());
    Assert.assertEquals(TxnStore.INITIATED_RESPONSE, rsp.getCompacts().get(0).getState());
    //make sure we are checking the right (latest) compaction entry
    Assert.assertEquals(4, rsp.getCompacts().get(0).getId());

    // Run the Worker explicitly, in order to get the reference to the compactor MR job
    runWorker(conf);
    /*createReader(FileSystem fs, Path path) throws IOException {
     */
    //we just ran Major compaction so we should have a base_x in tblName2 that has the new files
    // Get the name of a file and look at its properties to see if orc.compress.size was respected.
    rs = execSelectAndDumpData("select distinct INPUT__FILE__NAME from " + tblName2,
      driver, "Find Compacted Orc File");
    Assert.assertTrue("empty rs?", rs.size() > 0);
    p = new Path(rs.get(0));
    try (Reader orcReader = OrcFile.createReader(p.getFileSystem(conf), p)){
      Assert.assertEquals("File written with wrong buffer size",
          3141, orcReader.getCompressionSize());
    }
  }

  @Test
  public void testCompactionInfoEquals() {
    CompactionInfo compactionInfo = new CompactionInfo("dbName", "tableName", "partName", CompactionType.MINOR);
    CompactionInfo compactionInfo1 = new CompactionInfo("dbName", "tableName", "partName", CompactionType.MINOR);
    assertEquals("The object must be equal", compactionInfo, compactionInfo);

    Assert.assertNotEquals("The object must be not equal", compactionInfo, new Object());
    assertEquals("The object must be equal", compactionInfo, compactionInfo1);
  }

  @Test
  public void testCompactionInfoHashCode() {
    CompactionInfo compactionInfo = new CompactionInfo("dbName", "tableName", "partName", CompactionType.MINOR);
    CompactionInfo compactionInfo1 = new CompactionInfo("dbName", "tableName", "partName", CompactionType.MINOR);

    Assert.assertEquals("The hash codes must be equal", compactionInfo.hashCode(), compactionInfo1.hashCode());
  }

  @Test
  public void testDisableCompactionDuringReplLoad() throws Exception {
    String tblName = "discomp";
    String database = "discomp_db";
    executeStatementOnDriver("drop database if exists " + database + " cascade", driver);
    executeStatementOnDriver("create database " + database, driver);
    executeStatementOnDriver("CREATE TABLE " + database + "." + tblName + "(a INT, b STRING) " +
            " PARTITIONED BY(ds string)" +
            " CLUSTERED BY(a) INTO 2 BUCKETS" + //currently ACID requires table to be bucketed
            " STORED AS ORC TBLPROPERTIES ('transactional'='true')", driver);
    executeStatementOnDriver("insert into " + database + "." + tblName + " partition (ds) values (1, 'fred', " +
            "'today'), (2, 'wilma', 'yesterday')", driver);

    executeStatementOnDriver("ALTER TABLE " + database + "." + tblName +
            " SET TBLPROPERTIES ( 'hive.repl.first.inc.pending' = 'true')", driver);
    List<ShowCompactResponseElement> compacts = getCompactionList();
    Assert.assertEquals(0, compacts.size());

    executeStatementOnDriver("alter database " + database +
            " set dbproperties ('hive.repl.first.inc.pending' = 'true')", driver);
    executeStatementOnDriver("ALTER TABLE " + database + "." + tblName +
            " SET TBLPROPERTIES ( 'hive.repl.first.inc.pending' = 'false')", driver);
    compacts = getCompactionList();
    Assert.assertEquals(0, compacts.size());

    executeStatementOnDriver("alter database " + database +
            " set dbproperties ('hive.repl.first.inc.pending' = 'false')", driver);
    executeStatementOnDriver("ALTER TABLE " + database + "." + tblName +
            " SET TBLPROPERTIES ( 'hive.repl.first.inc.pending' = 'false')", driver);
    compacts = getCompactionList();
    Assert.assertEquals(2, compacts.size());
    List<String> partNames = new ArrayList<>();
    for (int i = 0; i < compacts.size(); i++) {
      Assert.assertEquals(database, compacts.get(i).getDbname());
      Assert.assertEquals(tblName, compacts.get(i).getTablename());
      Assert.assertEquals("initiated", compacts.get(i).getState());
      partNames.add(compacts.get(i).getPartitionname());
    }
    Collections.sort(partNames);
    Assert.assertEquals("ds=today", partNames.get(0));
    Assert.assertEquals("ds=yesterday", partNames.get(1));
    executeStatementOnDriver("drop database if exists " + database + " cascade", driver);

    // Finish the scheduled compaction for ttp2
    runWorker(conf);
    runCleaner(conf);
  }

  /**
   * Tests compaction of tables that were populated by LOAD DATA INPATH statements.
   *
   * In this scenario original ORC files are a structured in the following way:
   * comp3
   * |--delta_0000001_0000001_0000
   *    |--000000_0
   * |--delta_0000002_0000002_0000
   *    |--000000_0
   *    |--000001_0
   *
   * ..where comp3 table is not bucketed.
   *
   * @throws Exception
   */
  @Test
  public void testCompactionOnDataLoadedInPath() throws Exception {
    // Setup of LOAD INPATH scenario.
    executeStatementOnDriver("drop table if exists comp0", driver);
    executeStatementOnDriver("drop table if exists comp1", driver);
    executeStatementOnDriver("drop table if exists comp3", driver);

    executeStatementOnDriver("create external table comp0 (a string)", driver);
    executeStatementOnDriver("insert into comp0 values ('1111111111111')", driver);
    executeStatementOnDriver("insert into comp0 values ('2222222222222')", driver);
    executeStatementOnDriver("insert into comp0 values ('3333333333333')", driver);
    executeStatementOnDriver("create external table comp1 stored as orc as select * from comp0", driver);

    executeStatementOnDriver("create table comp3 (a string) stored as orc " +
        "TBLPROPERTIES ('transactional'='true')", driver);

    IMetaStoreClient hmsClient = new HiveMetaStoreClient(conf);
    Table table = hmsClient.getTable("default", "comp1");
    FileSystem fs = FileSystem.get(conf);
    Path path000 = fs.listStatus(new Path(table.getSd().getLocation()))[0].getPath();
    Path path001 = new Path(path000.toString().replace("000000", "000001"));
    Path path002 = new Path(path000.toString().replace("000000", "000002"));
    fs.copyFromLocalFile(path000, path001);
    fs.copyFromLocalFile(path000, path002);

    executeStatementOnDriver("load data inpath '" + path002.toString() + "' into table comp3", driver);
    executeStatementOnDriver("load data inpath '" + path002.getParent().toString() + "' into table comp3", driver);

    // Run compaction.
    TxnStore txnHandler = TxnUtils.getTxnStore(conf);
    CompactionRequest rqst = new CompactionRequest("default", "comp3", CompactionType.MAJOR);
    txnHandler.compact(rqst);
    runWorker(conf);
    ShowCompactRequest scRqst = new ShowCompactRequest();
    List<ShowCompactResponseElement> compacts = txnHandler.showCompact(scRqst).getCompacts();
    assertEquals(1, compacts.size());
    assertEquals(TxnStore.CLEANING_RESPONSE, compacts.get(0).getState());

    runCleaner(conf);
    compacts = txnHandler.showCompact(scRqst).getCompacts();
    assertEquals(1, compacts.size());
    assertEquals(TxnStore.SUCCEEDED_RESPONSE, compacts.get(0).getState());

    // Check compacted content and file structure.
    table = hmsClient.getTable("default", "comp3");
    List<String> rs = execSelectAndDumpData("select * from comp3", driver, "select");
    assertEquals(9, rs.size());
    assertEquals(3, rs.stream().filter("1111111111111"::equals).count());
    assertEquals(3, rs.stream().filter("2222222222222"::equals).count());
    assertEquals(3, rs.stream().filter("3333333333333"::equals).count());

    FileStatus[] files = fs.listStatus(new Path(table.getSd().getLocation()));
    // base dir
    assertEquals(1, files.length);
    assertEquals("base_0000002_v0000012", files[0].getPath().getName());
    files = fs.listStatus(files[0].getPath(), AcidUtils.bucketFileFilter);
    // files
    assertEquals(2, files.length);
    Arrays.stream(files).filter(p->"bucket_00000".equals(p.getPath().getName())).count();
    Arrays.stream(files).filter(p->"bucket_00001".equals(p.getPath().getName())).count();

    // Another insert into the newly compacted table.
    executeStatementOnDriver("insert into comp3 values ('4444444444444')", driver);

    // Compact with extra row too.
    txnHandler.compact(rqst);
    runWorker(conf);
    compacts = txnHandler.showCompact(scRqst).getCompacts();
    assertEquals(2, compacts.size());
    assertEquals(TxnStore.CLEANING_RESPONSE, compacts.get(0).getState());

    runCleaner(conf);
    compacts = txnHandler.showCompact(scRqst).getCompacts();
    assertEquals(2, compacts.size());
    assertEquals(TxnStore.SUCCEEDED_RESPONSE, compacts.get(0).getState());

    // Check compacted content and file structure.
    rs = execSelectAndDumpData("select * from comp3", driver, "select");
    assertEquals(10, rs.size());
    assertEquals(3, rs.stream().filter("1111111111111"::equals).count());
    assertEquals(3, rs.stream().filter("2222222222222"::equals).count());
    assertEquals(3, rs.stream().filter("3333333333333"::equals).count());
    assertEquals(1, rs.stream().filter("4444444444444"::equals).count());

    files = fs.listStatus(new Path(table.getSd().getLocation()));
    // base dir
    assertEquals(1, files.length);
    assertEquals("base_0000003_v0000016", files[0].getPath().getName());
    files = fs.listStatus(files[0].getPath(), AcidUtils.bucketFileFilter);
    // files
    assertEquals(2, files.length);
    Arrays.stream(files).filter(p->"bucket_00000".equals(p.getPath().getName())).count();
    Arrays.stream(files).filter(p->"bucket_00001".equals(p.getPath().getName())).count();

  }

  @Test
  public void testCompactionDataLoadedWithInsertOverwrite() throws Exception {
    String externalTableName = "test_comp_txt";
    String tableName = "test_comp";
    executeStatementOnDriver("DROP TABLE IF EXISTS " + externalTableName, driver);
    executeStatementOnDriver("DROP TABLE IF EXISTS " + tableName, driver);
    executeStatementOnDriver("CREATE EXTERNAL TABLE " + externalTableName + "(a int, b int, c int) STORED AS TEXTFILE",
        driver);
    executeStatementOnDriver(
        "CREATE TABLE " + tableName + "(a int, b int, c int) STORED AS ORC TBLPROPERTIES('transactional'='true')",
        driver);

    executeStatementOnDriver("INSERT INTO " + externalTableName + " values (1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4)",
        driver);
    executeStatementOnDriver("INSERT OVERWRITE TABLE " + tableName + " SELECT * FROM test_comp_txt", driver);

    executeStatementOnDriver("UPDATE " + tableName + " SET b=55, c=66 WHERE a=2", driver);
    executeStatementOnDriver("DELETE FROM " + tableName + " WHERE a=4", driver);
    executeStatementOnDriver("UPDATE " + tableName + " SET b=77 WHERE a=1", driver);

    executeStatementOnDriver("SELECT * FROM " + tableName + " ORDER BY a", driver);
    ArrayList<String> valuesReadFromHiveDriver = new ArrayList<>();
    driver.getResults(valuesReadFromHiveDriver);
    Assert.assertEquals(3, valuesReadFromHiveDriver.size());
    Assert.assertEquals("1\t77\t1", valuesReadFromHiveDriver.get(0));
    Assert.assertEquals("2\t55\t66", valuesReadFromHiveDriver.get(1));
    Assert.assertEquals("3\t3\t3", valuesReadFromHiveDriver.get(2));

    runMajorCompaction("default", tableName);

    // Validate after compaction.
    executeStatementOnDriver("SELECT * FROM " + tableName + " ORDER BY a", driver);
    valuesReadFromHiveDriver = new ArrayList<>();
    driver.getResults(valuesReadFromHiveDriver);
    Assert.assertEquals(3, valuesReadFromHiveDriver.size());
    Assert.assertEquals("1\t77\t1", valuesReadFromHiveDriver.get(0));
    Assert.assertEquals("2\t55\t66", valuesReadFromHiveDriver.get(1));
  }

  @Test
  public void testAcidDirCacheOnDropTable() throws Exception {
    int cacheDurationInMinutes = 10;
    AcidUtils.initDirCache(cacheDurationInMinutes);
    HiveConf.setBoolVar(conf, ConfVars.HIVE_COMPACTOR_GATHER_STATS, false);
    String dbName = "default";
    String tblName = "adc_table";

    // First phase, populate the cache
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("create table " + tblName + " (a string) stored as orc " +
            "TBLPROPERTIES ('transactional'='true', 'hive.exec.orc.split.strategy'='BI')", driver);
    executeStatementOnDriver("insert into " + tblName + " values ('a')", driver);
    executeStatementOnDriver("insert into " + tblName + " values ('b')", driver);
    runMajorCompaction(dbName, tblName);
    runCleaner(conf);

    HiveConf.setIntVar(driver.getConf(), ConfVars.HIVE_TXN_ACID_DIR_CACHE_DURATION, cacheDurationInMinutes);
    executeStatementOnDriver("select * from " + tblName + " order by a", driver);

    // Second phase, the previous data should be cleaned
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("create table " + tblName + " (a string) stored as orc " +
            "TBLPROPERTIES ('transactional'='true', 'hive.exec.orc.split.strategy'='BI')", driver);
    executeStatementOnDriver("insert into " + tblName + " values ('c')", driver);
    executeStatementOnDriver("insert into " + tblName + " values ('d')", driver);
    runMajorCompaction(dbName, tblName);
    runCleaner(conf);

    HiveConf.setIntVar(driver.getConf(), ConfVars.HIVE_TXN_ACID_DIR_CACHE_DURATION, cacheDurationInMinutes);
    List<String> rs = execSelectAndDumpData("select * from " + tblName + " order by a", driver, "select");
    Assert.assertEquals(2, rs.size());
    Assert.assertEquals("c", rs.get(0));
    Assert.assertEquals("d", rs.get(1));
  }

  @Test
  public void testAcidDirCacheOnDropPartitionedTable() throws Exception {
    int cacheDurationInMinutes = 10;
    AcidUtils.initDirCache(cacheDurationInMinutes);
    HiveConf.setBoolVar(conf, ConfVars.HIVE_COMPACTOR_GATHER_STATS, false);
    String dbName = "default";
    String tblName = "adc_part_table";

    // First phase, populate the cache
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("create table " + tblName + " (a string) PARTITIONED BY (p string) stored as orc " +
            "TBLPROPERTIES ('transactional'='true', 'hive.exec.orc.split.strategy'='BI')", driver);
    executeStatementOnDriver("insert into " + tblName + " values ('a', 'p1')", driver);
    executeStatementOnDriver("insert into " + tblName + " values ('b', 'p1')", driver);
    executeStatementOnDriver("insert into " + tblName + " values ('a', 'p2')", driver);
    executeStatementOnDriver("insert into " + tblName + " values ('b', 'p2')", driver);
    runMajorCompaction(dbName, tblName, "p=p1", "p=p2");
    runCleaner(conf);

    HiveConf.setIntVar(driver.getConf(), ConfVars.HIVE_TXN_ACID_DIR_CACHE_DURATION, cacheDurationInMinutes);
    executeStatementOnDriver("select a from " + tblName + " order by a", driver);

    // Second phase, the previous data should be cleaned
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("create table " + tblName + " (a string) PARTITIONED BY (p string) stored as orc " +
            "TBLPROPERTIES ('transactional'='true', 'hive.exec.orc.split.strategy'='BI')", driver);
    executeStatementOnDriver("insert into " + tblName + " values ('c', 'p1')", driver);
    executeStatementOnDriver("insert into " + tblName + " values ('d', 'p1')", driver);
    executeStatementOnDriver("insert into " + tblName + " values ('c', 'p2')", driver);
    executeStatementOnDriver("insert into " + tblName + " values ('d', 'p2')", driver);
    runMajorCompaction(dbName, tblName, "p=p1", "p=p2");
    runCleaner(conf);

    HiveConf.setIntVar(driver.getConf(), ConfVars.HIVE_TXN_ACID_DIR_CACHE_DURATION, cacheDurationInMinutes);
    List<String> rs = execSelectAndDumpData("select a from " + tblName + " order by a", driver, "select");
    Assert.assertEquals(4, rs.size());
    Assert.assertEquals("c", rs.get(0));
    Assert.assertEquals("c", rs.get(1));
    Assert.assertEquals("d", rs.get(2));
    Assert.assertEquals("d", rs.get(2));
  }

  private List<ShowCompactResponseElement> getCompactionList() throws Exception {
    conf.setIntVar(HiveConf.ConfVars.HIVE_COMPACTOR_DELTA_NUM_THRESHOLD, 0);
    runInitiator(conf);

    TxnStore txnHandler = TxnUtils.getTxnStore(conf);
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    return rsp.getCompacts();
  }

  private void verifyCompactions(List<ShowCompactResponseElement> compacts, SortedSet<String> partNames, String tblName) {
    for (ShowCompactResponseElement compact : compacts) {
      Assert.assertEquals("default", compact.getDbname());
      Assert.assertEquals(tblName, compact.getTablename());
      Assert.assertEquals("initiated", compact.getState());
      partNames.add(compact.getPartitionname());
    }
  }

  private void processStreamingAPI(String dbName, String tblName)
      throws StreamingException {
      List<CompactorTestUtil.StreamingConnectionOption> options = Lists
          .newArrayList(new CompactorTestUtil.StreamingConnectionOption(false, false),
              new CompactorTestUtil.StreamingConnectionOption(false, false),
              new CompactorTestUtil.StreamingConnectionOption(true, false));
      CompactorTestUtil.runStreamingAPI(conf, dbName, tblName, options);
  }
}
