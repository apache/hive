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
package org.apache.hadoop.hive.ql.txn.compactor;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.StringableMap;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.ShowCompactRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponseElement;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * Tests for the worker thread and its MR jobs.
 * todo: most delta files in this test suite use txn id range, i.e. [N,N+M]
 * That means that they all look like they were created by compaction or by streaming api.
 * Delta files created by SQL should have [N,N] range (and a suffix in v1.3 and later)
 * Need to change some of these to have better test coverage.
 */
public class TestWorker extends CompactorTest {
  static final private String CLASS_NAME = TestWorker.class.getName();
  static final private Logger LOG = LoggerFactory.getLogger(CLASS_NAME);

  public TestWorker() throws Exception {
    super();
  }

  @Test
  public void nothing() throws Exception {
    // Test that the whole things works when there's nothing in the queue.  This is just a
    // survival test.
    startWorker();
  }

  @Test
  public void stringableMap() throws Exception {
    // Empty map case
    StringableMap m = new StringableMap(new HashMap<String, String>());
    String s = m.toString();
    Assert.assertEquals("0:", s);
    m = new StringableMap(s);
    Assert.assertEquals(0, m.size());

    Map<String, String> base = new HashMap<String, String>();
    base.put("mary", "poppins");
    base.put("bert", null);
    base.put(null, "banks");
    m = new StringableMap(base);
    s = m.toString();
    m = new StringableMap(s);
    Assert.assertEquals(3, m.size());
    Map<String, Boolean> saw = new HashMap<String, Boolean>(3);
    saw.put("mary", false);
    saw.put("bert", false);
    saw.put(null, false);
    for (Map.Entry<String, String> e : m.entrySet()) {
      saw.put(e.getKey(), true);
      if ("mary".equals(e.getKey())) Assert.assertEquals("poppins", e.getValue());
      else if ("bert".equals(e.getKey())) Assert.assertNull(e.getValue());
      else if (null == e.getKey()) Assert.assertEquals("banks", e.getValue());
      else Assert.fail("Unexpected value " + e.getKey());
    }
    Assert.assertEquals(3, saw.size());
    Assert.assertTrue(saw.get("mary"));
    Assert.assertTrue(saw.get("bert"));
    Assert.assertTrue(saw.get(null));
   }

  @Test
  public void stringableList() throws Exception {
    // Empty list case
    CompactorMR.StringableList ls = new CompactorMR.StringableList();
    String s = ls.toString();
    Assert.assertEquals("0:", s);
    ls = new CompactorMR.StringableList(s);
    Assert.assertEquals(0, ls.size());

    ls = new CompactorMR.StringableList();
    ls.add(new Path("/tmp"));
    ls.add(new Path("/usr"));
    s = ls.toString();
    Assert.assertTrue("Expected 2:4:/tmp4:/usr or 2:4:/usr4:/tmp, got " + s,
        "2:4:/tmp4:/usr".equals(s) || "2:4:/usr4:/tmp".equals(s));
    ls = new CompactorMR.StringableList(s);
    Assert.assertEquals(2, ls.size());
    boolean sawTmp = false, sawUsr = false;
    for (Path p : ls) {
      if ("/tmp".equals(p.toString())) sawTmp = true;
      else if ("/usr".equals(p.toString())) sawUsr = true;
      else Assert.fail("Unexpected path " + p.toString());
    }
    Assert.assertTrue(sawTmp);
    Assert.assertTrue(sawUsr);
  }

  @Test
  public void inputSplit() throws Exception {
    String basename = "/warehouse/foo/base_1";
    String delta1 = "/warehouse/foo/delta_2_3";
    String delta2 = "/warehouse/foo/delta_4_7";

    HiveConf conf = new HiveConf();
    Path file = new Path(System.getProperty("java.io.tmpdir") +
        System.getProperty("file.separator") + "newWriteInputSplitTest");
    FileSystem fs = FileSystem.get(conf);
    FSDataOutputStream os = fs.create(file);
    for (int i = 0; i < 10; i++) {
      os.writeBytes("mary had a little lamb its fleece was white as snow\n");
    }
    os.close();
    List<Path> files = new ArrayList<Path>(1);
    files.add(file);

    Path[] deltas = new Path[2];
    deltas[0] = new Path(delta1);
    deltas[1] = new Path(delta2);

    CompactorMR.CompactorInputSplit split =
        new CompactorMR.CompactorInputSplit(conf, 3, files, new Path(basename), deltas);

    Assert.assertEquals(520L, split.getLength());
    String[] locations = split.getLocations();
    Assert.assertEquals(1, locations.length);
    Assert.assertEquals("localhost", locations[0]);

    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    DataOutput out = new DataOutputStream(buf);
    split.write(out);

    split = new CompactorMR.CompactorInputSplit();
    DataInput in = new DataInputStream(new ByteArrayInputStream(buf.toByteArray()));
    split.readFields(in);

    Assert.assertEquals(3, split.getBucket());
    Assert.assertEquals(basename, split.getBaseDir().toString());
    deltas = split.getDeltaDirs();
    Assert.assertEquals(2, deltas.length);
    Assert.assertEquals(delta1, deltas[0].toString());
    Assert.assertEquals(delta2, deltas[1].toString());
  }

  @Test
  public void inputSplitNullBase() throws Exception {
    String delta1 = "/warehouse/foo/delta_2_3";
    String delta2 = "/warehouse/foo/delta_4_7";

    HiveConf conf = new HiveConf();
    Path file = new Path(System.getProperty("java.io.tmpdir") +
        System.getProperty("file.separator") + "newWriteInputSplitTest");
    FileSystem fs = FileSystem.get(conf);
    FSDataOutputStream os = fs.create(file);
    for (int i = 0; i < 10; i++) {
      os.writeBytes("mary had a little lamb its fleece was white as snow\n");
    }
    os.close();
    List<Path> files = new ArrayList<Path>(1);
    files.add(file);

    Path[] deltas = new Path[2];
    deltas[0] = new Path(delta1);
    deltas[1] = new Path(delta2);

    CompactorMR.CompactorInputSplit split =
        new CompactorMR.CompactorInputSplit(conf, 3, files, null, deltas);

    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    DataOutput out = new DataOutputStream(buf);
    split.write(out);

    split = new CompactorMR.CompactorInputSplit();
    DataInput in = new DataInputStream(new ByteArrayInputStream(buf.toByteArray()));
    split.readFields(in);

    Assert.assertEquals(3, split.getBucket());
    Assert.assertNull(split.getBaseDir());
    deltas = split.getDeltaDirs();
    Assert.assertEquals(2, deltas.length);
    Assert.assertEquals(delta1, deltas[0].toString());
    Assert.assertEquals(delta2, deltas[1].toString());
  }

  @Test
  public void sortedTable() throws Exception {
    List<Order> sortCols = new ArrayList<Order>(1);
    sortCols.add(new Order("b", 1));

    Table t = newTable("default", "st", false, new HashMap<String, String>(), sortCols, false);

    addBaseFile(t, null, 20L, 20);
    addDeltaFile(t, null, 21L, 22L, 2);
    addDeltaFile(t, null, 23L, 24L, 2);
    addDeltaFile(t, null, 21L, 24L, 4);

    burnThroughTransactions(25);

    CompactionRequest rqst = new CompactionRequest("default", "st", CompactionType.MINOR);
    txnHandler.compact(rqst);

    startWorker();

    // There should still be four directories in the location.
    FileSystem fs = FileSystem.get(conf);
    FileStatus[] stat = fs.listStatus(new Path(t.getSd().getLocation()));
    Assert.assertEquals(4, stat.length);
  }

  @Test
  public void sortedPartition() throws Exception {
    List<Order> sortCols = new ArrayList<Order>(1);
    sortCols.add(new Order("b", 1));

    Table t = newTable("default", "sp", true, new HashMap<String, String>(), sortCols, false);
    Partition p = newPartition(t, "today", sortCols);

    addBaseFile(t, p, 20L, 20);
    addDeltaFile(t, p, 21L, 22L, 2);
    addDeltaFile(t, p, 23L, 24L, 2);
    addDeltaFile(t, p, 21L, 24L, 4);

    burnThroughTransactions(25);

    CompactionRequest rqst = new CompactionRequest("default", "sp", CompactionType.MINOR);
    rqst.setPartitionname("ds=today");
    txnHandler.compact(rqst);

    startWorker();

    // There should still be four directories in the location.
    FileSystem fs = FileSystem.get(conf);
    FileStatus[] stat = fs.listStatus(new Path(p.getSd().getLocation()));
    Assert.assertEquals(4, stat.length);
  }

  @Test
  public void minorTableWithBase() throws Exception {
    LOG.debug("Starting minorTableWithBase");
    Table t = newTable("default", "mtwb", false);

    addBaseFile(t, null, 20L, 20);
    addDeltaFile(t, null, 21L, 22L, 2);
    addDeltaFile(t, null, 23L, 24L, 2);

    burnThroughTransactions(25);

    CompactionRequest rqst = new CompactionRequest("default", "mtwb", CompactionType.MINOR);
    txnHandler.compact(rqst);

    startWorker();//adds delta and delete_delta

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    Assert.assertEquals(1, compacts.size());
    Assert.assertEquals("ready for cleaning", compacts.get(0).getState());

    // There should still now be 5 directories in the location
    FileSystem fs = FileSystem.get(conf);
    FileStatus[] stat = fs.listStatus(new Path(t.getSd().getLocation()));
    Assert.assertEquals(5, stat.length);

    // Find the new delta file and make sure it has the right contents
    boolean sawNewDelta = false;
    for (int i = 0; i < stat.length; i++) {
      if (stat[i].getPath().getName().equals(makeDeltaDirNameCompacted(21, 24))) {
        sawNewDelta = true;
        FileStatus[] buckets = fs.listStatus(stat[i].getPath());
        Assert.assertEquals(2, buckets.length);
        Assert.assertTrue(buckets[0].getPath().getName().matches("bucket_0000[01]"));
        Assert.assertTrue(buckets[1].getPath().getName().matches("bucket_0000[01]"));
        Assert.assertEquals(104L, buckets[0].getLen());
        Assert.assertEquals(104L, buckets[1].getLen());
      }
      if (stat[i].getPath().getName().equals(makeDeleteDeltaDirNameCompacted(21, 24))) {
        sawNewDelta = true;
        FileStatus[] buckets = fs.listStatus(stat[i].getPath());
        Assert.assertEquals(2, buckets.length);
        Assert.assertTrue(buckets[0].getPath().getName().matches("bucket_0000[01]"));
        Assert.assertTrue(buckets[1].getPath().getName().matches("bucket_0000[01]"));
        Assert.assertEquals(104L, buckets[0].getLen());
        Assert.assertEquals(104L, buckets[1].getLen());
      }
      else {
        LOG.debug("This is not the delta file you are looking for " + stat[i].getPath().getName());
      }
    }
    Assert.assertTrue(sawNewDelta);
  }

  /**
   * todo: fix https://issues.apache.org/jira/browse/HIVE-9995
   * @throws Exception
   */
  @Test
  public void minorWithOpenInMiddle() throws Exception {
    LOG.debug("Starting minorWithOpenInMiddle");
    Table t = newTable("default", "mtwb", false);

    addBaseFile(t, null, 20L, 20);
    addDeltaFile(t, null, 21L, 22L, 2);
    addDeltaFile(t, null, 23L, 25L, 3);
    addLengthFile(t, null, 23L, 25L, 3);
    addDeltaFile(t, null, 26L, 27L, 2);
    burnThroughTransactions(27, new HashSet<Long>(Arrays.asList(23L)), null);

    CompactionRequest rqst = new CompactionRequest("default", "mtwb", CompactionType.MINOR);
    txnHandler.compact(rqst);

    startWorker();

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    Assert.assertEquals(1, compacts.size());
    Assert.assertEquals("ready for cleaning", compacts.get(0).getState());

    // There should still now be 5 directories in the location
    FileSystem fs = FileSystem.get(conf);
    FileStatus[] stat = fs.listStatus(new Path(t.getSd().getLocation()));
    Assert.assertEquals(5, stat.length);

    // Find the new delta file and make sure it has the right contents
    Arrays.sort(stat);
    Assert.assertEquals("base_20", stat[0].getPath().getName());
    Assert.assertEquals(makeDeleteDeltaDirNameCompacted(21, 22), stat[1].getPath().getName());
    Assert.assertEquals(makeDeltaDirNameCompacted(21, 22), stat[2].getPath().getName());
    Assert.assertEquals(makeDeltaDirName(23, 25), stat[3].getPath().getName());
    Assert.assertEquals(makeDeltaDirName(26, 27), stat[4].getPath().getName());
  }

  @Test
  public void minorWithAborted() throws Exception {
    LOG.debug("Starting minorWithAborted");
    Table t = newTable("default", "mtwb", false);

    addBaseFile(t, null, 20L, 20);
    addDeltaFile(t, null, 21L, 22L, 2);
    addDeltaFile(t, null, 23L, 25L, 3);
    addLengthFile(t, null, 23L, 25L, 3);
    addDeltaFile(t, null, 26L, 27L, 2);
    burnThroughTransactions(27, null, new HashSet<Long>(Arrays.asList(24L, 25L)));

    CompactionRequest rqst = new CompactionRequest("default", "mtwb", CompactionType.MINOR);
    txnHandler.compact(rqst);

    startWorker();

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    Assert.assertEquals(1, compacts.size());
    Assert.assertEquals("ready for cleaning", compacts.get(0).getState());

    // There should still now be 6 directories in the location
    FileSystem fs = FileSystem.get(conf);
    FileStatus[] stat = fs.listStatus(new Path(t.getSd().getLocation()));
    Assert.assertEquals(6, stat.length);

    // Find the new delta file and make sure it has the right contents
    Arrays.sort(stat);
    Assert.assertEquals("base_20", stat[0].getPath().getName());
    Assert.assertEquals(makeDeleteDeltaDirNameCompacted(21, 27), stat[1].getPath().getName());
    Assert.assertEquals(makeDeltaDirName(21, 22), stat[2].getPath().getName());
    Assert.assertEquals(makeDeltaDirNameCompacted(21, 27), stat[3].getPath().getName());
    Assert.assertEquals(makeDeltaDirName(23, 25), stat[4].getPath().getName());
    Assert.assertEquals(makeDeltaDirName(26, 27), stat[5].getPath().getName());
  }

  @Test
  public void minorPartitionWithBase() throws Exception {
    Table t = newTable("default", "mpwb", true);
    Partition p = newPartition(t, "today");

    addBaseFile(t, p, 20L, 20);
    addDeltaFile(t, p, 21L, 22L, 2);
    addDeltaFile(t, p, 23L, 24L, 2);

    burnThroughTransactions(25);

    CompactionRequest rqst = new CompactionRequest("default", "mpwb", CompactionType.MINOR);
    rqst.setPartitionname("ds=today");
    txnHandler.compact(rqst);

    startWorker();//this will create delta_20_24 and delete_delta_20_24. See MockRawReader

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    Assert.assertEquals(1, compacts.size());
    Assert.assertEquals("ready for cleaning", compacts.get(0).getState());

    // There should still be four directories in the location.
    FileSystem fs = FileSystem.get(conf);
    FileStatus[] stat = fs.listStatus(new Path(p.getSd().getLocation()));
    Assert.assertEquals(5, stat.length);

    // Find the new delta file and make sure it has the right contents
    boolean sawNewDelta = false;
    for (int i = 0; i < stat.length; i++) {
      if (stat[i].getPath().getName().equals(makeDeltaDirNameCompacted(21, 24))) {
        sawNewDelta = true;
        FileStatus[] buckets = fs.listStatus(stat[i].getPath());
        Assert.assertEquals(2, buckets.length);
        Assert.assertTrue(buckets[0].getPath().getName().matches("bucket_0000[01]"));
        Assert.assertTrue(buckets[1].getPath().getName().matches("bucket_0000[01]"));
        Assert.assertEquals(104L, buckets[0].getLen());
        Assert.assertEquals(104L, buckets[1].getLen());
      }
      if (stat[i].getPath().getName().equals(makeDeleteDeltaDirNameCompacted(21, 24))) {
          sawNewDelta = true;
          FileStatus[] buckets = fs.listStatus(stat[i].getPath());
          Assert.assertEquals(2, buckets.length);
          Assert.assertTrue(buckets[0].getPath().getName().matches("bucket_0000[01]"));
          Assert.assertTrue(buckets[1].getPath().getName().matches("bucket_0000[01]"));
          Assert.assertEquals(104L, buckets[0].getLen());
          Assert.assertEquals(104L, buckets[1].getLen());
        } else {
        LOG.debug("This is not the delta file you are looking for " + stat[i].getPath().getName());
      }
    }
    Assert.assertTrue(sawNewDelta);
  }

  @Test
  public void minorTableNoBase() throws Exception {
    LOG.debug("Starting minorTableWithBase");
    Table t = newTable("default", "mtnb", false);

    addDeltaFile(t, null, 1L, 2L, 2);
    addDeltaFile(t, null, 3L, 4L, 2);

    burnThroughTransactions(5);

    CompactionRequest rqst = new CompactionRequest("default", "mtnb", CompactionType.MINOR);
    txnHandler.compact(rqst);

    startWorker();

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    Assert.assertEquals(1, compacts.size());
    Assert.assertEquals("ready for cleaning", compacts.get(0).getState());

    // There should still now be 5 directories in the location
    FileSystem fs = FileSystem.get(conf);
    FileStatus[] stat = fs.listStatus(new Path(t.getSd().getLocation()));
    Assert.assertEquals(4, stat.length);

    // Find the new delta file and make sure it has the right contents
    boolean sawNewDelta = false;
    for (int i = 0; i < stat.length; i++) {
      if (stat[i].getPath().getName().equals(makeDeltaDirNameCompacted(1, 4))) {
        sawNewDelta = true;
        FileStatus[] buckets = fs.listStatus(stat[i].getPath());
        Assert.assertEquals(2, buckets.length);
        Assert.assertTrue(buckets[0].getPath().getName().matches("bucket_0000[01]"));
        Assert.assertTrue(buckets[1].getPath().getName().matches("bucket_0000[01]"));
        Assert.assertEquals(104L, buckets[0].getLen());
        Assert.assertEquals(104L, buckets[1].getLen());
      }
      if (stat[i].getPath().getName().equals(makeDeleteDeltaDirNameCompacted(1, 4))) {
        sawNewDelta = true;
        FileStatus[] buckets = fs.listStatus(stat[i].getPath());
        Assert.assertEquals(2, buckets.length);
        Assert.assertTrue(buckets[0].getPath().getName().matches("bucket_0000[01]"));
        Assert.assertTrue(buckets[1].getPath().getName().matches("bucket_0000[01]"));
        Assert.assertEquals(104L, buckets[0].getLen());
        Assert.assertEquals(104L, buckets[1].getLen());
      } else {
        LOG.debug("This is not the delta file you are looking for " + stat[i].getPath().getName());
      }
    }
    Assert.assertTrue(sawNewDelta);
  }

  @Test
  public void majorTableWithBase() throws Exception {
    LOG.debug("Starting majorTableWithBase");
    Table t = newTable("default", "matwb", false);

    addBaseFile(t, null, 20L, 20);
    addDeltaFile(t, null, 21L, 22L, 2);
    addDeltaFile(t, null, 23L, 24L, 2);

    burnThroughTransactions(25);

    CompactionRequest rqst = new CompactionRequest("default", "matwb", CompactionType.MAJOR);
    txnHandler.compact(rqst);

    startWorker();

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    Assert.assertEquals(1, compacts.size());
    Assert.assertEquals("ready for cleaning", compacts.get(0).getState());

    // There should still now be 5 directories in the location
    FileSystem fs = FileSystem.get(conf);
    FileStatus[] stat = fs.listStatus(new Path(t.getSd().getLocation()));
    Assert.assertEquals(4, stat.length);

    // Find the new delta file and make sure it has the right contents
    boolean sawNewBase = false;
    for (int i = 0; i < stat.length; i++) {
      if (stat[i].getPath().getName().equals("base_0000024")) {
        sawNewBase = true;
        FileStatus[] buckets = fs.listStatus(stat[i].getPath(), FileUtils.HIDDEN_FILES_PATH_FILTER);
        Assert.assertEquals(2, buckets.length);
        Assert.assertTrue(buckets[0].getPath().getName().matches("bucket_0000[01]"));
        Assert.assertTrue(buckets[1].getPath().getName().matches("bucket_0000[01]"));
        Assert.assertEquals(624L, buckets[0].getLen());
        Assert.assertEquals(624L, buckets[1].getLen());
      } else {
        LOG.debug("This is not the file you are looking for " + stat[i].getPath().getName());
      }
    }
    Assert.assertTrue(sawNewBase);
  }

  @Test
  public void minorNoBaseLotsOfDeltas() throws Exception {
    compactNoBaseLotsOfDeltas(CompactionType.MINOR);
  }
  @Test
  public void majorNoBaseLotsOfDeltas() throws Exception {
    compactNoBaseLotsOfDeltas(CompactionType.MAJOR);
  }

  /**
   * These tests are starting to be a hack.  The files writtern by addDeltaFile() are not proper
   * Acid files and the {@link CompactorTest.MockRawReader} performs no merging of delta files and
   * fakes isDelete() as a shortcut.  This makes files created on disk to not be representative of
   * what they should look like in a real system.
   * Making {@link org.apache.hadoop.hive.ql.txn.compactor.CompactorTest.MockRawReader} do proper
   * delete event handling would be duplicating either OrcRawRecordMerger or VectorizedOrcAcidRowBatchReaer.
   * @param type
   * @throws Exception
   */
  private void compactNoBaseLotsOfDeltas(CompactionType type) throws Exception {
    conf.setIntVar(HiveConf.ConfVars.COMPACTOR_MAX_NUM_DELTA, 2);
    Table t = newTable("default", "mapwb", true);
    Partition p = newPartition(t, "today");

//    addBaseFile(t, p, 20L, 20);
    addDeltaFile(t, p, 21L, 21L, 2);
    addDeltaFile(t, p, 23L, 23L, 2);
    //make it look like streaming API use case
    addDeltaFile(t, p, 25L, 29L, 2);
    addDeltaFile(t, p, 31L, 32L, 3);
    //make it looks like 31-32 has been compacted, but not cleaned
    addDeltaFile(t, p, 31L, 33L, 5);
    addDeltaFile(t, p, 35L, 35L, 1);

    /*since COMPACTOR_MAX_NUM_DELTA=2,
    we expect files 1,2 to be minor compacted by 1 job to produce delta_21_23
    * 3,5 to be minor compacted by 2nd job (file 4 is obsolete) to make delta_25_33 (4th is skipped)
    *
    * and then the 'requested'
    * minor compaction to combine delta_21_23, delta_25_33 and delta_35_35 to make delta_21_35
    * or major compaction to create base_35*/
    burnThroughTransactions(35);
    CompactionRequest rqst = new CompactionRequest("default", "mapwb", type);
    rqst.setPartitionname("ds=today");
    txnHandler.compact(rqst);

    startWorker();

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    Assert.assertEquals(1, compacts.size());
    Assert.assertEquals("ready for cleaning", compacts.get(0).getState());

    FileSystem fs = FileSystem.get(conf);
    FileStatus[] stat = fs.listStatus(new Path(p.getSd().getLocation()));
    /* delete_delta_21_23 and delete_delta_25_33 which are created as a result of compacting*/
    int numFilesExpected = 11 + (type == CompactionType.MINOR ? 1 : 0);
    Assert.assertEquals(numFilesExpected, stat.length);

    // Find the new delta file and make sure it has the right contents
    BitSet matchesFound = new BitSet(numFilesExpected);
    for (int i = 0, j = 0; i < stat.length; i++) {
      if(stat[i].getPath().getName().equals(makeDeleteDeltaDirNameCompacted(21,23))) {
        matchesFound.set(j++);
      }
      if(stat[i].getPath().getName().equals(makeDeleteDeltaDirNameCompacted(25,33))) {
        matchesFound.set(j++);
      }
      if(stat[i].getPath().getName().equals(makeDeltaDirName(21,21))) {
        matchesFound.set(j++);
      }
      else if(stat[i].getPath().getName().equals(makeDeltaDirName(23, 23))) {
        matchesFound.set(j++);
      }
      else if(stat[i].getPath().getName().equals(makeDeltaDirNameCompacted(25, 29))) {
        matchesFound.set(j++);
      }
      else if(stat[i].getPath().getName().equals(makeDeltaDirNameCompacted(31, 32))) {
        matchesFound.set(j++);
      }
      else if(stat[i].getPath().getName().equals(makeDeltaDirNameCompacted(31, 33))) {
        matchesFound.set(j++);
      }
      else if(stat[i].getPath().getName().equals(makeDeltaDirName(35, 35))) {
        matchesFound.set(j++);
      }
      else if(stat[i].getPath().getName().equals(makeDeltaDirNameCompacted(21,23))) {
        matchesFound.set(j++);
      }
      else if(stat[i].getPath().getName().equals(makeDeltaDirNameCompacted(25,33))) {
        matchesFound.set(j++);
      }
      switch (type) {
        case MINOR:
          if(stat[i].getPath().getName().equals(makeDeltaDirNameCompacted(21,35))) {
            matchesFound.set(j++);
          }
          if (stat[i].getPath().getName().equals(makeDeleteDeltaDirNameCompacted(21, 35))) {
            matchesFound.set(j++);
          }
          break;
        case MAJOR:
          if(stat[i].getPath().getName().equals(AcidUtils.baseDir(35))) {
            matchesFound.set(j++);
          }
          break;
        default:
          throw new IllegalStateException();
      }
    }
    StringBuilder sb = null;
    for(int i = 0; i < stat.length; i++) {
      if(!matchesFound.get(i)) {
        if(sb == null) {
          sb = new StringBuilder("Some files are missing at index: ");
        }
        sb.append(i).append(",");
      }
    }
    if (sb != null) {
      Assert.assertTrue(sb.toString(), false);
    }
  }
  @Test
  public void majorPartitionWithBase() throws Exception {
    LOG.debug("Starting majorPartitionWithBase");
    Table t = newTable("default", "mapwb", true);
    Partition p = newPartition(t, "today");

    addBaseFile(t, p, 20L, 20);
    addDeltaFile(t, p, 21L, 22L, 2);
    addDeltaFile(t, p, 23L, 24L, 2);

    burnThroughTransactions(25);

    CompactionRequest rqst = new CompactionRequest("default", "mapwb", CompactionType.MAJOR);
    rqst.setPartitionname("ds=today");
    txnHandler.compact(rqst);

    startWorker();

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    Assert.assertEquals(1, compacts.size());
    Assert.assertEquals("ready for cleaning", compacts.get(0).getState());

    // There should still be four directories in the location.
    FileSystem fs = FileSystem.get(conf);
    FileStatus[] stat = fs.listStatus(new Path(p.getSd().getLocation()));
    Assert.assertEquals(4, stat.length);

    // Find the new delta file and make sure it has the right contents
    boolean sawNewBase = false;
    for (int i = 0; i < stat.length; i++) {
      if (stat[i].getPath().getName().equals("base_0000024")) {
        sawNewBase = true;
        FileStatus[] buckets = fs.listStatus(stat[i].getPath(), FileUtils.HIDDEN_FILES_PATH_FILTER);
        Assert.assertEquals(2, buckets.length);
        Assert.assertTrue(buckets[0].getPath().getName().matches("bucket_0000[01]"));
        Assert.assertTrue(buckets[1].getPath().getName().matches("bucket_0000[01]"));
        Assert.assertEquals(624L, buckets[0].getLen());
        Assert.assertEquals(624L, buckets[1].getLen());
      } else {
        LOG.debug("This is not the file you are looking for " + stat[i].getPath().getName());
      }
    }
    Assert.assertTrue(sawNewBase);
  }

  @Test
  public void majorTableNoBase() throws Exception {
    LOG.debug("Starting majorTableNoBase");
    Table t = newTable("default", "matnb", false);

    addDeltaFile(t, null, 1L, 2L, 2);
    addDeltaFile(t, null, 3L, 4L, 2);

    burnThroughTransactions(4);

    CompactionRequest rqst = new CompactionRequest("default", "matnb", CompactionType.MAJOR);
    txnHandler.compact(rqst);

    startWorker();

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    Assert.assertEquals(1, compacts.size());
    Assert.assertEquals("ready for cleaning", compacts.get(0).getState());

    // There should now be 3 directories in the location
    FileSystem fs = FileSystem.get(conf);
    FileStatus[] stat = fs.listStatus(new Path(t.getSd().getLocation()));
    Assert.assertEquals(3, stat.length);

    // Find the new delta file and make sure it has the right contents
    boolean sawNewBase = false;
    for (int i = 0; i < stat.length; i++) {
      if (stat[i].getPath().getName().equals("base_0000004")) {
        sawNewBase = true;
        FileStatus[] buckets = fs.listStatus(stat[i].getPath(), FileUtils.HIDDEN_FILES_PATH_FILTER);
        Assert.assertEquals(2, buckets.length);
        Assert.assertTrue(buckets[0].getPath().getName().matches("bucket_0000[01]"));
        Assert.assertTrue(buckets[1].getPath().getName().matches("bucket_0000[01]"));
        Assert.assertEquals(104L, buckets[0].getLen());
        Assert.assertEquals(104L, buckets[1].getLen());
      } else {
        LOG.debug("This is not the file you are looking for " + stat[i].getPath().getName());
      }
    }
    Assert.assertTrue(sawNewBase);
  }

  @Test
  public void majorTableLegacy() throws Exception {
    LOG.debug("Starting majorTableLegacy");
    Table t = newTable("default", "matl", false);

    addLegacyFile(t, null, 20);
    addDeltaFile(t, null, 21L, 22L, 2);
    addDeltaFile(t, null, 23L, 24L, 2);

    burnThroughTransactions(25);

    CompactionRequest rqst = new CompactionRequest("default", "matl", CompactionType.MAJOR);
    txnHandler.compact(rqst);

    startWorker();

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    Assert.assertEquals(1, compacts.size());
    Assert.assertEquals("ready for cleaning", compacts.get(0).getState());

    // There should still now be 5 directories in the location
    FileSystem fs = FileSystem.get(conf);
    FileStatus[] stat = fs.listStatus(new Path(t.getSd().getLocation()));
    //Assert.assertEquals(4, stat.length);

    // Find the new delta file and make sure it has the right contents
    boolean sawNewBase = false;
    for (int i = 0; i < stat.length; i++) {
      if (stat[i].getPath().getName().equals("base_0000024")) {
        sawNewBase = true;
        FileStatus[] buckets = fs.listStatus(stat[i].getPath(), FileUtils.HIDDEN_FILES_PATH_FILTER);
        Assert.assertEquals(2, buckets.length);
        Assert.assertTrue(buckets[0].getPath().getName().matches("bucket_0000[01]"));
        Assert.assertTrue(buckets[1].getPath().getName().matches("bucket_0000[01]"));
        Assert.assertEquals(624L, buckets[0].getLen());
        Assert.assertEquals(624L, buckets[1].getLen());
      } else {
        LOG.debug("This is not the file you are looking for " + stat[i].getPath().getName());
      }
    }
    Assert.assertTrue(sawNewBase);
  }

  @Test
  public void minorTableLegacy() throws Exception {
    LOG.debug("Starting minorTableLegacy");
    Table t = newTable("default", "mtl", false);

    addLegacyFile(t, null, 20);
    addDeltaFile(t, null, 21L, 22L, 2);
    addDeltaFile(t, null, 23L, 24L, 2);

    burnThroughTransactions(25);

    CompactionRequest rqst = new CompactionRequest("default", "mtl", CompactionType.MINOR);
    txnHandler.compact(rqst);

    startWorker();

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    Assert.assertEquals(1, compacts.size());
    Assert.assertEquals("ready for cleaning", compacts.get(0).getState());

    // There should still now be 5 directories in the location
    FileSystem fs = FileSystem.get(conf);
    FileStatus[] stat = fs.listStatus(new Path(t.getSd().getLocation()));

    // Find the new delta file and make sure it has the right contents
    boolean sawNewDelta = false;
    for (int i = 0; i < stat.length; i++) {
      if (stat[i].getPath().getName().equals(makeDeltaDirNameCompacted(21, 24))) {
        sawNewDelta = true;
        FileStatus[] buckets = fs.listStatus(stat[i].getPath());
        Assert.assertEquals(2, buckets.length);
        Assert.assertTrue(buckets[0].getPath().getName().matches("bucket_0000[01]"));
        Assert.assertTrue(buckets[1].getPath().getName().matches("bucket_0000[01]"));
      } else {
        LOG.debug("This is not the file you are looking for " + stat[i].getPath().getName());
      }
    }
    Assert.assertTrue(sawNewDelta);
  }

  @Test
  public void majorPartitionWithBaseMissingBuckets() throws Exception {
    LOG.debug("Starting majorPartitionWithBaseMissingBuckets");
    Table t = newTable("default", "mapwbmb", true);
    Partition p = newPartition(t, "today");


    addBaseFile(t, p, 20L, 20, 2, false);
    addDeltaFile(t, p, 21L, 22L, 2, 2, false);
    addDeltaFile(t, p, 23L, 26L, 4);

    burnThroughTransactions(27);

    CompactionRequest rqst = new CompactionRequest("default", "mapwbmb", CompactionType.MAJOR);
    rqst.setPartitionname("ds=today");
    txnHandler.compact(rqst);

    startWorker();

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    Assert.assertEquals(1, compacts.size());
    Assert.assertEquals("ready for cleaning", compacts.get(0).getState());

    // There should still be four directories in the location.
    FileSystem fs = FileSystem.get(conf);
    FileStatus[] stat = fs.listStatus(new Path(p.getSd().getLocation()));
    Assert.assertEquals(4, stat.length);

    // Find the new delta file and make sure it has the right contents
    boolean sawNewBase = false;
    for (int i = 0; i < stat.length; i++) {
      if (stat[i].getPath().getName().equals("base_0000026")) {
        sawNewBase = true;
        FileStatus[] buckets = fs.listStatus(stat[i].getPath(), FileUtils.HIDDEN_FILES_PATH_FILTER);
        Assert.assertEquals(2, buckets.length);
        Assert.assertTrue(buckets[0].getPath().getName().matches("bucket_0000[01]"));
        Assert.assertTrue(buckets[1].getPath().getName().matches("bucket_0000[01]"));
        // Bucket 0 should be small and bucket 1 should be large, make sure that's the case
        Assert.assertTrue(
            ("bucket_00000".equals(buckets[0].getPath().getName()) && 104L == buckets[0].getLen()
            && "bucket_00001".equals(buckets[1].getPath().getName()) && 676L == buckets[1]
                .getLen())
            ||
            ("bucket_00000".equals(buckets[1].getPath().getName()) && 104L == buckets[1].getLen()
            && "bucket_00001".equals(buckets[0].getPath().getName()) && 676L == buckets[0]
                .getLen())
        );
      } else {
        LOG.debug("This is not the file you are looking for " + stat[i].getPath().getName());
      }
    }
    Assert.assertTrue(sawNewBase);
  }

  @Test
  public void majorWithOpenInMiddle() throws Exception {
    LOG.debug("Starting majorWithOpenInMiddle");
    Table t = newTable("default", "mtwb", false);

    addBaseFile(t, null, 20L, 20);
    addDeltaFile(t, null, 21L, 22L, 2);
    addDeltaFile(t, null, 23L, 25L, 3);
    addLengthFile(t, null, 23L, 25L, 3);
    addDeltaFile(t, null, 26L, 27L, 2);
    burnThroughTransactions(27, new HashSet<Long>(Arrays.asList(23L)), null);

    CompactionRequest rqst = new CompactionRequest("default", "mtwb", CompactionType.MAJOR);
    txnHandler.compact(rqst);

    startWorker();

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    Assert.assertEquals(1, compacts.size());
    Assert.assertEquals("ready for cleaning", compacts.get(0).getState());

    // There should still now be 5 directories in the location
    FileSystem fs = FileSystem.get(conf);
    FileStatus[] stat = fs.listStatus(new Path(t.getSd().getLocation()));
    Assert.assertEquals(5, stat.length);

    // Find the new delta file and make sure it has the right contents
    Arrays.sort(stat);
    Assert.assertEquals("base_0000022", stat[0].getPath().getName());
    Assert.assertEquals("base_20", stat[1].getPath().getName());
    Assert.assertEquals(makeDeltaDirName(21, 22), stat[2].getPath().getName());
    Assert.assertEquals(makeDeltaDirName(23, 25), stat[3].getPath().getName());
    Assert.assertEquals(makeDeltaDirName(26, 27), stat[4].getPath().getName());
  }

  @Test
  public void majorWithAborted() throws Exception {
    LOG.debug("Starting majorWithAborted");
    Table t = newTable("default", "mtwb", false);

    addBaseFile(t, null, 20L, 20);
    addDeltaFile(t, null, 21L, 22L, 2);
    addDeltaFile(t, null, 23L, 25L, 3);
    addLengthFile(t, null, 23L, 25L, 3);
    addDeltaFile(t, null, 26L, 27L, 2);
    burnThroughTransactions(27, null, new HashSet<Long>(Arrays.asList(24L, 25L)));

    CompactionRequest rqst = new CompactionRequest("default", "mtwb", CompactionType.MAJOR);
    txnHandler.compact(rqst);

    startWorker();

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    Assert.assertEquals(1, compacts.size());
    Assert.assertEquals("ready for cleaning", compacts.get(0).getState());

    // There should still now be 5 directories in the location
    FileSystem fs = FileSystem.get(conf);
    FileStatus[] stat = fs.listStatus(new Path(t.getSd().getLocation()));
    Assert.assertEquals(5, stat.length);

    // Find the new delta file and make sure it has the right contents
    Arrays.sort(stat);
    Assert.assertEquals("base_0000027", stat[0].getPath().getName());
    Assert.assertEquals("base_20", stat[1].getPath().getName());
    Assert.assertEquals(makeDeltaDirName(21, 22), stat[2].getPath().getName());
    Assert.assertEquals(makeDeltaDirName(23, 25), stat[3].getPath().getName());
    Assert.assertEquals(makeDeltaDirName(26, 27), stat[4].getPath().getName());
  }
  @Override
  boolean useHive130DeltaDirName() {
    return false;
  }

  @Test
  public void droppedTable() throws Exception {
    Table t = newTable("default", "dt", false);

    addDeltaFile(t, null, 1L, 2L, 2);
    addDeltaFile(t, null, 3L, 4L, 2);
    burnThroughTransactions(4);

    CompactionRequest rqst = new CompactionRequest("default", "dt", CompactionType.MAJOR);
    txnHandler.compact(rqst);

    ms.dropTable("default", "dt");

    startWorker();

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    Assert.assertEquals(1, compacts.size());
    Assert.assertTrue(TxnStore.SUCCEEDED_RESPONSE.equals(compacts.get(0).getState()));
  }

  @Test
  public void droppedPartition() throws Exception {
    Table t = newTable("default", "dp", true);
    Partition p = newPartition(t, "today");

    addBaseFile(t, p, 20L, 20);
    addDeltaFile(t, p, 21L, 22L, 2);
    addDeltaFile(t, p, 23L, 24L, 2);

    burnThroughTransactions(25);

    CompactionRequest rqst = new CompactionRequest("default", "dp", CompactionType.MINOR);
    rqst.setPartitionname("ds=today");
    txnHandler.compact(rqst);

    ms.dropPartition("default", "dp", Collections.singletonList("today"), true);

    startWorker();

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    Assert.assertEquals(1, compacts.size());
    Assert.assertTrue(TxnStore.SUCCEEDED_RESPONSE.equals(rsp.getCompacts().get(0).getState()));
  }

  @After
  public void tearDown() throws Exception {
    compactorTestCleanup();
  }
}
