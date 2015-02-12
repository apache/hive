/**
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * Tests for the worker thread and its MR jobs.
 */
public class TestWorker extends CompactorTest {
  static final private String CLASS_NAME = TestWorker.class.getName();
  static final private Log LOG = LogFactory.getLog(CLASS_NAME);

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
    CompactorMR.StringableMap m = new CompactorMR.StringableMap(new HashMap<String, String>());
    String s = m.toString();
    Assert.assertEquals("0:", s);
    m = new CompactorMR.StringableMap(s);
    Assert.assertEquals(0, m.size());

    Map<String, String> base = new HashMap<String, String>();
    base.put("mary", "poppins");
    base.put("bert", null);
    base.put(null, "banks");
    m = new CompactorMR.StringableMap(base);
    s = m.toString();
    m = new CompactorMR.StringableMap(s);
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

    Table t = newTable("default", "st", false, new HashMap<String, String>(), sortCols);

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

    Table t = newTable("default", "sp", true, new HashMap<String, String>(), sortCols);
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
      if (stat[i].getPath().getName().equals("delta_0000021_0000024")) {
        sawNewDelta = true;
        FileStatus[] buckets = fs.listStatus(stat[i].getPath());
        Assert.assertEquals(2, buckets.length);
        Assert.assertTrue(buckets[0].getPath().getName().matches("bucket_0000[01]"));
        Assert.assertTrue(buckets[1].getPath().getName().matches("bucket_0000[01]"));
        Assert.assertEquals(208L, buckets[0].getLen());
        Assert.assertEquals(208L, buckets[1].getLen());
      } else {
        LOG.debug("This is not the delta file you are looking for " + stat[i].getPath().getName());
      }
    }
    Assert.assertTrue(sawNewDelta);
  }

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
    Assert.assertEquals("delta_0000021_0000022", stat[1].getPath().getName());
    Assert.assertEquals("delta_21_22", stat[2].getPath().getName());
    Assert.assertEquals("delta_23_25", stat[3].getPath().getName());
    Assert.assertEquals("delta_26_27", stat[4].getPath().getName());
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

    // There should still now be 5 directories in the location
    FileSystem fs = FileSystem.get(conf);
    FileStatus[] stat = fs.listStatus(new Path(t.getSd().getLocation()));
    Assert.assertEquals(5, stat.length);

    // Find the new delta file and make sure it has the right contents
    Arrays.sort(stat);
    Assert.assertEquals("base_20", stat[0].getPath().getName());
    Assert.assertEquals("delta_0000021_0000027", stat[1].getPath().getName());
    Assert.assertEquals("delta_21_22", stat[2].getPath().getName());
    Assert.assertEquals("delta_23_25", stat[3].getPath().getName());
    Assert.assertEquals("delta_26_27", stat[4].getPath().getName());
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
    boolean sawNewDelta = false;
    for (int i = 0; i < stat.length; i++) {
      if (stat[i].getPath().getName().equals("delta_0000021_0000024")) {
        sawNewDelta = true;
        FileStatus[] buckets = fs.listStatus(stat[i].getPath());
        Assert.assertEquals(2, buckets.length);
        Assert.assertTrue(buckets[0].getPath().getName().matches("bucket_0000[01]"));
        Assert.assertTrue(buckets[1].getPath().getName().matches("bucket_0000[01]"));
        Assert.assertEquals(208L, buckets[0].getLen());
        Assert.assertEquals(208L, buckets[1].getLen());
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
    Assert.assertEquals(3, stat.length);

    // Find the new delta file and make sure it has the right contents
    boolean sawNewDelta = false;
    for (int i = 0; i < stat.length; i++) {
      if (stat[i].getPath().getName().equals("delta_0000001_0000004")) {
        sawNewDelta = true;
        FileStatus[] buckets = fs.listStatus(stat[i].getPath());
        Assert.assertEquals(2, buckets.length);
        Assert.assertTrue(buckets[0].getPath().getName().matches("bucket_0000[01]"));
        Assert.assertTrue(buckets[1].getPath().getName().matches("bucket_0000[01]"));
        Assert.assertEquals(208L, buckets[0].getLen());
        Assert.assertEquals(208L, buckets[1].getLen());
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
        FileStatus[] buckets = fs.listStatus(stat[i].getPath());
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
        FileStatus[] buckets = fs.listStatus(stat[i].getPath());
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
        FileStatus[] buckets = fs.listStatus(stat[i].getPath());
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
        FileStatus[] buckets = fs.listStatus(stat[i].getPath());
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
      if (stat[i].getPath().getName().equals("delta_0000021_0000024")) {
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
        FileStatus[] buckets = fs.listStatus(stat[i].getPath());
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
    Assert.assertEquals("delta_21_22", stat[2].getPath().getName());
    Assert.assertEquals("delta_23_25", stat[3].getPath().getName());
    Assert.assertEquals("delta_26_27", stat[4].getPath().getName());
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
    Assert.assertEquals("delta_21_22", stat[2].getPath().getName());
    Assert.assertEquals("delta_23_25", stat[3].getPath().getName());
    Assert.assertEquals("delta_26_27", stat[4].getPath().getName());
  }
}
