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

package org.apache.hadoop.hive.ql.io;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.LinkedHashMap;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.HiveInputFormat.HiveInputSplit;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.mockito.InOrder;

/**
 * TestHiveBinarySearchRecordReader.
 *
 */
public class TestHiveBinarySearchRecordReader extends TestCase {

  private RCFileRecordReader rcfReader;
  private JobConf conf;
  private TestHiveInputSplit hiveSplit;
  private HiveContextAwareRecordReader hbsReader;
  private IOContext ioContext;

  private static class TestHiveInputSplit extends HiveInputSplit {

    @Override
    public long getStart() {
      return 0;
    }

    @Override
    public long getLength() {
      return 100;
    }

    @Override
    public Path getPath() {
      return new Path("/");
    }
  }

  private static class TestHiveRecordReader<K extends WritableComparable, V extends Writable>
      extends HiveContextAwareRecordReader<K, V> {

    public TestHiveRecordReader(RecordReader recordReader, JobConf conf) throws IOException {
      super(recordReader, conf);
    }

    @Override
    public K createKey() {
      return null;
    }

    @Override
    public V createValue() {
      return null;
    }

    @Override
    public long getPos() throws IOException {
      return 0;
    }

    @Override
    public boolean doNext(K key, V value) throws IOException {
      return super.doNext(key, value);
    }

    @Override
    public void doClose() throws IOException {

    }

  }

  private void resetIOContext() {
    conf.set(Utilities.INPUT_NAME, "TestHiveBinarySearchRecordReader");
    ioContext = IOContextMap.get(conf);
    ioContext.setUseSorted(false);
    ioContext.setBinarySearching(false);
    ioContext.setEndBinarySearch(false);
    ioContext.setComparison(null);
    ioContext.setGenericUDFClassName(null);
  }

  private void init() throws IOException {
    conf = new JobConf();
    resetIOContext();
    rcfReader = mock(RCFileRecordReader.class);
    when(rcfReader.next((LongWritable)anyObject(),
                        (BytesRefArrayWritable )anyObject())).thenReturn(true);
    // Since the start is 0, and the length is 100, the first call to sync should be with the value
    // 50 so return that for getPos()
    when(rcfReader.getPos()).thenReturn(50L);
    conf.setBoolean("hive.input.format.sorted", true);

    TableDesc tblDesc = Utilities.defaultTd;
    PartitionDesc partDesc = new PartitionDesc(tblDesc, null);
    LinkedHashMap<Path, PartitionDesc> pt = new LinkedHashMap<>();
    pt.put(new Path("/tmp/testfolder"), partDesc);
    MapredWork mrwork = new MapredWork();
    mrwork.getMapWork().setPathToPartitionInfo(pt);
    Utilities.setMapRedWork(conf, mrwork,new Path("/tmp/" + System.getProperty("user.name"), "hive"));

    hiveSplit = new TestHiveInputSplit();
    hbsReader = new TestHiveRecordReader(rcfReader, conf);
    hbsReader.initIOContext(hiveSplit, conf, Class.class, rcfReader);
  }

  private boolean executeDoNext(HiveContextAwareRecordReader hbsReader) throws IOException {
     return hbsReader.next(hbsReader.createKey(), hbsReader.createValue());
  }

  public void testNonLinearGreaterThan() throws Exception {
    init();
    Assert.assertTrue(executeDoNext(hbsReader));
    verify(rcfReader).sync(50);

    ioContext.setComparison(1);
    when(rcfReader.getPos()).thenReturn(25L);

    // By setting the comparison to greater, the search should use the block [0, 50]
    Assert.assertTrue(executeDoNext(hbsReader));
    verify(rcfReader).sync(25);
  }

  public void testNonLinearLessThan() throws Exception {
    init();
    Assert.assertTrue(executeDoNext(hbsReader));
    verify(rcfReader).sync(50);

    ioContext.setComparison(-1);
    when(rcfReader.getPos()).thenReturn(75L);

    // By setting the comparison to less, the search should use the block [50, 100]
    Assert.assertTrue(executeDoNext(hbsReader));
    verify(rcfReader).sync(75);
  }

  public void testNonLinearEqualTo() throws Exception {
    init();
    Assert.assertTrue(executeDoNext(hbsReader));
    verify(rcfReader).sync(50);

    ioContext.setComparison(0);
    when(rcfReader.getPos()).thenReturn(25L);

    // By setting the comparison to equal, the search should use the block [0, 50]
    Assert.assertTrue(executeDoNext(hbsReader));
    verify(rcfReader).sync(25);
  }

  public void testHitLastBlock() throws Exception {
    init();
    Assert.assertTrue(executeDoNext(hbsReader));
    verify(rcfReader).sync(50);

    ioContext.setComparison(-1);
    when(rcfReader.getPos()).thenReturn(100L);

    // When sync is called it will return 100, the value signaling the end of the file, this should
    // result in a call to sync to the beginning of the block it was searching [50, 100], and it
    // should continue normally
    Assert.assertTrue(executeDoNext(hbsReader));
    InOrder inOrder = inOrder(rcfReader);
    inOrder.verify(rcfReader).sync(75);
    inOrder.verify(rcfReader).sync(50);
    Assert.assertFalse(ioContext.isBinarySearching());
  }

  public void testHitSamePositionTwice() throws Exception {
    init();
    Assert.assertTrue(executeDoNext(hbsReader));
    verify(rcfReader).sync(50);

    ioContext.setComparison(1);

    // When getPos is called it should return the same value, signaling the end of the search, so
    // the search should continue linearly and it should sync to the beginning of the block [0, 50]
    Assert.assertTrue(executeDoNext(hbsReader));
    InOrder inOrder = inOrder(rcfReader);
    inOrder.verify(rcfReader).sync(25);
    inOrder.verify(rcfReader).sync(0);
    Assert.assertFalse(ioContext.isBinarySearching());
  }

  public void testResetRange() throws Exception {
    init();
    InOrder inOrder = inOrder(rcfReader);
    Assert.assertTrue(executeDoNext(hbsReader));
    inOrder.verify(rcfReader).sync(50);

    ioContext.setComparison(-1);
    when(rcfReader.getPos()).thenReturn(75L);

    Assert.assertTrue(executeDoNext(hbsReader));
    inOrder.verify(rcfReader).sync(75);

    ioContext.setEndBinarySearch(true);

    // This should make the search linear, sync to the beginning of the block being searched
    // [50, 100], set the comparison to be null, and the flag to reset the range should be unset
    Assert.assertTrue(executeDoNext(hbsReader));
    inOrder.verify(rcfReader).sync(50);
    Assert.assertFalse(ioContext.isBinarySearching());
    Assert.assertFalse(ioContext.shouldEndBinarySearch());
  }

  public void testEqualOpClass() throws Exception {
    init();
    ioContext.setGenericUDFClassName(GenericUDFOPEqual.class.getName());
    Assert.assertTrue(ioContext.isBinarySearching());
    Assert.assertTrue(executeDoNext(hbsReader));
    ioContext.setBinarySearching(false);
    ioContext.setComparison(-1);
    Assert.assertTrue(executeDoNext(hbsReader));
    ioContext.setComparison(0);
    Assert.assertTrue(executeDoNext(hbsReader));
    ioContext.setComparison(1);
    Assert.assertFalse(executeDoNext(hbsReader));
  }

  public void testLessThanOpClass() throws Exception {
    init();
    ioContext.setGenericUDFClassName(GenericUDFOPLessThan.class.getName());
    Assert.assertTrue(executeDoNext(hbsReader));
    Assert.assertFalse(ioContext.isBinarySearching());
    ioContext.setComparison(-1);
    Assert.assertTrue(executeDoNext(hbsReader));
    ioContext.setComparison(0);
    Assert.assertFalse(executeDoNext(hbsReader));
    ioContext.setComparison(1);
    Assert.assertFalse(executeDoNext(hbsReader));
  }

  public void testLessThanOrEqualOpClass() throws Exception {
    init();
    ioContext.setGenericUDFClassName(GenericUDFOPEqualOrLessThan.class.getName());
    Assert.assertTrue(executeDoNext(hbsReader));
    Assert.assertFalse(ioContext.isBinarySearching());
    ioContext.setComparison(-1);
    Assert.assertTrue(executeDoNext(hbsReader));
    ioContext.setComparison(0);
    Assert.assertTrue(executeDoNext(hbsReader));
    ioContext.setComparison(1);
    Assert.assertFalse(executeDoNext(hbsReader));
  }

  public void testGreaterThanOpClass() throws Exception {
    init();
    ioContext.setGenericUDFClassName(GenericUDFOPGreaterThan.class.getName());
    Assert.assertTrue(ioContext.isBinarySearching());
    Assert.assertTrue(executeDoNext(hbsReader));
    ioContext.setBinarySearching(false);
    ioContext.setComparison(-1);
    Assert.assertTrue(executeDoNext(hbsReader));
    ioContext.setComparison(0);
    Assert.assertTrue(executeDoNext(hbsReader));
    ioContext.setComparison(1);
    Assert.assertTrue(executeDoNext(hbsReader));
  }

  public void testGreaterThanOrEqualOpClass() throws Exception {
    init();
    ioContext.setGenericUDFClassName(GenericUDFOPEqualOrGreaterThan.class.getName());
    Assert.assertTrue(ioContext.isBinarySearching());
    Assert.assertTrue(executeDoNext(hbsReader));
    ioContext.setBinarySearching(false);
    ioContext.setComparison(-1);
    Assert.assertTrue(executeDoNext(hbsReader));
    ioContext.setComparison(0);
    Assert.assertTrue(executeDoNext(hbsReader));
    ioContext.setComparison(1);
    Assert.assertTrue(executeDoNext(hbsReader));
  }

  public static void main(String[] args) throws Exception {
    new TestHiveBinarySearchRecordReader().testNonLinearGreaterThan();
    new TestHiveBinarySearchRecordReader().testNonLinearLessThan();
    new TestHiveBinarySearchRecordReader().testNonLinearEqualTo();
    new TestHiveBinarySearchRecordReader().testHitLastBlock();
    new TestHiveBinarySearchRecordReader().testHitSamePositionTwice();
    new TestHiveBinarySearchRecordReader().testResetRange();
    new TestHiveBinarySearchRecordReader().testEqualOpClass();
    new TestHiveBinarySearchRecordReader().testLessThanOpClass();
    new TestHiveBinarySearchRecordReader().testLessThanOrEqualOpClass();
    new TestHiveBinarySearchRecordReader().testGreaterThanOpClass();
    new TestHiveBinarySearchRecordReader().testGreaterThanOrEqualOpClass();
  }
}
