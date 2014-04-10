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

package org.apache.hadoop.hive.ql.io.orc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidTxnListImpl;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.RecordIdentifier;
import org.apache.hadoop.hive.ql.io.RecordUpdater;
import org.apache.hadoop.hive.ql.io.orc.OrcRawRecordMerger.OriginalReaderPair;
import org.apache.hadoop.hive.ql.io.orc.OrcRawRecordMerger.ReaderKey;
import org.apache.hadoop.hive.ql.io.orc.OrcRawRecordMerger.ReaderPair;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Test;
import org.mockito.MockSettings;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestOrcRawRecordMerger {

  private static final Log LOG = LogFactory.getLog(TestOrcRawRecordMerger.class);

  @Test
  public void testOrdering() throws Exception {
    ReaderKey left = new ReaderKey(100, 200, 1200, 300);
    ReaderKey right = new ReaderKey();
    right.setValues(100, 200, 1000, 200);
    assertTrue(right.compareTo(left) < 0);
    assertTrue(left.compareTo(right) > 0);
    assertEquals(false, left.equals(right));
    left.set(right);
    assertTrue(right.compareTo(left) == 0);
    assertEquals(true, right.equals(left));
    right.setRowId(2000);
    assertTrue(right.compareTo(left) > 0);
    left.setValues(1, 2, 3, 4);
    right.setValues(100, 2, 3, 4);
    assertTrue(left.compareTo(right) < 0);
    assertTrue(right.compareTo(left) > 0);
    left.setValues(1, 2, 3, 4);
    right.setValues(1, 100, 3, 4);
    assertTrue(left.compareTo(right) < 0);
    assertTrue(right.compareTo(left) > 0);
    left.setValues(1, 2, 3, 100);
    right.setValues(1, 2, 3, 4);
    assertTrue(left.compareTo(right) < 0);
    assertTrue(right.compareTo(left) > 0);

    // ensure that we are consistent when comparing to the base class
    RecordIdentifier ri = new RecordIdentifier(1, 2, 3);
    assertEquals(1, ri.compareTo(left));
    assertEquals(-1, left.compareTo(ri));
    assertEquals(false, ri.equals(left));
    assertEquals(false, left.equals(ri));
  }

  private static void setRow(OrcStruct event,
                             int operation,
                             long originalTransaction,
                             int bucket,
                             long rowId,
                             long currentTransaction,
                             String value) {
    event.setFieldValue(OrcRecordUpdater.OPERATION, new IntWritable(operation));
    event.setFieldValue(OrcRecordUpdater.ORIGINAL_TRANSACTION,
        new LongWritable(originalTransaction));
    event.setFieldValue(OrcRecordUpdater.BUCKET, new IntWritable(bucket));
    event.setFieldValue(OrcRecordUpdater.ROW_ID, new LongWritable(rowId));
    event.setFieldValue(OrcRecordUpdater.CURRENT_TRANSACTION,
        new LongWritable(currentTransaction));
    OrcStruct row = new OrcStruct(1);
    row.setFieldValue(0, new Text(value));
    event.setFieldValue(OrcRecordUpdater.ROW, row);
  }

  private static String value(OrcStruct event) {
    return OrcRecordUpdater.getRow(event).getFieldValue(0).toString();
  }

  private List<StripeInformation> createStripes(long... rowCounts) {
    long offset = 0;
    List<StripeInformation> result =
        new ArrayList<StripeInformation>(rowCounts.length);
    for(long count: rowCounts) {
      OrcProto.StripeInformation.Builder stripe =
          OrcProto.StripeInformation.newBuilder();
      stripe.setDataLength(800).setIndexLength(100).setFooterLength(100)
          .setNumberOfRows(count).setOffset(offset);
      offset += 1000;
      result.add(new ReaderImpl.StripeInformationImpl(stripe.build()));
    }
    return result;
  }

  // can add .verboseLogging() to cause Mockito to log invocations
  private final MockSettings settings = Mockito.withSettings();
  private final Path tmpDir = new Path(System.getProperty("test.tmp.dir",
      "target" + File.separator + "test" + File.separator + "tmp"));

  private Reader createMockReader() throws IOException {
    Reader reader = Mockito.mock(Reader.class, settings);
    RecordReader recordReader = Mockito.mock(RecordReader.class, settings);
    OrcStruct row1 = new OrcStruct(OrcRecordUpdater.FIELDS);
    setRow(row1, OrcRecordUpdater.INSERT_OPERATION, 10, 20, 20, 100, "first");
    OrcStruct row2 = new OrcStruct(OrcRecordUpdater.FIELDS);
    setRow(row2, OrcRecordUpdater.INSERT_OPERATION, 10, 20, 30, 110, "second");
    OrcStruct row3 = new OrcStruct(OrcRecordUpdater.FIELDS);
    setRow(row3, OrcRecordUpdater.INSERT_OPERATION, 10, 20, 40, 120, "third");
    OrcStruct row4 = new OrcStruct(OrcRecordUpdater.FIELDS);
    setRow(row4, OrcRecordUpdater.INSERT_OPERATION, 40, 50, 60, 130, "fourth");
    OrcStruct row5 = new OrcStruct(OrcRecordUpdater.FIELDS);
    setRow(row5, OrcRecordUpdater.INSERT_OPERATION, 40, 50, 61, 140, "fifth");
    Mockito.when(reader.rowsOptions(Mockito.any(Reader.Options.class)))
        .thenReturn(recordReader);

    Mockito.when(recordReader.hasNext()).
        thenReturn(true, true, true, true, true, false);

    Mockito.when(recordReader.getProgress()).thenReturn(1.0f);

    Mockito.when(recordReader.next(null)).thenReturn(row1);
    Mockito.when(recordReader.next(row1)).thenReturn(row2);
    Mockito.when(recordReader.next(row2)).thenReturn(row3);
    Mockito.when(recordReader.next(row3)).thenReturn(row4);
    Mockito.when(recordReader.next(row4)).thenReturn(row5);

    return reader;
  }

  @Test
  public void testReaderPair() throws Exception {
    ReaderKey key = new ReaderKey();
    Reader reader = createMockReader();
    RecordIdentifier minKey = new RecordIdentifier(10, 20, 30);
    RecordIdentifier maxKey = new RecordIdentifier(40, 50, 60);
    ReaderPair pair = new ReaderPair(key, reader, 20, minKey, maxKey,
        new Reader.Options());
    RecordReader recordReader = pair.recordReader;
    assertEquals(10, key.getTransactionId());
    assertEquals(20, key.getBucketId());
    assertEquals(40, key.getRowId());
    assertEquals(120, key.getCurrentTransactionId());
    assertEquals("third", value(pair.nextRecord));

    pair.next(pair.nextRecord);
    assertEquals(40, key.getTransactionId());
    assertEquals(50, key.getBucketId());
    assertEquals(60, key.getRowId());
    assertEquals(130, key.getCurrentTransactionId());
    assertEquals("fourth", value(pair.nextRecord));

    pair.next(pair.nextRecord);
    assertEquals(null, pair.nextRecord);
    Mockito.verify(recordReader).close();
  }

  @Test
  public void testReaderPairNoMin() throws Exception {
    ReaderKey key = new ReaderKey();
    Reader reader = createMockReader();

    ReaderPair pair = new ReaderPair(key, reader, 20, null, null,
        new Reader.Options());
    RecordReader recordReader = pair.recordReader;
    assertEquals(10, key.getTransactionId());
    assertEquals(20, key.getBucketId());
    assertEquals(20, key.getRowId());
    assertEquals(100, key.getCurrentTransactionId());
    assertEquals("first", value(pair.nextRecord));

    pair.next(pair.nextRecord);
    assertEquals(10, key.getTransactionId());
    assertEquals(20, key.getBucketId());
    assertEquals(30, key.getRowId());
    assertEquals(110, key.getCurrentTransactionId());
    assertEquals("second", value(pair.nextRecord));

    pair.next(pair.nextRecord);
    assertEquals(10, key.getTransactionId());
    assertEquals(20, key.getBucketId());
    assertEquals(40, key.getRowId());
    assertEquals(120, key.getCurrentTransactionId());
    assertEquals("third", value(pair.nextRecord));

    pair.next(pair.nextRecord);
    assertEquals(40, key.getTransactionId());
    assertEquals(50, key.getBucketId());
    assertEquals(60, key.getRowId());
    assertEquals(130, key.getCurrentTransactionId());
    assertEquals("fourth", value(pair.nextRecord));

    pair.next(pair.nextRecord);
    assertEquals(40, key.getTransactionId());
    assertEquals(50, key.getBucketId());
    assertEquals(61, key.getRowId());
    assertEquals(140, key.getCurrentTransactionId());
    assertEquals("fifth", value(pair.nextRecord));

    pair.next(pair.nextRecord);
    assertEquals(null, pair.nextRecord);
    Mockito.verify(recordReader).close();
  }

  private static OrcStruct createOriginalRow(String value) {
    OrcStruct result = new OrcStruct(1);
    result.setFieldValue(0, new Text(value));
    return result;
  }

  private Reader createMockOriginalReader() throws IOException {
    Reader reader = Mockito.mock(Reader.class, settings);
    RecordReader recordReader = Mockito.mock(RecordReader.class, settings);
    OrcStruct row1 = createOriginalRow("first");
    OrcStruct row2 = createOriginalRow("second");
    OrcStruct row3 = createOriginalRow("third");
    OrcStruct row4 = createOriginalRow("fourth");
    OrcStruct row5 = createOriginalRow("fifth");

    Mockito.when(reader.rowsOptions(Mockito.any(Reader.Options.class)))
        .thenReturn(recordReader);
    Mockito.when(recordReader.hasNext()).
        thenReturn(true, true, true, true, true, false);
    Mockito.when(recordReader.getRowNumber()).thenReturn(0L, 1L, 2L, 3L, 4L);
    Mockito.when(recordReader.next(null)).thenReturn(row1);
    Mockito.when(recordReader.next(row1)).thenReturn(row2);
    Mockito.when(recordReader.next(row2)).thenReturn(row3);
    Mockito.when(recordReader.next(row3)).thenReturn(row4);
    Mockito.when(recordReader.next(row4)).thenReturn(row5);
    return reader;
  }

  @Test
  public void testOriginalReaderPair() throws Exception {
    ReaderKey key = new ReaderKey();
    Reader reader = createMockOriginalReader();
    RecordIdentifier minKey = new RecordIdentifier(0, 10, 1);
    RecordIdentifier maxKey = new RecordIdentifier(0, 10, 3);
    boolean[] includes = new boolean[]{true, true};
    ReaderPair pair = new OriginalReaderPair(key, reader, 10, minKey, maxKey,
        new Reader.Options().include(includes));
    RecordReader recordReader = pair.recordReader;
    assertEquals(0, key.getTransactionId());
    assertEquals(10, key.getBucketId());
    assertEquals(2, key.getRowId());
    assertEquals(0, key.getCurrentTransactionId());
    assertEquals("third", value(pair.nextRecord));

    pair.next(pair.nextRecord);
    assertEquals(0, key.getTransactionId());
    assertEquals(10, key.getBucketId());
    assertEquals(3, key.getRowId());
    assertEquals(0, key.getCurrentTransactionId());
    assertEquals("fourth", value(pair.nextRecord));

    pair.next(pair.nextRecord);
    assertEquals(null, pair.nextRecord);
    Mockito.verify(recordReader).close();
  }

  private static ValidTxnList createMaximalTxnList() {
    return new ValidTxnListImpl(Long.MAX_VALUE + ":");
  }

  @Test
  public void testOriginalReaderPairNoMin() throws Exception {
    ReaderKey key = new ReaderKey();
    Reader reader = createMockOriginalReader();
    ReaderPair pair = new OriginalReaderPair(key, reader, 10, null, null,
        new Reader.Options());
    assertEquals("first", value(pair.nextRecord));
    assertEquals(0, key.getTransactionId());
    assertEquals(10, key.getBucketId());
    assertEquals(0, key.getRowId());
    assertEquals(0, key.getCurrentTransactionId());

    pair.next(pair.nextRecord);
    assertEquals("second", value(pair.nextRecord));
    assertEquals(0, key.getTransactionId());
    assertEquals(10, key.getBucketId());
    assertEquals(1, key.getRowId());
    assertEquals(0, key.getCurrentTransactionId());

    pair.next(pair.nextRecord);
    assertEquals("third", value(pair.nextRecord));
    assertEquals(0, key.getTransactionId());
    assertEquals(10, key.getBucketId());
    assertEquals(2, key.getRowId());
    assertEquals(0, key.getCurrentTransactionId());

    pair.next(pair.nextRecord);
    assertEquals("fourth", value(pair.nextRecord));
    assertEquals(0, key.getTransactionId());
    assertEquals(10, key.getBucketId());
    assertEquals(3, key.getRowId());
    assertEquals(0, key.getCurrentTransactionId());

    pair.next(pair.nextRecord);
    assertEquals("fifth", value(pair.nextRecord));
    assertEquals(0, key.getTransactionId());
    assertEquals(10, key.getBucketId());
    assertEquals(4, key.getRowId());
    assertEquals(0, key.getCurrentTransactionId());

    pair.next(pair.nextRecord);
    assertEquals(null, pair.nextRecord);
    Mockito.verify(pair.recordReader).close();
  }

  @Test
  public void testNewBase() throws Exception {
    Configuration conf = new Configuration();
    conf.set("columns", "col1");
    conf.set("columns.types", "string");
    Reader reader = Mockito.mock(Reader.class, settings);
    RecordReader recordReader = Mockito.mock(RecordReader.class, settings);

    List<OrcProto.Type> types = new ArrayList<OrcProto.Type>();
    OrcProto.Type.Builder typeBuilder = OrcProto.Type.newBuilder();
    typeBuilder.setKind(OrcProto.Type.Kind.STRUCT).addSubtypes(1)
        .addSubtypes(2).addSubtypes(3).addSubtypes(4).addSubtypes(5)
        .addSubtypes(6);
    types.add(typeBuilder.build());
    types.add(null);
    types.add(null);
    types.add(null);
    types.add(null);
    types.add(null);
    typeBuilder.clearSubtypes();
    typeBuilder.addSubtypes(7);
    types.add(typeBuilder.build());

    Mockito.when(reader.getTypes()).thenReturn(types);
    Mockito.when(reader.rowsOptions(Mockito.any(Reader.Options.class)))
        .thenReturn(recordReader);

    OrcStruct row1 = new OrcStruct(OrcRecordUpdater.FIELDS);
    setRow(row1, OrcRecordUpdater.INSERT_OPERATION, 10, 20, 20, 100, "first");
    OrcStruct row2 = new OrcStruct(OrcRecordUpdater.FIELDS);
    setRow(row2, OrcRecordUpdater.INSERT_OPERATION, 10, 20, 30, 110, "second");
    OrcStruct row3 = new OrcStruct(OrcRecordUpdater.FIELDS);
    setRow(row3, OrcRecordUpdater.INSERT_OPERATION, 10, 20, 40, 120, "third");
    OrcStruct row4 = new OrcStruct(OrcRecordUpdater.FIELDS);
    setRow(row4, OrcRecordUpdater.INSERT_OPERATION, 40, 50, 60, 130, "fourth");
    OrcStruct row5 = new OrcStruct(OrcRecordUpdater.FIELDS);
    setRow(row5, OrcRecordUpdater.INSERT_OPERATION, 40, 50, 61, 140, "fifth");

    Mockito.when(recordReader.hasNext()).
        thenReturn(true, true, true, true, true, false);

    Mockito.when(recordReader.getProgress()).thenReturn(1.0f);

    Mockito.when(recordReader.next(null)).thenReturn(row1, row4);
    Mockito.when(recordReader.next(row1)).thenReturn(row2);
    Mockito.when(recordReader.next(row2)).thenReturn(row3);
    Mockito.when(recordReader.next(row3)).thenReturn(row5);

    Mockito.when(reader.getMetadataValue(OrcRecordUpdater.ACID_KEY_INDEX_NAME))
        .thenReturn(ByteBuffer.wrap("10,20,30;40,50,60;40,50,61"
            .getBytes("UTF-8")));
    Mockito.when(reader.getStripes())
        .thenReturn(createStripes(2, 2, 1));

    OrcRawRecordMerger merger = new OrcRawRecordMerger(conf, false, reader,
        false, 10, createMaximalTxnList(),
        new Reader.Options().range(1000, 1000), null);
    RecordReader rr = merger.getCurrentReader().recordReader;
    assertEquals(0, merger.getOtherReaders().size());

    assertEquals(new RecordIdentifier(10, 20, 30), merger.getMinKey());
    assertEquals(new RecordIdentifier(40, 50, 60), merger.getMaxKey());
    RecordIdentifier id = merger.createKey();
    OrcStruct event = merger.createValue();

    assertEquals(true, merger.next(id, event));
    assertEquals(10, id.getTransactionId());
    assertEquals(20, id.getBucketId());
    assertEquals(40, id.getRowId());
    assertEquals("third", getValue(event));

    assertEquals(true, merger.next(id, event));
    assertEquals(40, id.getTransactionId());
    assertEquals(50, id.getBucketId());
    assertEquals(60, id.getRowId());
    assertEquals("fourth", getValue(event));

    assertEquals(false, merger.next(id, event));
    assertEquals(1.0, merger.getProgress(), 0.01);
    merger.close();
    Mockito.verify(rr).close();
    Mockito.verify(rr).getProgress();

    StructObjectInspector eventObjectInspector =
        (StructObjectInspector) merger.getObjectInspector();
    List<? extends StructField> fields =
        eventObjectInspector.getAllStructFieldRefs();
    assertEquals(OrcRecordUpdater.FIELDS, fields.size());
    assertEquals("operation",
        fields.get(OrcRecordUpdater.OPERATION).getFieldName());
    assertEquals("currentTransaction",
        fields.get(OrcRecordUpdater.CURRENT_TRANSACTION).getFieldName());
    assertEquals("originalTransaction",
        fields.get(OrcRecordUpdater.ORIGINAL_TRANSACTION).getFieldName());
    assertEquals("bucket",
        fields.get(OrcRecordUpdater.BUCKET).getFieldName());
    assertEquals("rowId",
        fields.get(OrcRecordUpdater.ROW_ID).getFieldName());
    StructObjectInspector rowObjectInspector =
        (StructObjectInspector) fields.get(OrcRecordUpdater.ROW)
            .getFieldObjectInspector();
    assertEquals("col1",
        rowObjectInspector.getAllStructFieldRefs().get(0).getFieldName());
  }

  static class MyRow {
    Text col1;
    MyRow(String val) {
      col1 = new Text(val);
    }
  }

  static String getValue(OrcStruct event) {
    return OrcRecordUpdater.getRow(event).getFieldValue(0).toString();
  }

  @Test
  public void testEmpty() throws Exception {
    final int BUCKET = 0;
    Configuration conf = new Configuration();
    OrcOutputFormat of = new OrcOutputFormat();
    FileSystem fs = FileSystem.getLocal(conf);
    Path root = new Path(tmpDir, "testEmpty").makeQualified(fs);
    fs.delete(root, true);
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (MyRow.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }

    // write the empty base
    AcidOutputFormat.Options options = new AcidOutputFormat.Options(conf)
        .inspector(inspector).bucket(BUCKET).writingBase(true)
        .maximumTransactionId(100);
    of.getRecordUpdater(root, options).close(false);

    ValidTxnList txnList = new ValidTxnListImpl("200:");
    AcidUtils.Directory directory = AcidUtils.getAcidState(root, conf, txnList);

    Path basePath = AcidUtils.createBucketFile(directory.getBaseDirectory(),
        BUCKET);
    Reader baseReader = OrcFile.createReader(basePath,
        OrcFile.readerOptions(conf));
    OrcRawRecordMerger merger =
        new OrcRawRecordMerger(conf, true, baseReader, false, BUCKET,
            createMaximalTxnList(), new Reader.Options(),
            AcidUtils.getPaths(directory.getCurrentDirectories()));
    RecordIdentifier key = merger.createKey();
    OrcStruct value = merger.createValue();
    assertEquals(false, merger.next(key, value));
  }

  /**
   * Test the OrcRecordUpdater with the OrcRawRecordMerger when there is
   * a base and a delta.
   * @throws Exception
   */
  @Test
  public void testNewBaseAndDelta() throws Exception {
    final int BUCKET = 10;
    String[] values = new String[]{"first", "second", "third", "fourth",
                                   "fifth", "sixth", "seventh", "eighth",
                                   "ninth", "tenth"};
    Configuration conf = new Configuration();
    OrcOutputFormat of = new OrcOutputFormat();
    FileSystem fs = FileSystem.getLocal(conf);
    Path root = new Path(tmpDir, "testNewBaseAndDelta").makeQualified(fs);
    fs.delete(root, true);
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (MyRow.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }

    // write the base
    AcidOutputFormat.Options options = new AcidOutputFormat.Options(conf)
        .inspector(inspector).bucket(BUCKET);
    RecordUpdater ru = of.getRecordUpdater(root,
        options.writingBase(true).maximumTransactionId(100));
    for(String v: values) {
      ru.insert(0, new MyRow(v));
    }
    ru.close(false);

    // write a delta
    ru = of.getRecordUpdater(root, options.writingBase(false)
        .minimumTransactionId(200).maximumTransactionId(200));
    ru.update(200, 0, 0, new MyRow("update 1"));
    ru.update(200, 0, 2, new MyRow("update 2"));
    ru.update(200, 0, 3, new MyRow("update 3"));
    ru.delete(200, 0, 7);
    ru.delete(200, 0, 8);
    ru.close(false);

    ValidTxnList txnList = new ValidTxnListImpl("200:");
    AcidUtils.Directory directory = AcidUtils.getAcidState(root, conf, txnList);

    assertEquals(new Path(root, "base_0000100"), directory.getBaseDirectory());
    assertEquals(new Path(root, "delta_0000200_0000200"),
        directory.getCurrentDirectories().get(0).getPath());

    Path basePath = AcidUtils.createBucketFile(directory.getBaseDirectory(),
        BUCKET);
    Reader baseReader = OrcFile.createReader(basePath,
        OrcFile.readerOptions(conf));
    OrcRawRecordMerger merger =
        new OrcRawRecordMerger(conf, true, baseReader, false, BUCKET,
            createMaximalTxnList(), new Reader.Options(),
            AcidUtils.getPaths(directory.getCurrentDirectories()));
    assertEquals(null, merger.getMinKey());
    assertEquals(null, merger.getMaxKey());
    RecordIdentifier id = merger.createKey();
    OrcStruct event = merger.createValue();

    assertEquals(true, merger.next(id, event));
    assertEquals(OrcRecordUpdater.UPDATE_OPERATION,
        OrcRecordUpdater.getOperation(event));
    assertEquals(new ReaderKey(0, BUCKET, 0, 200), id);
    assertEquals("update 1", getValue(event));

    assertEquals(true, merger.next(id, event));
    assertEquals(OrcRecordUpdater.INSERT_OPERATION,
        OrcRecordUpdater.getOperation(event));
    assertEquals(new ReaderKey(0, BUCKET, 1, 0), id);
    assertEquals("second", getValue(event));

    assertEquals(true, merger.next(id, event));
    assertEquals(OrcRecordUpdater.UPDATE_OPERATION,
        OrcRecordUpdater.getOperation(event));
    assertEquals(new ReaderKey(0, BUCKET, 2, 200), id);
    assertEquals("update 2", getValue(event));

    assertEquals(true, merger.next(id, event));
    assertEquals(OrcRecordUpdater.UPDATE_OPERATION,
        OrcRecordUpdater.getOperation(event));
    assertEquals(new ReaderKey(0, BUCKET, 3, 200), id);
    assertEquals("update 3", getValue(event));

    assertEquals(true, merger.next(id, event));
    assertEquals(OrcRecordUpdater.INSERT_OPERATION,
        OrcRecordUpdater.getOperation(event));
    assertEquals(new ReaderKey(0, BUCKET, 4, 0), id);
    assertEquals("fifth", getValue(event));

    assertEquals(true, merger.next(id, event));
    assertEquals(OrcRecordUpdater.INSERT_OPERATION,
        OrcRecordUpdater.getOperation(event));
    assertEquals(new ReaderKey(0, BUCKET, 5, 0), id);
    assertEquals("sixth", getValue(event));

    assertEquals(true, merger.next(id, event));
    assertEquals(OrcRecordUpdater.INSERT_OPERATION,
        OrcRecordUpdater.getOperation(event));
    assertEquals(new ReaderKey(0, BUCKET, 6, 0), id);
    assertEquals("seventh", getValue(event));

    assertEquals(true, merger.next(id, event));
    assertEquals(OrcRecordUpdater.DELETE_OPERATION,
        OrcRecordUpdater.getOperation(event));
    assertEquals(new ReaderKey(0, BUCKET, 7, 200), id);
    assertEquals(null, OrcRecordUpdater.getRow(event));

    assertEquals(true, merger.next(id, event));
    assertEquals(OrcRecordUpdater.DELETE_OPERATION,
        OrcRecordUpdater.getOperation(event));
    assertEquals(new ReaderKey(0, BUCKET, 8, 200), id);
    assertEquals(null, OrcRecordUpdater.getRow(event));

    assertEquals(true, merger.next(id, event));
    assertEquals(OrcRecordUpdater.INSERT_OPERATION,
        OrcRecordUpdater.getOperation(event));
    assertEquals(new ReaderKey(0, BUCKET, 9, 0), id);
    assertEquals("tenth", getValue(event));

    assertEquals(false, merger.next(id, event));
    merger.close();

    // make a merger that doesn't collapse events
    merger = new OrcRawRecordMerger(conf, false, baseReader, false, BUCKET,
            createMaximalTxnList(), new Reader.Options(),
            AcidUtils.getPaths(directory.getCurrentDirectories()));

    assertEquals(true, merger.next(id, event));
    assertEquals(OrcRecordUpdater.UPDATE_OPERATION,
        OrcRecordUpdater.getOperation(event));
    assertEquals(new ReaderKey(0, BUCKET, 0, 200), id);
    assertEquals("update 1", getValue(event));

    assertEquals(true, merger.next(id, event));
    assertEquals(OrcRecordUpdater.INSERT_OPERATION,
        OrcRecordUpdater.getOperation(event));
    assertEquals(new ReaderKey(0, BUCKET, 0, 0), id);
    assertEquals("first", getValue(event));

    assertEquals(true, merger.next(id, event));
    assertEquals(OrcRecordUpdater.INSERT_OPERATION,
        OrcRecordUpdater.getOperation(event));
    assertEquals(new ReaderKey(0, BUCKET, 1, 0), id);
    assertEquals("second", getValue(event));

    assertEquals(true, merger.next(id, event));
    assertEquals(OrcRecordUpdater.UPDATE_OPERATION,
        OrcRecordUpdater.getOperation(event));
    assertEquals(new ReaderKey(0, BUCKET, 2, 200), id);
    assertEquals("update 2", getValue(event));

    assertEquals(true, merger.next(id, event));
    assertEquals(OrcRecordUpdater.INSERT_OPERATION,
        OrcRecordUpdater.getOperation(event));
    assertEquals(new ReaderKey(0, BUCKET, 2, 0), id);
    assertEquals("third", getValue(event));

    assertEquals(true, merger.next(id, event));
    assertEquals(OrcRecordUpdater.UPDATE_OPERATION,
        OrcRecordUpdater.getOperation(event));
    assertEquals(new ReaderKey(0, BUCKET, 3, 200), id);
    assertEquals("update 3", getValue(event));

    assertEquals(true, merger.next(id, event));
    assertEquals(OrcRecordUpdater.INSERT_OPERATION,
        OrcRecordUpdater.getOperation(event));
    assertEquals(new ReaderKey(0, BUCKET, 3, 0), id);
    assertEquals("fourth", getValue(event));

    assertEquals(true, merger.next(id, event));
    assertEquals(OrcRecordUpdater.INSERT_OPERATION,
        OrcRecordUpdater.getOperation(event));
    assertEquals(new ReaderKey(0, BUCKET, 4, 0), id);
    assertEquals("fifth", getValue(event));

    assertEquals(true, merger.next(id, event));
    assertEquals(OrcRecordUpdater.INSERT_OPERATION,
        OrcRecordUpdater.getOperation(event));
    assertEquals(new ReaderKey(0, BUCKET, 5, 0), id);
    assertEquals("sixth", getValue(event));

    assertEquals(true, merger.next(id, event));
    assertEquals(OrcRecordUpdater.INSERT_OPERATION,
        OrcRecordUpdater.getOperation(event));
    assertEquals(new ReaderKey(0, BUCKET, 6, 0), id);
    assertEquals("seventh", getValue(event));

    assertEquals(true, merger.next(id, event));
    assertEquals(OrcRecordUpdater.DELETE_OPERATION,
        OrcRecordUpdater.getOperation(event));
    assertEquals(new ReaderKey(0, BUCKET, 7, 200), id);
    assertEquals(null, OrcRecordUpdater.getRow(event));

    assertEquals(true, merger.next(id, event));
    assertEquals(OrcRecordUpdater.INSERT_OPERATION,
        OrcRecordUpdater.getOperation(event));
    assertEquals(new ReaderKey(0, BUCKET, 7, 0), id);
    assertEquals("eighth", getValue(event));

    assertEquals(true, merger.next(id, event));
    assertEquals(OrcRecordUpdater.DELETE_OPERATION,
        OrcRecordUpdater.getOperation(event));
    assertEquals(new ReaderKey(0, BUCKET, 8, 200), id);
    assertEquals(null, OrcRecordUpdater.getRow(event));

    assertEquals(true, merger.next(id, event));
    assertEquals(OrcRecordUpdater.INSERT_OPERATION,
        OrcRecordUpdater.getOperation(event));
    assertEquals(new ReaderKey(0, BUCKET, 8, 0), id);
    assertEquals("ninth", getValue(event));

    assertEquals(true, merger.next(id, event));
    assertEquals(OrcRecordUpdater.INSERT_OPERATION,
        OrcRecordUpdater.getOperation(event));
    assertEquals(new ReaderKey(0, BUCKET, 9, 0), id);
    assertEquals("tenth", getValue(event));

    assertEquals(false, merger.next(id, event));
    merger.close();

    // try ignoring the 200 transaction and make sure it works still
    ValidTxnList txns = new ValidTxnListImpl("2000:200");
    merger =
        new OrcRawRecordMerger(conf, true, baseReader, false, BUCKET,
            txns, new Reader.Options(),
            AcidUtils.getPaths(directory.getCurrentDirectories()));
    for(int i=0; i < values.length; ++i) {
      assertEquals(true, merger.next(id, event));
      LOG.info("id = " + id + "event = " + event);
      assertEquals(OrcRecordUpdater.INSERT_OPERATION,
          OrcRecordUpdater.getOperation(event));
      assertEquals(new ReaderKey(0, BUCKET, i, 0), id);
      assertEquals(values[i], getValue(event));
    }

    assertEquals(false, merger.next(id, event));
    merger.close();
  }

  static class BigRow {
    int myint;
    long mylong;
    Text mytext;
    float myfloat;
    double mydouble;

    BigRow(int myint, long mylong, String mytext, float myfloat, double mydouble) {
      this.myint = myint;
      this.mylong = mylong;
      this.mytext = new Text(mytext);
      this.myfloat = myfloat;
      this.mydouble = mydouble;
    }
  }

  /**
   * Test the OrcRecordUpdater with the OrcRawRecordMerger when there is
   * a base and a delta.
   * @throws Exception
   */
  @Test
  public void testRecordReaderOldBaseAndDelta() throws Exception {
    final int BUCKET = 10;
    Configuration conf = new Configuration();
    OrcOutputFormat of = new OrcOutputFormat();
    FileSystem fs = FileSystem.getLocal(conf);
    Path root = new Path(tmpDir, "testOldBaseAndDelta").makeQualified(fs);
    fs.delete(root, true);
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (BigRow.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }

    // write the base
    MemoryManager mgr = new MemoryManager(conf){
      int rowsAddedSinceCheck = 0;

      synchronized void addedRow() throws IOException {
        if (++rowsAddedSinceCheck >= 2) {
          notifyWriters();
          rowsAddedSinceCheck = 0;
        }
      }
    };
    // make 5 stripes with 2 rows each
    Writer writer = OrcFile.createWriter(new Path(root, "0000010_0"),
        OrcFile.writerOptions(conf).inspector(inspector).fileSystem(fs)
        .blockPadding(false).bufferSize(10000).compress(CompressionKind.NONE)
        .stripeSize(1).memory(mgr).version(OrcFile.Version.V_0_11));
    String[] values= new String[]{"ignore.1", "0.1", "ignore.2", "ignore.3",
       "2.0", "2.1", "3.0", "ignore.4", "ignore.5", "ignore.6"};
    for(int i=0; i < values.length; ++i) {
      writer.addRow(new BigRow(i, i, values[i], i, i));
    }
    writer.close();

    // write a delta
    AcidOutputFormat.Options options = new AcidOutputFormat.Options(conf)
        .writingBase(false).minimumTransactionId(1).maximumTransactionId(1)
        .bucket(BUCKET).inspector(inspector).filesystem(fs);
    RecordUpdater ru = of.getRecordUpdater(root, options);
    values = new String[]{"0.0", null, null, "1.1", null, null, null,
        "ignore.7"};
    for(int i=0; i < values.length; ++i) {
      if (values[i] != null) {
        ru.update(1, 0, i, new BigRow(i, i, values[i], i, i));
      }
    }
    ru.delete(100, 0, 9);
    ru.close(false);

    // write a delta
    options = options.minimumTransactionId(2).maximumTransactionId(2);
    ru = of.getRecordUpdater(root, options);
    values = new String[]{null, null, "1.0", null, null, null, null, "3.1"};
    for(int i=0; i < values.length; ++i) {
      if (values[i] != null) {
        ru.update(2, 0, i, new BigRow(i, i, values[i], i, i));
      }
    }
    ru.delete(100, 0, 8);
    ru.close(false);

    InputFormat inf = new OrcInputFormat();
    JobConf job = new JobConf();
    job.set("mapred.min.split.size", "1");
    job.set("mapred.max.split.size", "2");
    job.set("mapred.input.dir", root.toString());
    InputSplit[] splits = inf.getSplits(job, 5);
    assertEquals(5, splits.length);
    org.apache.hadoop.mapred.RecordReader<NullWritable, OrcStruct> rr;

    // loop through the 5 splits and read each
    for(int i=0; i < 4; ++i) {
      System.out.println("starting split " + i);
      rr = inf.getRecordReader(splits[i], job, Reporter.NULL);
      NullWritable key = rr.createKey();
      OrcStruct value = rr.createValue();

      // there should be exactly two rows per a split
      for(int j=0; j < 2; ++j) {
        System.out.println("i = " + i + ", j = " + j);
        assertEquals(true, rr.next(key, value));
        System.out.println("record = " + value);
        assertEquals(i + "." + j, value.getFieldValue(2).toString());
      }
      assertEquals(false, rr.next(key, value));
    }
    rr = inf.getRecordReader(splits[4], job, Reporter.NULL);
    assertEquals(false, rr.next(rr.createKey(), rr.createValue()));
  }

  /**
   * Test the RecordReader when there is a new base and a delta.
   * @throws Exception
   */
  @Test
  public void testRecordReaderNewBaseAndDelta() throws Exception {
    final int BUCKET = 10;
    Configuration conf = new Configuration();
    OrcOutputFormat of = new OrcOutputFormat();
    FileSystem fs = FileSystem.getLocal(conf);
    Path root = new Path(tmpDir, "testRecordReaderNewBaseAndDelta").makeQualified(fs);
    fs.delete(root, true);
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (BigRow.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }

    // write the base
    MemoryManager mgr = new MemoryManager(conf){
      int rowsAddedSinceCheck = 0;

      synchronized void addedRow() throws IOException {
        if (++rowsAddedSinceCheck >= 2) {
          notifyWriters();
          rowsAddedSinceCheck = 0;
        }
      }
    };

    // make 5 stripes with 2 rows each
    OrcRecordUpdater.OrcOptions options = (OrcRecordUpdater.OrcOptions)
        new OrcRecordUpdater.OrcOptions(conf)
        .writingBase(true).minimumTransactionId(0).maximumTransactionId(0)
        .bucket(BUCKET).inspector(inspector).filesystem(fs);
    options.orcOptions(OrcFile.writerOptions(conf)
      .stripeSize(1).blockPadding(false).compress(CompressionKind.NONE)
      .memory(mgr));
    RecordUpdater ru = of.getRecordUpdater(root, options);
    String[] values= new String[]{"ignore.1", "0.1", "ignore.2", "ignore.3",
        "2.0", "2.1", "3.0", "ignore.4", "ignore.5", "ignore.6"};
    for(int i=0; i < values.length; ++i) {
      ru.insert(0, new BigRow(i, i, values[i], i, i));
    }
    ru.close(false);

    // write a delta
    options.writingBase(false).minimumTransactionId(1).maximumTransactionId(1);
    ru = of.getRecordUpdater(root, options);
    values = new String[]{"0.0", null, null, "1.1", null, null, null,
        "ignore.7"};
    for(int i=0; i < values.length; ++i) {
      if (values[i] != null) {
        ru.update(1, 0, i, new BigRow(i, i, values[i], i, i));
      }
    }
    ru.delete(100, 0, 9);
    ru.close(false);

    // write a delta
    options.minimumTransactionId(2).maximumTransactionId(2);
    ru = of.getRecordUpdater(root, options);
    values = new String[]{null, null, "1.0", null, null, null, null, "3.1"};
    for(int i=0; i < values.length; ++i) {
      if (values[i] != null) {
        ru.update(2, 0, i, new BigRow(i, i, values[i], i, i));
      }
    }
    ru.delete(100, 0, 8);
    ru.close(false);

    InputFormat inf = new OrcInputFormat();
    JobConf job = new JobConf();
    job.set("mapred.min.split.size", "1");
    job.set("mapred.max.split.size", "2");
    job.set("mapred.input.dir", root.toString());
    InputSplit[] splits = inf.getSplits(job, 5);
    assertEquals(5, splits.length);
    org.apache.hadoop.mapred.RecordReader<NullWritable, OrcStruct> rr;

    // loop through the 5 splits and read each
    for(int i=0; i < 4; ++i) {
      System.out.println("starting split " + i);
      rr = inf.getRecordReader(splits[i], job, Reporter.NULL);
      NullWritable key = rr.createKey();
      OrcStruct value = rr.createValue();

      // there should be exactly two rows per a split
      for(int j=0; j < 2; ++j) {
        System.out.println("i = " + i + ", j = " + j);
        assertEquals(true, rr.next(key, value));
        System.out.println("record = " + value);
        assertEquals(i + "." + j, value.getFieldValue(2).toString());
      }
      assertEquals(false, rr.next(key, value));
    }
    rr = inf.getRecordReader(splits[4], job, Reporter.NULL);
    assertEquals(false, rr.next(rr.createKey(), rr.createValue()));
  }

  /**
   * Test the RecordReader when there is a new base and a delta.
   * @throws Exception
   */
  @Test
  public void testRecordReaderDelta() throws Exception {
    final int BUCKET = 0;
    Configuration conf = new Configuration();
    OrcOutputFormat of = new OrcOutputFormat();
    FileSystem fs = FileSystem.getLocal(conf);
    Path root = new Path(tmpDir, "testRecordReaderDelta").makeQualified(fs);
    fs.delete(root, true);
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (MyRow.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }

    // write a delta
    AcidOutputFormat.Options options =
        new AcidOutputFormat.Options(conf)
            .bucket(BUCKET).inspector(inspector).filesystem(fs)
            .writingBase(false).minimumTransactionId(1).maximumTransactionId(1);
    RecordUpdater ru = of.getRecordUpdater(root, options);
    String[] values = new String[]{"a", "b", "c", "d", "e"};
    for(int i=0; i < values.length; ++i) {
      ru.insert(1, new MyRow(values[i]));
    }
    ru.close(false);

    // write a delta
    options.minimumTransactionId(2).maximumTransactionId(2);
    ru = of.getRecordUpdater(root, options);
    values = new String[]{"f", "g", "h", "i", "j"};
    for(int i=0; i < values.length; ++i) {
      ru.insert(2, new MyRow(values[i]));
    }
    ru.close(false);

    InputFormat inf = new OrcInputFormat();
    JobConf job = new JobConf();
    job.set("mapred.min.split.size", "1");
    job.set("mapred.max.split.size", "2");
    job.set("mapred.input.dir", root.toString());
    job.set("bucket_count", "1");
    InputSplit[] splits = inf.getSplits(job, 5);
    assertEquals(1, splits.length);
    org.apache.hadoop.mapred.RecordReader<NullWritable, OrcStruct> rr;
    rr = inf.getRecordReader(splits[0], job, Reporter.NULL);
    values = new String[]{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"};
    OrcStruct row = rr.createValue();
    for(int i = 0; i < values.length; ++i) {
      System.out.println("Checking " + i);
      assertEquals(true, rr.next(NullWritable.get(), row));
      assertEquals(values[i], row.getFieldValue(0).toString());
    }
    assertEquals(false, rr.next(NullWritable.get(), row));
  }

  /**
   * Test the RecordReader when the delta has been flushed, but not closed.
   * @throws Exception
   */
  @Test
  public void testRecordReaderIncompleteDelta() throws Exception {
    final int BUCKET = 1;
    Configuration conf = new Configuration();
    OrcOutputFormat of = new OrcOutputFormat();
    FileSystem fs = FileSystem.getLocal(conf).getRaw();
    Path root = new Path(tmpDir, "testRecordReaderIncompleteDelta").makeQualified(fs);
    fs.delete(root, true);
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (MyRow.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }

    // write a base
    AcidOutputFormat.Options options =
        new AcidOutputFormat.Options(conf)
            .writingBase(true).minimumTransactionId(0).maximumTransactionId(0)
            .bucket(BUCKET).inspector(inspector).filesystem(fs);
    RecordUpdater ru = of.getRecordUpdater(root, options);
    String[] values= new String[]{"1", "2", "3", "4", "5"};
    for(int i=0; i < values.length; ++i) {
      ru.insert(0, new MyRow(values[i]));
    }
    ru.close(false);

    // write a delta
    options.writingBase(false).minimumTransactionId(10)
        .maximumTransactionId(19);
    ru = of.getRecordUpdater(root, options);
    values = new String[]{"6", "7", "8"};
    for(int i=0; i < values.length; ++i) {
      ru.insert(1, new MyRow(values[i]));
    }
    InputFormat inf = new OrcInputFormat();
    JobConf job = new JobConf();
    job.set("mapred.input.dir", root.toString());
    job.set("bucket_count", "2");

    // read the keys before the delta is flushed
    InputSplit[] splits = inf.getSplits(job, 1);
    assertEquals(2, splits.length);
    org.apache.hadoop.mapred.RecordReader<NullWritable, OrcStruct> rr =
        inf.getRecordReader(splits[0], job, Reporter.NULL);
    NullWritable key = rr.createKey();
    OrcStruct value = rr.createValue();
    System.out.println("Looking at split " + splits[0]);
    for(int i=1; i < 6; ++i) {
      System.out.println("Checking row " + i);
      assertEquals(true, rr.next(key, value));
      assertEquals(Integer.toString(i), value.getFieldValue(0).toString());
    }
    assertEquals(false, rr.next(key, value));

    ru.flush();
    ru.flush();
    values = new String[]{"9", "10"};
    for(int i=0; i < values.length; ++i) {
      ru.insert(3, new MyRow(values[i]));
    }
    ru.flush();

    splits = inf.getSplits(job, 1);
    assertEquals(2, splits.length);
    rr = inf.getRecordReader(splits[0], job, Reporter.NULL);
    Path sideFile = new Path(root +
        "/delta_0000010_0000019/bucket_00001_flush_length");
    assertEquals(true, fs.exists(sideFile));
    assertEquals(24, fs.getFileStatus(sideFile).getLen());

    for(int i=1; i < 11; ++i) {
      assertEquals(true, rr.next(key, value));
      assertEquals(Integer.toString(i), value.getFieldValue(0).toString());
    }
    assertEquals(false, rr.next(key, value));
  }

}
