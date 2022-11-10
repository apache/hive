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

package org.apache.hadoop.hive.ql.io.orc;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hive.common.ValidReadTxnList;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.io.AcidDirectory;
import org.apache.hadoop.hive.ql.io.BucketCodec;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcConf;
import org.apache.orc.StripeInformation;
import org.apache.orc.impl.OrcAcidUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidReaderWriteIdList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.RecordIdentifier;
import org.apache.hadoop.hive.ql.io.RecordUpdater;
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
import org.apache.orc.OrcProto;
import org.junit.Test;
import org.mockito.MockSettings;
import org.mockito.Mockito;

import com.google.common.collect.Lists;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class TestOrcRawRecordMerger {

  private static final Logger LOG = LoggerFactory.getLogger(TestOrcRawRecordMerger.class);
  @Test
  public void testOrdering() {
    ReaderKey left = new ReaderKey(100, 200, 1200, 300);
    ReaderKey right = new ReaderKey();
    right.setValues(100, 200, 1000, 200, false);
    assertTrue(right.compareTo(left) < 0);
    assertTrue(left.compareTo(right) > 0);
    assertNotEquals(left, right);
    left.set(right);
    assertEquals(0, right.compareTo(left));
    assertEquals(right, left);
    right.setRowId(2000);
    assertTrue(right.compareTo(left) > 0);
    left.setValues(1, 2, 3, 4, false);
    right.setValues(100, 2, 3, 4, false);
    assertTrue(left.compareTo(right) < 0);
    assertTrue(right.compareTo(left) > 0);
    left.setValues(1, 2, 3, 4, false);
    right.setValues(1, 100, 3, 4, false);
    assertTrue(left.compareTo(right) < 0);
    assertTrue(right.compareTo(left) > 0);
    left.setValues(1, 2, 3, 100, false);
    right.setValues(1, 2, 3, 4, false);
    assertTrue(left.compareTo(right) < 0);
    assertTrue(right.compareTo(left) > 0);

    // ensure that we are consistent when comparing to the base class
    RecordIdentifier ri = new RecordIdentifier(1, 2, 3);
    assertEquals(1, ri.compareTo(left));
    assertEquals(-1, left.compareTo(ri));
    assertNotEquals(ri, left);
    assertNotEquals(left, ri);
  }

  @Test
  public void testIntersect() {
    OrcRawRecordMerger.KeyInterval ki1 = generateKeyInterval(1000L, 2000L);
    OrcRawRecordMerger.KeyInterval ki2 = generateKeyInterval(1500L, 2500L);
    checkIntersect(ki1, ki2, true);

    ki2 = generateKeyInterval(500L, 1000L);
    checkIntersect(ki1, ki2, true);

    ki2 = generateKeyInterval(500L, 999L);
    checkIntersect(ki1, ki2, false);

    ki2 = generateKeyInterval(500L, null);
    checkIntersect(ki1, ki2, true);

    ki2 = generateKeyInterval(2500L, null);
    checkIntersect(ki1, ki2, false);

    ki2 = generateKeyInterval(null, null);
    checkIntersect(ki1, ki2, true);
  }

  private static OrcRawRecordMerger.KeyInterval generateKeyInterval(Long minRowId, Long maxRowId) {
    RecordIdentifier min = null;
    if (minRowId != null) {
      min = new RecordIdentifier(1, 100, minRowId);
    }
    RecordIdentifier max = null;
    if (maxRowId != null) {
      max = new RecordIdentifier(1, 100, maxRowId);
    }
    return new OrcRawRecordMerger.KeyInterval(min, max);
  }

  private static void checkIntersect(OrcRawRecordMerger.KeyInterval ki1, OrcRawRecordMerger.KeyInterval ki2,
      boolean isIntersect)
  {
    if (isIntersect) {
      assertTrue(ki1.isIntersects(ki2));
      assertTrue(ki2.isIntersects(ki1));
    } else {
      assertFalse(ki1.isIntersects(ki2));
      assertFalse(ki2.isIntersects(ki1));
    }
  }

  private static void setRow(OrcStruct event,
                             int operation,
                             long originalTransaction,
                             int bucket,
                             long rowId,
                             long currentTransaction,
                             String value) {
    event.setFieldValue(OrcRecordUpdater.OPERATION, new IntWritable(operation));
    event.setFieldValue(OrcRecordUpdater.ORIGINAL_WRITEID,
        new LongWritable(originalTransaction));
    event.setFieldValue(OrcRecordUpdater.BUCKET, new IntWritable(bucket));
    event.setFieldValue(OrcRecordUpdater.ROW_ID, new LongWritable(rowId));
    event.setFieldValue(OrcRecordUpdater.CURRENT_WRITEID,
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
        new ArrayList<>(rowCounts.length);
    int stripeCount = 0;
    for(long count: rowCounts) {
      OrcProto.StripeInformation.Builder stripe =
          OrcProto.StripeInformation.newBuilder();
      stripe.setDataLength(800).setIndexLength(100).setFooterLength(100)
          .setNumberOfRows(count).setOffset(offset);
      offset += 1000;
      result.add(new ReaderImpl.StripeInformationImpl(stripe.build(), stripeCount++, -1, null));
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
    when(reader.rowsOptions(any(Reader.Options.class), any(HiveConf.class)))
        .thenReturn(recordReader);

    when(recordReader.hasNext()).
        thenReturn(true, true, true, true, true, false);

    when(recordReader.getProgress()).thenReturn(1.0f);

    when(recordReader.next(null)).thenReturn(row1);
    when(recordReader.next(row1)).thenReturn(row2);
    when(recordReader.next(row2)).thenReturn(row3);
    when(recordReader.next(row3)).thenReturn(row4);
    when(recordReader.next(row4)).thenReturn(row5);

    return reader;
  }

  private static <T> void checkReaderRecord(
      int writeId, int bucketProperty, int rowId, int currWriteId, T value, ReaderKey key, ReaderPair pair) {
    assertEquals(writeId, key.getWriteId());
    assertEquals(bucketProperty, key.getBucketProperty());
    assertEquals(rowId, key.getRowId());
    assertEquals(currWriteId, key.getCurrentWriteId());
    assertEquals(value, value(pair.nextRecord()));
  }

  private static <T> void checkMergerRecord(
      long writeId, int bucketProperty, long rowId, T value, RecordIdentifier key, OrcStruct event) {
    assertEquals(value, getValue(event));
    assertEquals(writeId, key.getWriteId());
    assertEquals(bucketProperty, key.getBucketProperty());
    assertEquals(rowId, key.getRowId());
  }

  private static <T> void checkMergerRecord(
      int operationType, T value, ReaderKey key, RecordIdentifier id, OrcStruct event) {
    checkEvent(operationType, event);
    checkMergerRecord(key.getWriteId(), key.getBucketProperty(), key.getRowId(),
        value, id, event);
  }

  private static void checkEvent(int operationType, OrcStruct event) {
    assertEquals(operationType, OrcRecordUpdater.getOperation(event));
    if (operationType == OrcRecordUpdater.DELETE_OPERATION) {
      assertNull(OrcRecordUpdater.getRow(event));
    }
  }

  @Test
  public void testReaderPair() throws Exception {
    ReaderKey key = new ReaderKey();
    Reader reader = createMockReader();
    RecordIdentifier minKey = new RecordIdentifier(10, 20, 30);
    RecordIdentifier maxKey = new RecordIdentifier(40, 50, 60);
    ReaderPair pair = new OrcRawRecordMerger.ReaderPairAcid(key, reader, minKey, maxKey,
        new Reader.Options(), new HiveConf());
    RecordReader recordReader = pair.getRecordReader();
    checkReaderRecord(10, 20, 40, 120, "third", key, pair);

    pair.next(pair.nextRecord());
    checkReaderRecord(40, 50, 60, 130, "fourth", key, pair);

    pair.next(pair.nextRecord());
    assertNull(pair.nextRecord());
    Mockito.verify(recordReader).close();
  }

  @Test
  public void testReaderPairNoMinMaxKeys() throws Exception {
    ReaderKey key = new ReaderKey();
    Reader reader = createMockReader();

    // null min and max keys forces a full scan of all records
    ReaderPair pair = new OrcRawRecordMerger.ReaderPairAcid(key, reader, null, null,
        new Reader.Options(), new HiveConf());
    RecordReader recordReader = pair.getRecordReader();
    checkReaderRecord(10, 20, 20, 100, "first", key, pair);

    pair.next(pair.nextRecord());
    checkReaderRecord(10, 20, 30, 110, "second", key, pair);

    pair.next(pair.nextRecord());
    checkReaderRecord(10, 20, 40, 120, "third", key, pair);

    pair.next(pair.nextRecord());
    checkReaderRecord(40, 50, 60, 130, "fourth", key, pair);

    pair.next(pair.nextRecord());
    checkReaderRecord(40, 50, 61, 140, "fifth", key, pair);

    pair.next(pair.nextRecord());
    assertNull(pair.nextRecord());
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

    // HiveConf argument of rowsOptions can be null
    when(reader.rowsOptions(any(Reader.Options.class), any()))
        .thenReturn(recordReader);
    when(recordReader.hasNext()).
        thenReturn(true, true, true, true, true, false);
    when(recordReader.getRowNumber()).thenReturn(0L, 1L, 2L, 3L, 4L);
    when(recordReader.next(null)).thenReturn(row1);
    when(recordReader.next(row1)).thenReturn(row2);
    when(recordReader.next(row2)).thenReturn(row3);
    when(recordReader.next(row3)).thenReturn(row4);
    when(recordReader.next(row4)).thenReturn(row5);
    return reader;
  }

  @Test
  public void testOriginalReaderPair() throws Exception {
    int BUCKET = 10;
    ReaderKey key = new ReaderKey();
    Configuration conf = new Configuration();
    int bucketProperty = OrcRawRecordMerger.encodeBucketId(conf, BUCKET, 0);
    Reader reader = createMockOriginalReader();
    RecordIdentifier minKey = new RecordIdentifier(0, bucketProperty, 1);
    RecordIdentifier maxKey = new RecordIdentifier(0, bucketProperty, 3);
    boolean[] includes = new boolean[]{true, true};
    FileSystem fs = FileSystem.getLocal(conf);
    Path root = new Path(tmpDir, "testOriginalReaderPair");
    fs.makeQualified(root);
    fs.create(root);
    ReaderPair pair = new OrcRawRecordMerger.OriginalReaderPairToRead(key, reader, BUCKET, minKey, maxKey,
        new Reader.Options().include(includes), new OrcRawRecordMerger.Options().rootPath(root),
            conf, new ValidReaderWriteIdList(), 0);

    // min/max keys drives the record reading, skipping to the 3rd row and stopping at the 4th
    RecordReader recordReader = pair.getRecordReader();
    checkReaderRecord(0, bucketProperty, 2, 0, "third", key, pair);

    pair.next(pair.nextRecord());
    checkReaderRecord(0, bucketProperty, 3, 0, "fourth", key, pair);

    pair.next(pair.nextRecord());
    assertNull(pair.nextRecord());
    Mockito.verify(recordReader).close();
  }

  private static ValidWriteIdList createMaximalTxnList() {
    return new ValidReaderWriteIdList();
  }

  @Test
  public void testOriginalReaderPairNoMinMaxKeys() throws Exception {
    int BUCKET = 10;
    ReaderKey key = new ReaderKey();
    Reader reader = createMockOriginalReader();
    Configuration conf = new Configuration();
    int bucketProperty = OrcRawRecordMerger.encodeBucketId(conf, BUCKET, 0);
    FileSystem fs = FileSystem.getLocal(conf);
    Path root = new Path(tmpDir, "testOriginalReaderPairNoMinMaxKeys");
    fs.makeQualified(root);
    fs.create(root);

    // null min and max keys forces a full scan of all records
    ReaderPair pair = new OrcRawRecordMerger.OriginalReaderPairToRead(key, reader, BUCKET, null, null,
        new Reader.Options(), new OrcRawRecordMerger.Options().rootPath(root), conf,
        new ValidReaderWriteIdList(), 0);
    checkReaderRecord(0, bucketProperty, 0, 0, "first", key, pair);

    pair.next(pair.nextRecord());
    checkReaderRecord(0, bucketProperty, 1, 0, "second", key, pair);

    pair.next(pair.nextRecord());
    checkReaderRecord(0, bucketProperty, 2, 0, "third", key, pair);

    pair.next(pair.nextRecord());
    checkReaderRecord(0, bucketProperty, 3, 0, "fourth", key, pair);

    pair.next(pair.nextRecord());
    checkReaderRecord(0, bucketProperty, 4, 0, "fifth", key, pair);

    pair.next(pair.nextRecord());
    assertNull(pair.nextRecord());
    Mockito.verify(pair.getRecordReader()).close();
  }

  private Reader buildMockReaderMerger(List<String> columnNames, List<OrcProto.Type.Kind> columnKinds,
      boolean addHiveAcidKeyIndexMetadata) throws IOException {
    Reader reader = Mockito.mock(Reader.class, settings);
    RecordReader recordReader = Mockito.mock(RecordReader.class, settings);

    when(reader.getTypes()).thenReturn(buildReaderTypes(columnNames, columnKinds));
    when(reader.rowsOptions(any(Reader.Options.class), any()))
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

    when(recordReader.getProgress()).thenReturn(1.0f);

    if (addHiveAcidKeyIndexMetadata) {
      // hasNext() and next() need re-checking after fixing https://issues.apache.org/jira/browse/HIVE-26150
      when(recordReader.hasNext()).
          thenReturn(true, true, true, true, true, false);

      when(recordReader.next(null)).thenReturn(row1, row4);
      when(recordReader.next(row1)).thenReturn(row2);
      when(recordReader.next(row2)).thenReturn(row3);
      when(recordReader.next(row3)).thenReturn(row5);
    } else {
      // it should be (true x 4, false), see https://issues.apache.org/jira/browse/HIVE-26150
      when(recordReader.hasNext()).
          thenReturn(true, true, true, true, true, true, true, true, true, false);

      when(recordReader.next(null)).thenReturn(row1);
      when(recordReader.next(row1)).thenReturn(row2);
      when(recordReader.next(row2)).thenReturn(row3);
      when(recordReader.next(row3)).thenReturn(row4);
      when(recordReader.next(row4)).thenReturn(row5);
    }

    when(reader.hasMetadataValue(OrcRecordUpdater.ACID_KEY_INDEX_NAME))
        .thenReturn(addHiveAcidKeyIndexMetadata);
    if (addHiveAcidKeyIndexMetadata) {
      when(reader.getMetadataValue(OrcRecordUpdater.ACID_KEY_INDEX_NAME))
          .thenReturn(ByteBuffer.wrap("10,20,30;40,50,60;40,50,61".getBytes(StandardCharsets.UTF_8)));
    }
    when(reader.getStripes()).thenReturn(createStripes(2, 2, 1));

    return reader;
  }

  @Test
  public void testNewBase() throws Exception {
    final List<String> columnNames = Collections.singletonList("col1");
    final List<String> columnTypes = Collections.singletonList("string");
    final List<OrcProto.Type.Kind> columnKinds = Collections.singletonList(OrcProto.Type.Kind.STRING);

    final Configuration conf = new Configuration();
    conf.set(IOConstants.SCHEMA_EVOLUTION_COLUMNS, columnNames.get(0));
    conf.set(IOConstants.SCHEMA_EVOLUTION_COLUMNS_TYPES, columnTypes.get(0));
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_TRANSACTIONAL_TABLE_SCAN, true);

    final Reader reader = buildMockReaderMerger(columnNames, columnKinds, true);

    final OrcRawRecordMerger merger = new OrcRawRecordMerger(conf, false, reader,
        false, 10, createMaximalTxnList(),
        new Reader.Options().range(1000, 1000), null, new OrcRawRecordMerger.Options());
    final RecordReader rr = merger.getCurrentReader().getRecordReader();

    assertEquals(0, merger.getOtherReaders().size());
    assertEquals(merger.getMinKey().toString(), new RecordIdentifier(10, 20, 30), merger.getMinKey());
    assertEquals(merger.getMaxKey().toString(), new RecordIdentifier(40, 50, 60), merger.getMaxKey());

    final RecordIdentifier id = merger.createKey();
    final OrcStruct event = merger.createValue();
    assertTrue(merger.next(id, event));
    checkMergerRecord(10, 20, 40, "third", id, event);

    assertTrue(merger.next(id, event));
    checkMergerRecord(40, 50, 60, "fourth", id, event);

    assertFalse(merger.next(id, event));
    assertEquals(1.0, merger.getProgress(), 0.01);
    merger.close();
    Mockito.verify(rr).close();
    Mockito.verify(rr).getProgress();

    assertRecordMergerStructFields(merger, columnNames);
  }

  @Test
  public void testNewBaseNoHiveAcidKeyIndexMetadata() throws Exception {
    final List<String> columnNames = Collections.singletonList("col1");
    final List<String> columnTypes = Collections.singletonList("string");
    final List<OrcProto.Type.Kind> columnKinds = Collections.singletonList(OrcProto.Type.Kind.STRING);

    final Configuration conf = new Configuration();
    conf.set(IOConstants.SCHEMA_EVOLUTION_COLUMNS, columnNames.get(0));
    conf.set(IOConstants.SCHEMA_EVOLUTION_COLUMNS_TYPES, columnTypes.get(0));
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_TRANSACTIONAL_TABLE_SCAN, true);

    final Reader reader = buildMockReaderMerger(columnNames, columnKinds, false);
    final OrcRawRecordMerger merger = new OrcRawRecordMerger(conf, true, reader,
        false, 10, createMaximalTxnList(),
        new Reader.Options().range(1000, 1000), null, new OrcRawRecordMerger.Options());
    final RecordReader rr = merger.getCurrentReader().getRecordReader();
    assertEquals(0, merger.getOtherReaders().size());

    assertNull(merger.getMinKey());
    assertNull(merger.getMaxKey());
    RecordIdentifier id = merger.createKey();
    OrcStruct event = merger.createValue();

    // minKey/maxKey = null => full record scan
    assertTrue(merger.next(id, event));
    checkMergerRecord(10, 20, 20, "first", id, event);

    assertTrue(merger.next(id, event));
    checkMergerRecord(10, 20, 30, "second", id, event);

    assertTrue(merger.next(id, event));
    checkMergerRecord(10, 20, 40, "third", id, event);

    assertTrue(merger.next(id, event));
    checkMergerRecord(40, 50, 60, "fourth", id, event);

    assertTrue(merger.next(id, event));
    checkMergerRecord(40, 50, 61, "fifth", id, event);

    assertEquals(1.0, merger.getProgress(), 0.01);
    merger.close();
    Mockito.verify(rr).close();
    Mockito.verify(rr).getProgress();

    assertRecordMergerStructFields(merger, columnNames);
  }

  private static List<OrcProto.Type> buildReaderTypes(List<String> columns, List<OrcProto.Type.Kind> columnTypes) {
    final List<OrcProto.Type> types = new ArrayList<>();
    final OrcProto.Type.Builder typeBuilder = OrcProto.Type.newBuilder();

    typeBuilder.setKind(OrcProto.Type.Kind.STRUCT).addSubtypes(1)
        .addSubtypes(2).addSubtypes(3).addSubtypes(4).addSubtypes(5)
        .addSubtypes(6);
    typeBuilder.addAllFieldNames(Lists.newArrayList(
        OrcRecordUpdater.OPERATION_FIELD_NAME,
        OrcRecordUpdater.CURRENT_WRITEID_FIELD_NAME,
        OrcRecordUpdater.BUCKET_FIELD_NAME,
        OrcRecordUpdater.ROW_ID_FIELD_NAME,
        OrcRecordUpdater.CURRENT_WRITEID_FIELD_NAME,
        OrcRecordUpdater.ROW_FIELD_NAME));

    types.add(typeBuilder.build());
    types.add(null);
    types.add(null);
    types.add(null);
    types.add(null);
    types.add(null);
    typeBuilder.clearSubtypes();
    typeBuilder.addSubtypes(7);
    typeBuilder.addAllFieldNames(columns);
    types.add(typeBuilder.build());
    typeBuilder.clear();
    columnTypes.forEach(typeBuilder::setKind);
    types.add(typeBuilder.build());

    return types;
  }

  private static void assertRecordMergerStructFields(OrcRawRecordMerger merger, List<String> columns) {
    StructObjectInspector eventObjectInspector =
        (StructObjectInspector) merger.getObjectInspector();
    List<? extends StructField> fields =
        eventObjectInspector.getAllStructFieldRefs();

    // check ACID (internal) columns
    assertEquals(OrcRecordUpdater.FIELDS, fields.size());
    assertEquals(OrcRecordUpdater.OPERATION_FIELD_NAME,
        fields.get(OrcRecordUpdater.OPERATION).getFieldName());
    assertEquals(OrcRecordUpdater.CURRENT_WRITEID_FIELD_NAME,
        fields.get(OrcRecordUpdater.CURRENT_WRITEID).getFieldName());
    assertEquals(OrcRecordUpdater.ORIGINAL_WRITEID_FIELD_NAME,
        fields.get(OrcRecordUpdater.ORIGINAL_WRITEID).getFieldName());
    assertEquals(OrcRecordUpdater.BUCKET_FIELD_NAME,
        fields.get(OrcRecordUpdater.BUCKET).getFieldName());
    assertEquals(OrcRecordUpdater.ROW_ID_FIELD_NAME,
        fields.get(OrcRecordUpdater.ROW_ID).getFieldName());
    StructObjectInspector rowObjectInspector =
        (StructObjectInspector) fields.get(OrcRecordUpdater.ROW)
            .getFieldObjectInspector();

    // check actual row columns (that is, user data)
    for (int i = 0; i < columns.size(); i++) {
      assertEquals(columns.get(i),
          rowObjectInspector.getAllStructFieldRefs().get(i).getFieldName());
    }
  }

  static class MyRow {
    Text col1;
    RecordIdentifier ROW__ID;

    MyRow(String val) {
      col1 = new Text(val);
    }

    MyRow(String val, long rowId, long origTxn, int bucket) {
      col1 = new Text(val);
      ROW__ID = new RecordIdentifier(origTxn, bucket, rowId);
    }

    static String getColumnNamesProperty() {
      return "col1,ROW__ID";
    }
    static String getColumnTypesProperty() {
      return "string:struct<writeId:bigint,bucketId:int,rowId:bigint>";
    }

  }

  static String getValue(OrcStruct event) {
    OrcStruct struct = OrcRecordUpdater.getRow(event);
    return struct == null ? null : struct.getFieldValue(0).toString();
  }

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  /**
   * {@link org.apache.hive.hcatalog.streaming.TestStreaming#testInterleavedTransactionBatchCommits} has more tests
   */
  @Test
  public void testGetLogicalLength() throws Exception {
    final int BUCKET = 0;
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf);
    OrcOutputFormat of = new OrcOutputFormat();
    Path root = new Path(tmpDir, "testLogicalEmpty").makeQualified(fs);
    fs.delete(root, true);
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
        (MyRow.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    /*create delta_1_1_0/bucket0 with 1 row and close the file*/
    AcidOutputFormat.Options options = new AcidOutputFormat.Options(conf)
        .inspector(inspector).bucket(BUCKET).writingBase(false).minimumWriteId(1)
        .maximumWriteId(2).finalDestination(root);
    Path delta1_2_0 = new Path(root, AcidUtils.deltaSubdir(
        options.getMinimumWriteId(), options.getMaximumWriteId(), options.getStatementId()));
    Path bucket0 = AcidUtils.createBucketFile(delta1_2_0, BUCKET);
    Path bucket0SideFile = OrcAcidUtils.getSideFile(bucket0);

    RecordUpdater ru = of.getRecordUpdater(root, options);
    ru.insert(options.getMaximumWriteId(), new MyRow("first"));
    ru.close(false);

    FileStatus bucket0File = fs.getFileStatus(bucket0);
    AcidUtils.getLogicalLength(fs, bucket0File);
    Assert.assertTrue("no " + bucket0, fs.exists(bucket0));
    Assert.assertFalse("unexpected " + bucket0SideFile, fs.exists(bucket0SideFile));
    //test getLogicalLength() w/o side file
    Assert.assertEquals("closed file size mismatch", bucket0File.getLen(),
      AcidUtils.getLogicalLength(fs, bucket0File));

    //create an empty (invalid) side file - make sure getLogicalLength() throws
    FSDataOutputStream flushLengths = fs.create(bucket0SideFile, true, 8);
    flushLengths.close();
    expectedException.expect(IOException.class);
    expectedException.expectMessage(bucket0SideFile.getName() + " found but is not readable");
    AcidUtils.getLogicalLength(fs, bucket0File);
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
        .maximumWriteId(100).finalDestination(root);
    of.getRecordUpdater(root, options).close(false);
    conf.set(ValidTxnList.VALID_TXNS_KEY,
        new ValidReadTxnList(new long[0], new BitSet(), 1000, Long.MAX_VALUE).writeToString());
    ValidWriteIdList writeIdList = new ValidReaderWriteIdList("testEmpty:200:" + Long.MAX_VALUE);
    AcidDirectory directory = AcidUtils.getAcidState(fs, root, conf, writeIdList, null, false);

    Path basePath = AcidUtils.createBucketFile(directory.getBaseDirectory(),
        BUCKET);
    Reader baseReader = OrcFile.createReader(basePath,
        OrcFile.readerOptions(conf));
    conf.set(IOConstants.SCHEMA_EVOLUTION_COLUMNS, MyRow.getColumnNamesProperty());
    conf.set(IOConstants.SCHEMA_EVOLUTION_COLUMNS_TYPES, MyRow.getColumnTypesProperty());
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_TRANSACTIONAL_TABLE_SCAN, true);
    OrcRawRecordMerger merger =
        new OrcRawRecordMerger(conf, true, baseReader, false, BUCKET,
            createMaximalTxnList(), new Reader.Options(),
            AcidUtils.getPaths(directory.getCurrentDirectories()), new OrcRawRecordMerger.Options().isCompacting(false));
    RecordIdentifier key = merger.createKey();
    OrcStruct value = merger.createValue();
    assertFalse(merger.next(key, value));
  }

  /**
   * Test the OrcRecordUpdater with the OrcRawRecordMerger when there is
   * a base and a delta.
   * @throws Exception
   */
  @Test
  public void testNewBaseAndDelta() throws Exception {
    testNewBaseAndDelta(false);
    testNewBaseAndDelta(true);
  }
  private void testNewBaseAndDelta(boolean use130Format) throws Exception {
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
        .inspector(inspector).bucket(BUCKET).finalDestination(root);
    final int BUCKET_PROPERTY = BucketCodec.V1.encode(options);
    if(!use130Format) {
      options.statementId(-1);
    }
    RecordUpdater ru = of.getRecordUpdater(root,
        options.writingBase(true).maximumWriteId(100));
    for(String v: values) {
      ru.insert(0, new MyRow(v));
    }
    ru.close(false);

    // write a delta
    ru = of.getRecordUpdater(root, options.writingBase(false)
        .minimumWriteId(200).maximumWriteId(200).recordIdColumn(1));
    ru.update(200, new MyRow("update 1", 0, 0, BUCKET_PROPERTY));
    ru.update(200, new MyRow("update 2", 2, 0, BUCKET_PROPERTY));
    ru.update(200, new MyRow("update 3", 3, 0, BUCKET_PROPERTY));
    ru.delete(200, new MyRow("", 7, 0, BUCKET_PROPERTY));
    ru.delete(200, new MyRow("", 8, 0, BUCKET_PROPERTY));
    ru.close(false);

    conf.set(ValidTxnList.VALID_TXNS_KEY,
        new ValidReadTxnList(new long[0], new BitSet(), 1000, Long.MAX_VALUE).writeToString());
    ValidWriteIdList writeIdList = new ValidReaderWriteIdList("testNewBaseAndDelta:200:" + Long.MAX_VALUE);
    AcidDirectory directory = AcidUtils.getAcidState(fs, root, conf, writeIdList, null, use130Format);

    assertEquals(new Path(root, "base_0000100"), directory.getBaseDirectory());
    assertEquals(new Path(root, use130Format ?
        AcidUtils.deleteDeltaSubdir(200,200,0) : AcidUtils.deleteDeltaSubdir(200,200)),
        directory.getCurrentDirectories().get(0).getPath());
    assertEquals(new Path(root, use130Format ?
        AcidUtils.deltaSubdir(200,200,0) : AcidUtils.deltaSubdir(200,200)),
      directory.getCurrentDirectories().get(1).getPath());

    Path basePath = AcidUtils.createBucketFile(directory.getBaseDirectory(),
        BUCKET);
    Path deltaPath = AcidUtils.createBucketFile(directory.getCurrentDirectories().get(1).getPath(),
      BUCKET);
    Path deleteDeltaDir = directory.getCurrentDirectories().get(0).getPath();

    conf.set(IOConstants.SCHEMA_EVOLUTION_COLUMNS, MyRow.getColumnNamesProperty());
    conf.set(IOConstants.SCHEMA_EVOLUTION_COLUMNS_TYPES, MyRow.getColumnTypesProperty());
    AcidUtils.setAcidOperationalProperties(conf, true, null);
    conf.setBoolean(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL, true);

    //the first "split" is for base/
    Reader baseReader = OrcFile.createReader(basePath,
        OrcFile.readerOptions(conf));
    OrcRawRecordMerger merger =
        new OrcRawRecordMerger(conf, true, baseReader, false, BUCKET,
            createMaximalTxnList(), new Reader.Options(),
            new Path[] {deleteDeltaDir}, new OrcRawRecordMerger.Options().isCompacting(false));
    assertNull(merger.getMinKey());
    assertNull(merger.getMaxKey());
    RecordIdentifier id = merger.createKey();
    OrcStruct event = merger.createValue();

    assertTrue(merger.next(id, event));
    checkMergerRecord(OrcRecordUpdater.DELETE_OPERATION, null,
        new ReaderKey(0, BUCKET_PROPERTY, 0, 200), id, event);

    assertTrue(merger.next(id, event));
    checkMergerRecord(OrcRecordUpdater.INSERT_OPERATION, "second",
        new ReaderKey(0, BUCKET_PROPERTY, 1, 0), id, event);

    assertTrue(merger.next(id, event));
    checkMergerRecord(OrcRecordUpdater.DELETE_OPERATION, null,
        new ReaderKey(0, BUCKET_PROPERTY, 2, 200), id, event);

    assertTrue(merger.next(id, event));
    checkMergerRecord(OrcRecordUpdater.DELETE_OPERATION, null,
        new ReaderKey(0, BUCKET_PROPERTY, 3, 200), id, event);

    assertTrue(merger.next(id, event));
    checkMergerRecord(OrcRecordUpdater.INSERT_OPERATION, "fifth",
        new ReaderKey(0, BUCKET_PROPERTY, 4, 0), id, event);

    assertTrue(merger.next(id, event));
    checkMergerRecord(OrcRecordUpdater.INSERT_OPERATION, "sixth",
        new ReaderKey(0, BUCKET_PROPERTY, 5, 0), id, event);

    assertTrue(merger.next(id, event));
    checkMergerRecord(OrcRecordUpdater.INSERT_OPERATION, "seventh",
        new ReaderKey(0, BUCKET_PROPERTY, 6, 0), id, event);

    assertTrue(merger.next(id, event));
    checkMergerRecord(OrcRecordUpdater.DELETE_OPERATION, null,
        new ReaderKey(0, BUCKET_PROPERTY, 7, 200), id, event);

    assertTrue(merger.next(id, event));
    checkMergerRecord(OrcRecordUpdater.DELETE_OPERATION, null,
        new ReaderKey(0, BUCKET_PROPERTY, 8, 200), id, event);

    assertTrue(merger.next(id, event));
    checkMergerRecord(OrcRecordUpdater.INSERT_OPERATION, "tenth",
        new ReaderKey(0, BUCKET_PROPERTY, 9, 0), id, event);

    assertFalse(merger.next(id, event));
    merger.close();

    //second "split" is delta_200_200
    baseReader = OrcFile.createReader(deltaPath,
      OrcFile.readerOptions(conf));
    merger =
      new OrcRawRecordMerger(conf, true, baseReader, false, BUCKET,
        createMaximalTxnList(), new Reader.Options(),
        new Path[] {deleteDeltaDir}, new OrcRawRecordMerger.Options().isCompacting(false));
    assertNull(merger.getMinKey());
    assertNull(merger.getMaxKey());

    assertTrue(merger.next(id, event));
    checkMergerRecord(OrcRecordUpdater.DELETE_OPERATION, null,
        new ReaderKey(0, BUCKET_PROPERTY, 0, 200), id, event);

    assertTrue(merger.next(id, event));
    checkMergerRecord(OrcRecordUpdater.DELETE_OPERATION, null,
        new ReaderKey(0, BUCKET_PROPERTY, 2, 200), id, event);

    assertTrue(merger.next(id, event));
    checkMergerRecord(OrcRecordUpdater.DELETE_OPERATION, null,
        new ReaderKey(0, BUCKET_PROPERTY, 3, 200), id, event);

    assertTrue(merger.next(id, event));
    checkMergerRecord(OrcRecordUpdater.DELETE_OPERATION, null,
        new ReaderKey(0, BUCKET_PROPERTY, 7, 200), id, event);

    assertTrue(merger.next(id, event));
    checkMergerRecord(OrcRecordUpdater.DELETE_OPERATION, null,
        new ReaderKey(0, BUCKET_PROPERTY, 8, 200), id, event);

    assertTrue(merger.next(id, event));
    checkMergerRecord(OrcRecordUpdater.INSERT_OPERATION, "update 1",
        new ReaderKey(200, BUCKET_PROPERTY, 0, 200), id, event);

    assertTrue(merger.next(id, event));
    checkMergerRecord(OrcRecordUpdater.INSERT_OPERATION, "update 2",
        new ReaderKey(200, BUCKET_PROPERTY, 1, 200), id, event);

    assertTrue(merger.next(id, event));
    checkMergerRecord(OrcRecordUpdater.INSERT_OPERATION, "update 3",
        new ReaderKey(200, BUCKET_PROPERTY, 2, 200), id, event);

    assertFalse(merger.next(id, event));
    merger.close();

    //now run as if it's a minor Compaction so we don't collapse events
    //here there is only 1 "split" since we only have data for 1 bucket
    merger =
      new OrcRawRecordMerger(conf, false, null, false, BUCKET,
        createMaximalTxnList(), new Reader.Options(),
        AcidUtils.getPaths(directory.getCurrentDirectories()), new OrcRawRecordMerger.Options().isCompacting(true));
    assertNull(merger.getMinKey());
    assertNull(merger.getMaxKey());

    assertTrue(merger.next(id, event));
    //minor comp, so we ignore 'base_0000100' files so all Deletes end up first since
    // they all modify primordial rows
    checkMergerRecord(OrcRecordUpdater.DELETE_OPERATION, null,
        new ReaderKey(0, BUCKET_PROPERTY, 0, 200), id, event);

    assertTrue(merger.next(id, event));
    checkMergerRecord(OrcRecordUpdater.DELETE_OPERATION, null,
        new ReaderKey(0, BUCKET_PROPERTY, 2, 200), id, event);

    assertTrue(merger.next(id, event));
    checkMergerRecord(OrcRecordUpdater.DELETE_OPERATION, null,
        new ReaderKey(0, BUCKET_PROPERTY, 3, 200), id, event);

    assertTrue(merger.next(id, event));
    checkMergerRecord(OrcRecordUpdater.DELETE_OPERATION, null,
        new ReaderKey(0, BUCKET_PROPERTY, 7, 200), id, event);

    assertTrue(merger.next(id, event));
    checkMergerRecord(OrcRecordUpdater.DELETE_OPERATION, null,
        new ReaderKey(0, BUCKET_PROPERTY, 8, 200), id, event);

    //data from delta_200_200
    assertTrue(merger.next(id, event));
    checkMergerRecord(OrcRecordUpdater.INSERT_OPERATION, "update 1",
        new ReaderKey(200, BUCKET_PROPERTY, 0, 200), id, event);

    assertTrue(merger.next(id, event));
    checkMergerRecord(OrcRecordUpdater.INSERT_OPERATION, "update 2",
        new ReaderKey(200, BUCKET_PROPERTY, 1, 200), id, event);

    assertTrue(merger.next(id, event));
    checkMergerRecord(OrcRecordUpdater.INSERT_OPERATION, "update 3",
        new ReaderKey(200, BUCKET_PROPERTY, 2, 200), id, event);

    assertFalse(merger.next(id, event));
    merger.close();

    //now run as if it's a major Compaction so we collapse events
    //here there is only 1 "split" since we only have data for 1 bucket
    baseReader = OrcFile.createReader(basePath,
      OrcFile.readerOptions(conf));
    merger =
      new OrcRawRecordMerger(conf, true, null, false, BUCKET,
        createMaximalTxnList(), new Reader.Options(),
        AcidUtils.getPaths(directory.getCurrentDirectories()), new OrcRawRecordMerger.Options()
        .isCompacting(true).isMajorCompaction(true).baseDir(new Path(root, "base_0000100")));
    assertNull(merger.getMinKey());
    assertNull(merger.getMaxKey());

    assertTrue(merger.next(id, event));
    checkMergerRecord(OrcRecordUpdater.DELETE_OPERATION, null,
        new ReaderKey(0, BUCKET_PROPERTY, 0, 200), id, event);

    assertTrue(merger.next(id, event));
    checkMergerRecord(OrcRecordUpdater.INSERT_OPERATION, "second",
        new ReaderKey(0, BUCKET_PROPERTY, 1, 0), id, event);

    assertTrue(merger.next(id, event));
    checkMergerRecord(OrcRecordUpdater.DELETE_OPERATION, null,
        new ReaderKey(0, BUCKET_PROPERTY, 2, 200), id, event);

    assertTrue(merger.next(id, event));
    checkMergerRecord(OrcRecordUpdater.DELETE_OPERATION, null,
        new ReaderKey(0, BUCKET_PROPERTY, 3, 200), id, event);

    assertTrue(merger.next(id, event));
    checkMergerRecord(OrcRecordUpdater.INSERT_OPERATION, "fifth",
        new ReaderKey(0, BUCKET_PROPERTY, 4, 0), id, event);

    assertTrue(merger.next(id, event));
    checkMergerRecord(OrcRecordUpdater.INSERT_OPERATION, "sixth",
        new ReaderKey(0, BUCKET_PROPERTY, 5, 0), id, event);

    assertTrue(merger.next(id, event));
    checkMergerRecord(OrcRecordUpdater.INSERT_OPERATION, "seventh",
        new ReaderKey(0, BUCKET_PROPERTY, 6, 0), id, event);

    assertTrue(merger.next(id, event));
    checkMergerRecord(OrcRecordUpdater.DELETE_OPERATION, null,
        new ReaderKey(0, BUCKET_PROPERTY, 7, 200), id, event);

    assertTrue(merger.next(id, event));
    checkMergerRecord(OrcRecordUpdater.DELETE_OPERATION, null,
        new ReaderKey(0, BUCKET_PROPERTY, 8, 200), id, event);

    assertTrue(merger.next(id, event));
    checkMergerRecord(OrcRecordUpdater.INSERT_OPERATION, "tenth",
        new ReaderKey(0, BUCKET_PROPERTY, 9, 0), id, event);

    //data from delta_200_200
    assertTrue(merger.next(id, event));
    checkMergerRecord(OrcRecordUpdater.INSERT_OPERATION, "update 1",
        new ReaderKey(200, BUCKET_PROPERTY, 0, 200), id, event);

    assertTrue(merger.next(id, event));
    checkMergerRecord(OrcRecordUpdater.INSERT_OPERATION, "update 2",
        new ReaderKey(200, BUCKET_PROPERTY, 1, 200), id, event);

    assertTrue(merger.next(id, event));
    checkMergerRecord(OrcRecordUpdater.INSERT_OPERATION, "update 3",
        new ReaderKey(200, BUCKET_PROPERTY, 2, 200), id, event);

    assertFalse(merger.next(id, event));
    merger.close();

    // try ignoring the 200 transaction and make sure it works still
    ValidWriteIdList writeIds = new ValidReaderWriteIdList("testNewBaseAndDelta:2000:200:200");
    //again 1st split is for base/
    baseReader = OrcFile.createReader(basePath,
      OrcFile.readerOptions(conf));
    merger =
      new OrcRawRecordMerger(conf, false, baseReader, false, BUCKET,
              writeIds, new Reader.Options(),
        new Path[] {deleteDeltaDir}, new OrcRawRecordMerger.Options().isCompacting(false));

    assertNull(merger.getMinKey());
    assertNull(merger.getMaxKey());

    for(int i=0; i < values.length; ++i) {
      assertTrue(merger.next(id, event));
      LOG.info("id = " + id + "event = " + event);
      checkMergerRecord(OrcRecordUpdater.INSERT_OPERATION, values[i],
          new ReaderKey(0, BUCKET_PROPERTY, i, 0), id, event);
    }
    assertFalse(merger.next(id, event));
    merger.close();

    // 2nd split is for delta_200_200 which is filtered out entirely by "txns"
    baseReader = OrcFile.createReader(deltaPath,
      OrcFile.readerOptions(conf));
    merger =
      new OrcRawRecordMerger(conf, false, baseReader, false, BUCKET,
              writeIds, new Reader.Options(),
        new Path[] {deleteDeltaDir}, new OrcRawRecordMerger.Options().isCompacting(false));

    assertNull(merger.getMinKey());
    assertNull(merger.getMaxKey());
    assertFalse(merger.next(id, event));
    merger.close();
  }

  static class BigRow {
    int myint;
    long mylong;
    Text mytext;
    float myfloat;
    double mydouble;
    RecordIdentifier ROW__ID;

    BigRow(int myint, long mylong, String mytext, float myfloat, double mydouble) {
      this.myint = myint;
      this.mylong = mylong;
      this.mytext = new Text(mytext);
      this.myfloat = myfloat;
      this.mydouble = mydouble;
      ROW__ID = null;
    }

    BigRow(int myint, long mylong, String mytext, float myfloat, double mydouble,
                    long rowId, long origTxn, int bucket) {
      this.myint = myint;
      this.mylong = mylong;
      this.mytext = new Text(mytext);
      this.myfloat = myfloat;
      this.mydouble = mydouble;
      ROW__ID = new RecordIdentifier(origTxn, bucket, rowId);
    }

    BigRow(long rowId, long origTxn, int bucket) {
      ROW__ID = new RecordIdentifier(origTxn, bucket, rowId);
    }

    static String getColumnNamesProperty() {
      return "myint,mylong,mytext,myfloat,mydouble,ROW__ID";
    }
    static String getColumnTypesProperty() {
      return "int:bigint:string:float:double:struct<writeId:bigint,bucketId:int,rowId:bigint>";
    }
  }

  /**
   * Test the OrcRecordUpdater with the OrcRawRecordMerger when there is
   * a base and a delta.
   * @throws Exception
   * @see #testRecordReaderNewBaseAndDelta()
   */
  @Test
  public void testRecordReaderOldBaseAndDelta() throws Exception {
    final int BUCKET = 10;
    Configuration conf = new Configuration();
    OrcConf.ROWS_BETWEEN_CHECKS.setLong(conf, 2);
    OrcOutputFormat of = new OrcOutputFormat();
    FileSystem fs = FileSystem.getLocal(conf);
    Path root = new Path(tmpDir, "testOldBaseAndDelta").makeQualified(fs);
    fs.delete(root, true);
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (BigRow.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    // make 5 stripes with 2 rows each
    Writer writer = OrcFile.createWriter(new Path(root, "0000010_0"),
        OrcFile.writerOptions(conf).inspector(inspector).fileSystem(fs)
        .blockPadding(false).bufferSize(10000).compress(CompressionKind.NONE)
        .stripeSize(1).batchSize(2).version(OrcFile.Version.V_0_11));
    String[] values= new String[]{"ignore.1", "0.1", "ignore.2", "ignore.3",
       "2.0", "2.1", "3.0", "ignore.4", "ignore.5", "ignore.6"};
    for(int i=0; i < values.length; ++i) {
      writer.addRow(new BigRow(i, i, values[i], i, i));
    }
    writer.close();

    // write a delta
    AcidOutputFormat.Options options = new AcidOutputFormat.Options(conf)
        .writingBase(false).minimumWriteId(1).maximumWriteId(1)
        .bucket(BUCKET).inspector(inspector).filesystem(fs).recordIdColumn(5)
        .finalDestination(root);

    final int BUCKET_PROPERTY = BucketCodec.V1.encode(options);

    RecordUpdater ru = of.getRecordUpdater(root, options);
    values = new String[]{"0.0", null, null, "1.1", null, null, null,
        "ignore.7"};
    for(int i=0; i < values.length; ++i) {
      if (values[i] != null) {
        ru.update(1, new BigRow(i, i, values[i], i, i, i, 0, BUCKET_PROPERTY));
      }
    }
    ru.delete(1, new BigRow(9, 0, BUCKET_PROPERTY));
    ru.close(false);//this doesn't create a key index presumably because writerOptions are not set on 'options'

    // write a delta
    options = options.minimumWriteId(100).maximumWriteId(100);
    ru = of.getRecordUpdater(root, options);
    values = new String[]{null, null, "1.0", null, null, null, null, "3.1"};
    for(int i=0; i < values.length - 1; ++i) {
      if (values[i] != null) {
        ru.update(100, new BigRow(i, i, values[i], i, i, i, 0, BUCKET_PROPERTY));
      }
    }
    //do this before next update so that delte_delta is properly sorted
    ru.delete(100, new BigRow(8, 0, BUCKET_PROPERTY));
    //because row 8 was updated and thus has a different RecordIdentifier now
    ru.update(100, new BigRow(7, 7, values[values.length - 1], 7, 7, 2, 1, BUCKET_PROPERTY));

    ru.close(false);

    MyResult[] expected = new MyResult[10];
    int k = 0;
    expected[k++] = new MyResult(0, "0.0");
    expected[k++] = new MyResult(1, "0.1");
    expected[k++] = new MyResult(2, "1.0");
    expected[k++] = new MyResult(3, "1.1");
    expected[k++] = new MyResult(4, "2.0");
    expected[k++] = new MyResult(5, "2.1");
    expected[k++] = new MyResult(6, "3.0");
    expected[k] = new MyResult(7, "3.1");

    InputFormat inf = new OrcInputFormat();
    JobConf job = new JobConf();
    job.set(ValidTxnList.VALID_TXNS_KEY,
        new ValidReadTxnList(new long[0], new BitSet(), 1000, Long.MAX_VALUE).writeToString());
    job.set(IOConstants.SCHEMA_EVOLUTION_COLUMNS, BigRow.getColumnNamesProperty());
    job.set(IOConstants.SCHEMA_EVOLUTION_COLUMNS_TYPES, BigRow.getColumnTypesProperty());
    AcidUtils.setAcidOperationalProperties(job, true, null);

    job.setBoolean(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL, true);
    job.set("mapred.min.split.size", "1");
    job.set("mapred.max.split.size", "2");
    job.set("mapred.input.dir", root.toString());
    InputSplit[] splits = inf.getSplits(job, 5);
    assertEquals(7, splits.length);
    org.apache.hadoop.mapred.RecordReader<NullWritable, OrcStruct> rr;

    for(InputSplit split : splits) {
      rr = inf.getRecordReader(split, job, Reporter.NULL);
      NullWritable key = rr.createKey();
      OrcStruct value = rr.createValue();
      while(rr.next(key, value)) {
        MyResult mr = new MyResult(Integer.parseInt(value.getFieldValue(0).toString()), value.getFieldValue(2).toString());
        int i = 0;
        for(; i < expected.length; i++) {
          if(mr.equals(expected[i])) {
            expected[i] = null;
            break;
          }
        }
        if(i >= expected.length) {
          //not found
          fail("Found unexpected row: " + mr);
        }
      }
    }
    for(MyResult mr : expected) {
      assertNull("Expected " + mr + " not found in any InputSplit", mr);
    }
  }

  /**
   * Test the RecordReader when there is a new base and a delta.
   * This test creates multiple stripes in both base and delta files which affects how many splits
   * are created on read.  With ORC-228 this could be done in E2E fashion with a query or
   * streaming ingest writing data.
   * @see #testRecordReaderOldBaseAndDelta()
   * @throws Exception
   */
  @Test
  public void testRecordReaderNewBaseAndDelta() throws Exception {
    final int BUCKET = 11;
    Configuration conf = new Configuration();
    OrcConf.ROWS_BETWEEN_CHECKS.setLong(conf, 2);
    OrcOutputFormat of = new OrcOutputFormat();
    FileSystem fs = FileSystem.getLocal(conf);
    Path root = new Path(tmpDir, "testRecordReaderNewBaseAndDelta").makeQualified(fs);
    fs.delete(root, true);
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (BigRow.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }

    // make 5 stripes with 2 rows each
    OrcRecordUpdater.OrcOptions options = (OrcRecordUpdater.OrcOptions)
        new OrcRecordUpdater.OrcOptions(conf)
        .writingBase(true).minimumWriteId(0).maximumWriteId(0)
        .bucket(BUCKET).inspector(inspector).filesystem(fs);

    final int BUCKET_PROPERTY = BucketCodec.V1.encode(options);

    options.orcOptions(OrcFile.writerOptions(conf)
        .stripeSize(1).blockPadding(false)
        .compress(CompressionKind.NONE)
        .batchSize(2));
    options.finalDestination(root);
    RecordUpdater ru = of.getRecordUpdater(root, options);
    String[] values= new String[]{"ignore.1", "0.1", "ignore.2", "ignore.3",
        "2.0", "2.1", "3.0", "ignore.4", "ignore.5", "ignore.6"};
    for(int i=0; i < values.length; ++i) {
      ru.insert(0, new BigRow(i, i, values[i], i, i));
    }
    ru.close(false);

    // write a delta
    options.writingBase(false).minimumWriteId(1).maximumWriteId(1)
        .recordIdColumn(5);
    ru = of.getRecordUpdater(root, options);
    values = new String[]{"0.0", null, null, "1.1", null, null, null,
        "ignore.7"};
    for(int i=0; i < values.length; ++i) {
      if (values[i] != null) {
        ru.update(1, new BigRow(i, i, values[i], i, i, i, 0, BUCKET_PROPERTY));
      }
    }
    ru.delete(1, new BigRow(9, 0, BUCKET_PROPERTY));
    ru.close(false);

    // write a delta
    options.minimumWriteId(100).maximumWriteId(100);
    ru = of.getRecordUpdater(root, options);
    values = new String[]{null, null, "1.0", null, null, null, null, "3.1"};
    for(int i=0; i < values.length - 1; ++i) {
      if (values[i] != null) {
        ru.update(100, new BigRow(i, i, values[i], i, i, i, 0, BUCKET_PROPERTY));
      }
    }
    //do this before next update so that delte_delta is properly sorted
    ru.delete(100, new BigRow(8, 0, BUCKET_PROPERTY));
    //because row 8 was updated and thus has a different RecordIdentifier now
    ru.update(100, new BigRow(7, 7, values[values.length - 1], 7, 7, 2, 1, BUCKET_PROPERTY));
    ru.close(false);
    MyResult[] expected = new MyResult[10];
    int k = 0;
    expected[k++] = new MyResult(0, "0.0");
    expected[k++] = new MyResult(1, "0.1");
    expected[k++] = new MyResult(2, "1.0");
    expected[k++] = new MyResult(3, "1.1");
    expected[k++] = new MyResult(4, "2.0");
    expected[k++] = new MyResult(5, "2.1");
    expected[k++] = new MyResult(6, "3.0");
    expected[k] = new MyResult(7, "3.1");

    InputFormat inf = new OrcInputFormat();
    JobConf job = new JobConf();
    job.set(ValidTxnList.VALID_TXNS_KEY,
        new ValidReadTxnList(new long[0], new BitSet(), 1000, Long.MAX_VALUE).writeToString());
    job.set("mapred.min.split.size", "1");
    job.set("mapred.max.split.size", "2");
    job.set("mapred.input.dir", root.toString());
    job.set(IOConstants.SCHEMA_EVOLUTION_COLUMNS, BigRow.getColumnNamesProperty());
    job.set(IOConstants.SCHEMA_EVOLUTION_COLUMNS_TYPES, BigRow.getColumnTypesProperty());
    AcidUtils.setAcidOperationalProperties(job, true, null);

    job.setBoolean(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL, true);
    InputSplit[] splits = inf.getSplits(job, 5);
    //base has 10 rows, so 5 splits, 1 delta has 2 rows so 1 split, and 1 delta has 3 so 2 splits
    assertEquals(8, splits.length);
    org.apache.hadoop.mapred.RecordReader<NullWritable, OrcStruct> rr;

    for(InputSplit split : splits) {
      rr = inf.getRecordReader(split, job, Reporter.NULL);
      NullWritable key = rr.createKey();
      OrcStruct value = rr.createValue();
      while(rr.next(key, value)) {
        MyResult mr = new MyResult(Integer.parseInt(value.getFieldValue(0).toString()), value.getFieldValue(2).toString());
        int i = 0;
        for(; i < expected.length; i++) {
          if(mr.equals(expected[i])) {
            expected[i] = null;
            break;
          }
        }
        if(i >= expected.length) {
          //not found
          fail("Found unexpected row: " + mr);
        }
      }
    }
    for(MyResult mr : expected) {
      assertNull("Expected " + mr + " not found in any InputSplit", mr);
    }
  }
  private static final class MyResult {
    private final int myInt;
    private final String myText;
    MyResult(int myInt, String myText) {
      this.myInt = myInt;
      this.myText = myText;
    }
    @Override
    public boolean equals(Object t) {
      if(!(t instanceof MyResult)) {
        return false;
      }
      MyResult that = (MyResult)t;
      return myInt == that.myInt && myText.equals(that.myText);
    }
    @Override
    public String toString() {
      return "(" + myInt + "," + myText +")";
    }
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
            .writingBase(false).minimumWriteId(1).maximumWriteId(1)
          .finalDestination(root);
    RecordUpdater ru = of.getRecordUpdater(root, options);
    String[][] values = {new String[]{"a", "b", "c", "d", "e"}, new String[]{"f", "g", "h", "i", "j"}};
    for(int i=0; i < values[0].length; ++i) {
      ru.insert(1, new MyRow(values[0][i]));
    }
    ru.close(false);

    // write a delta
    options.minimumWriteId(2).maximumWriteId(2);
    ru = of.getRecordUpdater(root, options);
    for(int i=0; i < values[1].length; ++i) {
      ru.insert(2, new MyRow(values[1][i]));
    }
    ru.close(false);

    InputFormat inf = new OrcInputFormat();
    JobConf job = new JobConf();
    job.set(ValidTxnList.VALID_TXNS_KEY,
        new ValidReadTxnList(new long[0], new BitSet(), 1000, Long.MAX_VALUE).writeToString());
    job.set("mapred.min.split.size", "1");
    job.set("mapred.max.split.size", "2");
    job.set("mapred.input.dir", root.toString());
    job.set("bucket_count", "1");
    job.set(IOConstants.SCHEMA_EVOLUTION_COLUMNS, MyRow.getColumnNamesProperty());
    job.set(IOConstants.SCHEMA_EVOLUTION_COLUMNS_TYPES, MyRow.getColumnTypesProperty());
    AcidUtils.setAcidOperationalProperties(job, true, null);

    job.setBoolean(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL, true);
    InputSplit[] splits = inf.getSplits(job, 5);
    assertEquals(2, splits.length);
    org.apache.hadoop.mapred.RecordReader<NullWritable, OrcStruct> rr;
    for(int j = 0; j < splits.length; j++) {
      InputSplit split = splits[j];
      rr = inf.getRecordReader(split, job, Reporter.NULL);
      OrcStruct row = rr.createValue();
      for (int i = 0; i < values[j].length; ++i) {
        System.out.println("Checking " + i);
        String msg = "split[" + j + "] at i=" + i;
        assertTrue(msg, rr.next(NullWritable.get(), row));
        assertEquals(msg, values[j][i], row.getFieldValue(0).toString());
      }
      assertFalse(rr.next(NullWritable.get(), row));
    }
  }

  /**
   * Test the RecordReader when the delta has been flushed, but not closed.
   * @throws Exception
   */
  @Test
  public void testRecordReaderIncompleteDelta() throws Exception {
    testRecordReaderIncompleteDelta(false);
    testRecordReaderIncompleteDelta(true);
  }
  /**
   * 
   * @param use130Format true means use delta_0001_0001_0000 format, else delta_0001_00001
   */
  private void testRecordReaderIncompleteDelta(boolean use130Format) throws Exception {
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
            .writingBase(true).minimumWriteId(0).maximumWriteId(0)
            .bucket(BUCKET).inspector(inspector).filesystem(fs).finalDestination(root);
    if(!use130Format) {
      options.statementId(-1);
    }
    RecordUpdater ru = of.getRecordUpdater(root, options);
    String[] values= new String[]{"1", "2", "3", "4", "5"};
    for (String s : values) {
      ru.insert(0, new MyRow(s));
    }
    ru.close(false);

    // write a delta
    options.writingBase(false).minimumWriteId(10)
        .maximumWriteId(19);
    ru = of.getRecordUpdater(root, options);
    values = new String[]{"6", "7", "8"};
    for (String s : values) {
      ru.insert(1, new MyRow(s));
    }
    InputFormat inf = new OrcInputFormat();
    JobConf job = new JobConf();
    job.set(ValidTxnList.VALID_TXNS_KEY,
        new ValidReadTxnList(new long[0], new BitSet(), 1000, Long.MAX_VALUE).writeToString());
    job.set("mapred.input.dir", root.toString());
    job.set("bucket_count", "2");
    job.set(IOConstants.SCHEMA_EVOLUTION_COLUMNS, MyRow.getColumnNamesProperty());
    job.set(IOConstants.SCHEMA_EVOLUTION_COLUMNS_TYPES, MyRow.getColumnTypesProperty());
    AcidUtils.setAcidOperationalProperties(job, true, null);

    job.setBoolean(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL, true);

    // read the keys before the delta is flushed
    InputSplit[] splits = inf.getSplits(job, 1);
    //1 split since we only have 1 bucket file in base/.  delta is not flushed (committed) yet, i.e. empty
    assertEquals(1, splits.length);
    org.apache.hadoop.mapred.RecordReader<NullWritable, OrcStruct> rr =
        inf.getRecordReader(splits[0], job, Reporter.NULL);
    NullWritable key = rr.createKey();
    OrcStruct value = rr.createValue();
    System.out.println("Looking at split " + splits[0]);
    for(int i=1; i < 6; ++i) {
      System.out.println("Checking row " + i);
      assertTrue(rr.next(key, value));
      assertEquals(Integer.toString(i), value.getFieldValue(0).toString());
    }
    assertFalse(rr.next(key, value));

    ru.flush();
    ru.flush();
    values = new String[]{"9", "10"};
    for (String s : values) {
      ru.insert(3, new MyRow(s));
    }
    ru.flush();

    splits = inf.getSplits(job, 1);
    assertEquals(2, splits.length);
    Path sideFile = new Path(root + "/" + (use130Format ? AcidUtils.deltaSubdir(10,19,0) :
      AcidUtils.deltaSubdir(10,19)) + "/bucket_00001_flush_length");
    assertTrue(fs.exists(sideFile));
    assertEquals(32, fs.getFileStatus(sideFile).getLen());

    rr = inf.getRecordReader(splits[0], job, Reporter.NULL);
    for(int i=1; i <= 5; ++i) {
      assertTrue(rr.next(key, value));
      assertEquals(Integer.toString(i), value.getFieldValue(0).toString());
    }
    assertFalse(rr.next(key, value));

    rr = inf.getRecordReader(splits[1], job, Reporter.NULL);
    for(int i=6; i < 11; ++i) {
      assertTrue("i=" + i, rr.next(key, value));
      assertEquals(Integer.toString(i), value.getFieldValue(0).toString());
    }
    assertFalse(rr.next(key, value));
  }

}
