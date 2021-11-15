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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.BucketCodec;
import org.apache.hadoop.hive.ql.io.RecordIdentifier;
import org.apache.hadoop.hive.ql.io.RecordUpdater;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Reporter;
import org.apache.orc.impl.OrcAcidUtils;
import org.apache.orc.tools.FileDump;
import org.junit.Test;

public class TestOrcRecordUpdater {

  @Test
  public void testAccessors() throws Exception {
    OrcStruct event = new OrcStruct(OrcRecordUpdater.FIELDS);
    event.setFieldValue(OrcRecordUpdater.OPERATION,
        new IntWritable(OrcRecordUpdater.INSERT_OPERATION));
    event.setFieldValue(OrcRecordUpdater.CURRENT_WRITEID,
        new LongWritable(100));
    event.setFieldValue(OrcRecordUpdater.ORIGINAL_WRITEID,
        new LongWritable(50));
    event.setFieldValue(OrcRecordUpdater.BUCKET, new IntWritable(200));
    event.setFieldValue(OrcRecordUpdater.ROW_ID, new LongWritable(300));
    assertEquals(OrcRecordUpdater.INSERT_OPERATION,
        OrcRecordUpdater.getOperation(event));
    assertEquals(50, OrcRecordUpdater.getOriginalTransaction(event));
    assertEquals(100, OrcRecordUpdater.getCurrentTransaction(event));
    assertEquals(200, OrcRecordUpdater.getBucket(event));
    assertEquals(300, OrcRecordUpdater.getRowId(event));
  }

  Path workDir = new Path(System.getProperty("test.tmp.dir",
      "target" + File.separator + "test" + File.separator + "tmp"));

  static class MyRow {
    Text field;
    RecordIdentifier ROW__ID;

    MyRow(String val) {
      field = new Text(val);
      ROW__ID = null;
    }

    MyRow(String val, long rowId, long origTxn, int bucket) {
      field = new Text(val);
      ROW__ID = new RecordIdentifier(origTxn, bucket, rowId);
    }

  }

  @Test
  public void testWriter() throws Exception {
    Path root = new Path(workDir, "testWriter");
    Configuration conf = new Configuration();
    // Must use raw local because the checksummer doesn't honor flushes.
    FileSystem fs = FileSystem.getLocal(conf).getRaw();
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (MyRow.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    AcidOutputFormat.Options options = new AcidOutputFormat.Options(conf)
        .filesystem(fs)
        .bucket(10)
        .writingBase(false)
        .minimumWriteId(10)
        .maximumWriteId(19)
        .inspector(inspector)
        .reporter(Reporter.NULL)
        .finalDestination(root);
    RecordUpdater updater = new OrcRecordUpdater(root, options);
    updater.insert(11, new MyRow("first"));
    updater.insert(11, new MyRow("second"));
    updater.insert(11, new MyRow("third"));
    updater.flush();
    updater.insert(12, new MyRow("fourth"));
    updater.insert(12, new MyRow("fifth"));
    updater.flush();

    // Check the stats
    assertEquals(5L, updater.getStats().getRowCount());

    Path bucketPath = AcidUtils.createFilename(root, options);
    Path sidePath = OrcAcidUtils.getSideFile(bucketPath);
    DataInputStream side = fs.open(sidePath);

    // read the stopping point for the first flush and make sure we only see
    // 3 rows
    long len = side.readLong();
    len = side.readLong();
    Reader reader = OrcFile.createReader(bucketPath,
        new OrcFile.ReaderOptions(conf).filesystem(fs).maxLength(len));
    assertEquals(3, reader.getNumberOfRows());

    // read the second flush and make sure we see all 5 rows
    len = side.readLong();
    side.close();
    reader = OrcFile.createReader(bucketPath,
        new OrcFile.ReaderOptions(conf).filesystem(fs).maxLength(len));
    assertEquals(5, reader.getNumberOfRows());
    RecordReader rows = reader.rows();

    // check the contents of the file
    assertEquals(true, rows.hasNext());
    OrcStruct row = (OrcStruct) rows.next(null);
    assertEquals(OrcRecordUpdater.INSERT_OPERATION,
        OrcRecordUpdater.getOperation(row));
    assertEquals(11, OrcRecordUpdater.getCurrentTransaction(row));
    assertEquals(11, OrcRecordUpdater.getOriginalTransaction(row));
    assertEquals(10, getBucketId(row));
    assertEquals(0, OrcRecordUpdater.getRowId(row));
    assertEquals("first",
        OrcRecordUpdater.getRow(row).getFieldValue(0).toString());
    assertEquals(true, rows.hasNext());
    row = (OrcStruct) rows.next(null);
    assertEquals(1, OrcRecordUpdater.getRowId(row));
    assertEquals(10, getBucketId(row));
    assertEquals("second",
        OrcRecordUpdater.getRow(row).getFieldValue(0).toString());
    assertEquals(true, rows.hasNext());
    row = (OrcStruct) rows.next(null);
    assertEquals(2, OrcRecordUpdater.getRowId(row));
    assertEquals(10, getBucketId(row));
    assertEquals("third",
        OrcRecordUpdater.getRow(row).getFieldValue(0).toString());
    assertEquals(true, rows.hasNext());
    row = (OrcStruct) rows.next(null);
    assertEquals(12, OrcRecordUpdater.getCurrentTransaction(row));
    assertEquals(12, OrcRecordUpdater.getOriginalTransaction(row));
    assertEquals(10, getBucketId(row));
    assertEquals(0, OrcRecordUpdater.getRowId(row));
    assertEquals("fourth",
        OrcRecordUpdater.getRow(row).getFieldValue(0).toString());
    assertEquals(true, rows.hasNext());
    row = (OrcStruct) rows.next(null);
    assertEquals(1, OrcRecordUpdater.getRowId(row));
    assertEquals("fifth",
        OrcRecordUpdater.getRow(row).getFieldValue(0).toString());
    assertEquals(false, rows.hasNext());

    // add one more record and close
    updater.insert(20, new MyRow("sixth"));
    updater.close(false);
    reader = OrcFile.createReader(bucketPath,
        new OrcFile.ReaderOptions(conf).filesystem(fs));
    assertEquals(6, reader.getNumberOfRows());
    assertEquals(6L, updater.getStats().getRowCount());

    assertEquals(false, fs.exists(sidePath));
  }
  private static int getBucketId(OrcStruct row) {
    int bucketValue = OrcRecordUpdater.getBucket(row);
    return
      BucketCodec.determineVersion(bucketValue).decodeWriterId(bucketValue);
  }
  @Test
  public void testWriterTblProperties() throws Exception {
    Path root = new Path(workDir, "testWriterTblProperties");
    Configuration conf = new Configuration();
    // Must use raw local because the checksummer doesn't honor flushes.
    FileSystem fs = FileSystem.getLocal(conf).getRaw();
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (MyRow.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    Properties tblProps = new Properties();
    tblProps.setProperty("orc.compress", "SNAPPY");
    tblProps.setProperty("orc.compress.size", "8192");
    HiveConf.setIntVar(conf, HiveConf.ConfVars.HIVE_ORC_BASE_DELTA_RATIO, 4);
    AcidOutputFormat.Options options = new AcidOutputFormat.Options(conf)
        .filesystem(fs)
        .bucket(10)
        .writingBase(false)
        .minimumWriteId(10)
        .maximumWriteId(19)
        .inspector(inspector)
        .reporter(Reporter.NULL)
        .finalDestination(root)
        .tableProperties(tblProps);
    RecordUpdater updater = new OrcRecordUpdater(root, options);
    updater.insert(11, new MyRow("first"));
    updater.insert(11, new MyRow("second"));
    updater.insert(11, new MyRow("third"));
    updater.flush();
    updater.insert(12, new MyRow("fourth"));
    updater.insert(12, new MyRow("fifth"));
    updater.flush();

    PrintStream origOut = System.out;
    ByteArrayOutputStream myOut = new ByteArrayOutputStream();
    System.setOut(new PrintStream(myOut));
    FileDump.main(new String[]{root.toUri().toString()});
    System.out.flush();
    String outDump = new String(myOut.toByteArray());
    assertEquals(true, outDump.contains("Compression: SNAPPY"));
    assertEquals(true, outDump.contains("Compression size: 2048"));
    System.setOut(origOut);
    updater.close(false);
  }

  @Test
  public void testUpdates() throws Exception {
    Path root = new Path(workDir, "testUpdates");
    Configuration conf = new Configuration();
    FileSystem fs = root.getFileSystem(conf);
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (MyRow.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    int bucket = 20;
    AcidOutputFormat.Options options = new AcidOutputFormat.Options(conf)
        .filesystem(fs)
        .bucket(bucket)
        .writingBase(false)
        .minimumWriteId(100)
        .maximumWriteId(100)
        .inspector(inspector)
        .reporter(Reporter.NULL)
        .recordIdColumn(1)
        .finalDestination(root);
    RecordUpdater updater = new OrcRecordUpdater(root, options);
    updater.update(100, new MyRow("update", 30, 10, bucket));
    updater.delete(100, new MyRow("", 60, 40, bucket));
    assertEquals(-1L, updater.getStats().getRowCount());
    updater.close(false);
    Path bucketPath = AcidUtils.createFilename(root, options);

    Reader reader = OrcFile.createReader(bucketPath,
        new OrcFile.ReaderOptions(conf).filesystem(fs));
    assertEquals(1, reader.getNumberOfRows());

    RecordReader rows = reader.rows();

    // check the contents of the file
    assertEquals(true, rows.hasNext());
    OrcStruct row = (OrcStruct) rows.next(null);
    assertEquals(OrcRecordUpdater.INSERT_OPERATION,
        OrcRecordUpdater.getOperation(row));
    assertEquals(100, OrcRecordUpdater.getCurrentTransaction(row));
    assertEquals(100, OrcRecordUpdater.getOriginalTransaction(row));
    int bucketProperty = OrcRecordUpdater.getBucket(row);
    assertEquals(bucket, BucketCodec.determineVersion(bucketProperty).decodeWriterId(bucketProperty));
    assertEquals(0, OrcRecordUpdater.getRowId(row));
    assertEquals("update",
        OrcRecordUpdater.getRow(row).getFieldValue(0).toString());
    rows.close();

    options.writingDeleteDelta(true);
    bucketPath = AcidUtils.createFilename(root, options);
    reader = OrcFile.createReader(bucketPath,
      new OrcFile.ReaderOptions(conf).filesystem(fs));
    assertEquals(2, reader.getNumberOfRows());

    rows = reader.rows();
    assertEquals(true, rows.hasNext());
    row = (OrcStruct) rows.next(null);
    assertEquals(OrcRecordUpdater.DELETE_OPERATION, OrcRecordUpdater.getOperation(row));
    assertEquals(100, OrcRecordUpdater.getCurrentTransaction(row));
    assertEquals(10, OrcRecordUpdater.getOriginalTransaction(row));
    bucketProperty = OrcRecordUpdater.getBucket(row);
    assertEquals(bucket, BucketCodec.determineVersion(bucketProperty).decodeWriterId(bucketProperty));
    assertEquals(30, OrcRecordUpdater.getRowId(row));
    assertNull(OrcRecordUpdater.getRow(row));

    assertEquals(true, rows.hasNext());
    row = (OrcStruct) rows.next(null);
    assertEquals(OrcRecordUpdater.DELETE_OPERATION, OrcRecordUpdater.getOperation(row));
    assertEquals(100, OrcRecordUpdater.getCurrentTransaction(row));
    assertEquals(40, OrcRecordUpdater.getOriginalTransaction(row));
    bucketProperty = OrcRecordUpdater.getBucket(row);
    assertEquals(bucket, BucketCodec.determineVersion(bucketProperty).decodeWriterId(bucketProperty));
    assertEquals(60, OrcRecordUpdater.getRowId(row));
    assertNull(OrcRecordUpdater.getRow(row));

    assertEquals(false, rows.hasNext());
  }

  /*
    CharsetDecoder instances are not thread safe, so it can end up in an inconsistent state when reading multiple
    buffers parallel.
    E.g:
    java.lang.IllegalStateException: Current state = FLUSHED, new state = CODING_END
  */
  @Test
  public void testConcurrentParseKeyIndex() throws Exception {

    // Given
    Reader mockReader = mock(Reader.class);
    when(mockReader.hasMetadataValue(OrcRecordUpdater.ACID_KEY_INDEX_NAME)).thenReturn(true);

    // Create a large buffer
    final StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 3000; i++) {
      sb.append("100000,200000,300000;");
    }
    when(mockReader.getMetadataValue(OrcRecordUpdater.ACID_KEY_INDEX_NAME)).thenReturn(
            ByteBuffer.wrap(sb.toString().getBytes()));

    // When
    // Hit OrcRecordUpdater.parseKeyIndex with large parallelism
    final int parallelism = 4000;
    Callable<RecordIdentifier[]>[] r = new Callable[parallelism];
    for (int i = 0; i < parallelism; i++) {
      r[i] = () -> {
        return OrcRecordUpdater.parseKeyIndex(mockReader);
      };
    }
    ExecutorService executorService = Executors.newFixedThreadPool(parallelism);
    List<Future<RecordIdentifier[]>> res = executorService.invokeAll(Arrays.asList(r));

    // Then
    // Check for exceptions
    for (Future<RecordIdentifier[]> ri : res) {
      ri.get();
    }
  }
}
