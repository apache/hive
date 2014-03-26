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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.RecordUpdater;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.File;

import static org.junit.Assert.assertEquals;

public class TestOrcRecordUpdater {

  @Test
  public void testAccessors() throws Exception {
    OrcStruct event = new OrcStruct(OrcRecordUpdater.FIELDS);
    event.setFieldValue(OrcRecordUpdater.OPERATION,
        new IntWritable(OrcRecordUpdater.INSERT_OPERATION));
    event.setFieldValue(OrcRecordUpdater.CURRENT_TRANSACTION,
        new LongWritable(100));
    event.setFieldValue(OrcRecordUpdater.ORIGINAL_TRANSACTION,
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
    MyRow(String val) {
      field = new Text(val);
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
        .minimumTransactionId(10)
        .maximumTransactionId(19)
        .inspector(inspector)
        .reporter(Reporter.NULL);
    RecordUpdater updater = new OrcRecordUpdater(root, options);
    updater.insert(11, new MyRow("first"));
    updater.insert(11, new MyRow("second"));
    updater.insert(11, new MyRow("third"));
    updater.flush();
    updater.insert(12, new MyRow("fourth"));
    updater.insert(12, new MyRow("fifth"));
    updater.flush();
    Path bucketPath = AcidUtils.createFilename(root, options);
    Path sidePath = OrcRecordUpdater.getSideFile(bucketPath);
    DataInputStream side = fs.open(sidePath);

    // read the stopping point for the first flush and make sure we only see
    // 3 rows
    long len = side.readLong();
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
    assertEquals(10, OrcRecordUpdater.getBucket(row));
    assertEquals(0, OrcRecordUpdater.getRowId(row));
    assertEquals("first",
        OrcRecordUpdater.getRow(row).getFieldValue(0).toString());
    assertEquals(true, rows.hasNext());
    row = (OrcStruct) rows.next(null);
    assertEquals(1, OrcRecordUpdater.getRowId(row));
    assertEquals(10, OrcRecordUpdater.getBucket(row));
    assertEquals("second",
        OrcRecordUpdater.getRow(row).getFieldValue(0).toString());
    assertEquals(true, rows.hasNext());
    row = (OrcStruct) rows.next(null);
    assertEquals(2, OrcRecordUpdater.getRowId(row));
    assertEquals(10, OrcRecordUpdater.getBucket(row));
    assertEquals("third",
        OrcRecordUpdater.getRow(row).getFieldValue(0).toString());
    assertEquals(true, rows.hasNext());
    row = (OrcStruct) rows.next(null);
    assertEquals(12, OrcRecordUpdater.getCurrentTransaction(row));
    assertEquals(12, OrcRecordUpdater.getOriginalTransaction(row));
    assertEquals(10, OrcRecordUpdater.getBucket(row));
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
    assertEquals(false, fs.exists(sidePath));
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
    AcidOutputFormat.Options options = new AcidOutputFormat.Options(conf)
        .filesystem(fs)
        .bucket(20)
        .writingBase(false)
        .minimumTransactionId(100)
        .maximumTransactionId(100)
        .inspector(inspector)
        .reporter(Reporter.NULL);
    RecordUpdater updater = new OrcRecordUpdater(root, options);
    updater.update(100, 10, 30, new MyRow("update"));
    updater.delete(100, 40, 60);
    updater.close(false);
    Path bucketPath = AcidUtils.createFilename(root, options);

    Reader reader = OrcFile.createReader(bucketPath,
        new OrcFile.ReaderOptions(conf).filesystem(fs));
    assertEquals(2, reader.getNumberOfRows());

    RecordReader rows = reader.rows();

    // check the contents of the file
    assertEquals(true, rows.hasNext());
    OrcStruct row = (OrcStruct) rows.next(null);
    assertEquals(OrcRecordUpdater.UPDATE_OPERATION,
        OrcRecordUpdater.getOperation(row));
    assertEquals(100, OrcRecordUpdater.getCurrentTransaction(row));
    assertEquals(10, OrcRecordUpdater.getOriginalTransaction(row));
    assertEquals(20, OrcRecordUpdater.getBucket(row));
    assertEquals(30, OrcRecordUpdater.getRowId(row));
    assertEquals("update",
        OrcRecordUpdater.getRow(row).getFieldValue(0).toString());
    assertEquals(true, rows.hasNext());
    row = (OrcStruct) rows.next(null);
    assertEquals(100, OrcRecordUpdater.getCurrentTransaction(row));
    assertEquals(40, OrcRecordUpdater.getOriginalTransaction(row));
    assertEquals(20, OrcRecordUpdater.getBucket(row));
    assertEquals(60, OrcRecordUpdater.getRowId(row));
    assertEquals(null, OrcRecordUpdater.getRow(row));
    assertEquals(false, rows.hasNext());
  }
}
