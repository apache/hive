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

package org.apache.hadoop.hive.ql.io;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Properties;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.DefaultCodec;

public class TestRCFile extends TestCase {

  private static final Log LOG = LogFactory.getLog(TestRCFile.class);

  private static Configuration conf = new Configuration();

  private static ColumnarSerDe serDe;

  private static Path file;

  private static FileSystem fs;

  private static Properties tbl;

  static {
    try {
      fs = FileSystem.getLocal(conf);
      Path dir = new Path(System.getProperty("test.data.dir", ".") + "/mapred");
      file = new Path(dir, "test_rcfile");
      fs.delete(dir, true);
      // the SerDe part is from TestLazySimpleSerDe
      serDe = new ColumnarSerDe();
      // Create the SerDe
      tbl = createProperties();
      serDe.initialize(conf, tbl);
    } catch (Exception e) {
    }
  }

  // Data

  private static Writable[] expectedFieldsData = {
      new ByteWritable((byte) 123), new ShortWritable((short) 456),
      new IntWritable(789), new LongWritable(1000), new DoubleWritable(5.3),
      new Text("hive and hadoop"), null, null };

  private static Object[] expectedPartitalFieldsData = { null, null,
      new IntWritable(789), new LongWritable(1000), null, null, null, null };
  private static BytesRefArrayWritable patialS = new BytesRefArrayWritable();

  private static byte[][] bytesArray = null;

  private static BytesRefArrayWritable s = null;
  static {
    try {
      bytesArray = new byte[][] { "123".getBytes("UTF-8"),
          "456".getBytes("UTF-8"), "789".getBytes("UTF-8"),
          "1000".getBytes("UTF-8"), "5.3".getBytes("UTF-8"),
          "hive and hadoop".getBytes("UTF-8"), new byte[0], "NULL".getBytes("UTF-8") };
      s = new BytesRefArrayWritable(bytesArray.length);
      s.set(0, new BytesRefWritable("123".getBytes("UTF-8")));
      s.set(1, new BytesRefWritable("456".getBytes("UTF-8")));
      s.set(2, new BytesRefWritable("789".getBytes("UTF-8")));
      s.set(3, new BytesRefWritable("1000".getBytes("UTF-8")));
      s.set(4, new BytesRefWritable("5.3".getBytes("UTF-8")));
      s.set(5, new BytesRefWritable("hive and hadoop".getBytes("UTF-8")));
      s.set(6, new BytesRefWritable("NULL".getBytes("UTF-8")));
      s.set(7, new BytesRefWritable("NULL".getBytes("UTF-8")));

      // partial test init
      patialS.set(0, new BytesRefWritable("NULL".getBytes("UTF-8")));
      patialS.set(1, new BytesRefWritable("NULL".getBytes("UTF-8")));
      patialS.set(2, new BytesRefWritable("789".getBytes("UTF-8")));
      patialS.set(3, new BytesRefWritable("1000".getBytes("UTF-8")));
      patialS.set(4, new BytesRefWritable("NULL".getBytes("UTF-8")));
      patialS.set(5, new BytesRefWritable("NULL".getBytes("UTF-8")));
      patialS.set(6, new BytesRefWritable("NULL".getBytes("UTF-8")));
      patialS.set(7, new BytesRefWritable("NULL".getBytes("UTF-8")));

    } catch (UnsupportedEncodingException e) {
    }
  }

  public void testSimpleReadAndWrite() throws IOException, SerDeException {
    fs.delete(file, true);

    byte[][] record_1 = { "123".getBytes("UTF-8"), "456".getBytes("UTF-8"),
        "789".getBytes("UTF-8"), "1000".getBytes("UTF-8"),
        "5.3".getBytes("UTF-8"), "hive and hadoop".getBytes("UTF-8"),
        new byte[0], "NULL".getBytes("UTF-8") };
    byte[][] record_2 = { "100".getBytes("UTF-8"), "200".getBytes("UTF-8"),
        "123".getBytes("UTF-8"), "1000".getBytes("UTF-8"),
        "5.3".getBytes("UTF-8"), "hive and hadoop".getBytes("UTF-8"),
        new byte[0], "NULL".getBytes("UTF-8") };

    RCFileOutputFormat.setColumnNumber(conf, expectedFieldsData.length);
    RCFile.Writer writer = new RCFile.Writer(fs, conf, file, null,
        new DefaultCodec());
    BytesRefArrayWritable bytes = new BytesRefArrayWritable(record_1.length);
    for (int i = 0; i < record_1.length; i++) {
      BytesRefWritable cu = new BytesRefWritable(record_1[i], 0,
          record_1[i].length);
      bytes.set(i, cu);
    }
    writer.append(bytes);
    bytes.clear();
    for (int i = 0; i < record_2.length; i++) {
      BytesRefWritable cu = new BytesRefWritable(record_2[i], 0,
          record_2[i].length);
      bytes.set(i, cu);
    }
    writer.append(bytes);
    writer.close();

    Object[] expectedRecord_1 = { new ByteWritable((byte) 123),
        new ShortWritable((short) 456), new IntWritable(789),
        new LongWritable(1000), new DoubleWritable(5.3),
        new Text("hive and hadoop"), null, null };

    Object[] expectedRecord_2 = { new ByteWritable((byte) 100),
        new ShortWritable((short) 200), new IntWritable(123),
        new LongWritable(1000), new DoubleWritable(5.3),
        new Text("hive and hadoop"), null, null };

    RCFile.Reader reader = new RCFile.Reader(fs, file, conf);

    LongWritable rowID = new LongWritable();

    for (int i = 0; i < 2; i++) {
      reader.next(rowID);
      BytesRefArrayWritable cols = new BytesRefArrayWritable();
      reader.getCurrentRow(cols);
      cols.resetValid(8);
      Object row = serDe.deserialize(cols);

      StructObjectInspector oi = (StructObjectInspector) serDe
          .getObjectInspector();
      List<? extends StructField> fieldRefs = oi.getAllStructFieldRefs();
      assertEquals("Field size should be 8", 8, fieldRefs.size());
      for (int j = 0; j < fieldRefs.size(); j++) {
        Object fieldData = oi.getStructFieldData(row, fieldRefs.get(j));
        Object standardWritableData = ObjectInspectorUtils.copyToStandardObject(fieldData, 
            fieldRefs.get(j).getFieldObjectInspector(), ObjectInspectorCopyOption.WRITABLE);
        if (i == 0)
          assertEquals("Field " + i, standardWritableData, expectedRecord_1[j]);
        else
          assertEquals("Field " + i, standardWritableData, expectedRecord_2[j]);
      }
    }

    reader.close();
  }

  public void testWriteAndFullyRead() throws IOException, SerDeException {
    writeTest(fs, 10000, file, bytesArray);
    fullyReadTest(fs, 10000, file);
  }

  public void testWriteAndPartialRead() throws IOException, SerDeException {
    writeTest(fs, 10000, file, bytesArray);
    partialReadTest(fs, 10000, file);
  }

  /** For debugging and testing. */
  public static void main(String[] args) throws Exception {
    int count = 10000;
    boolean create = true;

    String usage = "Usage: RCFile " + "[-count N]" + " file";
    if (args.length == 0) {
      System.err.println(usage);
      System.exit(-1);
    }

    try {
      for (int i = 0; i < args.length; ++i) { // parse command line
        if (args[i] == null) {
          continue;
        } else if (args[i].equals("-count")) {
          count = Integer.parseInt(args[++i]);
        } else {
          // file is required parameter
          file = new Path(args[i]);
        }
      }

      if (file == null) {
        System.err.println(usage);
        System.exit(-1);
      }

      LOG.info("count = " + count);
      LOG.info("create = " + create);
      LOG.info("file = " + file);

      TestRCFile test = new TestRCFile();

      // test.performanceTest();

      test.testSimpleReadAndWrite();

      test.writeTest(fs, count, file, bytesArray);
      test.fullyReadTest(fs, count, file);
      test.partialReadTest(fs, count, file);
      System.out.println("Finished.");
    } finally {
      fs.close();
    }
  }

  private void writeTest(FileSystem fs, int count, Path file,
      byte[][] fieldsData) throws IOException, SerDeException {
    fs.delete(file, true);

    RCFileOutputFormat.setColumnNumber(conf, fieldsData.length);
    RCFile.Writer writer = new RCFile.Writer(fs, conf, file, null,
        new DefaultCodec());

    BytesRefArrayWritable bytes = new BytesRefArrayWritable(fieldsData.length);
    for (int i = 0; i < fieldsData.length; i++) {
      BytesRefWritable cu = null;
      cu = new BytesRefWritable(fieldsData[i], 0, fieldsData[i].length);
      bytes.set(i, cu);
    }

    for (int i = 0; i < count; i++) {
      writer.append(bytes);
    }
    writer.close();
    long fileLen = fs.getFileStatus(file).getLen();
    System.out.println("The file size of RCFile with " + bytes.size()
        + " number columns and " + count + " number rows is " + fileLen);
  }

  private static Properties createProperties() {
    Properties tbl = new Properties();

    // Set the configuration parameters
    tbl.setProperty(Constants.SERIALIZATION_FORMAT, "9");
    tbl.setProperty("columns",
        "abyte,ashort,aint,along,adouble,astring,anullint,anullstring");
    tbl.setProperty("columns.types",
        "tinyint:smallint:int:bigint:double:string:int:string");
    tbl.setProperty(Constants.SERIALIZATION_NULL_FORMAT, "NULL");
    return tbl;
  }

  public void fullyReadTest(FileSystem fs, int count, Path file)
      throws IOException, SerDeException {
    LOG.debug("reading " + count + " records");
    long start = System.currentTimeMillis();
    ColumnProjectionUtils.setFullyReadColumns(conf);
    RCFile.Reader reader = new RCFile.Reader(fs, file, conf);

    LongWritable rowID = new LongWritable();
    int actualRead = 0;
    BytesRefArrayWritable cols = new BytesRefArrayWritable();
    while (reader.next(rowID)) {
      reader.getCurrentRow(cols);
      cols.resetValid(8);
      Object row = serDe.deserialize(cols);

      StructObjectInspector oi = (StructObjectInspector) serDe
          .getObjectInspector();
      List<? extends StructField> fieldRefs = oi.getAllStructFieldRefs();
      assertEquals("Field size should be 8", 8, fieldRefs.size());
      for (int i = 0; i < fieldRefs.size(); i++) {
        Object fieldData = oi.getStructFieldData(row, fieldRefs.get(i));
        Object standardWritableData = ObjectInspectorUtils.copyToStandardObject(fieldData, 
            fieldRefs.get(i).getFieldObjectInspector(), ObjectInspectorCopyOption.WRITABLE);
        assertEquals("Field " + i, standardWritableData, expectedFieldsData[i]);
      }
      // Serialize
      assertEquals("Class of the serialized object should be BytesRefArrayWritable", BytesRefArrayWritable.class, serDe.getSerializedClass());
      BytesRefArrayWritable serializedText = (BytesRefArrayWritable) serDe
          .serialize(row, oi);
      assertEquals("Serialized data", s, serializedText);
      actualRead++;
    }
    reader.close();
    assertEquals("Expect " + count + " rows, actual read " + actualRead,
        actualRead, count);
    long cost = System.currentTimeMillis() - start;
    LOG.debug("reading fully costs:" + cost + " milliseconds");
  }

  private void partialReadTest(FileSystem fs, int count, Path file)
      throws IOException, SerDeException {
    LOG.debug("reading " + count + " records");
    long start = System.currentTimeMillis();
    java.util.ArrayList<Integer> readCols = new java.util.ArrayList<Integer>();
    readCols.add(Integer.valueOf(2));
    readCols.add(Integer.valueOf(3));
    ColumnProjectionUtils.setReadColumnIDs(conf, readCols);
    RCFile.Reader reader = new RCFile.Reader(fs, file, conf);

    LongWritable rowID = new LongWritable();
    BytesRefArrayWritable cols = new BytesRefArrayWritable();
    
    while (reader.next(rowID)) {
      reader.getCurrentRow(cols);
      cols.resetValid(8);
      Object row = serDe.deserialize(cols);

      StructObjectInspector oi = (StructObjectInspector) serDe
          .getObjectInspector();
      List<? extends StructField> fieldRefs = oi.getAllStructFieldRefs();
      assertEquals("Field size should be 8", 8, fieldRefs.size());

      for (int i : readCols) {
        Object fieldData = oi.getStructFieldData(row, fieldRefs.get(i));
        Object standardWritableData = ObjectInspectorUtils.copyToStandardObject(fieldData, 
            fieldRefs.get(i).getFieldObjectInspector(), ObjectInspectorCopyOption.WRITABLE);
        assertEquals("Field " + i, standardWritableData, expectedPartitalFieldsData[i]);
      }

      assertEquals("Class of the serialized object should be BytesRefArrayWritable", BytesRefArrayWritable.class, serDe.getSerializedClass());
      BytesRefArrayWritable serializedBytes = (BytesRefArrayWritable) serDe
          .serialize(row, oi);
      assertEquals("Serialized data", patialS, serializedBytes);
    }
    reader.close();
    long cost = System.currentTimeMillis() - start;
    LOG.debug("reading fully costs:" + cost + " milliseconds");
  }
}
