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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.io.InputFormatChecker;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class TestInputOutputFormat {

  Path workDir = new Path(System.getProperty("test.tmp.dir","target/test/tmp"));

  public static class MyRow implements Writable {
    int x;
    int y;
    MyRow(int x, int y) {
      this.x = x;
      this.y = y;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
      throw new UnsupportedOperationException("no write");
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
     throw new UnsupportedOperationException("no read");
    }
  }

  @Rule
  public TestName testCaseName = new TestName();
  JobConf conf;
  FileSystem fs;
  Path testFilePath;

  @Before
  public void openFileSystem () throws Exception {
    conf = new JobConf();
    fs = FileSystem.getLocal(conf);
    testFilePath = new Path(workDir, "TestInputOutputFormat." +
        testCaseName.getMethodName() + ".orc");
    fs.delete(testFilePath, false);
  }

  @Test
  public void testInOutFormat() throws Exception {
    Properties properties = new Properties();
    StructObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = (StructObjectInspector)
          ObjectInspectorFactory.getReflectionObjectInspector(MyRow.class,
              ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    SerDe serde = new OrcSerde();
    HiveOutputFormat<?, ?> outFormat = new OrcOutputFormat();
    FileSinkOperator.RecordWriter writer =
        outFormat.getHiveRecordWriter(conf, testFilePath, MyRow.class, true,
            properties, Reporter.NULL);
    writer.write(serde.serialize(new MyRow(1,2), inspector));
    writer.write(serde.serialize(new MyRow(2,2), inspector));
    writer.write(serde.serialize(new MyRow(3,2), inspector));
    writer.close(true);
    serde = new OrcSerde();
    properties.setProperty("columns", "x,y");
    properties.setProperty("columns.types", "int:int");
    serde.initialize(conf, properties);
    assertEquals(OrcSerde.OrcSerdeRow.class, serde.getSerializedClass());
    inspector = (StructObjectInspector) serde.getObjectInspector();
    assertEquals("struct<x:int,y:int>", inspector.getTypeName());
    InputFormat<?,?> in = new OrcInputFormat();
    FileInputFormat.setInputPaths(conf, testFilePath.toString());
    InputSplit[] splits = in.getSplits(conf, 1);
    assertEquals(1, splits.length);

    // the the validate input method
    ArrayList<FileStatus> fileList = new ArrayList<FileStatus>();
    assertEquals(false,
        ((InputFormatChecker) in).validateInput(fs, new HiveConf(), fileList));
    fileList.add(fs.getFileStatus(testFilePath));
    assertEquals(true,
        ((InputFormatChecker) in).validateInput(fs, new HiveConf(), fileList));
    fileList.add(fs.getFileStatus(workDir));
    assertEquals(false,
        ((InputFormatChecker) in).validateInput(fs, new HiveConf(), fileList));


    // read the whole file
    org.apache.hadoop.mapred.RecordReader reader =
        in.getRecordReader(splits[0], conf, Reporter.NULL);
    Object key = reader.createKey();
    Writable value = (Writable) reader.createValue();
    int rowNum = 0;
    List<? extends StructField> fields =inspector.getAllStructFieldRefs();
    IntObjectInspector intInspector =
        (IntObjectInspector) fields.get(0).getFieldObjectInspector();
    assertEquals(0.0, reader.getProgress(), 0.00001);
    assertEquals(0, reader.getPos());
    while (reader.next(key, value)) {
      assertEquals(++rowNum, intInspector.get(inspector.
          getStructFieldData(serde.deserialize(value), fields.get(0))));
      assertEquals(2, intInspector.get(inspector.
          getStructFieldData(serde.deserialize(value), fields.get(1))));
    }
    assertEquals(3, rowNum);
    assertEquals(1.0, reader.getProgress(), 0.00001);
    reader.close();

    // read just the first column
    conf.set("hive.io.file.readcolumn.ids", "0");
    reader = in.getRecordReader(splits[0], conf, Reporter.NULL);
    key = reader.createKey();
    value = (Writable) reader.createValue();
    rowNum = 0;
    fields = inspector.getAllStructFieldRefs();
    while (reader.next(key, value)) {
      assertEquals(++rowNum, intInspector.get(inspector.
          getStructFieldData(value, fields.get(0))));
      assertEquals(null, inspector.getStructFieldData(value, fields.get(1)));
    }
    assertEquals(3, rowNum);
    reader.close();
  }

  static class NestedRow implements Writable {
    int z;
    MyRow r;
    NestedRow(int x, int y, int z) {
      this.z = z;
      this.r = new MyRow(x,y);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
      throw new UnsupportedOperationException("unsupported");
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
      throw new UnsupportedOperationException("unsupported");
    }
  }

  @Test
  public void testMROutput() throws Exception {
    JobConf job = new JobConf(conf);
    Properties properties = new Properties();
    StructObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = (StructObjectInspector)
          ObjectInspectorFactory.getReflectionObjectInspector(NestedRow.class,
              ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    SerDe serde = new OrcSerde();
    OutputFormat<?, ?> outFormat = new OrcOutputFormat();
    RecordWriter writer =
        outFormat.getRecordWriter(fs, conf, testFilePath.toString(),
            Reporter.NULL);
    writer.write(NullWritable.get(),
        serde.serialize(new NestedRow(1,2,3), inspector));
    writer.write(NullWritable.get(),
        serde.serialize(new NestedRow(4,5,6), inspector));
    writer.write(NullWritable.get(),
        serde.serialize(new NestedRow(7,8,9), inspector));
    writer.close(Reporter.NULL);
    serde = new OrcSerde();
    properties.setProperty("columns", "z,r");
    properties.setProperty("columns.types", "int:struct<x:int,y:int>");
    serde.initialize(conf, properties);
    inspector = (StructObjectInspector) serde.getObjectInspector();
    InputFormat<?,?> in = new OrcInputFormat();
    FileInputFormat.setInputPaths(conf, testFilePath.toString());
    InputSplit[] splits = in.getSplits(conf, 1);
    assertEquals(1, splits.length);
    conf.set("hive.io.file.readcolumn.ids", "1");
    org.apache.hadoop.mapred.RecordReader reader =
        in.getRecordReader(splits[0], conf, Reporter.NULL);
    Object key = reader.createKey();
    Object value = reader.createValue();
    int rowNum = 0;
    List<? extends StructField> fields = inspector.getAllStructFieldRefs();
    StructObjectInspector inner = (StructObjectInspector)
        fields.get(1).getFieldObjectInspector();
    List<? extends StructField> inFields = inner.getAllStructFieldRefs();
    IntObjectInspector intInspector =
        (IntObjectInspector) fields.get(0).getFieldObjectInspector();
    while (reader.next(key, value)) {
      assertEquals(null, inspector.getStructFieldData(value, fields.get(0)));
      Object sub = inspector.getStructFieldData(value, fields.get(1));
      assertEquals(3*rowNum+1, intInspector.get(inner.getStructFieldData(sub,
          inFields.get(0))));
      assertEquals(3*rowNum+2, intInspector.get(inner.getStructFieldData(sub,
          inFields.get(1))));
      rowNum += 1;
    }
    assertEquals(3, rowNum);
    reader.close();

  }

  @Test
  public void testEmptyFile() throws Exception {
    JobConf job = new JobConf(conf);
    Properties properties = new Properties();
    HiveOutputFormat<?, ?> outFormat = new OrcOutputFormat();
    FileSinkOperator.RecordWriter writer =
        outFormat.getHiveRecordWriter(conf, testFilePath, MyRow.class, true,
            properties, Reporter.NULL);
    writer.close(true);
    properties.setProperty("columns", "x,y");
    properties.setProperty("columns.types", "int:int");
    SerDe serde = new OrcSerde();
    serde.initialize(conf, properties);
    InputFormat<?,?> in = new OrcInputFormat();
    FileInputFormat.setInputPaths(conf, testFilePath.toString());
    InputSplit[] splits = in.getSplits(conf, 1);
    assertEquals(1, splits.length);

    // read the whole file
    conf.set("hive.io.file.readcolumn.ids", "0,1");
    org.apache.hadoop.mapred.RecordReader reader =
        in.getRecordReader(splits[0], conf, Reporter.NULL);
    Object key = reader.createKey();
    Object value = reader.createValue();
    assertEquals(0.0, reader.getProgress(), 0.00001);
    assertEquals(0, reader.getPos());
    assertEquals(false, reader.next(key, value));
    reader.close();
    assertEquals(null, serde.getSerDeStats());
  }

  static class StringRow implements Writable {
    String str;
    String str2;
    StringRow(String s) {
      str = s;
      str2 = s;
    }
    @Override
    public void write(DataOutput dataOutput) throws IOException {
      throw new UnsupportedOperationException("no write");
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
      throw new UnsupportedOperationException("no read");
    }
  }

  @Test
  public void testDefaultTypes() throws Exception {
    JobConf job = new JobConf(conf);
    Properties properties = new Properties();
    StructObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = (StructObjectInspector)
          ObjectInspectorFactory.getReflectionObjectInspector(StringRow.class,
              ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    SerDe serde = new OrcSerde();
    HiveOutputFormat<?, ?> outFormat = new OrcOutputFormat();
    FileSinkOperator.RecordWriter writer =
        outFormat.getHiveRecordWriter(conf, testFilePath, StringRow.class,
            true, properties, Reporter.NULL);
    writer.write(serde.serialize(new StringRow("owen"), inspector));
    writer.write(serde.serialize(new StringRow("beth"), inspector));
    writer.write(serde.serialize(new StringRow("laurel"), inspector));
    writer.write(serde.serialize(new StringRow("hazen"), inspector));
    writer.write(serde.serialize(new StringRow("colin"), inspector));
    writer.write(serde.serialize(new StringRow("miles"), inspector));
    writer.close(true);
    serde = new OrcSerde();
    properties.setProperty("columns", "str,str2");
    serde.initialize(conf, properties);
    inspector = (StructObjectInspector) serde.getObjectInspector();
    assertEquals("struct<str:string,str2:string>", inspector.getTypeName());
    InputFormat<?,?> in = new OrcInputFormat();
    FileInputFormat.setInputPaths(conf, testFilePath.toString());
    InputSplit[] splits = in.getSplits(conf, 1);
    assertEquals(1, splits.length);

    // read the whole file
    org.apache.hadoop.mapred.RecordReader reader =
        in.getRecordReader(splits[0], conf, Reporter.NULL);
    Object key = reader.createKey();
    Writable value = (Writable) reader.createValue();
    List<? extends StructField> fields =inspector.getAllStructFieldRefs();
    StringObjectInspector strInspector = (StringObjectInspector)
        fields.get(0).getFieldObjectInspector();
    assertEquals(true, reader.next(key, value));
    assertEquals("owen", strInspector.getPrimitiveJavaObject(inspector.
        getStructFieldData(value, fields.get(0))));
    assertEquals(true, reader.next(key, value));
    assertEquals("beth", strInspector.getPrimitiveJavaObject(inspector.
        getStructFieldData(value, fields.get(0))));
    assertEquals(true, reader.next(key, value));
    assertEquals("laurel", strInspector.getPrimitiveJavaObject(inspector.
        getStructFieldData(value, fields.get(0))));
    assertEquals(true, reader.next(key, value));
    assertEquals("hazen", strInspector.getPrimitiveJavaObject(inspector.
        getStructFieldData(value, fields.get(0))));
    assertEquals(true, reader.next(key, value));
    assertEquals("colin", strInspector.getPrimitiveJavaObject(inspector.
        getStructFieldData(value, fields.get(0))));
    assertEquals(true, reader.next(key, value));
    assertEquals("miles", strInspector.getPrimitiveJavaObject(inspector.
        getStructFieldData(value, fields.get(0))));
    assertEquals(false, reader.next(key, value));
    reader.close();
  }
}
