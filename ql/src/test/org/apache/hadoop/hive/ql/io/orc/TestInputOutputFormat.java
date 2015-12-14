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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeSet;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapper;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.io.AcidInputFormat;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.io.InputFormatChecker;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat.SplitStrategy;
import org.apache.hadoop.hive.ql.io.sarg.ConvertAstToSearchArg;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.shims.CombineHiveKey;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.orc.OrcProto;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;

@SuppressWarnings({ "deprecation", "unchecked", "rawtypes" })
public class TestInputOutputFormat {

  public static String toKryo(SearchArgument sarg) {
    Output out = new Output(4 * 1024, 10 * 1024 * 1024);
    new Kryo().writeObject(out, sarg);
    out.close();
    return Base64.encodeBase64String(out.toBytes());
  }

  Path workDir = new Path(System.getProperty("test.tmp.dir","target/tmp"));
  static final int MILLIS_IN_DAY = 1000 * 60 * 60 * 24;
  private static final SimpleDateFormat DATE_FORMAT =
      new SimpleDateFormat("yyyy/MM/dd");
  private static final SimpleDateFormat TIME_FORMAT =
      new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
  private static final TimeZone LOCAL_TIMEZONE = TimeZone.getDefault();

  static {
    TimeZone gmt = TimeZone.getTimeZone("GMT+0");
    DATE_FORMAT.setTimeZone(gmt);
    TIME_FORMAT.setTimeZone(gmt);
  }

  public static class BigRow implements Writable {
    boolean booleanValue;
    byte byteValue;
    short shortValue;
    int intValue;
    long longValue;
    float floatValue;
    double doubleValue;
    String stringValue;
    HiveDecimal decimalValue;
    Date dateValue;
    Timestamp timestampValue;

    BigRow(long x) {
      booleanValue = x % 2 == 0;
      byteValue = (byte) x;
      shortValue = (short) x;
      intValue = (int) x;
      longValue = x;
      floatValue = x;
      doubleValue = x;
      stringValue = Long.toHexString(x);
      decimalValue = HiveDecimal.create(x);
      long millisUtc = x * MILLIS_IN_DAY;
      millisUtc -= LOCAL_TIMEZONE.getOffset(millisUtc);
      dateValue = new Date(millisUtc);
      timestampValue = new Timestamp(millisUtc);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
      throw new UnsupportedOperationException("no write");
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
      throw new UnsupportedOperationException("no read");
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("bigrow{booleanValue: ");
      builder.append(booleanValue);
      builder.append(", byteValue: ");
      builder.append(byteValue);
      builder.append(", shortValue: ");
      builder.append(shortValue);
      builder.append(", intValue: ");
      builder.append(intValue);
      builder.append(", longValue: ");
      builder.append(longValue);
      builder.append(", floatValue: ");
      builder.append(floatValue);
      builder.append(", doubleValue: ");
      builder.append(doubleValue);
      builder.append(", stringValue: ");
      builder.append(stringValue);
      builder.append(", decimalValue: ");
      builder.append(decimalValue);
      builder.append(", dateValue: ");
      builder.append(DATE_FORMAT.format(dateValue));
      builder.append(", timestampValue: ");
      builder.append(TIME_FORMAT.format(timestampValue));
      builder.append("}");
      return builder.toString();
    }
  }

  public static class BigRowField implements StructField {
    private final int id;
    private final String fieldName;
    private final ObjectInspector inspector;

    BigRowField(int id, String fieldName, ObjectInspector inspector) {
      this.id = id;
      this.fieldName = fieldName;
      this.inspector = inspector;
    }

    @Override
    public String getFieldName() {
      return fieldName;
    }

    @Override
    public ObjectInspector getFieldObjectInspector() {
      return inspector;
    }

    @Override
    public String getFieldComment() {
      return null;
    }

    @Override
    public int getFieldID() {
      return id;
    }

    @Override
    public String toString() {
      return "field " + id + " " + fieldName;
    }
  }

  public static class BigRowInspector extends StructObjectInspector {
    static final List<BigRowField> FIELDS = new ArrayList<BigRowField>();
    static {
      FIELDS.add(new BigRowField(0, "booleanValue",
          PrimitiveObjectInspectorFactory.javaBooleanObjectInspector));
      FIELDS.add(new BigRowField(1, "byteValue",
          PrimitiveObjectInspectorFactory.javaByteObjectInspector));
      FIELDS.add(new BigRowField(2, "shortValue",
          PrimitiveObjectInspectorFactory.javaShortObjectInspector));
      FIELDS.add(new BigRowField(3, "intValue",
          PrimitiveObjectInspectorFactory.javaIntObjectInspector));
      FIELDS.add(new BigRowField(4, "longValue",
          PrimitiveObjectInspectorFactory.javaLongObjectInspector));
      FIELDS.add(new BigRowField(5, "floatValue",
          PrimitiveObjectInspectorFactory.javaFloatObjectInspector));
      FIELDS.add(new BigRowField(6, "doubleValue",
          PrimitiveObjectInspectorFactory.javaDoubleObjectInspector));
      FIELDS.add(new BigRowField(7, "stringValue",
          PrimitiveObjectInspectorFactory.javaStringObjectInspector));
      FIELDS.add(new BigRowField(8, "decimalValue",
          PrimitiveObjectInspectorFactory.javaHiveDecimalObjectInspector));
      FIELDS.add(new BigRowField(9, "dateValue",
          PrimitiveObjectInspectorFactory.javaDateObjectInspector));
      FIELDS.add(new BigRowField(10, "timestampValue",
          PrimitiveObjectInspectorFactory.javaTimestampObjectInspector));
    }


    @Override
    public List<? extends StructField> getAllStructFieldRefs() {
      return FIELDS;
    }

    @Override
    public StructField getStructFieldRef(String fieldName) {
      for(StructField field: FIELDS) {
        if (field.getFieldName().equals(fieldName)) {
          return field;
        }
      }
      throw new IllegalArgumentException("Can't find field " + fieldName);
    }

    @Override
    public Object getStructFieldData(Object data, StructField fieldRef) {
      BigRow obj = (BigRow) data;
      switch (((BigRowField) fieldRef).id) {
        case 0:
          return obj.booleanValue;
        case 1:
          return obj.byteValue;
        case 2:
          return obj.shortValue;
        case 3:
          return obj.intValue;
        case 4:
          return obj.longValue;
        case 5:
          return obj.floatValue;
        case 6:
          return obj.doubleValue;
        case 7:
          return obj.stringValue;
        case 8:
          return obj.decimalValue;
        case 9:
          return obj.dateValue;
        case 10:
          return obj.timestampValue;
      }
      throw new IllegalArgumentException("No such field " + fieldRef);
    }

    @Override
    public List<Object> getStructFieldsDataAsList(Object data) {
      BigRow obj = (BigRow) data;
      List<Object> result = new ArrayList<Object>(11);
      result.add(obj.booleanValue);
      result.add(obj.byteValue);
      result.add(obj.shortValue);
      result.add(obj.intValue);
      result.add(obj.longValue);
      result.add(obj.floatValue);
      result.add(obj.doubleValue);
      result.add(obj.stringValue);
      result.add(obj.decimalValue);
      result.add(obj.dateValue);
      result.add(obj.timestampValue);
      return result;
    }

    @Override
    public String getTypeName() {
      return "struct<booleanValue:boolean,byteValue:tinyint," +
          "shortValue:smallint,intValue:int,longValue:bigint," +
          "floatValue:float,doubleValue:double,stringValue:string," +
          "decimalValue:decimal>";
    }

    @Override
    public Category getCategory() {
      return Category.STRUCT;
    }
  }

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


    static String getColumnNamesProperty() {
      return "x,y";
    }
    static String getColumnTypesProperty() {
      return "int:int";
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
  public void testOverlap() throws Exception {
    assertEquals(0, OrcInputFormat.SplitGenerator.getOverlap(100, 100,
        200, 100));
    assertEquals(0, OrcInputFormat.SplitGenerator.getOverlap(0, 1000,
        2000, 100));
    assertEquals(100, OrcInputFormat.SplitGenerator.getOverlap(1000, 1000,
        1500, 100));
    assertEquals(250, OrcInputFormat.SplitGenerator.getOverlap(1000, 250,
        500, 2000));
    assertEquals(100, OrcInputFormat.SplitGenerator.getOverlap(1000, 1000,
        1900, 1000));
    assertEquals(500, OrcInputFormat.SplitGenerator.getOverlap(2000, 1000,
        2500, 2000));
  }

  @Test
  public void testGetInputPaths() throws Exception {
    conf.set("mapred.input.dir", "a,b,c");
    assertArrayEquals(new Path[]{new Path("a"), new Path("b"), new Path("c")},
        OrcInputFormat.getInputPaths(conf));
    conf.set("mapred.input.dir", "/a/b/c/d/e");
    assertArrayEquals(new Path[]{new Path("/a/b/c/d/e")},
        OrcInputFormat.getInputPaths(conf));
    conf.set("mapred.input.dir", "/a/b/c\\,d,/e/f\\,g/h");
    assertArrayEquals(new Path[]{new Path("/a/b/c,d"), new Path("/e/f,g/h")},
        OrcInputFormat.getInputPaths(conf));
  }

  private FileSystem generateMockFiles(final int count, final int size) {
    final byte[] data = new byte[size];
    MockFile[] files = new MockFile[count];
    for (int i = 0; i < count; i++) {
      files[i] = new MockFile(String.format("mock:/a/b/part-%d", i), size, data);
    }
    return new MockFileSystem(conf, files);
  }

  @Test
  public void testSplitStrategySelection() throws Exception {

    conf.set("mapreduce.input.fileinputformat.split.maxsize", "500");
    conf.setLong(HiveConf.ConfVars.HIVE_ORC_CACHE_STRIPE_DETAILS_SIZE.varname,
        100);
    final int[] counts = { 1, 10, 100, 256 };
    final int[] sizes = { 100, 1000 };
    final int[] numSplits = { 1, 9, 10, 11, 99, 111 };
    final String[] strategyResults = new String[] {
    "ETLSplitStrategy", /* 1 files x 100 size for 1 splits */
    "ETLSplitStrategy", /* 1 files x 100 size for 9 splits */
    "ETLSplitStrategy", /* 1 files x 100 size for 10 splits */
    "ETLSplitStrategy", /* 1 files x 100 size for 11 splits */
    "ETLSplitStrategy", /* 1 files x 100 size for 99 splits */
    "ETLSplitStrategy", /* 1 files x 100 size for 111 splits */
    "ETLSplitStrategy", /* 1 files x 1000 size for 1 splits */
    "ETLSplitStrategy", /* 1 files x 1000 size for 9 splits */
    "ETLSplitStrategy", /* 1 files x 1000 size for 10 splits */
    "ETLSplitStrategy", /* 1 files x 1000 size for 11 splits */
    "ETLSplitStrategy", /* 1 files x 1000 size for 99 splits */
    "ETLSplitStrategy", /* 1 files x 1000 size for 111 splits */
    "BISplitStrategy", /* 10 files x 100 size for 1 splits */
    "BISplitStrategy", /* 10 files x 100 size for 9 splits */
    "ETLSplitStrategy", /* 10 files x 100 size for 10 splits */
    "ETLSplitStrategy", /* 10 files x 100 size for 11 splits */
    "ETLSplitStrategy", /* 10 files x 100 size for 99 splits */
    "ETLSplitStrategy", /* 10 files x 100 size for 111 splits */
    "ETLSplitStrategy", /* 10 files x 1000 size for 1 splits */
    "ETLSplitStrategy", /* 10 files x 1000 size for 9 splits */
    "ETLSplitStrategy", /* 10 files x 1000 size for 10 splits */
    "ETLSplitStrategy", /* 10 files x 1000 size for 11 splits */
    "ETLSplitStrategy", /* 10 files x 1000 size for 99 splits */
    "ETLSplitStrategy", /* 10 files x 1000 size for 111 splits */
    "BISplitStrategy", /* 100 files x 100 size for 1 splits */
    "BISplitStrategy", /* 100 files x 100 size for 9 splits */
    "BISplitStrategy", /* 100 files x 100 size for 10 splits */
    "BISplitStrategy", /* 100 files x 100 size for 11 splits */
    "BISplitStrategy", /* 100 files x 100 size for 99 splits */
    "ETLSplitStrategy", /* 100 files x 100 size for 111 splits */
    "ETLSplitStrategy", /* 100 files x 1000 size for 1 splits */
    "ETLSplitStrategy", /* 100 files x 1000 size for 9 splits */
    "ETLSplitStrategy", /* 100 files x 1000 size for 10 splits */
    "ETLSplitStrategy", /* 100 files x 1000 size for 11 splits */
    "ETLSplitStrategy", /* 100 files x 1000 size for 99 splits */
    "ETLSplitStrategy", /* 100 files x 1000 size for 111 splits */
    "BISplitStrategy", /* 256 files x 100 size for 1 splits */
    "BISplitStrategy", /* 256 files x 100 size for 9 splits */
    "BISplitStrategy", /* 256 files x 100 size for 10 splits */
    "BISplitStrategy", /* 256 files x 100 size for 11 splits */
    "BISplitStrategy", /* 256 files x 100 size for 99 splits */
    "BISplitStrategy", /* 256 files x 100 size for 111 splits */
    "ETLSplitStrategy", /* 256 files x 1000 size for 1 splits */
    "ETLSplitStrategy", /* 256 files x 1000 size for 9 splits */
    "ETLSplitStrategy", /* 256 files x 1000 size for 10 splits */
    "ETLSplitStrategy", /* 256 files x 1000 size for 11 splits */
    "ETLSplitStrategy", /* 256 files x 1000 size for 99 splits */
    "ETLSplitStrategy", /* 256 files x 1000 size for 111 splits */
    };

    int k = 0;

    for (int c : counts) {
      for (int s : sizes) {
        final FileSystem fs = generateMockFiles(c, s);
        for (int n : numSplits) {
          final OrcInputFormat.Context context = new OrcInputFormat.Context(
              conf, n);
          OrcInputFormat.FileGenerator gen = new OrcInputFormat.FileGenerator(
              context, fs, new MockPath(fs, "mock:/a/b"), false);
          final SplitStrategy splitStrategy = createSplitStrategy(context, gen);
          assertTrue(
              String.format(
                  "Split strategy for %d files x %d size for %d splits", c, s,
                  n),
              splitStrategy.getClass().getSimpleName()
                  .equals(strategyResults[k++]));
        }
      }
    }
  }

  @Test
  public void testFileGenerator() throws Exception {
    OrcInputFormat.Context context = new OrcInputFormat.Context(conf);
    MockFileSystem fs = new MockFileSystem(conf,
        new MockFile("mock:/a/b/part-00", 1000, new byte[0]),
        new MockFile("mock:/a/b/part-01", 1000, new byte[0]),
        new MockFile("mock:/a/b/_part-02", 1000, new byte[0]),
        new MockFile("mock:/a/b/.part-03", 1000, new byte[0]),
        new MockFile("mock:/a/b/part-04", 1000, new byte[0]));
    OrcInputFormat.FileGenerator gen =
      new OrcInputFormat.FileGenerator(context, fs,
          new MockPath(fs, "mock:/a/b"), false);
    OrcInputFormat.SplitStrategy splitStrategy = createSplitStrategy(context, gen);
    assertEquals(true, splitStrategy instanceof OrcInputFormat.BISplitStrategy);

    conf.set("mapreduce.input.fileinputformat.split.maxsize", "500");
    context = new OrcInputFormat.Context(conf);
    fs = new MockFileSystem(conf,
        new MockFile("mock:/a/b/part-00", 1000, new byte[1000]),
        new MockFile("mock:/a/b/part-01", 1000, new byte[1000]),
        new MockFile("mock:/a/b/_part-02", 1000, new byte[1000]),
        new MockFile("mock:/a/b/.part-03", 1000, new byte[1000]),
        new MockFile("mock:/a/b/part-04", 1000, new byte[1000]));
    gen = new OrcInputFormat.FileGenerator(context, fs,
            new MockPath(fs, "mock:/a/b"), false);
    splitStrategy = createSplitStrategy(context, gen);
    assertEquals(true, splitStrategy instanceof OrcInputFormat.ETLSplitStrategy);
  }

  @Test
  public void testEtlCombinedStrategy() throws Exception {
    conf.set(HiveConf.ConfVars.HIVE_ORC_SPLIT_STRATEGY.varname, "ETL");
    conf.set(HiveConf.ConfVars.HIVE_ORC_SPLIT_DIRECTORY_BATCH_MS.varname, "1000000");
    OrcInputFormat.Context context = new OrcInputFormat.Context(conf);
    MockFileSystem fs = new MockFileSystem(conf,
        new MockFile("mock:/a/1/part-00", 1000, new byte[0]),
        new MockFile("mock:/a/1/part-01", 1000, new byte[0]),
        new MockFile("mock:/a/2/part-00", 1000, new byte[0]),
        new MockFile("mock:/a/2/part-01", 1000, new byte[0]),
        new MockFile("mock:/a/3/base_0/1", 1000, new byte[0]),
        new MockFile("mock:/a/4/base_0/1", 1000, new byte[0]),
        new MockFile("mock:/a/5/base_0/1", 1000, new byte[0]),
        new MockFile("mock:/a/5/delta_0_25/1", 1000, new byte[0])
    );

    OrcInputFormat.CombinedCtx combineCtx = new OrcInputFormat.CombinedCtx();
    // The first directory becomes the base for combining.
    SplitStrategy<?> ss = createOrCombineStrategy(context, fs, "mock:/a/1", combineCtx);
    assertNull(ss);
    assertTrue(combineCtx.combined instanceof OrcInputFormat.ETLSplitStrategy);
    OrcInputFormat.ETLSplitStrategy etlSs = (OrcInputFormat.ETLSplitStrategy)combineCtx.combined;
    assertEquals(2, etlSs.files.size());
    assertTrue(etlSs.isOriginal);
    assertEquals(1, etlSs.dirs.size());
    // The second one should be combined into the first.
    ss = createOrCombineStrategy(context, fs, "mock:/a/2", combineCtx);
    assertNull(ss);
    assertTrue(combineCtx.combined instanceof OrcInputFormat.ETLSplitStrategy);
    assertEquals(4, etlSs.files.size());
    assertEquals(2, etlSs.dirs.size());
    // The third one has the base file, so it shouldn't be combined but could be a base.
    ss = createOrCombineStrategy(context, fs, "mock:/a/3", combineCtx);
    assertSame(etlSs, ss);
    assertEquals(4, etlSs.files.size());
    assertEquals(2, etlSs.dirs.size());
    assertTrue(combineCtx.combined instanceof OrcInputFormat.ETLSplitStrategy);
    etlSs = (OrcInputFormat.ETLSplitStrategy)combineCtx.combined;
    assertEquals(1, etlSs.files.size());
    assertFalse(etlSs.isOriginal);
    assertEquals(1, etlSs.dirs.size());
    // Try the first again, it would not be combined and we'd retain the old base (less files).
    ss = createOrCombineStrategy(context, fs, "mock:/a/1", combineCtx);
    assertTrue(ss instanceof OrcInputFormat.ETLSplitStrategy);
    assertNotSame(etlSs, ss);
    OrcInputFormat.ETLSplitStrategy rejectedEtlSs = (OrcInputFormat.ETLSplitStrategy)ss;
    assertEquals(2, rejectedEtlSs.files.size());
    assertEquals(1, rejectedEtlSs.dirs.size());
    assertTrue(rejectedEtlSs.isOriginal);
    assertEquals(1, etlSs.files.size());
    assertEquals(1, etlSs.dirs.size());
    // The fourth could be combined again.
    ss = createOrCombineStrategy(context, fs, "mock:/a/4", combineCtx);
    assertNull(ss);
    assertTrue(combineCtx.combined instanceof OrcInputFormat.ETLSplitStrategy);
    assertEquals(2, etlSs.files.size());
    assertEquals(2, etlSs.dirs.size());
    // The fifth will not be combined because of delta files.
    ss = createOrCombineStrategy(context, fs, "mock:/a/5", combineCtx);
    assertTrue(ss instanceof OrcInputFormat.ETLSplitStrategy);
    assertNotSame(etlSs, ss);
    assertEquals(2, etlSs.files.size());
    assertEquals(2, etlSs.dirs.size());
  }

  public SplitStrategy<?> createOrCombineStrategy(OrcInputFormat.Context context,
      MockFileSystem fs, String path, OrcInputFormat.CombinedCtx combineCtx) throws IOException {
    OrcInputFormat.AcidDirInfo adi = createAdi(context, fs, path);
    return OrcInputFormat.determineSplitStrategy(
        combineCtx, context, adi.fs, adi.splitPath, adi.acidInfo, adi.baseOrOriginalFiles);
  }

  public OrcInputFormat.AcidDirInfo createAdi(
      OrcInputFormat.Context context, MockFileSystem fs, String path) throws IOException {
    return new OrcInputFormat.FileGenerator(context, fs, new MockPath(fs, path), false).call();
  }

  private OrcInputFormat.SplitStrategy createSplitStrategy(
      OrcInputFormat.Context context, OrcInputFormat.FileGenerator gen) throws IOException {
    OrcInputFormat.AcidDirInfo adi = gen.call();
    return OrcInputFormat.determineSplitStrategy(
        null, context, adi.fs, adi.splitPath, adi.acidInfo, adi.baseOrOriginalFiles);
  }

  public static class MockBlock {
    int offset;
    int length;
    final String[] hosts;

    public MockBlock(String... hosts) {
      this.hosts = hosts;
    }

    @Override
    public String toString() {
      StringBuilder buffer = new StringBuilder();
      buffer.append("block{offset: ");
      buffer.append(offset);
      buffer.append(", length: ");
      buffer.append(length);
      buffer.append(", hosts: [");
      for(int i=0; i < hosts.length; i++) {
        if (i != 0) {
          buffer.append(", ");
        }
        buffer.append(hosts[i]);
      }
      buffer.append("]}");
      return buffer.toString();
    }
  }

  public static class MockFile {
    final Path path;
    int blockSize;
    int length;
    MockBlock[] blocks;
    byte[] content;

    public MockFile(String path, int blockSize, byte[] content,
                    MockBlock... blocks) {
      this.path = new Path(path);
      this.blockSize = blockSize;
      this.blocks = blocks;
      this.content = content;
      this.length = content.length;
      int offset = 0;
      for(MockBlock block: blocks) {
        block.offset = offset;
        block.length = Math.min(length - offset, blockSize);
        offset += block.length;
      }
    }

    @Override
    public String toString() {
      StringBuilder buffer = new StringBuilder();
      buffer.append("mockFile{path: ");
      buffer.append(path.toString());
      buffer.append(", blkSize: ");
      buffer.append(blockSize);
      buffer.append(", len: ");
      buffer.append(length);
      buffer.append(", blocks: [");
      for(int i=0; i < blocks.length; i++) {
        if (i != 0) {
          buffer.append(", ");
        }
        buffer.append(blocks[i]);
      }
      buffer.append("]}");
      return buffer.toString();
    }
  }

  static class MockInputStream extends FSInputStream {
    final MockFile file;
    int offset = 0;

    public MockInputStream(MockFile file) throws IOException {
      this.file = file;
    }

    @Override
    public void seek(long offset) throws IOException {
      this.offset = (int) offset;
    }

    @Override
    public long getPos() throws IOException {
      return offset;
    }

    @Override
    public boolean seekToNewSource(long l) throws IOException {
      return false;
    }

    @Override
    public int read() throws IOException {
      if (offset < file.length) {
        return file.content[offset++] & 0xff;
      }
      return -1;
    }
  }

  public static class MockPath extends Path {
    private final FileSystem fs;
    public MockPath(FileSystem fs, String path) {
      super(path);
      this.fs = fs;
    }
    @Override
    public FileSystem getFileSystem(Configuration conf) {
      return fs;
    }
  }

  public static class MockOutputStream extends FSDataOutputStream {
    private final MockFile file;

    public MockOutputStream(MockFile file) throws IOException {
      super(new DataOutputBuffer(), null);
      this.file = file;
    }

    /**
     * Set the blocks and their location for the file.
     * Must be called after the stream is closed or the block length will be
     * wrong.
     * @param blocks the list of blocks
     */
    public void setBlocks(MockBlock... blocks) {
      file.blocks = blocks;
      int offset = 0;
      int i = 0;
      while (offset < file.length && i < blocks.length) {
        blocks[i].offset = offset;
        blocks[i].length = Math.min(file.length - offset, file.blockSize);
        offset += blocks[i].length;
        i += 1;
      }
    }

    @Override
    public void close() throws IOException {
      super.close();
      DataOutputBuffer buf = (DataOutputBuffer) getWrappedStream();
      file.length = buf.getLength();
      file.content = new byte[file.length];
      System.arraycopy(buf.getData(), 0, file.content, 0, file.length);
    }

    @Override
    public String toString() {
      return "Out stream to " + file.toString();
    }
  }

  public static class MockFileSystem extends FileSystem {
    final List<MockFile> files = new ArrayList<MockFile>();
    Path workingDir = new Path("/");

    public MockFileSystem() {
      // empty
    }

    @Override
    public void initialize(URI uri, Configuration conf) {
      setConf(conf);
    }

    public MockFileSystem(Configuration conf, MockFile... files) {
      setConf(conf);
      this.files.addAll(Arrays.asList(files));
    }

    void clear() {
      files.clear();
    }

    @Override
    public URI getUri() {
      try {
        return new URI("mock:///");
      } catch (URISyntaxException err) {
        throw new IllegalArgumentException("huh?", err);
      }
    }

    @Override
    public FSDataInputStream open(Path path, int i) throws IOException {
      for(MockFile file: files) {
        if (file.path.equals(path)) {
          return new FSDataInputStream(new MockInputStream(file));
        }
      }
      throw new IOException("File not found: " + path);
    }

    @Override
    public FSDataOutputStream create(Path path, FsPermission fsPermission,
                                     boolean overwrite, int bufferSize,
                                     short replication, long blockSize,
                                     Progressable progressable
                                     ) throws IOException {
      MockFile file = null;
      for(MockFile currentFile: files) {
        if (currentFile.path.equals(path)) {
          file = currentFile;
          break;
        }
      }
      if (file == null) {
        file = new MockFile(path.toString(), (int) blockSize, new byte[0]);
        files.add(file);
      }
      return new MockOutputStream(file);
    }

    @Override
    public FSDataOutputStream append(Path path, int bufferSize,
                                     Progressable progressable
                                     ) throws IOException {
      return create(path, FsPermission.getDefault(), true, bufferSize,
          (short) 3, 256 * 1024, progressable);
    }

    @Override
    public boolean rename(Path path, Path path2) throws IOException {
      return false;
    }

    @Override
    public boolean delete(Path path) throws IOException {
      return false;
    }

    @Override
    public boolean delete(Path path, boolean b) throws IOException {
      return false;
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
      path = path.makeQualified(this);
      List<FileStatus> result = new ArrayList<FileStatus>();
      String pathname = path.toString();
      String pathnameAsDir = pathname + "/";
      Set<String> dirs = new TreeSet<String>();
      for(MockFile file: files) {
        String filename = file.path.toString();
        if (pathname.equals(filename)) {
          return new FileStatus[]{createStatus(file)};
        } else if (filename.startsWith(pathnameAsDir)) {
          String tail = filename.substring(pathnameAsDir.length());
          int nextSlash = tail.indexOf('/');
          if (nextSlash > 0) {
            dirs.add(tail.substring(0, nextSlash));
          } else {
            result.add(createStatus(file));
          }
        }
      }
      // for each directory add it once
      for(String dir: dirs) {
        result.add(createDirectory(new MockPath(this, pathnameAsDir + dir)));
      }
      return result.toArray(new FileStatus[result.size()]);
    }

    @Override
    public void setWorkingDirectory(Path path) {
      workingDir = path;
    }

    @Override
    public Path getWorkingDirectory() {
      return workingDir;
    }

    @Override
    public boolean mkdirs(Path path, FsPermission fsPermission) {
      return false;
    }

    private FileStatus createStatus(MockFile file) {
      return new FileStatus(file.length, false, 1, file.blockSize, 0, 0,
          FsPermission.createImmutable((short) 644), "owen", "group",
          file.path);
    }

    private FileStatus createDirectory(Path dir) {
      return new FileStatus(0, true, 0, 0, 0, 0,
          FsPermission.createImmutable((short) 755), "owen", "group", dir);
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
      path = path.makeQualified(this);
      String pathnameAsDir = path.toString() + "/";
      for(MockFile file: files) {
        if (file.path.equals(path)) {
          return createStatus(file);
        } else if (file.path.toString().startsWith(pathnameAsDir)) {
          return createDirectory(path);
        }
      }
      throw new FileNotFoundException("File " + path + " does not exist");
    }

    @Override
    public BlockLocation[] getFileBlockLocations(FileStatus stat,
                                                 long start, long len) {
      List<BlockLocation> result = new ArrayList<BlockLocation>();
      for(MockFile file: files) {
        if (file.path.equals(stat.getPath())) {
          for(MockBlock block: file.blocks) {
            if (OrcInputFormat.SplitGenerator.getOverlap(block.offset,
                block.length, start, len) > 0) {
              String[] topology = new String[block.hosts.length];
              for(int i=0; i < topology.length; ++i) {
                topology[i] = "/rack/ " + block.hosts[i];
              }
              result.add(new BlockLocation(block.hosts, block.hosts,
                  topology, block.offset, block.length));
            }
          }
          return result.toArray(new BlockLocation[result.size()]);
        }
      }
      return new BlockLocation[0];
    }

    @Override
    public String toString() {
      StringBuilder buffer = new StringBuilder();
      buffer.append("mockFs{files:[");
      for(int i=0; i < files.size(); ++i) {
        if (i != 0) {
          buffer.append(", ");
        }
        buffer.append(files.get(i));
      }
      buffer.append("]}");
      return buffer.toString();
    }
  }

  static void fill(DataOutputBuffer out, long length) throws IOException {
    for(int i=0; i < length; ++i) {
      out.write(0);
    }
  }

  /**
   * Create the binary contents of an ORC file that just has enough information
   * to test the getInputSplits.
   * @param stripeLengths the length of each stripe
   * @return the bytes of the file
   * @throws IOException
   */
  static byte[] createMockOrcFile(long... stripeLengths) throws IOException {
    OrcProto.Footer.Builder footer = OrcProto.Footer.newBuilder();
    final long headerLen = 3;
    long offset = headerLen;
    DataOutputBuffer buffer = new DataOutputBuffer();
    for(long stripeLength: stripeLengths) {
      footer.addStripes(OrcProto.StripeInformation.newBuilder()
                          .setOffset(offset)
                          .setIndexLength(0)
                          .setDataLength(stripeLength-10)
                          .setFooterLength(10)
                          .setNumberOfRows(1000));
      offset += stripeLength;
    }
    fill(buffer, offset);
    footer.addTypes(OrcProto.Type.newBuilder()
        .setKind(OrcProto.Type.Kind.STRUCT)
        .addFieldNames("col1")
        .addSubtypes(1));
    footer.addTypes(OrcProto.Type.newBuilder()
        .setKind(OrcProto.Type.Kind.STRING));
    footer.setNumberOfRows(1000 * stripeLengths.length)
          .setHeaderLength(headerLen)
          .setContentLength(offset - headerLen);
    footer.addStatistics(OrcProto.ColumnStatistics.newBuilder()
        .setNumberOfValues(1000 * stripeLengths.length).build());
    footer.addStatistics(OrcProto.ColumnStatistics.newBuilder()
        .setNumberOfValues(1000 * stripeLengths.length)
        .setStringStatistics(
            OrcProto.StringStatistics.newBuilder()
                .setMaximum("zzz")
                .setMinimum("aaa")
                .setSum(1000 * 3 * stripeLengths.length)
                .build()
        ).build());
    footer.build().writeTo(buffer);
    int footerEnd = buffer.getLength();
    OrcProto.PostScript ps =
      OrcProto.PostScript.newBuilder()
        .setCompression(OrcProto.CompressionKind.NONE)
        .setFooterLength(footerEnd - offset)
        .setMagic("ORC")
        .build();
    ps.writeTo(buffer);
    buffer.write(buffer.getLength() - footerEnd);
    byte[] result = new byte[buffer.getLength()];
    System.arraycopy(buffer.getData(), 0, result, 0, buffer.getLength());
    return result;
  }

  @Test
  public void testAddSplit() throws Exception {
    // create a file with 5 blocks spread around the cluster
    MockFileSystem fs = new MockFileSystem(conf,
        new MockFile("mock:/a/file", 500,
          createMockOrcFile(197, 300, 600, 200, 200, 100, 100, 100, 100, 100),
          new MockBlock("host1-1", "host1-2", "host1-3"),
          new MockBlock("host2-1", "host0", "host2-3"),
          new MockBlock("host0", "host3-2", "host3-3"),
          new MockBlock("host4-1", "host4-2", "host4-3"),
          new MockBlock("host5-1", "host5-2", "host5-3")));
    OrcInputFormat.Context context = new OrcInputFormat.Context(conf);
    OrcInputFormat.SplitGenerator splitter =
        new OrcInputFormat.SplitGenerator(new OrcInputFormat.SplitInfo(context, fs,
            AcidUtils.createOriginalObj(null, fs.getFileStatus(new Path("/a/file"))), null, true,
            new ArrayList<AcidInputFormat.DeltaMetaData>(), true, null, null));
    OrcSplit result = splitter.createSplit(0, 200, null);
    assertEquals(0, result.getStart());
    assertEquals(200, result.getLength());
    assertEquals("mock:/a/file", result.getPath().toString());
    String[] locs = result.getLocations();
    assertEquals(3, locs.length);
    assertEquals("host1-1", locs[0]);
    assertEquals("host1-2", locs[1]);
    assertEquals("host1-3", locs[2]);
    result = splitter.createSplit(500, 600, null);
    locs = result.getLocations();
    assertEquals(3, locs.length);
    assertEquals("host2-1", locs[0]);
    assertEquals("host0", locs[1]);
    assertEquals("host2-3", locs[2]);
    result = splitter.createSplit(0, 2500, null);
    locs = result.getLocations();
    assertEquals(1, locs.length);
    assertEquals("host0", locs[0]);
  }

  @Test
  public void testSplitGenerator() throws Exception {
    // create a file with 5 blocks spread around the cluster
    long[] stripeSizes =
        new long[]{197, 300, 600, 200, 200, 100, 100, 100, 100, 100};
    MockFileSystem fs = new MockFileSystem(conf,
        new MockFile("mock:/a/file", 500,
          createMockOrcFile(stripeSizes),
          new MockBlock("host1-1", "host1-2", "host1-3"),
          new MockBlock("host2-1", "host0", "host2-3"),
          new MockBlock("host0", "host3-2", "host3-3"),
          new MockBlock("host4-1", "host4-2", "host4-3"),
          new MockBlock("host5-1", "host5-2", "host5-3")));
    HiveConf.setLongVar(conf, HiveConf.ConfVars.MAPREDMAXSPLITSIZE, 300);
    HiveConf.setLongVar(conf, HiveConf.ConfVars.MAPREDMINSPLITSIZE, 200);
    OrcInputFormat.Context context = new OrcInputFormat.Context(conf);
    OrcInputFormat.SplitGenerator splitter =
        new OrcInputFormat.SplitGenerator(new OrcInputFormat.SplitInfo(context, fs,
            AcidUtils.createOriginalObj(null, fs.getFileStatus(new Path("/a/file"))), null, true,
            new ArrayList<AcidInputFormat.DeltaMetaData>(), true, null, null));
    List<OrcSplit> results = splitter.call();
    OrcSplit result = results.get(0);
    assertEquals(3, result.getStart());
    assertEquals(497, result.getLength());
    result = results.get(1);
    assertEquals(500, result.getStart());
    assertEquals(600, result.getLength());
    result = results.get(2);
    assertEquals(1100, result.getStart());
    assertEquals(400, result.getLength());
    result = results.get(3);
    assertEquals(1500, result.getStart());
    assertEquals(300, result.getLength());
    result = results.get(4);
    assertEquals(1800, result.getStart());
    assertEquals(200, result.getLength());
    // test min = 0, max = 0 generates each stripe
    HiveConf.setLongVar(conf, HiveConf.ConfVars.MAPREDMAXSPLITSIZE, 0);
    HiveConf.setLongVar(conf, HiveConf.ConfVars.MAPREDMINSPLITSIZE, 0);
    context = new OrcInputFormat.Context(conf);
    splitter = new OrcInputFormat.SplitGenerator(new OrcInputFormat.SplitInfo(context, fs,
      AcidUtils.createOriginalObj(null, fs.getFileStatus(new Path("/a/file"))), null, true,
        new ArrayList<AcidInputFormat.DeltaMetaData>(), true, null, null));
    results = splitter.call();
    for(int i=0; i < stripeSizes.length; ++i) {
      assertEquals("checking stripe " + i + " size",
          stripeSizes[i], results.get(i).getLength());
    }
  }

  @Test
  public void testProjectedColumnSize() throws Exception {
    long[] stripeSizes =
        new long[]{200, 200, 200, 200, 100};
    MockFileSystem fs = new MockFileSystem(conf,
        new MockFile("mock:/a/file", 500,
            createMockOrcFile(stripeSizes),
            new MockBlock("host1-1", "host1-2", "host1-3"),
            new MockBlock("host2-1", "host0", "host2-3"),
            new MockBlock("host0", "host3-2", "host3-3"),
            new MockBlock("host4-1", "host4-2", "host4-3"),
            new MockBlock("host5-1", "host5-2", "host5-3")));
    HiveConf.setLongVar(conf, HiveConf.ConfVars.MAPREDMAXSPLITSIZE, 300);
    HiveConf.setLongVar(conf, HiveConf.ConfVars.MAPREDMINSPLITSIZE, 200);
    conf.setBoolean(ColumnProjectionUtils.READ_ALL_COLUMNS, false);
    conf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, "0");
    OrcInputFormat.Context context = new OrcInputFormat.Context(conf);
    OrcInputFormat.SplitGenerator splitter =
        new OrcInputFormat.SplitGenerator(new OrcInputFormat.SplitInfo(context, fs,
            fs.getFileStatus(new Path("/a/file")), null, true,
            new ArrayList<AcidInputFormat.DeltaMetaData>(), true, null, null));
    List<OrcSplit> results = splitter.call();
    OrcSplit result = results.get(0);
    assertEquals(3, results.size());
    assertEquals(3, result.getStart());
    assertEquals(400, result.getLength());
    assertEquals(167468, result.getProjectedColumnsUncompressedSize());
    result = results.get(1);
    assertEquals(403, result.getStart());
    assertEquals(400, result.getLength());
    assertEquals(167468, result.getProjectedColumnsUncompressedSize());
    result = results.get(2);
    assertEquals(803, result.getStart());
    assertEquals(100, result.getLength());
    assertEquals(41867, result.getProjectedColumnsUncompressedSize());

    // test min = 0, max = 0 generates each stripe
    HiveConf.setLongVar(conf, HiveConf.ConfVars.MAPREDMAXSPLITSIZE, 0);
    HiveConf.setLongVar(conf, HiveConf.ConfVars.MAPREDMINSPLITSIZE, 0);
    context = new OrcInputFormat.Context(conf);
    splitter = new OrcInputFormat.SplitGenerator(new OrcInputFormat.SplitInfo(context, fs,
        fs.getFileStatus(new Path("/a/file")), null, true,
        new ArrayList<AcidInputFormat.DeltaMetaData>(),
        true, null, null));
    results = splitter.call();
    assertEquals(5, results.size());
    for (int i = 0; i < stripeSizes.length; ++i) {
      assertEquals("checking stripe " + i + " size",
          stripeSizes[i], results.get(i).getLength());
      if (i == stripeSizes.length - 1) {
        assertEquals(41867, results.get(i).getProjectedColumnsUncompressedSize());
      } else {
        assertEquals(83734, results.get(i).getProjectedColumnsUncompressedSize());
      }
    }

    // single split
    HiveConf.setLongVar(conf, HiveConf.ConfVars.MAPREDMAXSPLITSIZE, 1000);
    HiveConf.setLongVar(conf, HiveConf.ConfVars.MAPREDMINSPLITSIZE, 100000);
    context = new OrcInputFormat.Context(conf);
    splitter = new OrcInputFormat.SplitGenerator(new OrcInputFormat.SplitInfo(context, fs,
        fs.getFileStatus(new Path("/a/file")), null, true,
        new ArrayList<AcidInputFormat.DeltaMetaData>(),
        true, null, null));
    results = splitter.call();
    assertEquals(1, results.size());
    result = results.get(0);
    assertEquals(3, result.getStart());
    assertEquals(900, result.getLength());
    assertEquals(376804, result.getProjectedColumnsUncompressedSize());
  }

  @Test
  public void testInOutFormat() throws Exception {
    Properties properties = new Properties();
    properties.setProperty("columns", "x,y");
    properties.setProperty("columns.types", "int:int");
    StructObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = (StructObjectInspector)
          ObjectInspectorFactory.getReflectionObjectInspector(MyRow.class,
              ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    SerDe serde = new OrcSerde();
    HiveOutputFormat<?, ?> outFormat = new OrcOutputFormat();
    org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter writer =
        outFormat.getHiveRecordWriter(conf, testFilePath, MyRow.class, true,
            properties, Reporter.NULL);
    writer.write(serde.serialize(new MyRow(1,2), inspector));
    writer.write(serde.serialize(new MyRow(2,2), inspector));
    writer.write(serde.serialize(new MyRow(3,2), inspector));
    writer.close(true);
    serde = new OrcSerde();
    SerDeUtils.initializeSerDe(serde, conf, properties, null);
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
    conf.set("columns", MyRow.getColumnNamesProperty());
    conf.set("columns.types", MyRow.getColumnTypesProperty());
    org.apache.hadoop.mapred.RecordReader reader =
        in.getRecordReader(splits[0], conf, Reporter.NULL);
    Object key = reader.createKey();
    Writable value = (Writable) reader.createValue();
    int rowNum = 0;
    List<? extends StructField> fields =inspector.getAllStructFieldRefs();
    IntObjectInspector intInspector =
        (IntObjectInspector) fields.get(0).getFieldObjectInspector();
    assertEquals(0.33, reader.getProgress(), 0.01);
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
    ColumnProjectionUtils.appendReadColumns(conf, Collections.singletonList(0));
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

    // test the mapping of empty string to all columns
    ColumnProjectionUtils.setReadAllColumns(conf);
    reader = in.getRecordReader(splits[0], conf, Reporter.NULL);
    key = reader.createKey();
    value = (Writable) reader.createValue();
    rowNum = 0;
    fields = inspector.getAllStructFieldRefs();
    while (reader.next(key, value)) {
      assertEquals(++rowNum, intInspector.get(inspector.
          getStructFieldData(value, fields.get(0))));
      assertEquals(2, intInspector.get(inspector.
          getStructFieldData(serde.deserialize(value), fields.get(1))));
    }
    assertEquals(3, rowNum);
    reader.close();
  }

  static class SimpleRow implements Writable {
    Text z;

    public SimpleRow(Text t) {
      this.z = t;
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
    SerDeUtils.initializeSerDe(serde, conf, properties, null);
    inspector = (StructObjectInspector) serde.getObjectInspector();
    InputFormat<?,?> in = new OrcInputFormat();
    FileInputFormat.setInputPaths(conf, testFilePath.toString());
    InputSplit[] splits = in.getSplits(conf, 1);
    assertEquals(1, splits.length);
    ColumnProjectionUtils.appendReadColumns(conf, Collections.singletonList(1));
    conf.set("columns", "z,r");
    conf.set("columns.types", "int:struct<x:int,y:int>");
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
    Properties properties = new Properties();
    properties.setProperty("columns", "x,y");
    properties.setProperty("columns.types", "int:int");
    HiveOutputFormat<?, ?> outFormat = new OrcOutputFormat();
    org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter writer =
        outFormat.getHiveRecordWriter(conf, testFilePath, MyRow.class, true,
            properties, Reporter.NULL);
    writer.close(true);
    SerDe serde = new OrcSerde();
    SerDeUtils.initializeSerDe(serde, conf, properties, null);
    InputFormat<?,?> in = new OrcInputFormat();
    FileInputFormat.setInputPaths(conf, testFilePath.toString());
    InputSplit[] splits = in.getSplits(conf, 1);
    assertTrue(0 == splits.length);
    assertEquals(null, serde.getSerDeStats());
  }

  @Test(expected = RuntimeException.class)
  public void testSplitGenFailure() throws IOException {
    Properties properties = new Properties();
    HiveOutputFormat<?, ?> outFormat = new OrcOutputFormat();
    org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter writer =
        outFormat.getHiveRecordWriter(conf, testFilePath, MyRow.class, true,
            properties, Reporter.NULL);
    writer.close(true);
    InputFormat<?,?> in = new OrcInputFormat();
    fs.setPermission(testFilePath, FsPermission.createImmutable((short) 0333));
    FileInputFormat.setInputPaths(conf, testFilePath.toString());
    try {
      in.getSplits(conf, 1);
    } catch (RuntimeException e) {
      assertEquals(true, e.getMessage().contains("Permission denied"));
      throw e;
    }
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

    static String getColumnNamesProperty() {
      return "str,str2";
    }
    static String getColumnTypesProperty() {
      return "string:string";
    }

  }

  @Test
  public void testDefaultTypes() throws Exception {
    Properties properties = new Properties();
    properties.setProperty("columns", "str,str2");
    properties.setProperty("columns.types", "string:string");
    StructObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = (StructObjectInspector)
          ObjectInspectorFactory.getReflectionObjectInspector(StringRow.class,
              ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    SerDe serde = new OrcSerde();
    HiveOutputFormat<?, ?> outFormat = new OrcOutputFormat();
    org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter writer =
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
    SerDeUtils.initializeSerDe(serde, conf, properties, null);
    inspector = (StructObjectInspector) serde.getObjectInspector();
    assertEquals("struct<str:string,str2:string>", inspector.getTypeName());
    InputFormat<?,?> in = new OrcInputFormat();
    FileInputFormat.setInputPaths(conf, testFilePath.toString());
    InputSplit[] splits = in.getSplits(conf, 1);
    assertEquals(1, splits.length);

    // read the whole file
    conf.set("columns", StringRow.getColumnNamesProperty());
    conf.set("columns.types", StringRow.getColumnTypesProperty());
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

  /**
   * Create a mock execution environment that has enough detail that
   * ORC, vectorization, HiveInputFormat, and CombineHiveInputFormat don't
   * explode.
   * @param workDir a local filesystem work directory
   * @param warehouseDir a mock filesystem warehouse directory
   * @param tableName the table name
   * @param objectInspector object inspector for the row
   * @param isVectorized should run vectorized
   * @return a JobConf that contains the necessary information
   * @throws IOException
   * @throws HiveException
   */
  JobConf createMockExecutionEnvironment(Path workDir,
                                         Path warehouseDir,
                                         String tableName,
                                         ObjectInspector objectInspector,
                                         boolean isVectorized,
                                         int partitions
                                         ) throws IOException, HiveException {
    JobConf conf = new JobConf();
    Utilities.clearWorkMap(conf);
    conf.set("hive.exec.plan", workDir.toString());
    conf.set("mapred.job.tracker", "local");
    conf.set("hive.vectorized.execution.enabled", Boolean.toString(isVectorized));
    conf.set("fs.mock.impl", MockFileSystem.class.getName());
    conf.set("mapred.mapper.class", ExecMapper.class.getName());
    Path root = new Path(warehouseDir, tableName);
    // clean out previous contents
    ((MockFileSystem) root.getFileSystem(conf)).clear();
    // build partition strings
    String[] partPath = new String[partitions];
    StringBuilder buffer = new StringBuilder();
    for(int p=0; p < partitions; ++p) {
      partPath[p] = new Path(root, "p=" + p).toString();
      if (p != 0) {
        buffer.append(',');
      }
      buffer.append(partPath[p]);
    }
    conf.set("mapred.input.dir", buffer.toString());
    StringBuilder columnIds = new StringBuilder();
    StringBuilder columnNames = new StringBuilder();
    StringBuilder columnTypes = new StringBuilder();
    StructObjectInspector structOI = (StructObjectInspector) objectInspector;
    List<? extends StructField> fields = structOI.getAllStructFieldRefs();
    int numCols = fields.size();
    for(int i=0; i < numCols; ++i) {
      if (i != 0) {
        columnIds.append(',');
        columnNames.append(',');
        columnTypes.append(',');
      }
      columnIds.append(i);
      columnNames.append(fields.get(i).getFieldName());
      columnTypes.append(fields.get(i).getFieldObjectInspector().getTypeName());
    }
    conf.set("hive.io.file.readcolumn.ids", columnIds.toString());
    conf.set("partition_columns", "p");
    conf.set(serdeConstants.LIST_COLUMNS, columnNames.toString());
    conf.set(serdeConstants.LIST_COLUMN_TYPES, columnTypes.toString());
    MockFileSystem fs = (MockFileSystem) warehouseDir.getFileSystem(conf);
    fs.clear();

    Properties tblProps = new Properties();
    tblProps.put("name", tableName);
    tblProps.put("serialization.lib", OrcSerde.class.getName());
    tblProps.put("columns", columnNames.toString());
    tblProps.put("columns.types", columnTypes.toString());
    TableDesc tbl = new TableDesc(OrcInputFormat.class, OrcOutputFormat.class,
        tblProps);

    MapWork mapWork = new MapWork();
    mapWork.setVectorMode(isVectorized);
    if (isVectorized) {
      VectorizedRowBatchCtx vectorizedRowBatchCtx = new VectorizedRowBatchCtx();
      vectorizedRowBatchCtx.init(structOI, new String[0]);
      mapWork.setVectorizedRowBatchCtx(vectorizedRowBatchCtx);
    }
    mapWork.setUseBucketizedHiveInputFormat(false);
    LinkedHashMap<String, ArrayList<String>> aliasMap =
        new LinkedHashMap<String, ArrayList<String>>();
    ArrayList<String> aliases = new ArrayList<String>();
    aliases.add(tableName);
    LinkedHashMap<String, PartitionDesc> partMap =
        new LinkedHashMap<String, PartitionDesc>();
    for(int p=0; p < partitions; ++p) {
      aliasMap.put(partPath[p], aliases);
      LinkedHashMap<String, String> partSpec =
          new LinkedHashMap<String, String>();
      PartitionDesc part = new PartitionDesc(tbl, partSpec);
      partMap.put(partPath[p], part);
    }
    mapWork.setPathToAliases(aliasMap);
    mapWork.setPathToPartitionInfo(partMap);

    // write the plan out
    FileSystem localFs = FileSystem.getLocal(conf).getRaw();
    Path mapXml = new Path(workDir, "map.xml");
    localFs.delete(mapXml, true);
    FSDataOutputStream planStream = localFs.create(mapXml);
    SerializationUtilities.serializePlan(mapWork, planStream);
    planStream.close();
    return conf;
  }

  /**
   * Test vectorization, non-acid, non-combine.
   * @throws Exception
   */
  @Test
  public void testVectorization() throws Exception {
    // get the object inspector for MyRow
    StructObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = (StructObjectInspector)
          ObjectInspectorFactory.getReflectionObjectInspector(MyRow.class,
              ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    JobConf conf = createMockExecutionEnvironment(workDir, new Path("mock:///"),
        "vectorization", inspector, true, 1);

    // write the orc file to the mock file system
    Writer writer =
        OrcFile.createWriter(new Path(conf.get("mapred.input.dir") + "/0_0"),
           OrcFile.writerOptions(conf).blockPadding(false)
                  .bufferSize(1024).inspector(inspector));
    for(int i=0; i < 10; ++i) {
      writer.addRow(new MyRow(i, 2*i));
    }
    writer.close();
    ((MockOutputStream) ((WriterImpl) writer).getStream())
        .setBlocks(new MockBlock("host0", "host1"));

    // call getsplits
    HiveInputFormat<?,?> inputFormat =
        new HiveInputFormat<WritableComparable, Writable>();
    InputSplit[] splits = inputFormat.getSplits(conf, 10);
    assertEquals(1, splits.length);

    org.apache.hadoop.mapred.RecordReader<NullWritable, VectorizedRowBatch>
        reader = inputFormat.getRecordReader(splits[0], conf, Reporter.NULL);
    NullWritable key = reader.createKey();
    VectorizedRowBatch value = reader.createValue();
    assertEquals(true, reader.next(key, value));
    assertEquals(10, value.count());
    LongColumnVector col0 = (LongColumnVector) value.cols[0];
    for(int i=0; i < 10; i++) {
      assertEquals("checking " + i, i, col0.vector[i]);
    }
    assertEquals(false, reader.next(key, value));
  }

  /**
   * Test vectorization, non-acid, non-combine.
   * @throws Exception
   */
  @Test
  public void testVectorizationWithBuckets() throws Exception {
    // get the object inspector for MyRow
    StructObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = (StructObjectInspector)
          ObjectInspectorFactory.getReflectionObjectInspector(MyRow.class,
              ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    JobConf conf = createMockExecutionEnvironment(workDir, new Path("mock:///"),
        "vectorBuckets", inspector, true, 1);

    // write the orc file to the mock file system
    Writer writer =
        OrcFile.createWriter(new Path(conf.get("mapred.input.dir") + "/0_0"),
            OrcFile.writerOptions(conf).blockPadding(false)
                .bufferSize(1024).inspector(inspector));
    for(int i=0; i < 10; ++i) {
      writer.addRow(new MyRow(i, 2*i));
    }
    writer.close();
    ((MockOutputStream) ((WriterImpl) writer).getStream())
        .setBlocks(new MockBlock("host0", "host1"));

    // call getsplits
    conf.setInt(hive_metastoreConstants.BUCKET_COUNT, 3);
    HiveInputFormat<?,?> inputFormat =
        new HiveInputFormat<WritableComparable, Writable>();
    InputSplit[] splits = inputFormat.getSplits(conf, 10);
    assertEquals(1, splits.length);

    org.apache.hadoop.mapred.RecordReader<NullWritable, VectorizedRowBatch>
        reader = inputFormat.getRecordReader(splits[0], conf, Reporter.NULL);
    NullWritable key = reader.createKey();
    VectorizedRowBatch value = reader.createValue();
    assertEquals(true, reader.next(key, value));
    assertEquals(10, value.count());
    LongColumnVector col0 = (LongColumnVector) value.cols[0];
    for(int i=0; i < 10; i++) {
      assertEquals("checking " + i, i, col0.vector[i]);
    }
    assertEquals(false, reader.next(key, value));
  }

  // test acid with vectorization, no combine
  @Test
  public void testVectorizationWithAcid() throws Exception {
    StructObjectInspector inspector = new BigRowInspector();
    JobConf conf = createMockExecutionEnvironment(workDir, new Path("mock:///"),
        "vectorizationAcid", inspector, true, 1);

    // write the orc file to the mock file system
    Path partDir = new Path(conf.get("mapred.input.dir"));
    OrcRecordUpdater writer = new OrcRecordUpdater(partDir,
        new AcidOutputFormat.Options(conf).maximumTransactionId(10)
            .writingBase(true).bucket(0).inspector(inspector).finalDestination(partDir));
    for(int i=0; i < 100; ++i) {
      BigRow row = new BigRow(i);
      writer.insert(10, row);
    }
    WriterImpl baseWriter = (WriterImpl) writer.getWriter();
    writer.close(false);
    ((MockOutputStream) baseWriter.getStream())
        .setBlocks(new MockBlock("host0", "host1"));

    // call getsplits
    HiveInputFormat<?,?> inputFormat =
        new HiveInputFormat<WritableComparable, Writable>();
    InputSplit[] splits = inputFormat.getSplits(conf, 10);
    assertEquals(1, splits.length);

    org.apache.hadoop.mapred.RecordReader<NullWritable, VectorizedRowBatch>
          reader = inputFormat.getRecordReader(splits[0], conf, Reporter.NULL);
    NullWritable key = reader.createKey();
    VectorizedRowBatch value = reader.createValue();
    assertEquals(true, reader.next(key, value));
    assertEquals(100, value.count());
    LongColumnVector booleanColumn = (LongColumnVector) value.cols[0];
    LongColumnVector byteColumn = (LongColumnVector) value.cols[1];
    LongColumnVector shortColumn = (LongColumnVector) value.cols[2];
    LongColumnVector intColumn = (LongColumnVector) value.cols[3];
    LongColumnVector longColumn = (LongColumnVector) value.cols[4];
    DoubleColumnVector floatColumn = (DoubleColumnVector) value.cols[5];
    DoubleColumnVector doubleCoulmn = (DoubleColumnVector) value.cols[6];
    BytesColumnVector stringColumn = (BytesColumnVector) value.cols[7];
    DecimalColumnVector decimalColumn = (DecimalColumnVector) value.cols[8];
    LongColumnVector dateColumn = (LongColumnVector) value.cols[9];
    LongColumnVector timestampColumn = (LongColumnVector) value.cols[10];
    for(int i=0; i < 100; i++) {
      assertEquals("checking boolean " + i, i % 2 == 0 ? 1 : 0,
          booleanColumn.vector[i]);
      assertEquals("checking byte " + i, (byte) i,
          byteColumn.vector[i]);
      assertEquals("checking short " + i, (short) i, shortColumn.vector[i]);
      assertEquals("checking int " + i, i, intColumn.vector[i]);
      assertEquals("checking long " + i, i, longColumn.vector[i]);
      assertEquals("checking float " + i, i, floatColumn.vector[i], 0.0001);
      assertEquals("checking double " + i, i, doubleCoulmn.vector[i], 0.0001);
      Text strValue = new Text();
      strValue.set(stringColumn.vector[i], stringColumn.start[i],
          stringColumn.length[i]);
      assertEquals("checking string " + i, new Text(Long.toHexString(i)),
          strValue);
      assertEquals("checking decimal " + i, HiveDecimal.create(i),
          decimalColumn.vector[i].getHiveDecimal());
      assertEquals("checking date " + i, i, dateColumn.vector[i]);
      long millis = (long) i * MILLIS_IN_DAY;
      millis -= LOCAL_TIMEZONE.getOffset(millis);
      assertEquals("checking timestamp " + i, millis * 1000000L,
          timestampColumn.vector[i]);
    }
    assertEquals(false, reader.next(key, value));
  }

  // test non-vectorized, non-acid, combine
  @Test
  public void testCombinationInputFormat() throws Exception {
    // get the object inspector for MyRow
    StructObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = (StructObjectInspector)
          ObjectInspectorFactory.getReflectionObjectInspector(MyRow.class,
              ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    JobConf conf = createMockExecutionEnvironment(workDir, new Path("mock:///"),
        "combination", inspector, false, 1);

    // write the orc file to the mock file system
    Path partDir = new Path(conf.get("mapred.input.dir"));
    Writer writer =
        OrcFile.createWriter(new Path(partDir, "0_0"),
            OrcFile.writerOptions(conf).blockPadding(false)
                .bufferSize(1024).inspector(inspector));
    for(int i=0; i < 10; ++i) {
      writer.addRow(new MyRow(i, 2*i));
    }
    writer.close();
    MockOutputStream outputStream = (MockOutputStream) ((WriterImpl) writer).getStream();
    outputStream.setBlocks(new MockBlock("host0", "host1"));
    int length0 = outputStream.file.length;
    writer =
        OrcFile.createWriter(new Path(partDir, "1_0"),
            OrcFile.writerOptions(conf).blockPadding(false)
                .bufferSize(1024).inspector(inspector));
    for(int i=10; i < 20; ++i) {
      writer.addRow(new MyRow(i, 2*i));
    }
    writer.close();
    outputStream = (MockOutputStream) ((WriterImpl) writer).getStream();
    outputStream.setBlocks(new MockBlock("host1", "host2"));

    // call getsplits
    HiveInputFormat<?,?> inputFormat =
        new CombineHiveInputFormat<WritableComparable, Writable>();
    InputSplit[] splits = inputFormat.getSplits(conf, 1);
    assertEquals(1, splits.length);
    CombineHiveInputFormat.CombineHiveInputSplit split =
        (CombineHiveInputFormat.CombineHiveInputSplit) splits[0];

    // check split
    assertEquals(2, split.getNumPaths());
    assertEquals(partDir.toString() + "/0_0", split.getPath(0).toString());
    assertEquals(partDir.toString() + "/1_0", split.getPath(1).toString());
    assertEquals(length0, split.getLength(0));
    assertEquals(outputStream.file.length, split.getLength(1));
    assertEquals(0, split.getOffset(0));
    assertEquals(0, split.getOffset(1));
    // hadoop-1 gets 3 and hadoop-2 gets 0. *sigh*
    // best answer would be 1.
    assertTrue(3 >= split.getLocations().length);

    // read split
    org.apache.hadoop.mapred.RecordReader<CombineHiveKey, OrcStruct> reader =
        inputFormat.getRecordReader(split, conf, Reporter.NULL);
    CombineHiveKey key = reader.createKey();
    OrcStruct value = reader.createValue();
    for(int i=0; i < 20; i++) {
      assertEquals(true, reader.next(key, value));
      assertEquals(i, ((IntWritable) value.getFieldValue(0)).get());
    }
    assertEquals(false, reader.next(key, value));
  }

  // test non-vectorized, acid, combine
  @Test
  public void testCombinationInputFormatWithAcid() throws Exception {
    // get the object inspector for MyRow
    StructObjectInspector inspector;
    final int PARTITIONS = 2;
    final int BUCKETS = 3;
    synchronized (TestOrcFile.class) {
      inspector = (StructObjectInspector)
          ObjectInspectorFactory.getReflectionObjectInspector(MyRow.class,
              ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    JobConf conf = createMockExecutionEnvironment(workDir, new Path("mock:///"),
        "combinationAcid", inspector, false, PARTITIONS);

    // write the orc file to the mock file system
    Path[] partDir = new Path[PARTITIONS];
    String[] paths = conf.getStrings("mapred.input.dir");
    for(int p=0; p < PARTITIONS; ++p) {
      partDir[p] = new Path(paths[p]);
    }

    // write a base file in partition 0
    OrcRecordUpdater writer = new OrcRecordUpdater(partDir[0],
        new AcidOutputFormat.Options(conf).maximumTransactionId(10)
            .writingBase(true).bucket(0).inspector(inspector).finalDestination(partDir[0]));
    for(int i=0; i < 10; ++i) {
      writer.insert(10, new MyRow(i, 2 * i));
    }
    WriterImpl baseWriter = (WriterImpl) writer.getWriter();
    writer.close(false);

    MockOutputStream outputStream = (MockOutputStream) baseWriter.getStream();
    outputStream.setBlocks(new MockBlock("host1", "host2"));

    // write a delta file in partition 0
    writer = new OrcRecordUpdater(partDir[0],
        new AcidOutputFormat.Options(conf).maximumTransactionId(10)
            .writingBase(true).bucket(1).inspector(inspector).finalDestination(partDir[0]));
    for(int i=10; i < 20; ++i) {
      writer.insert(10, new MyRow(i, 2*i));
    }
    WriterImpl deltaWriter = (WriterImpl) writer.getWriter();
    outputStream = (MockOutputStream) deltaWriter.getStream();
    writer.close(false);
    outputStream.setBlocks(new MockBlock("host1", "host2"));

    // write three files in partition 1
    for(int bucket=0; bucket < BUCKETS; ++bucket) {
      Writer orc = OrcFile.createWriter(
          new Path(partDir[1], "00000" + bucket + "_0"),
          OrcFile.writerOptions(conf)
              .blockPadding(false)
              .bufferSize(1024)
              .inspector(inspector));
      orc.addRow(new MyRow(1, 2));
      outputStream = (MockOutputStream) ((WriterImpl) orc).getStream();
      orc.close();
      outputStream.setBlocks(new MockBlock("host3", "host4"));
    }

    // call getsplits
    conf.setInt(hive_metastoreConstants.BUCKET_COUNT, BUCKETS);
    HiveInputFormat<?,?> inputFormat =
        new CombineHiveInputFormat<WritableComparable, Writable>();
    InputSplit[] splits = inputFormat.getSplits(conf, 1);
    assertEquals(3, splits.length);
    HiveInputFormat.HiveInputSplit split =
        (HiveInputFormat.HiveInputSplit) splits[0];
    assertEquals("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat",
        split.inputFormatClassName());
    assertEquals("mock:/combinationAcid/p=0/base_0000010/bucket_00000",
        split.getPath().toString());
    assertEquals(0, split.getStart());
    assertEquals(607, split.getLength());
    split = (HiveInputFormat.HiveInputSplit) splits[1];
    assertEquals("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat",
        split.inputFormatClassName());
    assertEquals("mock:/combinationAcid/p=0/base_0000010/bucket_00001",
        split.getPath().toString());
    assertEquals(0, split.getStart());
    assertEquals(629, split.getLength());
    CombineHiveInputFormat.CombineHiveInputSplit combineSplit =
        (CombineHiveInputFormat.CombineHiveInputSplit) splits[2];
    assertEquals(BUCKETS, combineSplit.getNumPaths());
    for(int bucket=0; bucket < BUCKETS; ++bucket) {
      assertEquals("mock:/combinationAcid/p=1/00000" + bucket + "_0",
          combineSplit.getPath(bucket).toString());
      assertEquals(0, combineSplit.getOffset(bucket));
      assertEquals(241, combineSplit.getLength(bucket));
    }
    String[] hosts = combineSplit.getLocations();
    assertEquals(2, hosts.length);
  }

  @Test
  public void testSetSearchArgument() throws Exception {
    Reader.Options options = new Reader.Options();
    List<OrcProto.Type> types = new ArrayList<OrcProto.Type>();
    OrcProto.Type.Builder builder = OrcProto.Type.newBuilder();
    builder.setKind(OrcProto.Type.Kind.STRUCT)
        .addAllFieldNames(Arrays.asList("op", "otid", "bucket", "rowid", "ctid",
            "row"))
        .addAllSubtypes(Arrays.asList(1,2,3,4,5,6));
    types.add(builder.build());
    builder.clear().setKind(OrcProto.Type.Kind.INT);
    types.add(builder.build());
    types.add(builder.build());
    types.add(builder.build());
    types.add(builder.build());
    types.add(builder.build());
    builder.clear().setKind(OrcProto.Type.Kind.STRUCT)
        .addAllFieldNames(Arrays.asList("url", "purchase", "cost", "store"))
        .addAllSubtypes(Arrays.asList(7, 8, 9, 10));
    types.add(builder.build());
    builder.clear().setKind(OrcProto.Type.Kind.STRING);
    types.add(builder.build());
    builder.clear().setKind(OrcProto.Type.Kind.INT);
    types.add(builder.build());
    types.add(builder.build());
    types.add(builder.build());
    SearchArgument isNull = SearchArgumentFactory.newBuilder()
        .startAnd().isNull("cost", PredicateLeaf.Type.LONG).end().build();
    conf.set(ConvertAstToSearchArg.SARG_PUSHDOWN, toKryo(isNull));
    conf.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR,
        "url,cost");
    options.include(new boolean[]{true, true, false, true, false});
    OrcInputFormat.setSearchArgument(options, types, conf, false);
    String[] colNames = options.getColumnNames();
    assertEquals(null, colNames[0]);
    assertEquals("url", colNames[1]);
    assertEquals(null, colNames[2]);
    assertEquals("cost", colNames[3]);
    assertEquals(null, colNames[4]);
    SearchArgument arg = options.getSearchArgument();
    List<PredicateLeaf> leaves = arg.getLeaves();
    assertEquals("cost", leaves.get(0).getColumnName());
    assertEquals(PredicateLeaf.Operator.IS_NULL, leaves.get(0).getOperator());
  }

  @Test
  public void testSplitElimination() throws Exception {
    Properties properties = new Properties();
    properties.setProperty("columns", "z,r");
    properties.setProperty("columns.types", "int:struct<x:int,y:int>");
    StructObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = (StructObjectInspector)
          ObjectInspectorFactory.getReflectionObjectInspector(NestedRow.class,
              ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    SerDe serde = new OrcSerde();
    OutputFormat<?, ?> outFormat = new OrcOutputFormat();
    conf.setInt("mapred.max.split.size", 50);
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
    SearchArgument sarg =
        SearchArgumentFactory.newBuilder()
            .startAnd()
            .lessThan("z", PredicateLeaf.Type.LONG, new Long(0))
            .end()
            .build();
    conf.set("sarg.pushdown", toKryo(sarg));
    conf.set("hive.io.file.readcolumn.names", "z,r");
    SerDeUtils.initializeSerDe(serde, conf, properties, null);
    inspector = (StructObjectInspector) serde.getObjectInspector();
    InputFormat<?,?> in = new OrcInputFormat();
    FileInputFormat.setInputPaths(conf, testFilePath.toString());
    InputSplit[] splits = in.getSplits(conf, 1);
    assertEquals(0, splits.length);
  }

  @Test
  public void testSplitEliminationNullStats() throws Exception {
    Properties properties = new Properties();
    StructObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = (StructObjectInspector)
          ObjectInspectorFactory.getReflectionObjectInspector(SimpleRow.class,
              ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    SerDe serde = new OrcSerde();
    OutputFormat<?, ?> outFormat = new OrcOutputFormat();
    conf.setInt("mapred.max.split.size", 50);
    RecordWriter writer =
        outFormat.getRecordWriter(fs, conf, testFilePath.toString(),
            Reporter.NULL);
    writer.write(NullWritable.get(),
        serde.serialize(new SimpleRow(null), inspector));
    writer.write(NullWritable.get(),
        serde.serialize(new SimpleRow(null), inspector));
    writer.write(NullWritable.get(),
        serde.serialize(new SimpleRow(null), inspector));
    writer.close(Reporter.NULL);
    serde = new OrcSerde();
    SearchArgument sarg =
        SearchArgumentFactory.newBuilder()
            .startAnd()
            .lessThan("z", PredicateLeaf.Type.STRING, new String("foo"))
            .end()
            .build();
    conf.set("sarg.pushdown", toKryo(sarg));
    conf.set("hive.io.file.readcolumn.names", "z");
    properties.setProperty("columns", "z");
    properties.setProperty("columns.types", "string");
    SerDeUtils.initializeSerDe(serde, conf, properties, null);
    inspector = (StructObjectInspector) serde.getObjectInspector();
    InputFormat<?,?> in = new OrcInputFormat();
    FileInputFormat.setInputPaths(conf, testFilePath.toString());
    InputSplit[] splits = in.getSplits(conf, 1);
    assertEquals(0, splits.length);
  }

}
