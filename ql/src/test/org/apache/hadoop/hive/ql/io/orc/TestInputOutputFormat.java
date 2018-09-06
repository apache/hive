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

import static org.junit.Assert.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedExceptionAction;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeSet;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapper;
import org.apache.hadoop.hive.ql.exec.tez.ColumnarSplitSizeEstimator;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.io.AcidInputFormat;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.BucketCodec;
import org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.InputFormatChecker;
import org.apache.hadoop.hive.ql.io.RecordIdentifier;
import org.apache.hadoop.hive.ql.io.RecordUpdater;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat.Context;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat.SplitStrategy;
import org.apache.hadoop.hive.ql.io.sarg.ConvertAstToSearchArg;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.VectorPartitionDesc;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
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
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;
import org.apache.orc.FileFormatException;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcProto;
import org.apache.orc.StripeInformation;
import org.apache.orc.TypeDescription;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;

@SuppressWarnings({ "deprecation", "unchecked", "rawtypes" })
public class TestInputOutputFormat {
  private static final Logger LOG = LoggerFactory.getLogger(TestInputOutputFormat.class);

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
      dateValue = Date.ofEpochMilli(millisUtc);
      timestampValue = Timestamp.ofEpochMilli(millisUtc);
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


    static String getColumnNamesProperty() {
      return "booleanValue,byteValue,shortValue,intValue,longValue,floatValue,doubleValue,stringValue,decimalValue,dateValue,timestampValue";
    }
    static String getColumnTypesProperty() {
      return "boolean:tinyint:smallint:int:bigint:float:double:string:decimal(38,18):date:timestamp";
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
    conf.set(HiveConf.ConfVars.HIVE_ORC_CACHE_STRIPE_DETAILS_MEMORY_SIZE.varname, "10Mb");
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
              context, fs, new MockPath(fs, "mock:/a/b"), false, null);
          List<SplitStrategy<?>> splitStrategies = createSplitStrategies(context, gen);
          assertEquals(1, splitStrategies.size());
          final SplitStrategy splitStrategy = splitStrategies.get(0);
          assertTrue(
              String.format(
                  "Split strategy for %d files x %d size for %d splits", c, s,
                  n),
              splitStrategy.getClass().getSimpleName()
                  .equals(strategyResults[k++]));
        }
      }
    }

    k = 0;
    conf.set(ConfVars.HIVE_ORC_CACHE_STRIPE_DETAILS_MEMORY_SIZE.varname, "0");
    for (int c : counts) {
      for (int s : sizes) {
        final FileSystem fs = generateMockFiles(c, s);
        for (int n : numSplits) {
          final OrcInputFormat.Context context = new OrcInputFormat.Context(
              conf, n);
          OrcInputFormat.FileGenerator gen = new OrcInputFormat.FileGenerator(
              context, fs, new MockPath(fs, "mock:/a/b"), false, null);
          List<SplitStrategy<?>> splitStrategies = createSplitStrategies(context, gen);
          assertEquals(1, splitStrategies.size());
          final SplitStrategy splitStrategy = splitStrategies.get(0);
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
        new MockFile("mock:/a/b/part-00", 1000, new byte[1]),
        new MockFile("mock:/a/b/part-01", 1000, new byte[1]),
        new MockFile("mock:/a/b/_part-02", 1000, new byte[1]),
        new MockFile("mock:/a/b/.part-03", 1000, new byte[1]),
        new MockFile("mock:/a/b/part-04", 1000, new byte[1]));
    OrcInputFormat.FileGenerator gen =
      new OrcInputFormat.FileGenerator(context, fs,
          new MockPath(fs, "mock:/a/b"), false, null);
    List<OrcInputFormat.SplitStrategy<?>> splitStrategies = createSplitStrategies(context, gen);
    assertEquals(1, splitStrategies.size());
    assertEquals(true, splitStrategies.get(0) instanceof OrcInputFormat.BISplitStrategy);

    conf.set("mapreduce.input.fileinputformat.split.maxsize", "500");
    context = new OrcInputFormat.Context(conf);
    fs = new MockFileSystem(conf,
        new MockFile("mock:/a/b/part-00", 1000, new byte[1000]),
        new MockFile("mock:/a/b/part-01", 1000, new byte[1000]),
        new MockFile("mock:/a/b/_part-02", 1000, new byte[1000]),
        new MockFile("mock:/a/b/.part-03", 1000, new byte[1000]),
        new MockFile("mock:/a/b/part-04", 1000, new byte[1000]));
    gen = new OrcInputFormat.FileGenerator(context, fs,
            new MockPath(fs, "mock:/a/b"), false, null);
    splitStrategies = createSplitStrategies(context, gen);
    assertEquals(1, splitStrategies.size());
    assertEquals(true, splitStrategies.get(0) instanceof OrcInputFormat.ETLSplitStrategy);
  }

  @Test
  public void testACIDSplitStrategy() throws Exception {
    conf.set("bucket_count", "2");
    conf.setBoolean(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL, true);
    OrcInputFormat.Context context = new OrcInputFormat.Context(conf);
    MockFileSystem fs = new MockFileSystem(conf,
        new MockFile("mock:/a/delta_000_001/bucket_000000", 1000, new byte[1], new MockBlock("host1")),
        new MockFile("mock:/a/delta_000_001/bucket_000001", 1000, new byte[1], new MockBlock("host1")),
        new MockFile("mock:/a/delta_001_002/bucket_000000", 1000, new byte[1], new MockBlock("host1")),
        new MockFile("mock:/a/delta_001_002/bucket_000001", 1000, new byte[1], new MockBlock("host1")));
    OrcInputFormat.FileGenerator gen =
        new OrcInputFormat.FileGenerator(context, fs,
            new MockPath(fs, "mock:/a"), false, null);
    List<OrcInputFormat.SplitStrategy<?>> splitStrategies = createSplitStrategies(context, gen);
    assertEquals(true, splitStrategies.get(0) instanceof OrcInputFormat.ACIDSplitStrategy);
    List<OrcSplit> splits = ((OrcInputFormat.ACIDSplitStrategy)splitStrategies.get(0)).getSplits();
    ColumnarSplitSizeEstimator splitSizeEstimator = new ColumnarSplitSizeEstimator();
    for (OrcSplit split: splits) {
      assertEquals(1, splitSizeEstimator.getEstimatedSize(split));
    }
    assertEquals(4, splits.size());
  }

  @Test
  public void testACIDSplitStrategyForSplitUpdate() throws Exception {
    conf.set("bucket_count", "2");
    conf.set(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL, "true");
    conf.set(hive_metastoreConstants.TABLE_TRANSACTIONAL_PROPERTIES, "default");
    OrcInputFormat.Context context = new OrcInputFormat.Context(conf);

    // Case 1: Test with just originals => Single split strategy with two splits.
    MockFileSystem fs = new MockFileSystem(conf,
        new MockFile("mock:/a/b/000000_0", 1000, new byte[1], new MockBlock("host1")),
        new MockFile("mock:/a/b/000000_1", 1000, new byte[1], new MockBlock("host1")));
    OrcInputFormat.FileGenerator gen =
        new OrcInputFormat.FileGenerator(context, fs,
            new MockPath(fs, "mock:/a"), false, null);
    List<OrcInputFormat.SplitStrategy<?>> splitStrategies = createSplitStrategies(context, gen);
    assertEquals(1, splitStrategies.size());
    assertEquals(true, splitStrategies.get(0) instanceof OrcInputFormat.ACIDSplitStrategy);
    List<OrcSplit> splits = ((OrcInputFormat.ACIDSplitStrategy)splitStrategies.get(0)).getSplits();
    assertEquals(2, splits.size());
    assertEquals("mock:/a/b/000000_0", splits.get(0).getPath().toUri().toString());
    assertEquals("mock:/a/b/000000_1", splits.get(1).getPath().toUri().toString());
    assertTrue(splits.get(0).isOriginal());
    assertTrue(splits.get(1).isOriginal());

    // Case 2: Test with originals and base => Single split strategy with two splits on compacted
    // base since the presence of a base will make the originals obsolete.
    fs = new MockFileSystem(conf,
        new MockFile("mock:/a/b/000000_0", 1000, new byte[1], new MockBlock("host1")),
        new MockFile("mock:/a/b/000000_1", 1000, new byte[1], new MockBlock("host1")),
        new MockFile("mock:/a/base_0000001/bucket_00000", 1000, new byte[1], new MockBlock("host1")),
        new MockFile("mock:/a/base_0000001/bucket_00001", 1000, new byte[1], new MockBlock("host1")));
    gen = new OrcInputFormat.FileGenerator(context, fs, new MockPath(fs, "mock:/a"), false, null);
    splitStrategies = createSplitStrategies(context, gen);
    assertEquals(1, splitStrategies.size());
    assertEquals(true, splitStrategies.get(0) instanceof OrcInputFormat.ACIDSplitStrategy);
    splits = ((OrcInputFormat.ACIDSplitStrategy)splitStrategies.get(0)).getSplits();
    assertEquals(2, splits.size());
    assertEquals("mock:/a/base_0000001/bucket_00000", splits.get(0).getPath().toUri().toString());
    assertEquals("mock:/a/base_0000001/bucket_00001", splits.get(1).getPath().toUri().toString());
    assertFalse(splits.get(0).isOriginal());
    assertFalse(splits.get(1).isOriginal());

    // Case 3: Test with originals and deltas => Two split strategies with two splits for each.
    fs = new MockFileSystem(conf,
        new MockFile("mock:/a/b/000000_0", 1000, new byte[1], new MockBlock("host1")),
        new MockFile("mock:/a/b/000000_1", 1000, new byte[1], new MockBlock("host1")),
        new MockFile("mock:/a/delta_0000001_0000001_0000/bucket_00000", 1000, new byte[1], new MockBlock("host1")),
        new MockFile("mock:/a/delta_0000001_0000001_0000/bucket_00001", 1000, new byte[1], new MockBlock("host1")),
        new MockFile("mock:/a/delete_delta_0000001_0000001_0000/bucket_00000", 1000, new byte[1], new MockBlock("host1")),
        new MockFile("mock:/a/delete_delta_0000001_0000001_0000/bucket_00001", 1000, new byte[1], new MockBlock("host1")));
    gen = new OrcInputFormat.FileGenerator(context, fs, new MockPath(fs, "mock:/a"), false, null);
    splitStrategies = createSplitStrategies(context, gen);
    assertEquals(2, splitStrategies.size());
    assertEquals(true, splitStrategies.get(0) instanceof OrcInputFormat.ACIDSplitStrategy);
    splits = ((OrcInputFormat.ACIDSplitStrategy)splitStrategies.get(0)).getSplits();
    assertEquals(2, splits.size());
    assertEquals("mock:/a/b/000000_0", splits.get(0).getPath().toUri().toString());
    assertEquals("mock:/a/b/000000_1", splits.get(1).getPath().toUri().toString());
    assertTrue(splits.get(0).isOriginal());
    assertTrue(splits.get(1).isOriginal());
    assertEquals(true, splitStrategies.get(1) instanceof OrcInputFormat.ACIDSplitStrategy);
    splits = ((OrcInputFormat.ACIDSplitStrategy)splitStrategies.get(1)).getSplits();
    assertEquals(2, splits.size());
    assertEquals("mock:/a/delta_0000001_0000001_0000/bucket_00000", splits.get(0).getPath().toUri().toString());
    assertEquals("mock:/a/delta_0000001_0000001_0000/bucket_00001", splits.get(1).getPath().toUri().toString());
    assertFalse(splits.get(0).isOriginal());
    assertFalse(splits.get(1).isOriginal());

    // Case 4: Test with originals and deltas but now with only one bucket covered, i.e. we will
    // have originals & insert_deltas for only one bucket, but the delete_deltas will be for two
    // buckets => Two strategies with one split for each.
    // When split-update is enabled, we do not need to account for buckets that aren't covered.
    // The reason why we are able to do so is because the valid user data has already been considered
    // as base for the covered buckets. Hence, the uncovered buckets do not have any relevant
    // data and we can just ignore them.
    fs = new MockFileSystem(conf,
        new MockFile("mock:/a/b/000000_0", 1000, new byte[1], new MockBlock("host1")),
        new MockFile("mock:/a/delta_0000001_0000001_0000/bucket_00000", 1000, new byte[1], new MockBlock("host1")),
        new MockFile("mock:/a/delete_delta_0000001_0000001_0000/bucket_00000", 1000, new byte[1], new MockBlock("host1")),
        new MockFile("mock:/a/delete_delta_0000001_0000001_0000/bucket_00001", 1000, new byte[1], new MockBlock("host1")));
    gen = new OrcInputFormat.FileGenerator(context, fs, new MockPath(fs, "mock:/a"), false, null);
    splitStrategies = createSplitStrategies(context, gen);
    assertEquals(2, splitStrategies.size());
    assertEquals(true, splitStrategies.get(0) instanceof OrcInputFormat.ACIDSplitStrategy);
    splits = ((OrcInputFormat.ACIDSplitStrategy)splitStrategies.get(0)).getSplits();
    assertEquals(1, splits.size());
    assertEquals("mock:/a/b/000000_0", splits.get(0).getPath().toUri().toString());
    assertTrue(splits.get(0).isOriginal());
    assertEquals(true, splitStrategies.get(1) instanceof OrcInputFormat.ACIDSplitStrategy);
    splits = ((OrcInputFormat.ACIDSplitStrategy)splitStrategies.get(1)).getSplits();
    assertEquals(1, splits.size());
    assertEquals("mock:/a/delta_0000001_0000001_0000/bucket_00000", splits.get(0).getPath().toUri().toString());
    assertFalse(splits.get(0).isOriginal());

    // Case 5: Test with originals, compacted_base, insert_deltas, delete_deltas (exhaustive test)
    // This should just generate one strategy with splits for base and insert_deltas.
    fs = new MockFileSystem(conf,
        new MockFile("mock:/a/b/000000_0", 1000, new byte[1], new MockBlock("host1")),
        new MockFile("mock:/a/b/000000_1", 1000, new byte[1], new MockBlock("host1")),
        new MockFile("mock:/a/base_0000001/bucket_00000", 1000, new byte[1], new MockBlock("host1")),
        new MockFile("mock:/a/base_0000001/bucket_00001", 1000, new byte[1], new MockBlock("host1")),
        new MockFile("mock:/a/delta_0000002_0000002_0000/bucket_00000", 1000, new byte[1], new MockBlock("host1")),
        new MockFile("mock:/a/delta_0000002_0000002_0000/bucket_00001", 1000, new byte[1], new MockBlock("host1")),
        new MockFile("mock:/a/delete_delta_0000002_0000002_0000/bucket_00000", 1000, new byte[1], new MockBlock("host1")),
        new MockFile("mock:/a/delete_delta_0000002_0000002_0000/bucket_00001", 1000, new byte[1], new MockBlock("host1")));
    gen = new OrcInputFormat.FileGenerator(context, fs, new MockPath(fs, "mock:/a"), false, null);
    splitStrategies = createSplitStrategies(context, gen);
    assertEquals(1, splitStrategies.size());
    assertEquals(true, splitStrategies.get(0) instanceof OrcInputFormat.ACIDSplitStrategy);
    splits = ((OrcInputFormat.ACIDSplitStrategy)splitStrategies.get(0)).getSplits();
    assertEquals(4, splits.size());
    assertEquals("mock:/a/base_0000001/bucket_00000", splits.get(0).getPath().toUri().toString());
    assertEquals("mock:/a/base_0000001/bucket_00001", splits.get(1).getPath().toUri().toString());
    assertEquals("mock:/a/delta_0000002_0000002_0000/bucket_00000", splits.get(2).getPath().toUri().toString());
    assertEquals("mock:/a/delta_0000002_0000002_0000/bucket_00001", splits.get(3).getPath().toUri().toString());
    assertFalse(splits.get(0).isOriginal());
    assertFalse(splits.get(1).isOriginal());
    assertFalse(splits.get(2).isOriginal());
    assertFalse(splits.get(3).isOriginal());
  }

  @Test
  public void testFSCallsVectorizedOrcAcidRowBatchReader() throws IOException {
    try {
      MockFileSystem fs = new MockFileSystem(conf);
      MockFileSystem.addGlobalFile(
        new MockFile("mock:/a/delta_0000001_0000001_0000/bucket_00000", 1000, new byte[1], new MockBlock("host1")));
      MockFileSystem.addGlobalFile(
        new MockFile("mock:/a/delta_0000001_0000001_0000/bucket_00001", 1000, new byte[1], new MockBlock("host1")));
      MockFileSystem.addGlobalFile(
        new MockFile("mock:/a/delta_0000001_0000001_0000/bucket_00002", 1000, new byte[1], new MockBlock("host1")));
      MockFileSystem.addGlobalFile(
        new MockFile("mock:/a/delta_0000001_0000001_0000/bucket_00003", 1000, new byte[1], new MockBlock("host1")));
      MockFileSystem.addGlobalFile(
        new MockFile("mock:/a/delta_0000002_0000002_0000/bucket_00000", 1000, new byte[1], new MockBlock("host1")));
      MockFileSystem.addGlobalFile(
        new MockFile("mock:/a/delta_0000002_0000002_0000/bucket_00001", 1000, new byte[1], new MockBlock("host1")));
      MockFileSystem.addGlobalFile(
        new MockFile("mock:/a/delta_0000002_0000002_0000/bucket_00002", 1000, new byte[1], new MockBlock("host1")));
      MockFileSystem.addGlobalFile(
        new MockFile("mock:/a/delta_0000002_0000002_0000/bucket_00003", 1000, new byte[1], new MockBlock("host1")));

      conf.set("bucket_count", "4");
      //set up props for read
      conf.setBoolean(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL, true);
      AcidUtils.setAcidOperationalProperties(conf, true, null);
      conf.set(IOConstants.SCHEMA_EVOLUTION_COLUMNS,
        TestVectorizedOrcAcidRowBatchReader.DummyRow.getColumnNamesProperty());
      conf.set(IOConstants.SCHEMA_EVOLUTION_COLUMNS_TYPES,
        TestVectorizedOrcAcidRowBatchReader.DummyRow.getColumnTypesProperty());
      conf.setBoolean(HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED.varname, true);
      MockPath mockPath = new MockPath(fs, "mock:/a");
      conf.set("mapred.input.dir", mockPath.toString());
      conf.set("fs.defaultFS", "mock:///");
      conf.set("fs.mock.impl", MockFileSystem.class.getName());
      OrcInputFormat.Context context = new OrcInputFormat.Context(conf);

      OrcInputFormat.FileGenerator gen = new OrcInputFormat.FileGenerator(context, fs, new MockPath(fs, "mock:/a"),
        false, null);
      List<OrcInputFormat.SplitStrategy<?>> splitStrategies = createSplitStrategies(context, gen);
      assertEquals(1, splitStrategies.size());
      assertEquals(true, splitStrategies.get(0) instanceof OrcInputFormat.ACIDSplitStrategy);
      List<OrcSplit> splits = ((OrcInputFormat.ACIDSplitStrategy) splitStrategies.get(0)).getSplits();
      // marker comment to look at stats read ops in target/surefire-reports/*-output.txt
      System.out.println("STATS TRACE START - " + testCaseName.getMethodName());
      // when creating the reader below there are 2 read ops per bucket file (listStatus and open).
      // HIVE-19588 removes listStatus from the code path so there should only be one read ops (open) after HIVE-19588
      int readsBefore = fs.statistics.getReadOps();
      for (OrcSplit split : splits) {
        try {
          new VectorizedOrcAcidRowBatchReader(split, conf, Reporter.NULL, new VectorizedRowBatchCtx());
        } catch (FileFormatException e) {
          // this is expected as these mock files are not valid orc file
        }
      }
      int readsAfter = fs.statistics.getReadOps();
      System.out.println("STATS TRACE END - " + testCaseName.getMethodName());
      int delta = readsAfter - readsBefore;
      // 16 without HIVE-19588, 8 with HIVE-19588
      assertEquals(8, delta);
    } finally {
      MockFileSystem.clearGlobalFiles();
    }
  }

  @Test
  public void testBIStrategySplitBlockBoundary() throws Exception {
    conf.set(HiveConf.ConfVars.HIVE_ORC_SPLIT_STRATEGY.varname, "BI");
    OrcInputFormat.Context context = new OrcInputFormat.Context(conf);
    MockFileSystem fs = new MockFileSystem(conf,
        new MockFile("mock:/a/b/part-00", 1000, new byte[1], new MockBlock("host1", "host2")),
        new MockFile("mock:/a/b/part-01", 1000, new byte[1], new MockBlock("host1", "host2")),
        new MockFile("mock:/a/b/part-02", 1000, new byte[1], new MockBlock("host1", "host2")),
        new MockFile("mock:/a/b/part-03", 1000, new byte[1], new MockBlock("host1", "host2")),
        new MockFile("mock:/a/b/part-04", 1000, new byte[1], new MockBlock("host1", "host2")));
    OrcInputFormat.FileGenerator gen =
        new OrcInputFormat.FileGenerator(context, fs,
            new MockPath(fs, "mock:/a/b"), false, null);
    List<OrcInputFormat.SplitStrategy<?>> splitStrategies = createSplitStrategies(context, gen);
    assertEquals(1, splitStrategies.size());
    assertEquals(true, splitStrategies.get(0) instanceof OrcInputFormat.BISplitStrategy);
    List<OrcSplit> splits = ((OrcInputFormat.BISplitStrategy)splitStrategies.get(0)).getSplits();
    int numSplits = splits.size();
    assertEquals(5, numSplits);

    context = new OrcInputFormat.Context(conf);
    fs = new MockFileSystem(conf,
        new MockFile("mock:/a/b/part-00", 1000, new byte[1000], new MockBlock("host1", "host2")),
        new MockFile("mock:/a/b/part-01", 1000, new byte[1000], new MockBlock("host1", "host2")),
        new MockFile("mock:/a/b/part-02", 1000, new byte[1000], new MockBlock("host1", "host2")),
        new MockFile("mock:/a/b/part-03", 1000, new byte[1000], new MockBlock("host1", "host2")),
        new MockFile("mock:/a/b/part-04", 1000, new byte[1000], new MockBlock("host1", "host2")));
    gen = new OrcInputFormat.FileGenerator(context, fs,
        new MockPath(fs, "mock:/a/b"), false, null);
    splitStrategies = createSplitStrategies(context, gen);
    assertEquals(1, splitStrategies.size());
    assertEquals(true, splitStrategies.get(0) instanceof OrcInputFormat.BISplitStrategy);
    splits = ((OrcInputFormat.BISplitStrategy)splitStrategies.get(0)).getSplits();
    numSplits = splits.size();
    assertEquals(5, numSplits);

    context = new OrcInputFormat.Context(conf);
    fs = new MockFileSystem(conf,
        new MockFile("mock:/a/b/part-00", 1000, new byte[1100], new MockBlock("host1", "host2"),
            new MockBlock("host1", "host2")),
        new MockFile("mock:/a/b/part-01", 1000, new byte[1100], new MockBlock("host1", "host2"),
            new MockBlock("host1", "host2")),
        new MockFile("mock:/a/b/part-02", 1000, new byte[1100], new MockBlock("host1", "host2"),
            new MockBlock("host1", "host2")),
        new MockFile("mock:/a/b/part-03", 1000, new byte[1100], new MockBlock("host1", "host2"),
            new MockBlock("host1", "host2")),
        new MockFile("mock:/a/b/part-04", 1000, new byte[1100], new MockBlock("host1", "host2"),
            new MockBlock("host1", "host2")));
    gen = new OrcInputFormat.FileGenerator(context, fs,
        new MockPath(fs, "mock:/a/b"), false, null);
    splitStrategies = createSplitStrategies(context, gen);
    assertEquals(1, splitStrategies.size());
    assertEquals(true, splitStrategies.get(0) instanceof OrcInputFormat.BISplitStrategy);
    splits = ((OrcInputFormat.BISplitStrategy)splitStrategies.get(0)).getSplits();
    numSplits = splits.size();
    assertEquals(10, numSplits);

    context = new OrcInputFormat.Context(conf);
    fs = new MockFileSystem(conf,
        new MockFile("mock:/a/b/part-00", 1000, new byte[2000], new MockBlock("host1", "host2"),
            new MockBlock("host1", "host2")),
        new MockFile("mock:/a/b/part-01", 1000, new byte[2000], new MockBlock("host1", "host2"),
            new MockBlock("host1", "host2")),
        new MockFile("mock:/a/b/part-02", 1000, new byte[2000], new MockBlock("host1", "host2"),
            new MockBlock("host1", "host2")),
        new MockFile("mock:/a/b/part-03", 1000, new byte[2000], new MockBlock("host1", "host2"),
            new MockBlock("host1", "host2")),
        new MockFile("mock:/a/b/part-04", 1000, new byte[2000], new MockBlock("host1", "host2"),
            new MockBlock("host1", "host2")));
    gen = new OrcInputFormat.FileGenerator(context, fs,
        new MockPath(fs, "mock:/a/b"), false, null);
    splitStrategies = createSplitStrategies(context, gen);
    assertEquals(1, splitStrategies.size());
    assertEquals(true, splitStrategies.get(0) instanceof OrcInputFormat.BISplitStrategy);
    splits = ((OrcInputFormat.BISplitStrategy)splitStrategies.get(0)).getSplits();
    numSplits = splits.size();
    assertEquals(10, numSplits);

    context = new OrcInputFormat.Context(conf);
    fs = new MockFileSystem(conf,
        new MockFile("mock:/a/b/part-00", 1000, new byte[2200], new MockBlock("host1", "host2"),
            new MockBlock("host1", "host2"), new MockBlock("host1", "host2")),
        new MockFile("mock:/a/b/part-01", 1000, new byte[2200], new MockBlock("host1", "host2"),
            new MockBlock("host1", "host2"), new MockBlock("host1", "host2")),
        new MockFile("mock:/a/b/part-02", 1000, new byte[2200], new MockBlock("host1", "host2"),
            new MockBlock("host1", "host2"), new MockBlock("host1", "host2")),
        new MockFile("mock:/a/b/part-03", 1000, new byte[2200], new MockBlock("host1", "host2"),
            new MockBlock("host1", "host2"), new MockBlock("host1", "host2")),
        new MockFile("mock:/a/b/part-04", 1000, new byte[2200], new MockBlock("host1", "host2"),
            new MockBlock("host1", "host2"), new MockBlock("host1", "host2")));
    gen = new OrcInputFormat.FileGenerator(context, fs,
        new MockPath(fs, "mock:/a/b"), false, null);
    splitStrategies = createSplitStrategies(context, gen);
    assertEquals(1, splitStrategies.size());
    assertEquals(true, splitStrategies.get(0) instanceof OrcInputFormat.BISplitStrategy);
    splits = ((OrcInputFormat.BISplitStrategy)splitStrategies.get(0)).getSplits();
    numSplits = splits.size();
    assertEquals(15, numSplits);
  }

  @Test
  public void testEtlCombinedStrategy() throws Exception {
    conf.set(HiveConf.ConfVars.HIVE_ORC_SPLIT_STRATEGY.varname, "ETL");
    conf.set(HiveConf.ConfVars.HIVE_ORC_SPLIT_DIRECTORY_BATCH_MS.varname, "1000000");
    AcidUtils.setAcidOperationalProperties(conf, true, null);
    conf.setBoolean(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL, true);
    conf.set(hive_metastoreConstants.TABLE_TRANSACTIONAL_PROPERTIES, "default");

    OrcInputFormat.Context context = new OrcInputFormat.Context(conf);
    MockFileSystem fs = new MockFileSystem(conf,
        new MockFile("mock:/a/1/part-00", 1000, new byte[1]),
        new MockFile("mock:/a/1/part-01", 1000, new byte[1]),
        new MockFile("mock:/a/2/part-00", 1000, new byte[1]),
        new MockFile("mock:/a/2/part-01", 1000, new byte[1]),
        new MockFile("mock:/a/3/base_0/bucket_00001", 1000, new byte[1]),
        new MockFile("mock:/a/4/base_0/bucket_00001", 1000, new byte[1]),
        new MockFile("mock:/a/5/base_0/bucket_00001", 1000, new byte[1]),
        new MockFile("mock:/a/5/delta_0_25/bucket_00001", 1000, new byte[1]),
        new MockFile("mock:/a/6/delta_27_29/bucket_00001", 1000, new byte[1]),
        new MockFile("mock:/a/6/delete_delta_27_29/bucket_00001", 1000, new byte[1])
    );

    OrcInputFormat.CombinedCtx combineCtx = new OrcInputFormat.CombinedCtx();
    // The first directory becomes the base for combining.
    List<SplitStrategy<?>> ss = createOrCombineStrategies(context, fs, "mock:/a/1", combineCtx);
    assertTrue(ss.isEmpty());
    assertTrue(combineCtx.combined instanceof OrcInputFormat.ETLSplitStrategy);
    OrcInputFormat.ETLSplitStrategy etlSs = combineCtx.combined;
    assertEquals(2, etlSs.files.size());
    assertTrue(etlSs.isOriginal);
    assertEquals(1, etlSs.dirs.size());
    // The second one should be combined into the first.
    ss = createOrCombineStrategies(context, fs, "mock:/a/2", combineCtx);
    assertTrue(ss.isEmpty());
    assertTrue(combineCtx.combined instanceof OrcInputFormat.ETLSplitStrategy);
    assertEquals(4, etlSs.files.size());
    assertEquals(2, etlSs.dirs.size());
    // The third one has the base file, so it shouldn't be combined but could be a base.
    ss = createOrCombineStrategies(context, fs, "mock:/a/3", combineCtx);
    assertEquals(1, ss.size());
    assertSame(etlSs, ss.get(0));
    assertEquals(4, etlSs.files.size());
    assertEquals(2, etlSs.dirs.size());
    assertTrue(combineCtx.combined instanceof OrcInputFormat.ETLSplitStrategy);
    etlSs = combineCtx.combined;
    assertEquals(1, etlSs.files.size());
    assertFalse(etlSs.isOriginal);
    assertEquals(1, etlSs.dirs.size());
    // Try the first again, it would not be combined and we'd retain the old base (less files).
    ss = createOrCombineStrategies(context, fs, "mock:/a/1", combineCtx);
    assertEquals(1, ss.size());
    assertTrue(ss.get(0) instanceof OrcInputFormat.ETLSplitStrategy);
    assertNotSame(etlSs, ss.get(0));
    OrcInputFormat.ETLSplitStrategy rejectedEtlSs = (OrcInputFormat.ETLSplitStrategy)ss.get(0);
    assertEquals(2, rejectedEtlSs.files.size());
    assertEquals(1, rejectedEtlSs.dirs.size());
    assertTrue(rejectedEtlSs.isOriginal);
    assertEquals(1, etlSs.files.size());
    assertEquals(1, etlSs.dirs.size());
    // The fourth could be combined again.
    ss = createOrCombineStrategies(context, fs, "mock:/a/4", combineCtx);
    assertTrue(ss.isEmpty());
    assertTrue(combineCtx.combined instanceof OrcInputFormat.ETLSplitStrategy);
    assertEquals(2, etlSs.files.size());
    assertEquals(2, etlSs.dirs.size());
    // The fifth could be combined again.
    ss = createOrCombineStrategies(context, fs, "mock:/a/5", combineCtx);
    assertTrue(ss.isEmpty());
    assertTrue(combineCtx.combined instanceof OrcInputFormat.ETLSplitStrategy);
    assertEquals(4, etlSs.files.size());
    assertEquals(3, etlSs.dirs.size());

    // The sixth will not be combined because of delete delta files.  Is that desired? HIVE-18110
    ss = createOrCombineStrategies(context, fs, "mock:/a/6", combineCtx);
    assertEquals(1, ss.size());
    assertTrue(ss.get(0) instanceof OrcInputFormat.ETLSplitStrategy);
    assertNotSame(etlSs, ss);
    assertEquals(4, etlSs.files.size());
    assertEquals(3, etlSs.dirs.size());
  }

  public List<SplitStrategy<?>> createOrCombineStrategies(OrcInputFormat.Context context,
      MockFileSystem fs, String path, OrcInputFormat.CombinedCtx combineCtx) throws IOException {
    OrcInputFormat.AcidDirInfo adi = createAdi(context, fs, path);
    return OrcInputFormat.determineSplitStrategies(combineCtx, context,
        adi.fs, adi.splitPath, adi.baseFiles, adi.deleteEvents,
        null, null, true);
  }

  public OrcInputFormat.AcidDirInfo createAdi(
      OrcInputFormat.Context context, MockFileSystem fs, String path) throws IOException {
    return new OrcInputFormat.FileGenerator(
        context, fs, new MockPath(fs, path), false, null).call();
  }

  private List<OrcInputFormat.SplitStrategy<?>> createSplitStrategies(
      OrcInputFormat.Context context, OrcInputFormat.FileGenerator gen) throws IOException {
    OrcInputFormat.AcidDirInfo adi = gen.call();
    return OrcInputFormat.determineSplitStrategies(
        null, context, adi.fs, adi.splitPath, adi.baseFiles, adi.deleteEvents,
        null, null, true);
  }

  public static class MockBlock {
    int offset;
    int length;
    final String[] hosts;

    public MockBlock(String... hosts) {
      this.hosts = hosts;
    }

    public void setOffset(int offset) {
      this.offset = offset;
    }

    public void setLength(int length) {
      this.length = length;
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
    public int hashCode() {
      return path.hashCode() + 31 * length;
    }

    @Override
    public boolean equals(final Object obj) {
      if (!(obj instanceof MockFile)) { return false; }
      return ((MockFile) obj).path.equals(this.path) && ((MockFile) obj).length == this.length;
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
      MockBlock block = new MockBlock("host1");
      block.setLength(file.length);
      setBlocks(block);
      System.arraycopy(buf.getData(), 0, file.content, 0, file.length);
    }

    @Override
    public String toString() {
      return "Out stream to " + file.toString();
    }
  }

  /**
   * WARNING: detele(Path...) don't actually delete
   */
  public static class MockFileSystem extends FileSystem {
    final List<MockFile> files = new ArrayList<MockFile>();
    final Map<MockFile, FileStatus> fileStatusMap = new HashMap<>();
    Path workingDir = new Path("/");
    // statics for when the mock fs is created via FileSystem.get
    private static String blockedUgi = null;
    private final static List<MockFile> globalFiles = new ArrayList<MockFile>();
    protected Statistics statistics;

    public MockFileSystem() {
      // empty
    }

    @Override
    public void initialize(URI uri, Configuration conf) {
      setConf(conf);
      statistics = getStatistics("mock", getClass());
    }

    public MockFileSystem(Configuration conf, MockFile... files) {
      setConf(conf);
      this.files.addAll(Arrays.asList(files));
      statistics = getStatistics("mock", getClass());
    }

    public static void setBlockedUgi(String s) {
      blockedUgi = s;
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

    // increments file modification time
    public void touch(MockFile file) {
      if (fileStatusMap.containsKey(file)) {
        FileStatus fileStatus = fileStatusMap.get(file);
        FileStatus fileStatusNew = new FileStatus(fileStatus.getLen(), fileStatus.isDirectory(),
            fileStatus.getReplication(), fileStatus.getBlockSize(),
            fileStatus.getModificationTime() + 1, fileStatus.getAccessTime(),
            fileStatus.getPermission(), fileStatus.getOwner(), fileStatus.getGroup(),
            fileStatus.getPath());
        fileStatusMap.put(file, fileStatusNew);
      }
    }

    @SuppressWarnings("serial")
    public static class MockAccessDenied extends IOException {
    }

    @Override
    public FSDataInputStream open(Path path, int i) throws IOException {
      statistics.incrementReadOps(1);
      System.out.println("STATS: open - " + path);
      checkAccess();
      MockFile file = findFile(path);
      if (file != null) return new FSDataInputStream(new MockInputStream(file));
      throw new IOException("File not found: " + path);
    }

    private MockFile findFile(Path path) {
      for (MockFile file: files) {
        if (file.path.equals(path)) {
          return file;
        }
      }
      for (MockFile file: globalFiles) {
        if (file.path.equals(path)) {
          return file;
        }
      }
      return null;
    }

    private void checkAccess() throws IOException {
      if (blockedUgi == null) return;
      if (!blockedUgi.equals(UserGroupInformation.getCurrentUser().getShortUserName())) return;
      throw new MockAccessDenied();
    }

    @Override
    public FSDataOutputStream create(Path path, FsPermission fsPermission,
                                     boolean overwrite, int bufferSize,
                                     short replication, long blockSize,
                                     Progressable progressable
                                     ) throws IOException {
      statistics.incrementWriteOps(1);
      checkAccess();
      MockFile file = findFile(path);
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
      statistics.incrementWriteOps(1);
      checkAccess();
      return create(path, FsPermission.getDefault(), true, bufferSize,
          (short) 3, 256 * 1024, progressable);
    }

    @Override
    public boolean rename(Path path, Path path2) throws IOException {
      statistics.incrementWriteOps(1);
      checkAccess();
      return false;
    }

    @Override
    public boolean delete(Path path) throws IOException {
      statistics.incrementWriteOps(1);
      checkAccess();
      int removed = 0;
      for(int i = 0; i < files.size(); i++) {
        MockFile mf = files.get(i);
        if(path.equals(mf.path)) {
          files.remove(i);
          removed++;
          break;
        }
      }
      for(int i = 0; i < globalFiles.size(); i++) {
        MockFile mf = files.get(i);
        if(path.equals(mf.path)) {
          globalFiles.remove(i);
          removed++;
          break;
        }
      }
      return removed > 0;
    }

    @Override
    public boolean delete(Path path, boolean b) throws IOException {
      if(b) {
        throw new UnsupportedOperationException();
      }
      return delete(path);
    }

    @Override
    public RemoteIterator<LocatedFileStatus> listLocatedStatus(final Path f)
        throws IOException {
      return new RemoteIterator<LocatedFileStatus>() {
        private Iterator<LocatedFileStatus> iterator = listLocatedFileStatuses(f).iterator();

        @Override
        public boolean hasNext() throws IOException {
          return iterator.hasNext();
        }

        @Override
        public LocatedFileStatus next() throws IOException {
          return iterator.next();
        }
      };
    }

    private List<LocatedFileStatus> listLocatedFileStatuses(Path path) throws IOException {
      statistics.incrementReadOps(1);
      System.out.println("STATS: listLocatedFileStatuses - " + path);
      checkAccess();
      path = path.makeQualified(this);
      List<LocatedFileStatus> result = new ArrayList<>();
      String pathname = path.toString();
      String pathnameAsDir = pathname + "/";
      Set<String> dirs = new TreeSet<String>();
      MockFile file = findFile(path);
      if (file != null) {
        result.add(createLocatedStatus(file));
        return result;
      }
      findMatchingLocatedFiles(files, pathnameAsDir, dirs, result);
      findMatchingLocatedFiles(globalFiles, pathnameAsDir, dirs, result);
      // for each directory add it once
      for(String dir: dirs) {
        result.add(createLocatedDirectory(new MockPath(this, pathnameAsDir + dir)));
      }
      return result;
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
      statistics.incrementReadOps(1);
      System.out.println("STATS: listStatus - " + path);
      checkAccess();
      path = path.makeQualified(this);
      List<FileStatus> result = new ArrayList<FileStatus>();
      String pathname = path.toString();
      String pathnameAsDir = pathname + "/";
      Set<String> dirs = new TreeSet<String>();
      MockFile file = findFile(path);
      if (file != null) {
        return new FileStatus[]{createStatus(file)};
      }
      findMatchingFiles(files, pathnameAsDir, dirs, result);
      findMatchingFiles(globalFiles, pathnameAsDir, dirs, result);
      // for each directory add it once
      for(String dir: dirs) {
        result.add(createDirectory(new MockPath(this, pathnameAsDir + dir)));
      }
      return result.toArray(new FileStatus[result.size()]);
    }

    private void findMatchingFiles(
        List<MockFile> files, String pathnameAsDir, Set<String> dirs, List<FileStatus> result) {
      for (MockFile file: files) {
        String filename = file.path.toString();
        if (filename.startsWith(pathnameAsDir)) {
          String tail = filename.substring(pathnameAsDir.length());
          int nextSlash = tail.indexOf('/');
          if (nextSlash > 0) {
            dirs.add(tail.substring(0, nextSlash));
          } else {
            result.add(createStatus(file));
          }
        }
      }
    }

    private void findMatchingLocatedFiles(
        List<MockFile> files, String pathnameAsDir, Set<String> dirs, List<LocatedFileStatus> result)
        throws IOException {
      for (MockFile file: files) {
        String filename = file.path.toString();
        if (filename.startsWith(pathnameAsDir)) {
          String tail = filename.substring(pathnameAsDir.length());
          int nextSlash = tail.indexOf('/');
          if (nextSlash > 0) {
            dirs.add(tail.substring(0, nextSlash));
          } else {
            result.add(createLocatedStatus(file));
          }
        }
      }
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
      statistics.incrementWriteOps(1);
      return false;
    }

    private FileStatus createStatus(MockFile file) {
      if (fileStatusMap.containsKey(file)) {
        return fileStatusMap.get(file);
      }
      FileStatus fileStatus = new FileStatus(file.length, false, 1, file.blockSize, 0, 0,
          FsPermission.createImmutable((short) 644), "owen", "group",
          file.path);
      fileStatusMap.put(file, fileStatus);
      return fileStatus;
    }

    private FileStatus createDirectory(Path dir) {
      return new FileStatus(0, true, 0, 0, 0, 0,
          FsPermission.createImmutable((short) 755), "owen", "group", dir);
    }

    private LocatedFileStatus createLocatedStatus(MockFile file) throws IOException {
      FileStatus fileStatus = createStatus(file);
      return new LocatedFileStatus(fileStatus,
          getFileBlockLocationsImpl(fileStatus, 0, fileStatus.getLen(), false));
    }

    private LocatedFileStatus createLocatedDirectory(Path dir) throws IOException {
      FileStatus fileStatus = createDirectory(dir);
      return new LocatedFileStatus(fileStatus,
          getFileBlockLocationsImpl(fileStatus, 0, fileStatus.getLen(), false));
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
      statistics.incrementReadOps(1);
      System.out.println("STATS: getFileStatus - " + path);
      checkAccess();
      path = path.makeQualified(this);
      String pathnameAsDir = path.toString() + "/";
      MockFile file = findFile(path);
      if (file != null) return createStatus(file);
      for (MockFile dir : files) {
        if (dir.path.toString().startsWith(pathnameAsDir)) {
          return createDirectory(path);
        }
      }
      for (MockFile dir : globalFiles) {
        if (dir.path.toString().startsWith(pathnameAsDir)) {
          return createDirectory(path);
        }
      }
      throw new FileNotFoundException("File " + path + " does not exist");
    }

    @Override
    public BlockLocation[] getFileBlockLocations(FileStatus stat,
                                                 long start, long len) throws IOException {
      return getFileBlockLocationsImpl(stat, start, len, true);
    }

    private BlockLocation[] getFileBlockLocationsImpl(final FileStatus stat, final long start,
        final long len,
        final boolean updateStats) throws IOException {
      if (updateStats) {
        statistics.incrementReadOps(1);
        System.out.println("STATS: getFileBlockLocationsImpl - " + stat.getPath());
      }
      checkAccess();
      List<BlockLocation> result = new ArrayList<BlockLocation>();
      MockFile file = findFile(stat.getPath());
      if (file != null) {
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

    public static void addGlobalFile(MockFile mockFile) {
      globalFiles.add(mockFile);
    }

    public static void clearGlobalFiles() {
      globalFiles.clear();
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
            fs.getFileStatus(new Path("/a/file")), null, null, true,
            new ArrayList<AcidInputFormat.DeltaMetaData>(), true, null, null), null, true, true);
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
            fs.getFileStatus(new Path("/a/file")), null, null, true,
            new ArrayList<AcidInputFormat.DeltaMetaData>(), true, null, null), null, true, true);
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
      fs.getFileStatus(new Path("/a/file")), null, null, true,
        new ArrayList<AcidInputFormat.DeltaMetaData>(), true, null, null), null, true, true);
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
            fs.getFileStatus(new Path("/a/file")), null, null, true,
            new ArrayList<AcidInputFormat.DeltaMetaData>(), true, null, null), null, true, true);
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
        fs.getFileStatus(new Path("/a/file")), null, null, true,
        new ArrayList<AcidInputFormat.DeltaMetaData>(),
        true, null, null), null, true, true);
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
        fs.getFileStatus(new Path("/a/file")), null, null, true,
        new ArrayList<AcidInputFormat.DeltaMetaData>(),
        true, null, null), null, true, true);
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
    AbstractSerDe serde = new OrcSerde();
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
    conf.set(IOConstants.SCHEMA_EVOLUTION_COLUMNS, MyRow.getColumnNamesProperty());
    conf.set(IOConstants.SCHEMA_EVOLUTION_COLUMNS_TYPES, MyRow.getColumnTypesProperty());
    org.apache.hadoop.mapred.RecordReader reader =
        in.getRecordReader(splits[0], conf, Reporter.NULL);
    Object key = reader.createKey();
    Writable value = (Writable) reader.createValue();
    int rowNum = 0;
    List<? extends StructField> fields =inspector.getAllStructFieldRefs();
    IntObjectInspector intInspector =
        (IntObjectInspector) fields.get(0).getFieldObjectInspector();

    // UNDONE: Don't know why HIVE-12894 causes this to return 0?
    // assertEquals(0.33, reader.getProgress(), 0.01);

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
    AbstractSerDe serde = new OrcSerde();
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
    AbstractSerDe serde = new OrcSerde();
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
    writer.write(new OrcSerde().serialize(null,null));
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
    AbstractSerDe serde = new OrcSerde();
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
    String isVectorizedString = Boolean.toString(isVectorized);
    conf.set("hive.vectorized.execution.enabled", isVectorizedString);
    conf.set(Utilities.VECTOR_MODE, isVectorizedString);
    conf.set(Utilities.USE_VECTORIZED_INPUT_FILE_FORMAT, isVectorizedString);
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
    LinkedHashMap<Path, ArrayList<String>> aliasMap = new LinkedHashMap<>();
    ArrayList<String> aliases = new ArrayList<String>();
    aliases.add(tableName);
    LinkedHashMap<Path, PartitionDesc> partMap = new LinkedHashMap<>();
    for(int p=0; p < partitions; ++p) {
      Path path = new Path(partPath[p]);
      aliasMap.put(path, aliases);
      LinkedHashMap<String, String> partSpec =
          new LinkedHashMap<String, String>();
      PartitionDesc part = new PartitionDesc(tbl, partSpec);
      if (isVectorized) {
        part.setVectorPartitionDesc(
            VectorPartitionDesc.createVectorizedInputFileFormat("MockInputFileFormatClassName", false));
      }
      partMap.put(path, part);
    }
    mapWork.setPathToAliases(aliasMap);
    mapWork.setPathToPartitionInfo(partMap);

    // write the plan out
    FileSystem localFs = FileSystem.getLocal(conf).getRaw();
    Path mapXml = new Path(workDir, "map.xml");
    localFs.delete(mapXml, true);
    FSDataOutputStream planStream = localFs.create(mapXml);
    SerializationUtilities.serializePlan(mapWork, planStream);
    conf.setBoolean(Utilities.HAS_MAP_WORK, true);
    planStream.close();
    return conf;
  }

  /**
   * Set the mockblocks for a file after it has been written
   * @param path the path to modify
   * @param conf the configuration
   * @param blocks the blocks to uses
   * @throws IOException
   */
  static void setBlocks(Path path, Configuration conf,
                        MockBlock... blocks) throws IOException {
    FileSystem mockFs = path.getFileSystem(conf);
    MockOutputStream stream = (MockOutputStream) mockFs.create(path);
    stream.setBlocks(blocks);
  }

  static int getLength(Path path, Configuration conf) throws IOException {
    FileSystem mockFs = path.getFileSystem(conf);
    FileStatus stat = mockFs.getFileStatus(path);
    return (int) stat.getLen();
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
    Path path = new Path(conf.get("mapred.input.dir") + "/0_0");
    Writer writer =
        OrcFile.createWriter(path,
           OrcFile.writerOptions(conf).blockPadding(false)
                  .bufferSize(1024).inspector(inspector));
    for(int i=0; i < 10; ++i) {
      writer.addRow(new MyRow(i, 2*i));
    }
    writer.close();
    setBlocks(path, conf, new MockBlock("host0", "host1"));

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
    Path path = new Path(conf.get("mapred.input.dir") + "/0_0");
    Writer writer =
        OrcFile.createWriter(path,
            OrcFile.writerOptions(conf).blockPadding(false)
                .bufferSize(1024).inspector(inspector));
    for(int i=0; i < 10; ++i) {
      writer.addRow(new MyRow(i, 2*i));
    }
    writer.close();
    setBlocks(path, conf, new MockBlock("host0", "host1"));

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
        new AcidOutputFormat.Options(conf).maximumWriteId(10)
            .writingBase(true).bucket(0).inspector(inspector).finalDestination(partDir));
    for (int i = 0; i < 100; ++i) {
      BigRow row = new BigRow(i);
      writer.insert(10, row);
    }
    writer.close(false);
    Path path = new Path("mock:/vectorizationAcid/p=0/base_0000010/bucket_00000");
    setBlocks(path, conf, new MockBlock("host0", "host1"));

    // call getsplits
    HiveInputFormat<?, ?> inputFormat =
        new HiveInputFormat<WritableComparable, Writable>();
    InputSplit[] splits = inputFormat.getSplits(conf, 10);
    assertEquals(1, splits.length);

    conf.set(IOConstants.SCHEMA_EVOLUTION_COLUMNS, BigRow.getColumnNamesProperty());
    conf.set(IOConstants.SCHEMA_EVOLUTION_COLUMNS_TYPES, BigRow.getColumnTypesProperty());
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_TRANSACTIONAL_TABLE_SCAN, true);

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
    TimestampColumnVector timestampColumn = (TimestampColumnVector) value.cols[10];
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
      assertEquals("checking timestamp " + i, millis,
          timestampColumn.getTime(i));
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
    Path path = new Path("mock:/combination/p=0/0_0");
    setBlocks(path, conf, new MockBlock("host0", "host1"));
    MockFileSystem mockFs = (MockFileSystem) partDir.getFileSystem(conf);
    int length0 = getLength(path, conf);
    writer =
        OrcFile.createWriter(new Path(partDir, "1_0"),
            OrcFile.writerOptions(conf).blockPadding(false)
                .bufferSize(1024).inspector(inspector));
    for(int i=10; i < 20; ++i) {
      writer.addRow(new MyRow(i, 2*i));
    }
    writer.close();
    Path path1 = new Path("mock:/combination/p=0/1_0");
    setBlocks(path1, conf, new MockBlock("host1", "host2"));

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
    assertEquals(getLength(path1, conf), split.getLength(1));
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
        new AcidOutputFormat.Options(conf).maximumWriteId(10)
            .writingBase(true).bucket(0).inspector(inspector).finalDestination(partDir[0]));
    for(int i=0; i < 10; ++i) {
      writer.insert(10, new MyRow(i, 2 * i));
    }
    writer.close(false);

    // base file
    Path base0 = new Path("mock:/combinationAcid/p=0/base_0000010/bucket_00000");
    setBlocks(base0, conf, new MockBlock("host1", "host2"));

    // write a delta file in partition 0
    writer = new OrcRecordUpdater(partDir[0],
        new AcidOutputFormat.Options(conf).maximumWriteId(10)
            .writingBase(true).bucket(1).inspector(inspector).finalDestination(partDir[0]));
    for(int i=10; i < 20; ++i) {
      writer.insert(10, new MyRow(i, 2*i));
    }
    writer.close(false);
    Path base1 = new Path("mock:/combinationAcid/p=0/base_0000010/bucket_00001");
    setBlocks(base1, conf, new MockBlock("host1", "host2"));

    // write three files in partition 1
    for(int bucket=0; bucket < BUCKETS; ++bucket) {
      Path path = new Path(partDir[1], "00000" + bucket + "_0");
      Writer orc = OrcFile.createWriter(
          path,
          OrcFile.writerOptions(conf)
              .blockPadding(false)
              .bufferSize(1024)
              .inspector(inspector));
      orc.addRow(new MyRow(1, 2));
      orc.close();
      setBlocks(path, conf, new MockBlock("host3", "host4"));
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
    assertEquals(700, split.getLength());
    split = (HiveInputFormat.HiveInputSplit) splits[1];
    assertEquals("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat",
        split.inputFormatClassName());
    assertEquals("mock:/combinationAcid/p=0/base_0000010/bucket_00001",
        split.getPath().toString());
    assertEquals(0, split.getStart());
    assertEquals(724, split.getLength());
    CombineHiveInputFormat.CombineHiveInputSplit combineSplit =
        (CombineHiveInputFormat.CombineHiveInputSplit) splits[2];
    assertEquals(BUCKETS, combineSplit.getNumPaths());
    for(int bucket=0; bucket < BUCKETS; ++bucket) {
      assertEquals("mock:/combinationAcid/p=1/00000" + bucket + "_0",
          combineSplit.getPath(bucket).toString());
      assertEquals(0, combineSplit.getOffset(bucket));
      assertEquals(251, combineSplit.getLength(bucket));
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
        .addAllFieldNames(Arrays.asList("op", "owid", "bucket", "rowid", "cwid",
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
    AbstractSerDe serde = new OrcSerde();
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
    StructObjectInspector inspector = createSoi();
    AbstractSerDe serde = new OrcSerde();
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

  @Test
  public void testDoAs() throws Exception {
    conf.setInt(ConfVars.HIVE_ORC_COMPUTE_SPLITS_NUM_THREADS.varname, 1);
    conf.set(ConfVars.HIVE_ORC_SPLIT_STRATEGY.varname, "ETL");
    conf.setBoolean(ConfVars.HIVE_IN_TEST.varname, true);
    conf.setClass("fs.mock.impl", MockFileSystem.class, FileSystem.class);
    String badUser = UserGroupInformation.getCurrentUser().getShortUserName() + "-foo";
    MockFileSystem.setBlockedUgi(badUser);
    // TODO: could we instead get FS from path here and add normal files for every UGI?
    MockFileSystem.clearGlobalFiles();
    OrcInputFormat.Context.resetThreadPool(); // We need the size above to take effect.
    try {
      // OrcInputFormat will get a mock fs from FileSystem.get; add global files.
      MockFileSystem.addGlobalFile(new MockFile("mock:/ugi/1/file", 10000,
          createMockOrcFile(197, 300, 600), new MockBlock("host1-1", "host1-2", "host1-3")));
      MockFileSystem.addGlobalFile(new MockFile("mock:/ugi/2/file", 10000,
          createMockOrcFile(197, 300, 600), new MockBlock("host1-1", "host1-2", "host1-3")));
      FileInputFormat.setInputPaths(conf, "mock:/ugi/1");
      UserGroupInformation ugi = UserGroupInformation.createUserForTesting(badUser, new String[0]);
      assertEquals(0, OrcInputFormat.Context.getCurrentThreadPoolSize());
      try {
        ugi.doAs(new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            OrcInputFormat.generateSplitsInfo(conf, new Context(conf, -1, null));
            return null;
          }
        });
        fail("Didn't throw");
      } catch (Exception ex) {
        Throwable cause = ex;
        boolean found = false;
        while (cause != null) {
          if (cause instanceof MockFileSystem.MockAccessDenied) {
            found = true; // Expected.
            break;
          }
          cause = cause.getCause();
        }
        if (!found) throw ex; // Unexpected.
      }
      assertEquals(1, OrcInputFormat.Context.getCurrentThreadPoolSize());
      FileInputFormat.setInputPaths(conf, "mock:/ugi/2");
      List<OrcSplit> splits = OrcInputFormat.generateSplitsInfo(conf, new Context(conf, -1, null));
      assertEquals(1, splits.size());
    } finally {
      MockFileSystem.clearGlobalFiles();
    }
  }


  private StructObjectInspector createSoi() {
    synchronized (TestOrcFile.class) {
      return (StructObjectInspector)ObjectInspectorFactory.getReflectionObjectInspector(
          SimpleRow.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
  }

  @Test
  public void testSplitGenReadOps() throws Exception {
    MockFileSystem fs = new MockFileSystem(conf);
    conf.set("mapred.input.dir", "mock:///mocktable");
    conf.set("fs.defaultFS", "mock:///");
    conf.set("fs.mock.impl", MockFileSystem.class.getName());
    MockPath mockPath = new MockPath(fs, "mock:///mocktable");
    StructObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = (StructObjectInspector)
          ObjectInspectorFactory.getReflectionObjectInspector(MyRow.class,
              ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    Writer writer =
        OrcFile.createWriter(new Path(mockPath + "/0_0"),
            OrcFile.writerOptions(conf).blockPadding(false)
                .bufferSize(1024).inspector(inspector));
    for(int i=0; i < 10; ++i) {
      writer.addRow(new MyRow(i, 2*i));
    }
    writer.close();

    writer = OrcFile.createWriter(new Path(mockPath + "/0_1"),
        OrcFile.writerOptions(conf).blockPadding(false)
            .bufferSize(1024).inspector(inspector));
    for(int i=0; i < 10; ++i) {
      writer.addRow(new MyRow(i, 2*i));
    }
    writer.close();

    int readOpsBefore = -1;
    for (FileSystem.Statistics statistics : FileSystem.getAllStatistics()) {
      if (statistics.getScheme().equalsIgnoreCase("mock")) {
        readOpsBefore = statistics.getReadOps();
      }
    }
    assertTrue("MockFS has stats. Read ops not expected to be -1", readOpsBefore != -1);
    OrcInputFormat orcInputFormat = new OrcInputFormat();
    InputSplit[] splits = orcInputFormat.getSplits(conf, 2);
    int readOpsDelta = -1;
    for (FileSystem.Statistics statistics : FileSystem.getAllStatistics()) {
      if (statistics.getScheme().equalsIgnoreCase("mock")) {
        readOpsDelta = statistics.getReadOps() - readOpsBefore;
      }
    }
    // call-1: listLocatedStatus - mock:/mocktable
    // call-2: check existence of side file for mock:/mocktable/0_0
    // call-3: open - mock:/mocktable/0_0
    // call-4: check existence of side file for mock:/mocktable/0_1
    // call-5: open - mock:/mocktable/0_1
    assertEquals(5, readOpsDelta);

    assertEquals(2, splits.length);
    // revert back to local fs
    conf.set("fs.defaultFS", "file:///");
  }

  @Test
  public void testSplitGenReadOpsLocalCache() throws Exception {
    MockFileSystem fs = new MockFileSystem(conf);
    // creates the static cache
    MockPath mockPath = new MockPath(fs, "mock:///mocktbl");
    conf.set(ConfVars.HIVE_ORC_CACHE_STRIPE_DETAILS_MEMORY_SIZE.varname, "0");
    conf.set("mapred.input.dir", mockPath.toString());
    conf.set("fs.defaultFS", "mock:///");
    conf.set("fs.mock.impl", MockFileSystem.class.getName());
    StructObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = (StructObjectInspector)
          ObjectInspectorFactory.getReflectionObjectInspector(MyRow.class,
              ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    Writer writer =
        OrcFile.createWriter(new Path(mockPath + "/0_0"),
            OrcFile.writerOptions(conf).blockPadding(false)
                .bufferSize(1024).inspector(inspector));
    for (int i = 0; i < 10; ++i) {
      writer.addRow(new MyRow(i, 2 * i));
    }
    writer.close();

    writer = OrcFile.createWriter(new Path(mockPath + "/0_1"),
        OrcFile.writerOptions(conf).blockPadding(false)
            .bufferSize(1024).inspector(inspector));
    for (int i = 0; i < 10; ++i) {
      writer.addRow(new MyRow(i, 2 * i));
    }
    writer.close();

    int readOpsBefore = -1;
    for (FileSystem.Statistics statistics : FileSystem.getAllStatistics()) {
      if (statistics.getScheme().equalsIgnoreCase("mock")) {
        readOpsBefore = statistics.getReadOps();
      }
    }
    assertTrue("MockFS has stats. Read ops not expected to be -1", readOpsBefore != -1);
    OrcInputFormat orcInputFormat = new OrcInputFormat();
    InputSplit[] splits = orcInputFormat.getSplits(conf, 2);
    assertEquals(2, splits.length);
    int readOpsDelta = -1;
    for (FileSystem.Statistics statistics : FileSystem.getAllStatistics()) {
      if (statistics.getScheme().equalsIgnoreCase("mock")) {
        readOpsDelta = statistics.getReadOps() - readOpsBefore;
      }
    }
    // call-1: listLocatedStatus - mock:/mocktbl
    // call-2: check existence of side file for mock:/mocktbl/0_0
    // call-3: open - mock:/mocktbl/0_0
    // call-4: check existence of side file for  mock:/mocktbl/0_1
    // call-5: open - mock:/mocktbl/0_1
    assertEquals(5, readOpsDelta);

    // force BI to avoid reading footers
    conf.set(HiveConf.ConfVars.HIVE_ORC_SPLIT_STRATEGY.varname, "BI");
    for (FileSystem.Statistics statistics : FileSystem.getAllStatistics()) {
      if (statistics.getScheme().equalsIgnoreCase("mock")) {
        readOpsBefore = statistics.getReadOps();
      }
    }
    orcInputFormat = new OrcInputFormat();
    splits = orcInputFormat.getSplits(conf, 2);
    assertEquals(2, splits.length);
    for (FileSystem.Statistics statistics : FileSystem.getAllStatistics()) {
      if (statistics.getScheme().equalsIgnoreCase("mock")) {
        readOpsDelta = statistics.getReadOps() - readOpsBefore;
      }
    }
    // call-1: listLocatedStatus - mock:/mocktbl
    // call-2: check existence of side file for mock:/mocktbl/0_0
    // call-3: check existence of side file for  mock:/mocktbl/0_1
    assertEquals(3, readOpsDelta);

    // enable cache and use default strategy
    conf.set(ConfVars.HIVE_ORC_CACHE_STRIPE_DETAILS_MEMORY_SIZE.varname, "10Mb");
    conf.set(HiveConf.ConfVars.HIVE_ORC_SPLIT_STRATEGY.varname, "HYBRID");
    for (FileSystem.Statistics statistics : FileSystem.getAllStatistics()) {
      if (statistics.getScheme().equalsIgnoreCase("mock")) {
        readOpsBefore = statistics.getReadOps();
      }
    }
    orcInputFormat = new OrcInputFormat();
    splits = orcInputFormat.getSplits(conf, 2);
    assertEquals(2, splits.length);
    for (FileSystem.Statistics statistics : FileSystem.getAllStatistics()) {
      if (statistics.getScheme().equalsIgnoreCase("mock")) {
        readOpsDelta = statistics.getReadOps() - readOpsBefore;
      }
    }
    // call-1: listLocatedStatus - mock:/mocktbl
    // call-2: check existence of side file for mock:/mocktbl/0_0
    // call-3: open - mock:/mocktbl/0_0
    // call-4: check existence of side file for mock:/mocktbl/0_1
    // call-5: open - mock:/mocktbl/0_1
    assertEquals(5, readOpsDelta);

    for (FileSystem.Statistics statistics : FileSystem.getAllStatistics()) {
      if (statistics.getScheme().equalsIgnoreCase("mock")) {
        readOpsBefore = statistics.getReadOps();
      }
    }
    orcInputFormat = new OrcInputFormat();
    splits = orcInputFormat.getSplits(conf, 2);
    assertEquals(2, splits.length);
    for (FileSystem.Statistics statistics : FileSystem.getAllStatistics()) {
      if (statistics.getScheme().equalsIgnoreCase("mock")) {
        readOpsDelta = statistics.getReadOps() - readOpsBefore;
      }
    }
    // call-1: listLocatedStatus - mock:/mocktbl
    assertEquals(1, readOpsDelta);

    // revert back to local fs
    conf.set("fs.defaultFS", "file:///");
  }

  @Test
  public void testSplitGenReadOpsLocalCacheChangeFileLen() throws Exception {
    MockFileSystem fs = new MockFileSystem(conf);
    // creates the static cache
    MockPath mockPath = new MockPath(fs, "mock:///mocktbl1");
    conf.set("mapred.input.dir", mockPath.toString());
    conf.set("fs.defaultFS", "mock:///");
    conf.set("fs.mock.impl", MockFileSystem.class.getName());
    StructObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = (StructObjectInspector)
          ObjectInspectorFactory.getReflectionObjectInspector(MyRow.class,
              ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    Writer writer =
        OrcFile.createWriter(new Path(mockPath + "/0_0"),
            OrcFile.writerOptions(conf).blockPadding(false)
                .bufferSize(1024).inspector(inspector));
    for (int i = 0; i < 10; ++i) {
      writer.addRow(new MyRow(i, 2 * i));
    }
    writer.close();

    writer = OrcFile.createWriter(new Path(mockPath + "/0_1"),
        OrcFile.writerOptions(conf).blockPadding(false)
            .bufferSize(1024).inspector(inspector));
    for (int i = 0; i < 10; ++i) {
      writer.addRow(new MyRow(i, 2 * i));
    }
    writer.close();

    int readOpsBefore = -1;
    for (FileSystem.Statistics statistics : FileSystem.getAllStatistics()) {
      if (statistics.getScheme().equalsIgnoreCase("mock")) {
        readOpsBefore = statistics.getReadOps();
      }
    }
    assertTrue("MockFS has stats. Read ops not expected to be -1", readOpsBefore != -1);
    OrcInputFormat orcInputFormat = new OrcInputFormat();
    InputSplit[] splits = orcInputFormat.getSplits(conf, 2);
    assertEquals(2, splits.length);
    int readOpsDelta = -1;
    for (FileSystem.Statistics statistics : FileSystem.getAllStatistics()) {
      if (statistics.getScheme().equalsIgnoreCase("mock")) {
        readOpsDelta = statistics.getReadOps() - readOpsBefore;
      }
    }
    // call-1: listLocatedStatus - mock:/mocktable
    // call-2: check side file for mock:/mocktbl1/0_0
    // call-3: open - mock:/mocktbl1/0_0
    // call-4: check side file for  mock:/mocktbl1/0_1
    // call-5: open - mock:/mocktbl1/0_1
    assertEquals(5, readOpsDelta);

    // change file length and look for cache misses

    fs.clear();

    writer =
        OrcFile.createWriter(new Path(mockPath + "/0_0"),
            OrcFile.writerOptions(conf).blockPadding(false)
                .bufferSize(1024).inspector(inspector));
    for (int i = 0; i < 100; ++i) {
      writer.addRow(new MyRow(i, 2 * i));
    }
    writer.close();

    writer = OrcFile.createWriter(new Path(mockPath + "/0_1"),
        OrcFile.writerOptions(conf).blockPadding(false)
            .bufferSize(1024).inspector(inspector));
    for (int i = 0; i < 100; ++i) {
      writer.addRow(new MyRow(i, 2 * i));
    }
    writer.close();

    for (FileSystem.Statistics statistics : FileSystem.getAllStatistics()) {
      if (statistics.getScheme().equalsIgnoreCase("mock")) {
        readOpsBefore = statistics.getReadOps();
      }
    }
    orcInputFormat = new OrcInputFormat();
    splits = orcInputFormat.getSplits(conf, 2);
    assertEquals(2, splits.length);
    for (FileSystem.Statistics statistics : FileSystem.getAllStatistics()) {
      if (statistics.getScheme().equalsIgnoreCase("mock")) {
        readOpsDelta = statistics.getReadOps() - readOpsBefore;
      }
    }
    // call-1: listLocatedStatus - mock:/mocktable
    // call-2: check side file for mock:/mocktbl1/0_0
    // call-3: open - mock:/mocktbl1/0_0
    // call-4: check side file for  mock:/mocktbl1/0_1
    // call-5: open - mock:/mocktbl1/0_1
    assertEquals(5, readOpsDelta);

    for (FileSystem.Statistics statistics : FileSystem.getAllStatistics()) {
      if (statistics.getScheme().equalsIgnoreCase("mock")) {
        readOpsBefore = statistics.getReadOps();
      }
    }
    orcInputFormat = new OrcInputFormat();
    splits = orcInputFormat.getSplits(conf, 2);
    assertEquals(2, splits.length);
    for (FileSystem.Statistics statistics : FileSystem.getAllStatistics()) {
      if (statistics.getScheme().equalsIgnoreCase("mock")) {
        readOpsDelta = statistics.getReadOps() - readOpsBefore;
      }
    }
    // call-1: listLocatedStatus - mock:/mocktbl1
    assertEquals(1, readOpsDelta);

    // revert back to local fs
    conf.set("fs.defaultFS", "file:///");
  }

  @Test
  public void testSplitGenReadOpsLocalCacheChangeModificationTime() throws Exception {
    MockFileSystem fs = new MockFileSystem(conf);
    // creates the static cache
    MockPath mockPath = new MockPath(fs, "mock:///mocktbl2");
    conf.set("hive.orc.cache.use.soft.references", "true");
    conf.set("mapred.input.dir", mockPath.toString());
    conf.set("fs.defaultFS", "mock:///");
    conf.set("fs.mock.impl", MockFileSystem.class.getName());
    StructObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = (StructObjectInspector)
          ObjectInspectorFactory.getReflectionObjectInspector(MyRow.class,
              ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    Writer writer =
        OrcFile.createWriter(new Path(mockPath + "/0_0"),
            OrcFile.writerOptions(conf).blockPadding(false)
                .bufferSize(1024).inspector(inspector));
    for (int i = 0; i < 10; ++i) {
      writer.addRow(new MyRow(i, 2 * i));
    }
    writer.close();

    writer = OrcFile.createWriter(new Path(mockPath + "/0_1"),
        OrcFile.writerOptions(conf).blockPadding(false)
            .bufferSize(1024).inspector(inspector));
    for (int i = 0; i < 10; ++i) {
      writer.addRow(new MyRow(i, 2 * i));
    }
    writer.close();

    int readOpsBefore = -1;
    for (FileSystem.Statistics statistics : FileSystem.getAllStatistics()) {
      if (statistics.getScheme().equalsIgnoreCase("mock")) {
        readOpsBefore = statistics.getReadOps();
      }
    }
    assertTrue("MockFS has stats. Read ops not expected to be -1", readOpsBefore != -1);
    OrcInputFormat orcInputFormat = new OrcInputFormat();
    InputSplit[] splits = orcInputFormat.getSplits(conf, 2);
    assertEquals(2, splits.length);
    int readOpsDelta = -1;
    for (FileSystem.Statistics statistics : FileSystem.getAllStatistics()) {
      if (statistics.getScheme().equalsIgnoreCase("mock")) {
        readOpsDelta = statistics.getReadOps() - readOpsBefore;
      }
    }
    // call-1: listLocatedStatus - mock:/mocktbl2
    // call-2: check side file for mock:/mocktbl2/0_0
    // call-3: open - mock:/mocktbl2/0_0
    // call-4: check side file for  mock:/mocktbl2/0_1
    // call-5: open - mock:/mocktbl2/0_1
    assertEquals(5, readOpsDelta);

    // change file modification time and look for cache misses
    FileSystem fs1 = FileSystem.get(conf);
    MockFile mockFile = ((MockFileSystem) fs1).findFile(new Path(mockPath + "/0_0"));
    ((MockFileSystem) fs1).touch(mockFile);
    for (FileSystem.Statistics statistics : FileSystem.getAllStatistics()) {
      if (statistics.getScheme().equalsIgnoreCase("mock")) {
        readOpsBefore = statistics.getReadOps();
      }
    }
    orcInputFormat = new OrcInputFormat();
    splits = orcInputFormat.getSplits(conf, 2);
    assertEquals(2, splits.length);
    for (FileSystem.Statistics statistics : FileSystem.getAllStatistics()) {
      if (statistics.getScheme().equalsIgnoreCase("mock")) {
        readOpsDelta = statistics.getReadOps() - readOpsBefore;
      }
    }
    // call-1: listLocatedStatus - mock:/mocktbl2
    // call-2: check side file for  mock:/mocktbl2/0_1
    // call-3: open - mock:/mocktbl2/0_1
    assertEquals(3, readOpsDelta);

    // touch the next file
    fs1 = FileSystem.get(conf);
    mockFile = ((MockFileSystem) fs1).findFile(new Path(mockPath + "/0_1"));
    ((MockFileSystem) fs1).touch(mockFile);
    for (FileSystem.Statistics statistics : FileSystem.getAllStatistics()) {
      if (statistics.getScheme().equalsIgnoreCase("mock")) {
        readOpsBefore = statistics.getReadOps();
      }
    }
    orcInputFormat = new OrcInputFormat();
    splits = orcInputFormat.getSplits(conf, 2);
    assertEquals(2, splits.length);
    for (FileSystem.Statistics statistics : FileSystem.getAllStatistics()) {
      if (statistics.getScheme().equalsIgnoreCase("mock")) {
        readOpsDelta = statistics.getReadOps() - readOpsBefore;
      }
    }
    // call-1: listLocatedStatus - mock:/mocktbl2
    // call-2: check side file for  mock:/mocktbl2/0_0
    // call-3: open - mock:/mocktbl2/0_0
    assertEquals(3, readOpsDelta);

    for (FileSystem.Statistics statistics : FileSystem.getAllStatistics()) {
      if (statistics.getScheme().equalsIgnoreCase("mock")) {
        readOpsBefore = statistics.getReadOps();
      }
    }
    orcInputFormat = new OrcInputFormat();
    splits = orcInputFormat.getSplits(conf, 2);
    assertEquals(2, splits.length);
    for (FileSystem.Statistics statistics : FileSystem.getAllStatistics()) {
      if (statistics.getScheme().equalsIgnoreCase("mock")) {
        readOpsDelta = statistics.getReadOps() - readOpsBefore;
      }
    }
    // call-1: listLocatedStatus - mock:/mocktbl2
    assertEquals(1, readOpsDelta);

    // revert back to local fs
    conf.set("fs.defaultFS", "file:///");
  }

  @Test
  public void testNonVectorReaderNoFooterSerialize() throws Exception {
    MockFileSystem fs = new MockFileSystem(conf);
    MockPath mockPath = new MockPath(fs, "mock:///mocktable1");
    conf.set("hive.orc.splits.include.file.footer", "false");
    conf.set("mapred.input.dir", mockPath.toString());
    conf.set("fs.defaultFS", "mock:///");
    conf.set("fs.mock.impl", MockFileSystem.class.getName());
    StructObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = (StructObjectInspector)
          ObjectInspectorFactory.getReflectionObjectInspector(MyRow.class,
              ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    Writer writer =
        OrcFile.createWriter(new Path(mockPath + "/0_0"),
            OrcFile.writerOptions(conf).blockPadding(false)
                .bufferSize(1024).inspector(inspector));
    for (int i = 0; i < 10; ++i) {
      writer.addRow(new MyRow(i, 2 * i));
    }
    writer.close();

    writer = OrcFile.createWriter(new Path(mockPath + "/0_1"),
        OrcFile.writerOptions(conf).blockPadding(false)
            .bufferSize(1024).inspector(inspector));
    for (int i = 0; i < 10; ++i) {
      writer.addRow(new MyRow(i, 2 * i));
    }
    writer.close();

    OrcInputFormat orcInputFormat = new OrcInputFormat();
    InputSplit[] splits = orcInputFormat.getSplits(conf, 2);
    assertEquals(2, splits.length);
    int readOpsBefore = -1;
    for (FileSystem.Statistics statistics : FileSystem.getAllStatistics()) {
      if (statistics.getScheme().equalsIgnoreCase("mock")) {
        readOpsBefore = statistics.getReadOps();
      }
    }
    assertTrue("MockFS has stats. Read ops not expected to be -1", readOpsBefore != -1);

    for (InputSplit split : splits) {
      assertTrue("OrcSplit is expected", split instanceof OrcSplit);
      // ETL strategies will have start=3 (start of first stripe)
      assertTrue(split.toString().contains("start=3"));
      assertTrue(split.toString().contains("hasFooter=false"));
      assertTrue(split.toString().contains("hasBase=true"));
      assertTrue(split.toString().contains("deltas=0"));
      if (split instanceof OrcSplit) {
        assertFalse("No footer serialize test for non-vector reader, hasFooter is not expected in" +
            " orc splits.", ((OrcSplit) split).hasFooter());
      }
      orcInputFormat.getRecordReader(split, conf, null);
    }

    int readOpsDelta = -1;
    for (FileSystem.Statistics statistics : FileSystem.getAllStatistics()) {
      if (statistics.getScheme().equalsIgnoreCase("mock")) {
        readOpsDelta = statistics.getReadOps() - readOpsBefore;
      }
    }
    // call-1: open to read footer - split 1 => mock:/mocktable1/0_0
    // call-2: open to read data - split 1 => mock:/mocktable1/0_0
    // call-3: open to read footer - split 2 => mock:/mocktable1/0_1
    // call-4: open to read data - split 2 => mock:/mocktable1/0_1
    assertEquals(4, readOpsDelta);

    // revert back to local fs
    conf.set("fs.defaultFS", "file:///");
  }

  @Test
  public void testNonVectorReaderFooterSerialize() throws Exception {
    MockFileSystem fs = new MockFileSystem(conf);
    MockPath mockPath = new MockPath(fs, "mock:///mocktable2");
    conf.set("hive.orc.splits.include.file.footer", "true");
    conf.set("mapred.input.dir", mockPath.toString());
    conf.set("fs.defaultFS", "mock:///");
    conf.set("fs.mock.impl", MockFileSystem.class.getName());
    StructObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = (StructObjectInspector)
          ObjectInspectorFactory.getReflectionObjectInspector(MyRow.class,
              ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    Writer writer =
        OrcFile.createWriter(new Path(mockPath + "/0_0"),
            OrcFile.writerOptions(conf).blockPadding(false)
                .bufferSize(1024).inspector(inspector));
    for (int i = 0; i < 10; ++i) {
      writer.addRow(new MyRow(i, 2 * i));
    }
    writer.close();

    writer = OrcFile.createWriter(new Path(mockPath + "/0_1"),
        OrcFile.writerOptions(conf).blockPadding(false)
            .bufferSize(1024).inspector(inspector));
    for (int i = 0; i < 10; ++i) {
      writer.addRow(new MyRow(i, 2 * i));
    }
    writer.close();

    OrcInputFormat orcInputFormat = new OrcInputFormat();
    InputSplit[] splits = orcInputFormat.getSplits(conf, 2);
    assertEquals(2, splits.length);
    int readOpsBefore = -1;
    for (FileSystem.Statistics statistics : FileSystem.getAllStatistics()) {
      if (statistics.getScheme().equalsIgnoreCase("mock")) {
        readOpsBefore = statistics.getReadOps();
      }
    }
    assertTrue("MockFS has stats. Read ops not expected to be -1", readOpsBefore != -1);

    for (InputSplit split : splits) {
      assertTrue("OrcSplit is expected", split instanceof OrcSplit);
      // ETL strategies will have start=3 (start of first stripe)
      assertTrue(split.toString().contains("start=3"));
      assertTrue(split.toString().contains("hasFooter=true"));
      assertTrue(split.toString().contains("hasBase=true"));
      assertTrue(split.toString().contains("deltas=0"));
      if (split instanceof OrcSplit) {
        assertTrue("Footer serialize test for non-vector reader, hasFooter is expected in" +
            " orc splits.", ((OrcSplit) split).hasFooter());
      }
      orcInputFormat.getRecordReader(split, conf, null);
    }

    int readOpsDelta = -1;
    for (FileSystem.Statistics statistics : FileSystem.getAllStatistics()) {
      if (statistics.getScheme().equalsIgnoreCase("mock")) {
        readOpsDelta = statistics.getReadOps() - readOpsBefore;
      }
    }
    // call-1: open to read data - split 1 => mock:/mocktable2/0_0
    // call-2: open to read data - split 2 => mock:/mocktable2/0_1
    assertEquals(2, readOpsDelta);

    // revert back to local fs
    conf.set("fs.defaultFS", "file:///");
  }

  @Test
  public void testVectorReaderNoFooterSerialize() throws Exception {
    MockFileSystem fs = new MockFileSystem(conf);
    MockPath mockPath = new MockPath(fs, "mock:///mocktable3");
    conf.set("hive.orc.splits.include.file.footer", "false");
    conf.set("mapred.input.dir", mockPath.toString());
    conf.set("fs.defaultFS", "mock:///");
    conf.set("fs.mock.impl", MockFileSystem.class.getName());
    StructObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = (StructObjectInspector)
          ObjectInspectorFactory.getReflectionObjectInspector(MyRow.class,
              ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    JobConf jobConf = createMockExecutionEnvironment(workDir, new Path("mock:///"),
        "mocktable3", inspector, true, 0);
    Writer writer =
        OrcFile.createWriter(new Path(mockPath + "/0_0"),
            OrcFile.writerOptions(conf).blockPadding(false)
                .bufferSize(1024).inspector(inspector));
    for (int i = 0; i < 10; ++i) {
      writer.addRow(new MyRow(i, 2 * i));
    }
    writer.close();

    writer = OrcFile.createWriter(new Path(mockPath + "/0_1"),
        OrcFile.writerOptions(conf).blockPadding(false)
            .bufferSize(1024).inspector(inspector));
    for (int i = 0; i < 10; ++i) {
      writer.addRow(new MyRow(i, 2 * i));
    }
    writer.close();

    OrcInputFormat orcInputFormat = new OrcInputFormat();
    InputSplit[] splits = orcInputFormat.getSplits(conf, 2);
    assertEquals(2, splits.length);

    int readOpsBefore = -1;
    for (FileSystem.Statistics statistics : FileSystem.getAllStatistics()) {
      if (statistics.getScheme().equalsIgnoreCase("mock")) {
        readOpsBefore = statistics.getReadOps();
      }
    }
    assertTrue("MockFS has stats. Read ops not expected to be -1", readOpsBefore != -1);

    for (InputSplit split : splits) {
      assertTrue("OrcSplit is expected", split instanceof OrcSplit);
      // ETL strategies will have start=3 (start of first stripe)
      assertTrue(split.toString().contains("start=3"));
      assertTrue(split.toString().contains("hasFooter=false"));
      assertTrue(split.toString().contains("hasBase=true"));
      assertTrue(split.toString().contains("deltas=0"));
      if (split instanceof OrcSplit) {
        assertFalse("No footer serialize test for vector reader, hasFooter is not expected in" +
            " orc splits.", ((OrcSplit) split).hasFooter());
      }
      orcInputFormat.getRecordReader(split, jobConf, Reporter.NULL);
    }

    int readOpsDelta = -1;
    for (FileSystem.Statistics statistics : FileSystem.getAllStatistics()) {
      if (statistics.getScheme().equalsIgnoreCase("mock")) {
        readOpsDelta = statistics.getReadOps() - readOpsBefore;
      }
    }
    // call-1: open to read footer - split 1 => mock:/mocktable3/0_0
    // call-2: open to read data - split 1 => mock:/mocktable3/0_0
    // call-3: open to read footer - split 2 => mock:/mocktable3/0_1
    // call-4: open to read data - split 2 => mock:/mocktable3/0_1
    assertEquals(4, readOpsDelta);

    // revert back to local fs
    conf.set("fs.defaultFS", "file:///");
  }

  @Test
  public void testVectorReaderFooterSerialize() throws Exception {
    MockFileSystem fs = new MockFileSystem(conf);
    MockPath mockPath = new MockPath(fs, "mock:///mocktable4");
    conf.set("hive.orc.splits.include.file.footer", "true");
    conf.set("mapred.input.dir", mockPath.toString());
    conf.set("fs.defaultFS", "mock:///");
    conf.set("fs.mock.impl", MockFileSystem.class.getName());
    StructObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = (StructObjectInspector)
          ObjectInspectorFactory.getReflectionObjectInspector(MyRow.class,
              ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    JobConf jobConf = createMockExecutionEnvironment(workDir, new Path("mock:///"),
        "mocktable4", inspector, true, 0);
    Writer writer =
        OrcFile.createWriter(new Path(mockPath + "/0_0"),
            OrcFile.writerOptions(conf).blockPadding(false)
                .bufferSize(1024).inspector(inspector));
    for (int i = 0; i < 10; ++i) {
      writer.addRow(new MyRow(i, 2 * i));
    }
    writer.close();

    writer = OrcFile.createWriter(new Path(mockPath + "/0_1"),
        OrcFile.writerOptions(conf).blockPadding(false)
            .bufferSize(1024).inspector(inspector));
    for (int i = 0; i < 10; ++i) {
      writer.addRow(new MyRow(i, 2 * i));
    }
    writer.close();

    OrcInputFormat orcInputFormat = new OrcInputFormat();
    InputSplit[] splits = orcInputFormat.getSplits(conf, 2);
    assertEquals(2, splits.length);

    int readOpsBefore = -1;
    for (FileSystem.Statistics statistics : FileSystem.getAllStatistics()) {
      if (statistics.getScheme().equalsIgnoreCase("mock")) {
        readOpsBefore = statistics.getReadOps();
      }
    }
    assertTrue("MockFS has stats. Read ops not expected to be -1", readOpsBefore != -1);

    for (InputSplit split : splits) {
      assertTrue("OrcSplit is expected", split instanceof OrcSplit);
      // ETL strategies will have start=3 (start of first stripe)
      assertTrue(split.toString().contains("start=3"));
      assertTrue(split.toString().contains("hasFooter=true"));
      assertTrue(split.toString().contains("hasBase=true"));
      assertTrue(split.toString().contains("deltas=0"));
      if (split instanceof OrcSplit) {
        assertTrue("Footer serialize test for vector reader, hasFooter is expected in" +
            " orc splits.", ((OrcSplit) split).hasFooter());
      }
      orcInputFormat.getRecordReader(split, jobConf, Reporter.NULL);
    }

    int readOpsDelta = -1;
    for (FileSystem.Statistics statistics : FileSystem.getAllStatistics()) {
      if (statistics.getScheme().equalsIgnoreCase("mock")) {
        readOpsDelta = statistics.getReadOps() - readOpsBefore;
      }
    }
    // call-1: open to read data - split 1 => mock:/mocktable4/0_0
    // call-2: open to read data - split 2 => mock:/mocktable4/0_1
    assertEquals(2, readOpsDelta);

    // revert back to local fs
    conf.set("fs.defaultFS", "file:///");
  }

  @Test
  public void testACIDReaderNoFooterSerialize() throws Exception {
    MockFileSystem fs = new MockFileSystem(conf);
    MockPath mockPath = new MockPath(fs, "mock:///mocktable5");
    conf.set(ConfVars.HIVE_TRANSACTIONAL_TABLE_SCAN.varname, "true");
    conf.setBoolean(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL, true);
    conf.set(IOConstants.SCHEMA_EVOLUTION_COLUMNS, MyRow.getColumnNamesProperty());
    conf.set(IOConstants.SCHEMA_EVOLUTION_COLUMNS_TYPES, MyRow.getColumnTypesProperty());
    conf.set("hive.orc.splits.include.file.footer", "false");
    conf.set("mapred.input.dir", mockPath.toString());
    conf.set("fs.defaultFS", "mock:///");
    conf.set("fs.mock.impl", MockFileSystem.class.getName());
    StructObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = (StructObjectInspector)
          ObjectInspectorFactory.getReflectionObjectInspector(MyRow.class,
              ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    Writer writer =
        OrcFile.createWriter(new Path(mockPath + "/0_0"),
            OrcFile.writerOptions(conf).blockPadding(false)
                .bufferSize(1024).inspector(inspector));
    for (int i = 0; i < 10; ++i) {
      writer.addRow(new MyRow(i, 2 * i));
    }
    writer.close();

    writer = OrcFile.createWriter(new Path(mockPath + "/0_1"),
        OrcFile.writerOptions(conf).blockPadding(false)
            .bufferSize(1024).inspector(inspector));
    for (int i = 0; i < 10; ++i) {
      writer.addRow(new MyRow(i, 2 * i));
    }
    writer.close();

    OrcInputFormat orcInputFormat = new OrcInputFormat();
    InputSplit[] splits = orcInputFormat.getSplits(conf, 2);
    assertEquals(2, splits.length);
    int readOpsBefore = -1;
    for (FileSystem.Statistics statistics : FileSystem.getAllStatistics()) {
      if (statistics.getScheme().equalsIgnoreCase("mock")) {
        readOpsBefore = statistics.getReadOps();
      }
    }
    assertTrue("MockFS has stats. Read ops not expected to be -1", readOpsBefore != -1);

    for (InputSplit split : splits) {
      assertTrue("OrcSplit is expected", split instanceof OrcSplit);
      // ETL strategies will have start=3 (start of first stripe)
      assertTrue(split.toString().contains("start=3"));
      assertTrue(split.toString().contains("hasFooter=false"));
      assertTrue(split.toString().contains("hasBase=true"));
      assertTrue(split.toString().contains("deltas=0"));
      assertTrue(split.toString().contains("isOriginal=true"));
      if (split instanceof OrcSplit) {
        assertFalse("No footer serialize test for non-vector reader, hasFooter is not expected in" +
            " orc splits.", ((OrcSplit) split).hasFooter());
      }
      orcInputFormat.getRecordReader(split, conf, Reporter.NULL);
    }

    int readOpsDelta = -1;
    for (FileSystem.Statistics statistics : FileSystem.getAllStatistics()) {
      if (statistics.getScheme().equalsIgnoreCase("mock")) {
        readOpsDelta = statistics.getReadOps() - readOpsBefore;
      }
    }
    // call-1: open to read footer - split 1 => mock:/mocktable5/0_0
    // call-2: open to read data - split 1 => mock:/mocktable5/0_0
    // call-3: getAcidState - split 1 => mock:/mocktable5 (to compute offset for original read)
    // call-4: open to read footer - split 2 => mock:/mocktable5/0_1
    // call-5: open to read data - split 2 => mock:/mocktable5/0_1
    // call-6: getAcidState - split 2 => mock:/mocktable5 (to compute offset for original read)
    // call-7: open to read footer - split 2 => mock:/mocktable5/0_0 (to get row count)
    // call-8: file status - split 2 => mock:/mocktable5/0_0
    assertEquals(8, readOpsDelta);

    // revert back to local fs
    conf.set("fs.defaultFS", "file:///");
  }

  @Test
  public void testACIDReaderFooterSerialize() throws Exception {
    MockFileSystem fs = new MockFileSystem(conf);
    MockPath mockPath = new MockPath(fs, "mock:///mocktable6");
    conf.set(ConfVars.HIVE_TRANSACTIONAL_TABLE_SCAN.varname, "true");
    conf.setBoolean(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL, true);
    conf.set(IOConstants.SCHEMA_EVOLUTION_COLUMNS, MyRow.getColumnNamesProperty());
    conf.set(IOConstants.SCHEMA_EVOLUTION_COLUMNS_TYPES, MyRow.getColumnTypesProperty());
    conf.set("hive.orc.splits.include.file.footer", "true");
    conf.set("mapred.input.dir", mockPath.toString());
    conf.set("fs.defaultFS", "mock:///");
    conf.set("fs.mock.impl", MockFileSystem.class.getName());
    StructObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = (StructObjectInspector)
          ObjectInspectorFactory.getReflectionObjectInspector(MyRow.class,
              ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    Writer writer =
        OrcFile.createWriter(new Path(mockPath + "/0_0"),
            OrcFile.writerOptions(conf).blockPadding(false)
                .bufferSize(1024).inspector(inspector));
    for (int i = 0; i < 10; ++i) {
      writer.addRow(new MyRow(i, 2 * i));
    }
    writer.close();

    writer = OrcFile.createWriter(new Path(mockPath + "/0_1"),
        OrcFile.writerOptions(conf).blockPadding(false)
            .bufferSize(1024).inspector(inspector));
    for (int i = 0; i < 10; ++i) {
      writer.addRow(new MyRow(i, 2 * i));
    }
    writer.close();

    OrcInputFormat orcInputFormat = new OrcInputFormat();
    InputSplit[] splits = orcInputFormat.getSplits(conf, 2);
    assertEquals(2, splits.length);
    int readOpsBefore = -1;
    for (FileSystem.Statistics statistics : FileSystem.getAllStatistics()) {
      if (statistics.getScheme().equalsIgnoreCase("mock")) {
        readOpsBefore = statistics.getReadOps();
      }
    }
    assertTrue("MockFS has stats. Read ops not expected to be -1", readOpsBefore != -1);

    for (InputSplit split : splits) {
      assertTrue("OrcSplit is expected", split instanceof OrcSplit);
      // ETL strategies will have start=3 (start of first stripe)
      assertTrue(split.toString().contains("start=3"));
      assertTrue(split.toString().contains("hasFooter=true"));
      assertTrue(split.toString().contains("hasBase=true"));
      assertTrue(split.toString().contains("deltas=0"));
      assertTrue(split.toString().contains("isOriginal=true"));
      if (split instanceof OrcSplit) {
        assertTrue("Footer serialize test for ACID reader, hasFooter is expected in" +
            " orc splits.", ((OrcSplit) split).hasFooter());
      }
      orcInputFormat.getRecordReader(split, conf, Reporter.NULL);
    }

    int readOpsDelta = -1;
    for (FileSystem.Statistics statistics : FileSystem.getAllStatistics()) {
      if (statistics.getScheme().equalsIgnoreCase("mock")) {
        readOpsDelta = statistics.getReadOps() - readOpsBefore;
      }
    }
    // call-1: open to read data - split 1 => mock:/mocktable6/0_0
    // call-2: AcidUtils.getAcidState - split 1 => ls mock:/mocktable6
    // call-3: open to read data - split 2 => mock:/mocktable6/0_1
    // call-4: AcidUtils.getAcidState - split 2 => ls mock:/mocktable6
    // call-5: read footer - split 2 => mock:/mocktable6/0_0 (to get offset since it's original file)
    // call-6: file stat - split 2 => mock:/mocktable6/0_0
    assertEquals(6, readOpsDelta);

    // revert back to local fs
    conf.set("fs.defaultFS", "file:///");
  }

  @Test
  public void testACIDReaderNoFooterSerializeWithDeltas() throws Exception {
    conf.set("fs.defaultFS", "mock:///");
    conf.set("fs.mock.impl", MockFileSystem.class.getName());
    FileSystem fs = FileSystem.get(conf);
    MockPath mockPath = new MockPath(fs, "mock:///mocktable7");
    conf.set(IOConstants.SCHEMA_EVOLUTION_COLUMNS, MyRow.getColumnNamesProperty());
    conf.set(IOConstants.SCHEMA_EVOLUTION_COLUMNS_TYPES, MyRow.getColumnTypesProperty());
    conf.set("hive.orc.splits.include.file.footer", "false");
    conf.set("mapred.input.dir", mockPath.toString());
    StructObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = (StructObjectInspector)
          ObjectInspectorFactory.getReflectionObjectInspector(MyRow.class,
              ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    Writer writer =
        OrcFile.createWriter(new Path(mockPath + "/0_0"),
            OrcFile.writerOptions(conf).blockPadding(false)
                .bufferSize(1024).inspector(inspector));
    for (int i = 0; i < 10; ++i) {
      writer.addRow(new MyRow(i, 2 * i));
    }
    writer.close();

    AcidOutputFormat.Options options = new AcidOutputFormat.Options(conf).bucket(1).minimumWriteId(1)
        .maximumWriteId(1).inspector(inspector).finalDestination(mockPath);
    OrcOutputFormat of = new OrcOutputFormat();
    RecordUpdater ru = of.getRecordUpdater(mockPath, options);
    for (int i = 0; i < 10; ++i) {
      ru.insert(options.getMinimumWriteId(), new MyRow(i, 2 * i));
    }
    ru.close(false);//this deletes the side file

    //set up props for read
    conf.setBoolean(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL, true);
    AcidUtils.setAcidOperationalProperties(conf, true, null);


    OrcInputFormat orcInputFormat = new OrcInputFormat();
    InputSplit[] splits = orcInputFormat.getSplits(conf, 2);
    assertEquals(2, splits.length);
    int readOpsBefore = -1;
    for (FileSystem.Statistics statistics : FileSystem.getAllStatistics()) {
      if (statistics.getScheme().equalsIgnoreCase("mock")) {
        readOpsBefore = statistics.getReadOps();
      }
    }
    assertTrue("MockFS has stats. Read ops not expected to be -1", readOpsBefore != -1);

    for (InputSplit split : splits) {
      assertTrue("OrcSplit is expected", split instanceof OrcSplit);
      // ETL strategies will have start=3 (start of first stripe)
      assertTrue(split.toString().contains("start=3"));
      assertTrue(split.toString().contains("hasFooter=false"));
      assertTrue(split.toString().contains("hasBase=true"));
      assertFalse("No footer serialize test for ACID reader, hasFooter is not expected in" +
        " orc splits.", ((OrcSplit) split).hasFooter());
      orcInputFormat.getRecordReader(split, conf, Reporter.NULL);
    }

    int readOpsDelta = -1;
    for (FileSystem.Statistics statistics : FileSystem.getAllStatistics()) {
      if (statistics.getScheme().equalsIgnoreCase("mock")) {
        readOpsDelta = statistics.getReadOps() - readOpsBefore;
      }
    }
    // call-1: open(mock:/mocktable7/0_0)
    // call-2: open(mock:/mocktable7/0_0)
    // call-3: listLocatedFileStatuses(mock:/mocktable7)
    // call-4: getFileStatus(mock:/mocktable7/delta_0000001_0000001_0000/_metadata_acid)
    // call-5: open(mock:/mocktable7/delta_0000001_0000001_0000/bucket_00001)
    // call-6: getFileStatus(mock:/mocktable7/delta_0000001_0000001_0000/_metadata_acid)
    // call-7: open(mock:/mocktable7/delta_0000001_0000001_0000/bucket_00001)
    assertEquals(7, readOpsDelta);

    // revert back to local fs
    conf.set("fs.defaultFS", "file:///");
  }

  @Test
  public void testACIDReaderFooterSerializeWithDeltas() throws Exception {
    conf.set("fs.defaultFS", "mock:///");
    conf.set("fs.mock.impl", MockFileSystem.class.getName());
    FileSystem fs = FileSystem.get(conf);//ensures that FS object is cached so that everyone uses the same instance
    MockPath mockPath = new MockPath(fs, "mock:///mocktable8");
    conf.set(IOConstants.SCHEMA_EVOLUTION_COLUMNS, MyRow.getColumnNamesProperty());
    conf.set(IOConstants.SCHEMA_EVOLUTION_COLUMNS_TYPES, MyRow.getColumnTypesProperty());
    conf.set("hive.orc.splits.include.file.footer", "true");
    conf.set("mapred.input.dir", mockPath.toString());
    StructObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = (StructObjectInspector)
          ObjectInspectorFactory.getReflectionObjectInspector(MyRow.class,
              ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    Writer writer =
        OrcFile.createWriter(new Path(mockPath + "/0_0"),
            OrcFile.writerOptions(conf).blockPadding(false)
                .bufferSize(1024).inspector(inspector));
    for (int i = 0; i < 10; ++i) {
      writer.addRow(new MyRow(i, 2 * i));
    }
    writer.close();

    AcidOutputFormat.Options options = new AcidOutputFormat.Options(conf).bucket(1).minimumWriteId(1)
        .maximumWriteId(1).inspector(inspector).finalDestination(mockPath);
    OrcOutputFormat of = new OrcOutputFormat();
    RecordUpdater ru = of.getRecordUpdater(mockPath, options);
    for (int i = 0; i < 10; ++i) {
      ru.insert(options.getMinimumWriteId(), new MyRow(i, 2 * i));
    }
    ru.close(false);//this deletes the side file

    //set up props for read
    conf.setBoolean(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL, true);
    AcidUtils.setAcidOperationalProperties(conf, true, null);

    OrcInputFormat orcInputFormat = new OrcInputFormat();
    InputSplit[] splits = orcInputFormat.getSplits(conf, 2);
    assertEquals(2, splits.length);
    int readOpsBefore = -1;
    for (FileSystem.Statistics statistics : FileSystem.getAllStatistics()) {
      if (statistics.getScheme().equalsIgnoreCase("mock")) {
        readOpsBefore = statistics.getReadOps();
      }
    }
    assertTrue("MockFS has stats. Read ops not expected to be -1", readOpsBefore != -1);

    for (InputSplit split : splits) {
      assertTrue("OrcSplit is expected", split instanceof OrcSplit);
      // ETL strategies will have start=3 (start of first stripe)
      assertTrue(split.toString().contains("start=3"));
      assertTrue(split.toString().contains("hasFooter=true"));
      assertTrue(split.toString().contains("hasBase=true"));
      assertTrue("Footer serialize test for ACID reader, hasFooter is not expected in" +
        " orc splits.", ((OrcSplit) split).hasFooter());
      orcInputFormat.getRecordReader(split, conf, Reporter.NULL);
    }

    int readOpsDelta = -1;
    for (FileSystem.Statistics statistics : FileSystem.getAllStatistics()) {
      if (statistics.getScheme().equalsIgnoreCase("mock")) {
        readOpsDelta = statistics.getReadOps() - readOpsBefore;
      }
    }
    // call-1: open to read data - split 1 => mock:/mocktable8/0_0
    // call-2: listLocatedFileStatus(mock:/mocktable8)
    // call-3: getFileStatus(mock:/mocktable8/delta_0000001_0000001_0000/_metadata_acid)
    // call-4: getFileStatus(mock:/mocktable8/delta_0000001_0000001_0000/_metadata_acid)
    // call-5: open(mock:/mocktable8/delta_0000001_0000001_0000/bucket_00001)
    assertEquals(5, readOpsDelta);

    // revert back to local fs
    conf.set("fs.defaultFS", "file:///");
  }

  /**
   * also see {@link TestOrcFile#testPredicatePushdown()}
   * This tests that {@link RecordReader#getRowNumber()} works with multiple splits
   * @throws Exception
   */
  @Test
  public void testRowNumberUniquenessInDifferentSplits() throws Exception {
    Properties properties = new Properties();
    properties.setProperty("columns", "x,y");
    properties.setProperty("columns.types", "int:int");
    StructObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = (StructObjectInspector)
        ObjectInspectorFactory.getReflectionObjectInspector(MyRow.class,
          ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }

    // Save the conf variable values so that they can be restored later.
    long oldDefaultStripeSize = conf.getLong(OrcConf.STRIPE_SIZE.getHiveConfName(), -1L);
    long oldMaxSplitSize = conf.getLong(HiveConf.ConfVars.MAPREDMAXSPLITSIZE.varname, -1L);

    // Set the conf variable values for this test.
    long newStripeSize = 10000L; // 10000 bytes per stripe
    long newMaxSplitSize = 100L; // 1024 bytes per split
    conf.setLong(OrcConf.STRIPE_SIZE.getHiveConfName(), newStripeSize);
    conf.setLong(HiveConf.ConfVars.MAPREDMAXSPLITSIZE.varname, newMaxSplitSize);

    AbstractSerDe serde = new OrcSerde();
    HiveOutputFormat<?, ?> outFormat = new OrcOutputFormat();
    org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter writer =
      outFormat.getHiveRecordWriter(conf, testFilePath, MyRow.class, true,
        properties, Reporter.NULL);
    // The following loop should create 20 stripes in the orc file.
    for (int i = 0; i < newStripeSize * 10; ++i) {
      writer.write(serde.serialize(new MyRow(i,i+1), inspector));
    }
    writer.close(true);
    serde = new OrcSerde();
    SerDeUtils.initializeSerDe(serde, conf, properties, null);
    assertEquals(OrcSerde.OrcSerdeRow.class, serde.getSerializedClass());
    inspector = (StructObjectInspector) serde.getObjectInspector();
    assertEquals("struct<x:int,y:int>", inspector.getTypeName());
    InputFormat<?,?> in = new OrcInputFormat();
    FileInputFormat.setInputPaths(conf, testFilePath.toString());
    int numExpectedSplits = 20;
    InputSplit[] splits = in.getSplits(conf, numExpectedSplits);
    assertEquals(numExpectedSplits, splits.length);

    for (int i = 0; i < numExpectedSplits; ++i) {
      OrcSplit split = (OrcSplit) splits[i];
      Reader.Options orcReaderOptions = new Reader.Options();
      orcReaderOptions.range(split.getStart(), split.getLength());
      OrcFile.ReaderOptions qlReaderOptions = OrcFile.readerOptions(conf).maxLength(split.getFileLength());
      Reader reader = OrcFile.createReader(split.getPath(), qlReaderOptions);
      RecordReader recordReader = reader.rowsOptions(orcReaderOptions);
      for(int j = 0; recordReader.hasNext(); j++) {
        long rowNum = (i * 5000) + j;
        long rowNumActual = recordReader.getRowNumber();
        assertEquals("rowNum=" + rowNum, rowNum, rowNumActual);
        Object row = recordReader.next(null);
      }
      recordReader.close();
    }

    // Reset the conf variable values that we changed for this test.
    if (oldDefaultStripeSize != -1L) {
      conf.setLong(OrcConf.STRIPE_SIZE.getHiveConfName(), oldDefaultStripeSize);
    } else {
      // this means that nothing was set for default stripe size previously, so we should unset it.
      conf.unset(OrcConf.STRIPE_SIZE.getHiveConfName());
    }
    if (oldMaxSplitSize != -1L) {
      conf.setLong(HiveConf.ConfVars.MAPREDMAXSPLITSIZE.varname, oldMaxSplitSize);
    } else {
      // this means that nothing was set for default stripe size previously, so we should unset it.
      conf.unset(HiveConf.ConfVars.MAPREDMAXSPLITSIZE.varname);
    }
  }

  /**
   * Test schema evolution when using the reader directly.
   */
  @Test
  public void testSchemaEvolutionOldDecimal() throws Exception {
    TypeDescription fileSchema =
        TypeDescription.fromString("struct<a:int,b:struct<c:int>,d:string>");
    conf.set(ConfVars.HIVE_VECTORIZED_INPUT_FORMAT_SUPPORTS_ENABLED.varname, "decimal_64");
    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .fileSystem(fs)
            .setSchema(fileSchema)
            .compress(org.apache.orc.CompressionKind.NONE));
    VectorizedRowBatch batch = fileSchema.createRowBatch(1000);
    batch.size = 1000;
    LongColumnVector lcv = ((LongColumnVector) ((StructColumnVector) batch.cols[1]).fields[0]);
    for(int r=0; r < 1000; r++) {
      ((LongColumnVector) batch.cols[0]).vector[r] = r * 42;
      lcv.vector[r] = r * 10001;
      ((BytesColumnVector) batch.cols[2]).setVal(r,
          Integer.toHexString(r).getBytes(StandardCharsets.UTF_8));
    }
    writer.addRowBatch(batch);
    writer.close();
    TypeDescription readerSchema = TypeDescription.fromString(
        "struct<a:int,b:struct<c:int,future1:int>,d:string,future2:int>");
    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rowsOptions(new Reader.Options()
        .schema(readerSchema));
    batch = readerSchema.createRowBatch();
    lcv = ((LongColumnVector) ((StructColumnVector) batch.cols[1]).fields[0]);
    LongColumnVector future1 = ((LongColumnVector) ((StructColumnVector) batch.cols[1]).fields[1]);
    assertEquals(true, rows.nextBatch(batch));
    assertEquals(1000, batch.size);
    assertEquals(true, future1.isRepeating);
    assertEquals(true, future1.isNull[0]);
    assertEquals(true, batch.cols[3].isRepeating);
    assertEquals(true, batch.cols[3].isNull[0]);
    for(int r=0; r < batch.size; ++r) {
      assertEquals("row " + r, r * 42, ((LongColumnVector) batch.cols[0]).vector[r]);
      assertEquals("row " + r, r * 10001, lcv.vector[r]);
      assertEquals("row " + r, r * 10001, lcv.vector[r]);
      assertEquals("row " + r, Integer.toHexString(r),
          ((BytesColumnVector) batch.cols[2]).toString(r));
    }
    assertEquals(false, rows.nextBatch(batch));
    rows.close();

    // try it again with an include vector
    rows = reader.rowsOptions(new Reader.Options()
        .schema(readerSchema)
        .include(new boolean[]{false, true, true, true, false, false, true}));
    batch = readerSchema.createRowBatch();
    lcv = ((LongColumnVector) ((StructColumnVector) batch.cols[1]).fields[0]);
    future1 = ((LongColumnVector) ((StructColumnVector) batch.cols[1]).fields[1]);
    assertEquals(true, rows.nextBatch(batch));
    assertEquals(1000, batch.size);
    assertEquals(true, future1.isRepeating);
    assertEquals(true, future1.isNull[0]);
    assertEquals(true, batch.cols[3].isRepeating);
    assertEquals(true, batch.cols[3].isNull[0]);
    assertEquals(true, batch.cols[2].isRepeating);
    assertEquals(true, batch.cols[2].isNull[0]);
    for(int r=0; r < batch.size; ++r) {
      assertEquals("row " + r, r * 42, ((LongColumnVector) batch.cols[0]).vector[r]);
      assertEquals("row " + r, r * 10001, lcv.vector[r]);
    }
    assertEquals(false, rows.nextBatch(batch));
    rows.close();
  }

  /**
   * Test schema evolution when using the reader directly.
   */
  @Test
  public void testSchemaEvolutionDecimal64() throws Exception {
    TypeDescription fileSchema =
      TypeDescription.fromString("struct<a:int,b:struct<c:int>,d:string>");
    conf.set(ConfVars.HIVE_VECTORIZED_INPUT_FORMAT_SUPPORTS_ENABLED.varname, "decimal_64");
    Writer writer = OrcFile.createWriter(testFilePath,
      OrcFile.writerOptions(conf)
        .fileSystem(fs)
        .setSchema(fileSchema)
        .compress(org.apache.orc.CompressionKind.NONE));
    VectorizedRowBatch batch = fileSchema.createRowBatch(TypeDescription.RowBatchVersion.USE_DECIMAL64,1000);
    batch.size = 1000;
    LongColumnVector lcv = ((LongColumnVector) ((StructColumnVector) batch.cols[1]).fields[0]);
    for(int r=0; r < 1000; r++) {
      ((LongColumnVector) batch.cols[0]).vector[r] = r * 42;
      lcv.vector[r] = r * 10001;
      ((BytesColumnVector) batch.cols[2]).setVal(r,
        Integer.toHexString(r).getBytes(StandardCharsets.UTF_8));
    }
    writer.addRowBatch(batch);
    writer.close();
    TypeDescription readerSchema = TypeDescription.fromString(
      "struct<a:int,b:struct<c:int,future1:int>,d:string,future2:int>");
    Reader reader = OrcFile.createReader(testFilePath,
      OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rowsOptions(new Reader.Options()
      .schema(readerSchema));
    batch = readerSchema.createRowBatchV2();
    lcv = ((LongColumnVector) ((StructColumnVector) batch.cols[1]).fields[0]);
    LongColumnVector future1 = ((LongColumnVector) ((StructColumnVector) batch.cols[1]).fields[1]);
    assertEquals(true, rows.nextBatch(batch));
    assertEquals(1000, batch.size);
    assertEquals(true, future1.isRepeating);
    assertEquals(true, future1.isNull[0]);
    assertEquals(true, batch.cols[3].isRepeating);
    assertEquals(true, batch.cols[3].isNull[0]);
    for(int r=0; r < batch.size; ++r) {
      assertEquals("row " + r, r * 42, ((LongColumnVector) batch.cols[0]).vector[r]);
      assertEquals("row " + r, r * 10001, lcv.vector[r]);
      assertEquals("row " + r, r * 10001, lcv.vector[r]);
      assertEquals("row " + r, Integer.toHexString(r),
        ((BytesColumnVector) batch.cols[2]).toString(r));
    }
    assertEquals(false, rows.nextBatch(batch));
    rows.close();

    // try it again with an include vector
    rows = reader.rowsOptions(new Reader.Options()
      .schema(readerSchema)
      .include(new boolean[]{false, true, true, true, false, false, true}));
    batch = readerSchema.createRowBatchV2();
    lcv = ((LongColumnVector) ((StructColumnVector) batch.cols[1]).fields[0]);
    future1 = ((LongColumnVector) ((StructColumnVector) batch.cols[1]).fields[1]);
    assertEquals(true, rows.nextBatch(batch));
    assertEquals(1000, batch.size);
    assertEquals(true, future1.isRepeating);
    assertEquals(true, future1.isNull[0]);
    assertEquals(true, batch.cols[3].isRepeating);
    assertEquals(true, batch.cols[3].isNull[0]);
    assertEquals(true, batch.cols[2].isRepeating);
    assertEquals(true, batch.cols[2].isNull[0]);
    for(int r=0; r < batch.size; ++r) {
      assertEquals("row " + r, r * 42, ((LongColumnVector) batch.cols[0]).vector[r]);
      assertEquals("row " + r, r * 10001, lcv.vector[r]);
    }
    assertEquals(false, rows.nextBatch(batch));
    rows.close();
  }

  /**
   * Test column projection when using ACID.
   */
  @Test
  public void testColumnProjectionWithAcid() throws Exception {
    Path baseDir = new Path(workDir, "base_00100");
    testFilePath = new Path(baseDir, "bucket_00000");
    fs.mkdirs(baseDir);
    fs.delete(testFilePath, true);
    TypeDescription fileSchema =
        TypeDescription.fromString("struct<operation:int," +
            "originalTransaction:bigint,bucket:int,rowId:bigint," +
            "currentTransaction:bigint," +
            "row:struct<a:int,b:struct<c:int>,d:string>>");
    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .fileSystem(fs)
            .setSchema(fileSchema)
            .compress(org.apache.orc.CompressionKind.NONE));
    VectorizedRowBatch batch = fileSchema.createRowBatch(TypeDescription.RowBatchVersion.USE_DECIMAL64,1000);
    batch.size = 1000;
    StructColumnVector scv = (StructColumnVector)batch.cols[5];
    // operation
    batch.cols[0].isRepeating = true;
    ((LongColumnVector) batch.cols[0]).vector[0] = 0;
    // original transaction
    batch.cols[1].isRepeating = true;
    ((LongColumnVector) batch.cols[1]).vector[0] = 1;
    // bucket
    batch.cols[2].isRepeating = true;
    ((LongColumnVector) batch.cols[2]).vector[0] = 0;
    // current transaction
    batch.cols[4].isRepeating = true;
    ((LongColumnVector) batch.cols[4]).vector[0] = 1;

    LongColumnVector lcv = (LongColumnVector)
        ((StructColumnVector) scv.fields[1]).fields[0];
    for(int r=0; r < 1000; r++) {
      // row id
      ((LongColumnVector) batch.cols[3]).vector[r] = r;
      // a
      ((LongColumnVector) scv.fields[0]).vector[r] = r * 42;
      // b.c
      lcv.vector[r] = r * 10001;
      // d
      ((BytesColumnVector) scv.fields[2]).setVal(r,
          Integer.toHexString(r).getBytes(StandardCharsets.UTF_8));
    }
    writer.addRowBatch(batch);
    writer.addUserMetadata(OrcRecordUpdater.ACID_KEY_INDEX_NAME,
        ByteBuffer.wrap("0,0,999".getBytes(StandardCharsets.UTF_8)));
    writer.close();
    long fileLength = fs.getFileStatus(testFilePath).getLen();

    // test with same schema with include
    conf.set(ValidWriteIdList.VALID_WRITEIDS_KEY, "tbl:100:99:");
    conf.set(IOConstants.SCHEMA_EVOLUTION_COLUMNS, "a,b,d");
    conf.set(IOConstants.SCHEMA_EVOLUTION_COLUMNS_TYPES, "int,struct<c:int>,string");
    conf.set(ColumnProjectionUtils.READ_ALL_COLUMNS, "false");
    conf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, "0,2");
    OrcSplit split = new OrcSplit(testFilePath, null, 0, fileLength,
        new String[0], null, false, true,
        new ArrayList<AcidInputFormat.DeltaMetaData>(), fileLength, fileLength, workDir);
    OrcInputFormat inputFormat = new OrcInputFormat();
    AcidInputFormat.RowReader<OrcStruct> reader = inputFormat.getReader(split,
        new AcidInputFormat.Options(conf));
    int record = 0;
    RecordIdentifier id = reader.createKey();
    OrcStruct struct = reader.createValue();
    while (reader.next(id, struct)) {
      assertEquals("id " + record, record, id.getRowId());
      assertEquals("bucket " + record, 0, id.getBucketProperty());
      assertEquals("writeid " + record, 1, id.getWriteId());
      assertEquals("a " + record,
          42 * record, ((IntWritable) struct.getFieldValue(0)).get());
      assertEquals(null, struct.getFieldValue(1));
      assertEquals("d " + record,
          Integer.toHexString(record), struct.getFieldValue(2).toString());
      record += 1;
    }
    assertEquals(1000, record);
    reader.close();

    // test with schema evolution and include
    conf.set(IOConstants.SCHEMA_EVOLUTION_COLUMNS, "a,b,d,f");
    conf.set(IOConstants.SCHEMA_EVOLUTION_COLUMNS_TYPES, "int,struct<c:int,e:string>,string,int");
    conf.set(ColumnProjectionUtils.READ_ALL_COLUMNS, "false");
    conf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, "0,2,3");
    split = new OrcSplit(testFilePath, null, 0, fileLength,
        new String[0], null, false, true,
        new ArrayList<AcidInputFormat.DeltaMetaData>(), fileLength, fileLength, workDir);
    inputFormat = new OrcInputFormat();
    reader = inputFormat.getReader(split, new AcidInputFormat.Options(conf));
    record = 0;
    id = reader.createKey();
    struct = reader.createValue();
    while (reader.next(id, struct)) {
      assertEquals("id " + record, record, id.getRowId());
      assertEquals("bucket " + record, 0, id.getBucketProperty());
      assertEquals("writeid " + record, 1, id.getWriteId());
      assertEquals("a " + record,
          42 * record, ((IntWritable) struct.getFieldValue(0)).get());
      assertEquals(null, struct.getFieldValue(1));
      assertEquals("d " + record,
          Integer.toHexString(record), struct.getFieldValue(2).toString());
      assertEquals("f " + record, null, struct.getFieldValue(3));
      record += 1;
    }
    assertEquals(1000, record);
    reader.close();
  }

  @Test
  public void testAcidReadPastLastStripeOffset() throws Exception {
    Path baseDir = new Path(workDir, "base_00100");
    testFilePath = new Path(baseDir, "bucket_00000");
    fs.mkdirs(baseDir);
    fs.delete(testFilePath, true);
    TypeDescription fileSchema =
        TypeDescription.fromString("struct<operation:int," +
            "originalTransaction:bigint,bucket:int,rowId:bigint," +
            "currentTransaction:bigint," +
            "row:struct<a:int,b:struct<c:int>,d:string>>");

    OrcRecordUpdater.KeyIndexBuilder indexBuilder = new OrcRecordUpdater.KeyIndexBuilder("test");
    OrcFile.WriterOptions options = OrcFile.writerOptions(conf)
        .fileSystem(fs)
        .setSchema(fileSchema)
        .compress(org.apache.orc.CompressionKind.NONE)
        .callback(indexBuilder)
        .stripeSize(128);
    // Create ORC file with small stripe size so we can write multiple stripes.
    Writer writer = OrcFile.createWriter(testFilePath, options);
    VectorizedRowBatch batch = fileSchema.createRowBatch(TypeDescription.RowBatchVersion.USE_DECIMAL64,1000);
    batch.size = 1000;
    StructColumnVector scv = (StructColumnVector)batch.cols[5];
    // operation
    batch.cols[0].isRepeating = true;
    ((LongColumnVector) batch.cols[0]).vector[0] = OrcRecordUpdater.INSERT_OPERATION;
    // original transaction
    batch.cols[1].isRepeating = true;
    ((LongColumnVector) batch.cols[1]).vector[0] = 1;
    // bucket
    batch.cols[2].isRepeating = true;
    ((LongColumnVector) batch.cols[2]).vector[0] = BucketCodec.V1.encode(new AcidOutputFormat
        .Options(conf).bucket(0).statementId(0));
    // current transaction
    batch.cols[4].isRepeating = true;
    ((LongColumnVector) batch.cols[4]).vector[0] = 1;

    LongColumnVector lcv = (LongColumnVector)
        ((StructColumnVector) scv.fields[1]).fields[0];
    for(int r=0; r < 1000; r++) {
      // row id
      ((LongColumnVector) batch.cols[3]).vector[r] = r;
      // a
      ((LongColumnVector) scv.fields[0]).vector[r] = r * 42;
      // b.c
      lcv.vector[r] = r * 10001;
      // d
      ((BytesColumnVector) scv.fields[2]).setVal(r,
          Integer.toHexString(r).getBytes(StandardCharsets.UTF_8));
      indexBuilder.addKey(OrcRecordUpdater.INSERT_OPERATION,
          1, (int)(((LongColumnVector) batch.cols[2]).vector[0]), r);
    }

    // Minimum 5000 rows per stripe.
    for (int idx = 0; idx < 8; ++idx) {
      writer.addRowBatch(batch);
      // bucket
      batch.cols[2].isRepeating = true;
      ((LongColumnVector) batch.cols[2]).vector[0] = BucketCodec.V1.encode(new AcidOutputFormat
          .Options(conf).bucket(0).statementId(idx + 1));
      for(long row_id : ((LongColumnVector) batch.cols[3]).vector) {
        indexBuilder.addKey(OrcRecordUpdater.INSERT_OPERATION,
            1, (int)(((LongColumnVector) batch.cols[2]).vector[0]), row_id);
      }
    }
    writer.close();
    long fileLength = fs.getFileStatus(testFilePath).getLen();

    // Find the last stripe.
    Reader orcReader = OrcFile.createReader(fs, testFilePath);
    List<StripeInformation> stripes = orcReader.getStripes();
    StripeInformation lastStripe = stripes.get(stripes.size() - 1);
    long lastStripeOffset = lastStripe.getOffset();
    long lastStripeLength = lastStripe.getLength();

    RecordIdentifier[] keyIndex = OrcRecordUpdater.parseKeyIndex(orcReader);
    Assert.assertEquals("Index length doesn't match number of stripes",
        stripes.size(), keyIndex.length);
    Assert.assertEquals("1st Index entry mismatch",
        new RecordIdentifier(1, 536870916, 999), keyIndex[0]);
    Assert.assertEquals("2nd Index entry mismatch",
        new RecordIdentifier(1, 536870920, 999), keyIndex[1]);

    // test with same schema with include
    conf.set(ValidTxnList.VALID_TXNS_KEY, "100:99:");
    conf.set(IOConstants.SCHEMA_EVOLUTION_COLUMNS, "a,b,d");
    conf.set(IOConstants.SCHEMA_EVOLUTION_COLUMNS_TYPES, "int,struct<c:int>,string");
    conf.set(ColumnProjectionUtils.READ_ALL_COLUMNS, "false");
    conf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, "0,2");

    LOG.info("Last stripe " + stripes.size() +
        ", offset " + lastStripeOffset + ", length " + lastStripeLength);
    // Specify an OrcSplit that starts beyond the offset of the last stripe.
    OrcSplit split = new OrcSplit(testFilePath, null, lastStripeOffset + 1, lastStripeLength,
        new String[0], null, false, true,
        new ArrayList<AcidInputFormat.DeltaMetaData>(), fileLength, fileLength, workDir);
    OrcInputFormat inputFormat = new OrcInputFormat();
    AcidInputFormat.RowReader<OrcStruct> reader = inputFormat.getReader(split,
        new AcidInputFormat.Options(conf));

    int record = 0;
    RecordIdentifier id = reader.createKey();
    OrcStruct struct = reader.createValue();
    // Iterate through any records.
    // Because our read offset was past the stripe offset, the rows from the last stripe will
    // not be read. Thus 0 records.
    while (reader.next(id, struct)) {
      record += 1;
    }
    assertEquals(0, record);

    reader.close();
  }
}
