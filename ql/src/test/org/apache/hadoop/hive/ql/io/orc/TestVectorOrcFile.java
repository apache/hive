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

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampUtils;
import org.apache.hadoop.hive.ql.exec.vector.UnionColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hive.common.util.HiveTestUtils;
import org.apache.orc.BinaryColumnStatistics;
import org.apache.orc.BooleanColumnStatistics;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.CompressionKind;
import org.apache.orc.DecimalColumnStatistics;
import org.apache.orc.DoubleColumnStatistics;
import org.apache.orc.IntegerColumnStatistics;
import org.apache.orc.impl.MemoryManager;
import org.apache.orc.impl.MetadataReader;
import org.apache.orc.impl.OrcIndex;
import org.apache.orc.OrcProto;
import org.apache.orc.OrcUtils;
import org.apache.orc.StringColumnStatistics;
import org.apache.orc.StripeInformation;
import org.apache.orc.StripeStatistics;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.assertTrue;

/**
 * Tests for the vectorized reader and writer for ORC files.
 */
public class TestVectorOrcFile {

  public static class InnerStruct {
    int int1;
    Text string1 = new Text();
    InnerStruct(int int1, String string1) {
      this.int1 = int1;
      this.string1.set(string1);
    }

    public String toString() {
      return "{" + int1 + ", " + string1 + "}";
    }
  }

  public static class MiddleStruct {
    List<InnerStruct> list = new ArrayList<InnerStruct>();

    MiddleStruct(InnerStruct... items) {
      list.clear();
      list.addAll(Arrays.asList(items));
    }
  }

  public static class BigRow {
    Boolean boolean1;
    Byte byte1;
    Short short1;
    Integer int1;
    Long long1;
    Float float1;
    Double double1;
    BytesWritable bytes1;
    Text string1;
    MiddleStruct middle;
    List<InnerStruct> list = new ArrayList<InnerStruct>();
    Map<Text, InnerStruct> map = new HashMap<Text, InnerStruct>();

    BigRow(Boolean b1, Byte b2, Short s1, Integer i1, Long l1, Float f1,
           Double d1,
           BytesWritable b3, String s2, MiddleStruct m1,
           List<InnerStruct> l2, Map<String, InnerStruct> m2) {
      this.boolean1 = b1;
      this.byte1 = b2;
      this.short1 = s1;
      this.int1 = i1;
      this.long1 = l1;
      this.float1 = f1;
      this.double1 = d1;
      this.bytes1 = b3;
      if (s2 == null) {
        this.string1 = null;
      } else {
        this.string1 = new Text(s2);
      }
      this.middle = m1;
      this.list = l2;
      if (m2 != null) {
        this.map = new HashMap<Text, InnerStruct>();
        for (Map.Entry<String, InnerStruct> item : m2.entrySet()) {
          this.map.put(new Text(item.getKey()), item.getValue());
        }
      } else {
        this.map = null;
      }
    }
  }

  private static InnerStruct inner(int i, String s) {
    return new InnerStruct(i, s);
  }

  private static Map<String, InnerStruct> map(InnerStruct... items)  {
    Map<String, InnerStruct> result = new HashMap<String, InnerStruct>();
    for(InnerStruct i: items) {
      result.put(i.string1.toString(), i);
    }
    return result;
  }

  private static List<InnerStruct> list(InnerStruct... items) {
    List<InnerStruct> result = new ArrayList<InnerStruct>();
    result.addAll(Arrays.asList(items));
    return result;
  }

  private static BytesWritable bytes(int... items) {
    BytesWritable result = new BytesWritable();
    result.setSize(items.length);
    for(int i=0; i < items.length; ++i) {
      result.getBytes()[i] = (byte) items[i];
    }
    return result;
  }

  private static byte[] bytesArray(int... items) {
    byte[] result = new byte[items.length];
    for(int i=0; i < items.length; ++i) {
      result[i] = (byte) items[i];
    }
    return result;
  }

  private static ByteBuffer byteBuf(int... items) {
    ByteBuffer result = ByteBuffer.allocate(items.length);
    for(int item: items) {
      result.put((byte) item);
    }
    result.flip();
    return result;
  }

  Path workDir = new Path(System.getProperty("test.tmp.dir",
      "target" + File.separator + "test" + File.separator + "tmp"));

  Configuration conf;
  FileSystem fs;
  Path testFilePath;

  @Rule
  public TestName testCaseName = new TestName();

  @Before
  public void openFileSystem () throws Exception {
    conf = new Configuration();
    fs = FileSystem.getLocal(conf);
    testFilePath = new Path(workDir, "TestVectorOrcFile." +
        testCaseName.getMethodName() + ".orc");
    fs.delete(testFilePath, false);
  }

  @Test
  public void testReadFormat_0_11() throws Exception {
    Path oldFilePath =
        new Path(HiveTestUtils.getFileFromClasspath("orc-file-11-format.orc"));
    Reader reader = OrcFile.createReader(oldFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));

    int stripeCount = 0;
    int rowCount = 0;
    long currentOffset = -1;
    for(StripeInformation stripe : reader.getStripes()) {
      stripeCount += 1;
      rowCount += stripe.getNumberOfRows();
      if (currentOffset < 0) {
        currentOffset = stripe.getOffset() + stripe.getIndexLength()
            + stripe.getDataLength() + stripe.getFooterLength();
      } else {
        assertEquals(currentOffset, stripe.getOffset());
        currentOffset += stripe.getIndexLength() + stripe.getDataLength()
            + stripe.getFooterLength();
      }
    }
    assertEquals(reader.getNumberOfRows(), rowCount);
    assertEquals(2, stripeCount);

    // check the stats
    ColumnStatistics[] stats = reader.getStatistics();
    assertEquals(7500, stats[1].getNumberOfValues());
    assertEquals(3750, ((BooleanColumnStatistics) stats[1]).getFalseCount());
    assertEquals(3750, ((BooleanColumnStatistics) stats[1]).getTrueCount());
    assertEquals("count: 7500 hasNull: true true: 3750", stats[1].toString());

    assertEquals(2048, ((IntegerColumnStatistics) stats[3]).getMaximum());
    assertEquals(1024, ((IntegerColumnStatistics) stats[3]).getMinimum());
    assertEquals(true, ((IntegerColumnStatistics) stats[3]).isSumDefined());
    assertEquals(11520000, ((IntegerColumnStatistics) stats[3]).getSum());
    assertEquals("count: 7500 hasNull: true min: 1024 max: 2048 sum: 11520000",
        stats[3].toString());

    assertEquals(Long.MAX_VALUE,
        ((IntegerColumnStatistics) stats[5]).getMaximum());
    assertEquals(Long.MAX_VALUE,
        ((IntegerColumnStatistics) stats[5]).getMinimum());
    assertEquals(false, ((IntegerColumnStatistics) stats[5]).isSumDefined());
    assertEquals(
        "count: 7500 hasNull: true min: 9223372036854775807 max: 9223372036854775807",
        stats[5].toString());

    assertEquals(-15.0, ((DoubleColumnStatistics) stats[7]).getMinimum());
    assertEquals(-5.0, ((DoubleColumnStatistics) stats[7]).getMaximum());
    assertEquals(-75000.0, ((DoubleColumnStatistics) stats[7]).getSum(),
        0.00001);
    assertEquals("count: 7500 hasNull: true min: -15.0 max: -5.0 sum: -75000.0",
        stats[7].toString());

    assertEquals("count: 7500 hasNull: true min: bye max: hi sum: 0", stats[9].toString());

    // check the inspectors
    StructObjectInspector readerInspector = (StructObjectInspector) reader
        .getObjectInspector();
    assertEquals(ObjectInspector.Category.STRUCT, readerInspector.getCategory());
    assertEquals("struct<boolean1:boolean,byte1:tinyint,short1:smallint,"
        + "int1:int,long1:bigint,float1:float,double1:double,bytes1:"
        + "binary,string1:string,middle:struct<list:array<struct<int1:int,"
        + "string1:string>>>,list:array<struct<int1:int,string1:string>>,"
        + "map:map<string,struct<int1:int,string1:string>>,ts:timestamp,"
        + "decimal1:decimal(38,18)>", readerInspector.getTypeName());
    List<? extends StructField> fields = readerInspector
        .getAllStructFieldRefs();
    BooleanObjectInspector bo = (BooleanObjectInspector) readerInspector
        .getStructFieldRef("boolean1").getFieldObjectInspector();
    ByteObjectInspector by = (ByteObjectInspector) readerInspector
        .getStructFieldRef("byte1").getFieldObjectInspector();
    ShortObjectInspector sh = (ShortObjectInspector) readerInspector
        .getStructFieldRef("short1").getFieldObjectInspector();
    IntObjectInspector in = (IntObjectInspector) readerInspector
        .getStructFieldRef("int1").getFieldObjectInspector();
    LongObjectInspector lo = (LongObjectInspector) readerInspector
        .getStructFieldRef("long1").getFieldObjectInspector();
    FloatObjectInspector fl = (FloatObjectInspector) readerInspector
        .getStructFieldRef("float1").getFieldObjectInspector();
    DoubleObjectInspector dbl = (DoubleObjectInspector) readerInspector
        .getStructFieldRef("double1").getFieldObjectInspector();
    BinaryObjectInspector bi = (BinaryObjectInspector) readerInspector
        .getStructFieldRef("bytes1").getFieldObjectInspector();
    StringObjectInspector st = (StringObjectInspector) readerInspector
        .getStructFieldRef("string1").getFieldObjectInspector();
    StructObjectInspector mid = (StructObjectInspector) readerInspector
        .getStructFieldRef("middle").getFieldObjectInspector();
    List<? extends StructField> midFields = mid.getAllStructFieldRefs();
    ListObjectInspector midli = (ListObjectInspector) midFields.get(0)
        .getFieldObjectInspector();
    StructObjectInspector inner = (StructObjectInspector) midli
        .getListElementObjectInspector();
    List<? extends StructField> inFields = inner.getAllStructFieldRefs();
    ListObjectInspector li = (ListObjectInspector) readerInspector
        .getStructFieldRef("list").getFieldObjectInspector();
    MapObjectInspector ma = (MapObjectInspector) readerInspector
        .getStructFieldRef("map").getFieldObjectInspector();
    TimestampObjectInspector tso = (TimestampObjectInspector) readerInspector
        .getStructFieldRef("ts").getFieldObjectInspector();
    HiveDecimalObjectInspector dco = (HiveDecimalObjectInspector) readerInspector
        .getStructFieldRef("decimal1").getFieldObjectInspector();
    StringObjectInspector mk = (StringObjectInspector) ma
        .getMapKeyObjectInspector();
    RecordReader rows = reader.rows();
    Object row = rows.next(null);
    assertNotNull(row);
    // check the contents of the first row
    assertEquals(false,
        bo.get(readerInspector.getStructFieldData(row, fields.get(0))));
    assertEquals(1,
        by.get(readerInspector.getStructFieldData(row, fields.get(1))));
    assertEquals(1024,
        sh.get(readerInspector.getStructFieldData(row, fields.get(2))));
    assertEquals(65536,
        in.get(readerInspector.getStructFieldData(row, fields.get(3))));
    assertEquals(Long.MAX_VALUE,
        lo.get(readerInspector.getStructFieldData(row, fields.get(4))));
    assertEquals(1.0,
        fl.get(readerInspector.getStructFieldData(row, fields.get(5))), 0.00001);
    assertEquals(-15.0,
        dbl.get(readerInspector.getStructFieldData(row, fields.get(6))),
        0.00001);
    assertEquals(bytes(0, 1, 2, 3, 4),
        bi.getPrimitiveWritableObject(readerInspector.getStructFieldData(row,
            fields.get(7))));
    assertEquals("hi", st.getPrimitiveJavaObject(readerInspector
        .getStructFieldData(row, fields.get(8))));
    List<?> midRow = midli.getList(mid.getStructFieldData(
        readerInspector.getStructFieldData(row, fields.get(9)),
        midFields.get(0)));
    assertNotNull(midRow);
    assertEquals(2, midRow.size());
    assertEquals(1,
        in.get(inner.getStructFieldData(midRow.get(0), inFields.get(0))));
    assertEquals("bye", st.getPrimitiveJavaObject(inner.getStructFieldData(
        midRow.get(0), inFields.get(1))));
    assertEquals(2,
        in.get(inner.getStructFieldData(midRow.get(1), inFields.get(0))));
    assertEquals("sigh", st.getPrimitiveJavaObject(inner.getStructFieldData(
        midRow.get(1), inFields.get(1))));
    List<?> list = li.getList(readerInspector.getStructFieldData(row,
        fields.get(10)));
    assertEquals(2, list.size());
    assertEquals(3,
        in.get(inner.getStructFieldData(list.get(0), inFields.get(0))));
    assertEquals("good", st.getPrimitiveJavaObject(inner.getStructFieldData(
        list.get(0), inFields.get(1))));
    assertEquals(4,
        in.get(inner.getStructFieldData(list.get(1), inFields.get(0))));
    assertEquals("bad", st.getPrimitiveJavaObject(inner.getStructFieldData(
        list.get(1), inFields.get(1))));
    Map<?, ?> map = ma.getMap(readerInspector.getStructFieldData(row,
        fields.get(11)));
    assertEquals(0, map.size());
    assertEquals(Timestamp.valueOf("2000-03-12 15:00:00"),
        tso.getPrimitiveJavaObject(readerInspector.getStructFieldData(row,
            fields.get(12))));
    assertEquals(HiveDecimal.create("12345678.6547456"),
        dco.getPrimitiveJavaObject(readerInspector.getStructFieldData(row,
            fields.get(13))));

    // check the contents of second row
    assertEquals(true, rows.hasNext());
    rows.seekToRow(7499);
    row = rows.next(null);
    assertEquals(true,
        bo.get(readerInspector.getStructFieldData(row, fields.get(0))));
    assertEquals(100,
        by.get(readerInspector.getStructFieldData(row, fields.get(1))));
    assertEquals(2048,
        sh.get(readerInspector.getStructFieldData(row, fields.get(2))));
    assertEquals(65536,
        in.get(readerInspector.getStructFieldData(row, fields.get(3))));
    assertEquals(Long.MAX_VALUE,
        lo.get(readerInspector.getStructFieldData(row, fields.get(4))));
    assertEquals(2.0,
        fl.get(readerInspector.getStructFieldData(row, fields.get(5))), 0.00001);
    assertEquals(-5.0,
        dbl.get(readerInspector.getStructFieldData(row, fields.get(6))),
        0.00001);
    assertEquals(bytes(), bi.getPrimitiveWritableObject(readerInspector
        .getStructFieldData(row, fields.get(7))));
    assertEquals("bye", st.getPrimitiveJavaObject(readerInspector
        .getStructFieldData(row, fields.get(8))));
    midRow = midli.getList(mid.getStructFieldData(
        readerInspector.getStructFieldData(row, fields.get(9)),
        midFields.get(0)));
    assertNotNull(midRow);
    assertEquals(2, midRow.size());
    assertEquals(1,
        in.get(inner.getStructFieldData(midRow.get(0), inFields.get(0))));
    assertEquals("bye", st.getPrimitiveJavaObject(inner.getStructFieldData(
        midRow.get(0), inFields.get(1))));
    assertEquals(2,
        in.get(inner.getStructFieldData(midRow.get(1), inFields.get(0))));
    assertEquals("sigh", st.getPrimitiveJavaObject(inner.getStructFieldData(
        midRow.get(1), inFields.get(1))));
    list = li.getList(readerInspector.getStructFieldData(row, fields.get(10)));
    assertEquals(3, list.size());
    assertEquals(100000000,
        in.get(inner.getStructFieldData(list.get(0), inFields.get(0))));
    assertEquals("cat", st.getPrimitiveJavaObject(inner.getStructFieldData(
        list.get(0), inFields.get(1))));
    assertEquals(-100000,
        in.get(inner.getStructFieldData(list.get(1), inFields.get(0))));
    assertEquals("in", st.getPrimitiveJavaObject(inner.getStructFieldData(
        list.get(1), inFields.get(1))));
    assertEquals(1234,
        in.get(inner.getStructFieldData(list.get(2), inFields.get(0))));
    assertEquals("hat", st.getPrimitiveJavaObject(inner.getStructFieldData(
        list.get(2), inFields.get(1))));
    map = ma.getMap(readerInspector.getStructFieldData(row, fields.get(11)));
    assertEquals(2, map.size());
    boolean[] found = new boolean[2];
    for(Object key : map.keySet()) {
      String str = mk.getPrimitiveJavaObject(key);
      if (str.equals("chani")) {
        assertEquals(false, found[0]);
        assertEquals(5,
            in.get(inner.getStructFieldData(map.get(key), inFields.get(0))));
        assertEquals(str, st.getPrimitiveJavaObject(inner.getStructFieldData(
            map.get(key), inFields.get(1))));
        found[0] = true;
      } else if (str.equals("mauddib")) {
        assertEquals(false, found[1]);
        assertEquals(1,
            in.get(inner.getStructFieldData(map.get(key), inFields.get(0))));
        assertEquals(str, st.getPrimitiveJavaObject(inner.getStructFieldData(
            map.get(key), inFields.get(1))));
        found[1] = true;
      } else {
        throw new IllegalArgumentException("Unknown key " + str);
      }
    }
    assertEquals(true, found[0]);
    assertEquals(true, found[1]);
    assertEquals(Timestamp.valueOf("2000-03-12 15:00:01"),
        tso.getPrimitiveJavaObject(readerInspector.getStructFieldData(row,
            fields.get(12))));
    assertEquals(HiveDecimal.create("12345678.6547457"),
        dco.getPrimitiveJavaObject(readerInspector.getStructFieldData(row,
            fields.get(13))));

    // handle the close up
    assertEquals(false, rows.hasNext());
    rows.close();
  }

  @Test
  public void testTimestamp() throws Exception {
    ObjectInspector inspector;
    synchronized (TestVectorOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector(Timestamp.class,
          ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }

    TypeDescription schema = TypeDescription.createTimestamp();
    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf).setSchema(schema).stripeSize(100000)
            .bufferSize(10000).version(org.apache.orc.OrcFile.Version.V_0_11));
    List<Timestamp> tslist = Lists.newArrayList();
    tslist.add(Timestamp.valueOf("2037-01-01 00:00:00.000999"));
    tslist.add(Timestamp.valueOf("2003-01-01 00:00:00.000000222"));
    tslist.add(Timestamp.valueOf("1999-01-01 00:00:00.999999999"));
    tslist.add(Timestamp.valueOf("1995-01-01 00:00:00.688888888"));
    tslist.add(Timestamp.valueOf("2002-01-01 00:00:00.1"));
    tslist.add(Timestamp.valueOf("2010-03-02 00:00:00.000009001"));
    tslist.add(Timestamp.valueOf("2005-01-01 00:00:00.000002229"));
    tslist.add(Timestamp.valueOf("2006-01-01 00:00:00.900203003"));
    tslist.add(Timestamp.valueOf("2003-01-01 00:00:00.800000007"));
    tslist.add(Timestamp.valueOf("1996-08-02 00:00:00.723100809"));
    tslist.add(Timestamp.valueOf("1998-11-02 00:00:00.857340643"));
    tslist.add(Timestamp.valueOf("2008-10-02 00:00:00"));

    VectorizedRowBatch batch = new VectorizedRowBatch(1, 1024);
    LongColumnVector vec = new LongColumnVector(1024);
    batch.cols[0] = vec;
    batch.reset();
    batch.size = tslist.size();
    for (int i=0; i < tslist.size(); ++i) {
      Timestamp ts = tslist.get(i);
      vec.vector[i] = TimestampUtils.getTimeNanoSec(ts);
    }
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows(null);
    int idx = 0;
    while (rows.hasNext()) {
      Object row = rows.next(null);
      assertEquals(tslist.get(idx++).getNanos(), ((TimestampWritable) row).getNanos());
    }
    assertEquals(tslist.size(), rows.getRowNumber());
    assertEquals(0, writer.getSchema().getMaximumId());
    boolean[] expected = new boolean[] {false};
    boolean[] included = OrcUtils.includeColumns("", writer.getSchema());
    assertEquals(true, Arrays.equals(expected, included));
  }

  @Test
  public void testStringAndBinaryStatistics() throws Exception {

    TypeDescription schema = TypeDescription.createStruct()
        .addField("bytes1", TypeDescription.createBinary())
        .addField("string1", TypeDescription.createString());
    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .setSchema(schema)
                                         .stripeSize(100000)
                                         .bufferSize(10000));
    VectorizedRowBatch batch = schema.createRowBatch();
    batch.size = 4;
    BytesColumnVector field1 = (BytesColumnVector) batch.cols[0];
    BytesColumnVector field2 = (BytesColumnVector) batch.cols[1];
    field1.setVal(0, bytesArray(0, 1, 2, 3, 4));
    field1.setVal(1, bytesArray(0, 1, 2, 3));
    field1.setVal(2, bytesArray(0, 1, 2, 3, 4, 5));
    field1.noNulls = false;
    field1.isNull[3] = true;
    field2.setVal(0, "foo".getBytes());
    field2.setVal(1, "bar".getBytes());
    field2.noNulls = false;
    field2.isNull[2] = true;
    field2.setVal(3, "hi".getBytes());
    writer.addRowBatch(batch);
    writer.close();
    schema = writer.getSchema();
    assertEquals(2, schema.getMaximumId());

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));

    boolean[] expected = new boolean[] {false, false, true};
    boolean[] included = OrcUtils.includeColumns("string1", schema);
    assertEquals(true, Arrays.equals(expected, included));

    expected = new boolean[] {false, false, false};
    included = OrcUtils.includeColumns("", schema);
    assertEquals(true, Arrays.equals(expected, included));

    expected = new boolean[] {false, false, false};
    included = OrcUtils.includeColumns(null, schema);
    assertEquals(true, Arrays.equals(expected, included));

    // check the stats
    ColumnStatistics[] stats = reader.getStatistics();
    assertEquals(4, stats[0].getNumberOfValues());
    assertEquals("count: 4 hasNull: false", stats[0].toString());

    assertEquals(3, stats[1].getNumberOfValues());
    assertEquals(15, ((BinaryColumnStatistics) stats[1]).getSum());
    assertEquals("count: 3 hasNull: true sum: 15", stats[1].toString());

    assertEquals(3, stats[2].getNumberOfValues());
    assertEquals("bar", ((StringColumnStatistics) stats[2]).getMinimum());
    assertEquals("hi", ((StringColumnStatistics) stats[2]).getMaximum());
    assertEquals(8, ((StringColumnStatistics) stats[2]).getSum());
    assertEquals("count: 3 hasNull: true min: bar max: hi sum: 8",
        stats[2].toString());

    // check the inspectors
    StructObjectInspector readerInspector =
        (StructObjectInspector) reader.getObjectInspector();
    assertEquals(ObjectInspector.Category.STRUCT,
        readerInspector.getCategory());
    assertEquals("struct<bytes1:binary,string1:string>",
        readerInspector.getTypeName());
    List<? extends StructField> fields =
        readerInspector.getAllStructFieldRefs();
    BinaryObjectInspector bi = (BinaryObjectInspector) readerInspector.
        getStructFieldRef("bytes1").getFieldObjectInspector();
    StringObjectInspector st = (StringObjectInspector) readerInspector.
        getStructFieldRef("string1").getFieldObjectInspector();
    RecordReader rows = reader.rows();
    Object row = rows.next(null);
    assertNotNull(row);
    // check the contents of the first row
    assertEquals(bytes(0,1,2,3,4), bi.getPrimitiveWritableObject(
        readerInspector.getStructFieldData(row, fields.get(0))));
    assertEquals("foo", st.getPrimitiveJavaObject(readerInspector.
        getStructFieldData(row, fields.get(1))));

    // check the contents of second row
    assertEquals(true, rows.hasNext());
    row = rows.next(row);
    assertEquals(bytes(0,1,2,3), bi.getPrimitiveWritableObject(
        readerInspector.getStructFieldData(row, fields.get(0))));
    assertEquals("bar", st.getPrimitiveJavaObject(readerInspector.
        getStructFieldData(row, fields.get(1))));

    // check the contents of third row
    assertEquals(true, rows.hasNext());
    row = rows.next(row);
    assertEquals(bytes(0,1,2,3,4,5), bi.getPrimitiveWritableObject(
        readerInspector.getStructFieldData(row, fields.get(0))));
    assertNull(st.getPrimitiveJavaObject(readerInspector.
        getStructFieldData(row, fields.get(1))));

    // check the contents of fourth row
    assertEquals(true, rows.hasNext());
    row = rows.next(row);
    assertNull(bi.getPrimitiveWritableObject(
        readerInspector.getStructFieldData(row, fields.get(0))));
    assertEquals("hi", st.getPrimitiveJavaObject(readerInspector.
        getStructFieldData(row, fields.get(1))));

    // handle the close up
    assertEquals(false, rows.hasNext());
    rows.close();
  }


  @Test
  public void testStripeLevelStats() throws Exception {
    TypeDescription schema = TypeDescription.createStruct()
        .addField("int1", TypeDescription.createInt())
        .addField("string1", TypeDescription.createString());
    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .stripeSize(100000)
            .bufferSize(10000));
    VectorizedRowBatch batch = schema.createRowBatch();
    batch.size = 1000;
    LongColumnVector field1 = (LongColumnVector) batch.cols[0];
    BytesColumnVector field2 = (BytesColumnVector) batch.cols[1];
    field1.isRepeating = true;
    field2.isRepeating = true;
    for (int b = 0; b < 11; b++) {
      if (b >= 5) {
        if (b >= 10) {
          field1.vector[0] = 3;
          field2.setVal(0, "three".getBytes());
        } else {
          field1.vector[0] = 2;
          field2.setVal(0, "two".getBytes());
        }
      } else {
        field1.vector[0] = 1;
        field2.setVal(0, "one".getBytes());
      }
      writer.addRowBatch(batch);
    }

    writer.close();
    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));

    schema = writer.getSchema();
    assertEquals(2, schema.getMaximumId());
    boolean[] expected = new boolean[] {false, true, false};
    boolean[] included = OrcUtils.includeColumns("int1", schema);
    assertEquals(true, Arrays.equals(expected, included));

    List<StripeStatistics> stats = reader.getStripeStatistics();
    int numStripes = stats.size();
    assertEquals(3, numStripes);
    StripeStatistics ss1 = stats.get(0);
    StripeStatistics ss2 = stats.get(1);
    StripeStatistics ss3 = stats.get(2);

    assertEquals(5000, ss1.getColumnStatistics()[0].getNumberOfValues());
    assertEquals(5000, ss2.getColumnStatistics()[0].getNumberOfValues());
    assertEquals(1000, ss3.getColumnStatistics()[0].getNumberOfValues());

    assertEquals(5000, (ss1.getColumnStatistics()[1]).getNumberOfValues());
    assertEquals(5000, (ss2.getColumnStatistics()[1]).getNumberOfValues());
    assertEquals(1000, (ss3.getColumnStatistics()[1]).getNumberOfValues());
    assertEquals(1, ((IntegerColumnStatistics)ss1.getColumnStatistics()[1]).getMinimum());
    assertEquals(2, ((IntegerColumnStatistics)ss2.getColumnStatistics()[1]).getMinimum());
    assertEquals(3, ((IntegerColumnStatistics)ss3.getColumnStatistics()[1]).getMinimum());
    assertEquals(1, ((IntegerColumnStatistics)ss1.getColumnStatistics()[1]).getMaximum());
    assertEquals(2, ((IntegerColumnStatistics)ss2.getColumnStatistics()[1]).getMaximum());
    assertEquals(3, ((IntegerColumnStatistics)ss3.getColumnStatistics()[1]).getMaximum());
    assertEquals(5000, ((IntegerColumnStatistics)ss1.getColumnStatistics()[1]).getSum());
    assertEquals(10000, ((IntegerColumnStatistics)ss2.getColumnStatistics()[1]).getSum());
    assertEquals(3000, ((IntegerColumnStatistics)ss3.getColumnStatistics()[1]).getSum());

    assertEquals(5000, (ss1.getColumnStatistics()[2]).getNumberOfValues());
    assertEquals(5000, (ss2.getColumnStatistics()[2]).getNumberOfValues());
    assertEquals(1000, (ss3.getColumnStatistics()[2]).getNumberOfValues());
    assertEquals("one", ((StringColumnStatistics)ss1.getColumnStatistics()[2]).getMinimum());
    assertEquals("two", ((StringColumnStatistics)ss2.getColumnStatistics()[2]).getMinimum());
    assertEquals("three", ((StringColumnStatistics)ss3.getColumnStatistics()[2]).getMinimum());
    assertEquals("one", ((StringColumnStatistics)ss1.getColumnStatistics()[2]).getMaximum());
    assertEquals("two", ((StringColumnStatistics) ss2.getColumnStatistics()[2]).getMaximum());
    assertEquals("three", ((StringColumnStatistics)ss3.getColumnStatistics()[2]).getMaximum());
    assertEquals(15000, ((StringColumnStatistics)ss1.getColumnStatistics()[2]).getSum());
    assertEquals(15000, ((StringColumnStatistics)ss2.getColumnStatistics()[2]).getSum());
    assertEquals(5000, ((StringColumnStatistics)ss3.getColumnStatistics()[2]).getSum());

    RecordReaderImpl recordReader = (RecordReaderImpl) reader.rows();
    OrcProto.RowIndex[] index = recordReader.readRowIndex(0, null, null).getRowGroupIndex();
    assertEquals(3, index.length);
    List<OrcProto.RowIndexEntry> items = index[1].getEntryList();
    assertEquals(1, items.size());
    assertEquals(3, items.get(0).getPositionsCount());
    assertEquals(0, items.get(0).getPositions(0));
    assertEquals(0, items.get(0).getPositions(1));
    assertEquals(0, items.get(0).getPositions(2));
    assertEquals(1,
                 items.get(0).getStatistics().getIntStatistics().getMinimum());
    index = recordReader.readRowIndex(1, null, null).getRowGroupIndex();
    assertEquals(3, index.length);
    items = index[1].getEntryList();
    assertEquals(2,
        items.get(0).getStatistics().getIntStatistics().getMaximum());
  }

  private static void setInner(StructColumnVector inner, int rowId,
                               int i, String value) {
    ((LongColumnVector) inner.fields[0]).vector[rowId] = i;
    if (value != null) {
      ((BytesColumnVector) inner.fields[1]).setVal(rowId, value.getBytes());
    } else {
      inner.fields[1].isNull[rowId] = true;
      inner.fields[1].noNulls = false;
    }
  }

  private static void setInnerList(ListColumnVector list, int rowId,
                                   List<InnerStruct> value) {
    if (value != null) {
      if (list.childCount + value.size() > list.child.isNull.length) {
        list.child.ensureSize(list.childCount * 2, true);
      }
      list.lengths[rowId] = value.size();
      list.offsets[rowId] = list.childCount;
      for (int i = 0; i < list.lengths[rowId]; ++i) {
        InnerStruct inner = value.get(i);
        setInner((StructColumnVector) list.child, i + list.childCount,
            inner.int1, inner.string1.toString());
      }
      list.childCount += value.size();
    } else {
      list.isNull[rowId] = true;
      list.noNulls = false;
    }
  }

  private static void setInnerMap(MapColumnVector map, int rowId,
                                  Map<String, InnerStruct> value) {
    if (value != null) {
      if (map.childCount >= map.keys.isNull.length) {
        map.keys.ensureSize(map.childCount * 2, true);
        map.values.ensureSize(map.childCount * 2, true);
      }
      map.lengths[rowId] = value.size();
      int offset = map.childCount;
      map.offsets[rowId] = offset;

      for (Map.Entry<String, InnerStruct> entry : value.entrySet()) {
        ((BytesColumnVector) map.keys).setVal(offset, entry.getKey().getBytes());
        InnerStruct inner = entry.getValue();
        setInner((StructColumnVector) map.values, offset, inner.int1,
            inner.string1.toString());
        offset += 1;
      }
      map.childCount = offset;
    } else {
      map.isNull[rowId] = true;
      map.noNulls = false;
    }
  }

  private static void setMiddleStruct(StructColumnVector middle, int rowId,
                                      MiddleStruct value) {
    if (value != null) {
      setInnerList((ListColumnVector) middle.fields[0], rowId, value.list);
    } else {
      middle.isNull[rowId] = true;
      middle.noNulls = false;
    }
  }

  private static void setBigRow(VectorizedRowBatch batch, int rowId,
                                Boolean b1, Byte b2, Short s1,
                                Integer i1, Long l1, Float f1,
                                Double d1, BytesWritable b3, String s2,
                                MiddleStruct m1, List<InnerStruct> l2,
                                Map<String, InnerStruct> m2) {
    ((LongColumnVector) batch.cols[0]).vector[rowId] = b1 ? 1 : 0;
    ((LongColumnVector) batch.cols[1]).vector[rowId] = b2;
    ((LongColumnVector) batch.cols[2]).vector[rowId] = s1;
    ((LongColumnVector) batch.cols[3]).vector[rowId] = i1;
    ((LongColumnVector) batch.cols[4]).vector[rowId] = l1;
    ((DoubleColumnVector) batch.cols[5]).vector[rowId] = f1;
    ((DoubleColumnVector) batch.cols[6]).vector[rowId] = d1;
    if (b3 != null) {
      ((BytesColumnVector) batch.cols[7]).setVal(rowId, b3.getBytes(), 0,
          b3.getLength());
    } else {
      batch.cols[7].isNull[rowId] = true;
      batch.cols[7].noNulls = false;
    }
    if (s2 != null) {
      ((BytesColumnVector) batch.cols[8]).setVal(rowId, s2.getBytes());
    } else {
      batch.cols[8].isNull[rowId] = true;
      batch.cols[8].noNulls = false;
    }
    setMiddleStruct((StructColumnVector) batch.cols[9], rowId, m1);
    setInnerList((ListColumnVector) batch.cols[10], rowId, l2);
    setInnerMap((MapColumnVector) batch.cols[11], rowId, m2);
  }

  private static TypeDescription createInnerSchema() {
    return TypeDescription.createStruct()
        .addField("int1", TypeDescription.createInt())
        .addField("string1", TypeDescription.createString());
  }

  private static TypeDescription createBigRowSchema() {
    return TypeDescription.createStruct()
        .addField("boolean1", TypeDescription.createBoolean())
        .addField("byte1", TypeDescription.createByte())
        .addField("short1", TypeDescription.createShort())
        .addField("int1", TypeDescription.createInt())
        .addField("long1", TypeDescription.createLong())
        .addField("float1", TypeDescription.createFloat())
        .addField("double1", TypeDescription.createDouble())
        .addField("bytes1", TypeDescription.createBinary())
        .addField("string1", TypeDescription.createString())
        .addField("middle", TypeDescription.createStruct()
            .addField("list", TypeDescription.createList(createInnerSchema())))
        .addField("list", TypeDescription.createList(createInnerSchema()))
        .addField("map", TypeDescription.createMap(
            TypeDescription.createString(),
            createInnerSchema()));
  }

  static void assertArrayEquals(boolean[] expected, boolean[] actual) {
    assertEquals(expected.length, actual.length);
    boolean diff = false;
    for(int i=0; i < expected.length; ++i) {
      if (expected[i] != actual[i]) {
        System.out.println("Difference at " + i + " expected: " + expected[i] +
          " actual: " + actual[i]);
        diff = true;
      }
    }
    assertEquals(false, diff);
  }

  @Test
  public void test1() throws Exception {
    TypeDescription schema = createBigRowSchema();
    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .stripeSize(100000)
            .bufferSize(10000));
    VectorizedRowBatch batch = schema.createRowBatch();
    batch.size = 2;
    setBigRow(batch, 0, false, (byte) 1, (short) 1024, 65536,
        Long.MAX_VALUE, (float) 1.0, -15.0, bytes(0, 1, 2, 3, 4), "hi",
        new MiddleStruct(inner(1, "bye"), inner(2, "sigh")),
        list(inner(3, "good"), inner(4, "bad")),
        map());
    setBigRow(batch, 1, true, (byte) 100, (short) 2048, 65536,
        Long.MAX_VALUE, (float) 2.0, -5.0, bytes(), "bye",
        new MiddleStruct(inner(1, "bye"), inner(2, "sigh")),
        list(inner(100000000, "cat"), inner(-100000, "in"), inner(1234, "hat")),
        map(inner(5, "chani"), inner(1, "mauddib")));
    writer.addRowBatch(batch);
    writer.close();
    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));

    schema = writer.getSchema();
    assertEquals(23, schema.getMaximumId());
    boolean[] expected = new boolean[] {false, false, false, false, false,
        false, false, false, false, false,
        false, false, false, false, false,
        false, false, false, false, false,
        false, false, false, false};
    boolean[] included = OrcUtils.includeColumns("", schema);
    assertEquals(true, Arrays.equals(expected, included));

    expected = new boolean[] {false, true, false, false, false,
        false, false, false, false, true,
        true, true, true, true, true,
        false, false, false, false, true,
        true, true, true, true};
    included = OrcUtils.includeColumns("boolean1,string1,middle,map", schema);

    assertArrayEquals(expected, included);

    expected = new boolean[] {false, true, false, false, false,
        false, false, false, false, true,
        true, true, true, true, true,
        false, false, false, false, true,
        true, true, true, true};
    included = OrcUtils.includeColumns("boolean1,string1,middle,map", schema);
    assertArrayEquals(expected, included);

    expected = new boolean[] {false, true, true, true, true,
        true, true, true, true, true,
        true, true, true, true, true,
        true, true, true, true, true,
        true, true, true, true};
    included = OrcUtils.includeColumns(
        "boolean1,byte1,short1,int1,long1,float1,double1,bytes1,string1,middle,list,map",
        schema);
    assertEquals(true, Arrays.equals(expected, included));

    // check the stats
    ColumnStatistics[] stats = reader.getStatistics();
    assertEquals(2, stats[1].getNumberOfValues());
    assertEquals(1, ((BooleanColumnStatistics) stats[1]).getFalseCount());
    assertEquals(1, ((BooleanColumnStatistics) stats[1]).getTrueCount());
    assertEquals("count: 2 hasNull: false true: 1", stats[1].toString());

    assertEquals(2048, ((IntegerColumnStatistics) stats[3]).getMaximum());
    assertEquals(1024, ((IntegerColumnStatistics) stats[3]).getMinimum());
    assertEquals(true, ((IntegerColumnStatistics) stats[3]).isSumDefined());
    assertEquals(3072, ((IntegerColumnStatistics) stats[3]).getSum());
    assertEquals("count: 2 hasNull: false min: 1024 max: 2048 sum: 3072",
        stats[3].toString());

    StripeStatistics ss = reader.getStripeStatistics().get(0);
    assertEquals(2, ss.getColumnStatistics()[0].getNumberOfValues());
    assertEquals(1, ((BooleanColumnStatistics) ss.getColumnStatistics()[1]).getTrueCount());
    assertEquals(1024, ((IntegerColumnStatistics) ss.getColumnStatistics()[3]).getMinimum());
    assertEquals(2048, ((IntegerColumnStatistics) ss.getColumnStatistics()[3]).getMaximum());
    assertEquals(3072, ((IntegerColumnStatistics) ss.getColumnStatistics()[3]).getSum());
    assertEquals(-15.0, ((DoubleColumnStatistics) stats[7]).getMinimum());
    assertEquals(-5.0, ((DoubleColumnStatistics) stats[7]).getMaximum());
    assertEquals(-20.0, ((DoubleColumnStatistics) stats[7]).getSum(), 0.00001);
    assertEquals("count: 2 hasNull: false min: -15.0 max: -5.0 sum: -20.0",
        stats[7].toString());

    assertEquals("count: 2 hasNull: false min: bye max: hi sum: 5", stats[9].toString());

    // check the inspectors
    StructObjectInspector readerInspector =
        (StructObjectInspector) reader.getObjectInspector();
    assertEquals(ObjectInspector.Category.STRUCT,
        readerInspector.getCategory());
    assertEquals("struct<boolean1:boolean,byte1:tinyint,short1:smallint,"
        + "int1:int,long1:bigint,float1:float,double1:double,bytes1:"
        + "binary,string1:string,middle:struct<list:array<struct<int1:int,"
        + "string1:string>>>,list:array<struct<int1:int,string1:string>>,"
        + "map:map<string,struct<int1:int,string1:string>>>",
        readerInspector.getTypeName());
    List<? extends StructField> fields =
        readerInspector.getAllStructFieldRefs();
    BooleanObjectInspector bo = (BooleanObjectInspector) readerInspector.
        getStructFieldRef("boolean1").getFieldObjectInspector();
    ByteObjectInspector by = (ByteObjectInspector) readerInspector.
        getStructFieldRef("byte1").getFieldObjectInspector();
    ShortObjectInspector sh = (ShortObjectInspector) readerInspector.
        getStructFieldRef("short1").getFieldObjectInspector();
    IntObjectInspector in = (IntObjectInspector) readerInspector.
        getStructFieldRef("int1").getFieldObjectInspector();
    LongObjectInspector lo = (LongObjectInspector) readerInspector.
        getStructFieldRef("long1").getFieldObjectInspector();
    FloatObjectInspector fl = (FloatObjectInspector) readerInspector.
        getStructFieldRef("float1").getFieldObjectInspector();
    DoubleObjectInspector dbl = (DoubleObjectInspector) readerInspector.
        getStructFieldRef("double1").getFieldObjectInspector();
    BinaryObjectInspector bi = (BinaryObjectInspector) readerInspector.
        getStructFieldRef("bytes1").getFieldObjectInspector();
    StringObjectInspector st = (StringObjectInspector) readerInspector.
        getStructFieldRef("string1").getFieldObjectInspector();
    StructObjectInspector mid = (StructObjectInspector) readerInspector.
        getStructFieldRef("middle").getFieldObjectInspector();
    List<? extends StructField> midFields =
        mid.getAllStructFieldRefs();
    ListObjectInspector midli =
        (ListObjectInspector) midFields.get(0).getFieldObjectInspector();
    StructObjectInspector inner = (StructObjectInspector)
        midli.getListElementObjectInspector();
    List<? extends StructField> inFields = inner.getAllStructFieldRefs();
    ListObjectInspector li = (ListObjectInspector) readerInspector.
        getStructFieldRef("list").getFieldObjectInspector();
    MapObjectInspector ma = (MapObjectInspector) readerInspector.
        getStructFieldRef("map").getFieldObjectInspector();
    StringObjectInspector mk = (StringObjectInspector)
        ma.getMapKeyObjectInspector();
    RecordReader rows = reader.rows();
    Object row = rows.next(null);
    assertNotNull(row);
    // check the contents of the first row
    assertEquals(false,
        bo.get(readerInspector.getStructFieldData(row, fields.get(0))));
    assertEquals(1, by.get(readerInspector.getStructFieldData(row,
        fields.get(1))));
    assertEquals(1024, sh.get(readerInspector.getStructFieldData(row,
        fields.get(2))));
    assertEquals(65536, in.get(readerInspector.getStructFieldData(row,
        fields.get(3))));
    assertEquals(Long.MAX_VALUE, lo.get(readerInspector.
        getStructFieldData(row, fields.get(4))));
    assertEquals(1.0, fl.get(readerInspector.getStructFieldData(row,
        fields.get(5))), 0.00001);
    assertEquals(-15.0, dbl.get(readerInspector.getStructFieldData(row,
        fields.get(6))), 0.00001);
    assertEquals(bytes(0,1,2,3,4), bi.getPrimitiveWritableObject(
        readerInspector.getStructFieldData(row, fields.get(7))));
    assertEquals("hi", st.getPrimitiveJavaObject(readerInspector.
        getStructFieldData(row, fields.get(8))));
    List<?> midRow = midli.getList(mid.getStructFieldData(readerInspector.
        getStructFieldData(row, fields.get(9)), midFields.get(0)));
    assertNotNull(midRow);
    assertEquals(2, midRow.size());
    assertEquals(1, in.get(inner.getStructFieldData(midRow.get(0),
        inFields.get(0))));
    assertEquals("bye", st.getPrimitiveJavaObject(inner.getStructFieldData
        (midRow.get(0), inFields.get(1))));
    assertEquals(2, in.get(inner.getStructFieldData(midRow.get(1),
        inFields.get(0))));
    assertEquals("sigh", st.getPrimitiveJavaObject(inner.getStructFieldData
        (midRow.get(1), inFields.get(1))));
    List<?> list = li.getList(readerInspector.getStructFieldData(row,
        fields.get(10)));
    assertEquals(2, list.size());
    assertEquals(3, in.get(inner.getStructFieldData(list.get(0),
        inFields.get(0))));
    assertEquals("good", st.getPrimitiveJavaObject(inner.getStructFieldData
        (list.get(0), inFields.get(1))));
    assertEquals(4, in.get(inner.getStructFieldData(list.get(1),
        inFields.get(0))));
    assertEquals("bad", st.getPrimitiveJavaObject(inner.getStructFieldData
        (list.get(1), inFields.get(1))));
    Map<?,?> map = ma.getMap(readerInspector.getStructFieldData(row,
        fields.get(11)));
    assertEquals(0, map.size());

    // check the contents of second row
    assertEquals(true, rows.hasNext());
    row = rows.next(row);
    assertEquals(true,
        bo.get(readerInspector.getStructFieldData(row, fields.get(0))));
    assertEquals(100, by.get(readerInspector.getStructFieldData(row,
        fields.get(1))));
    assertEquals(2048, sh.get(readerInspector.getStructFieldData(row,
        fields.get(2))));
    assertEquals(65536, in.get(readerInspector.getStructFieldData(row,
        fields.get(3))));
    assertEquals(Long.MAX_VALUE, lo.get(readerInspector.
        getStructFieldData(row, fields.get(4))));
    assertEquals(2.0, fl.get(readerInspector.getStructFieldData(row,
        fields.get(5))), 0.00001);
    assertEquals(-5.0, dbl.get(readerInspector.getStructFieldData(row,
        fields.get(6))), 0.00001);
    assertEquals(bytes(), bi.getPrimitiveWritableObject(
        readerInspector.getStructFieldData(row, fields.get(7))));
    assertEquals("bye", st.getPrimitiveJavaObject(readerInspector.
        getStructFieldData(row, fields.get(8))));
    midRow = midli.getList(mid.getStructFieldData(readerInspector.
        getStructFieldData(row, fields.get(9)), midFields.get(0)));
    assertNotNull(midRow);
    assertEquals(2, midRow.size());
    assertEquals(1, in.get(inner.getStructFieldData(midRow.get(0),
        inFields.get(0))));
    assertEquals("bye", st.getPrimitiveJavaObject(inner.getStructFieldData
        (midRow.get(0), inFields.get(1))));
    assertEquals(2, in.get(inner.getStructFieldData(midRow.get(1),
        inFields.get(0))));
    assertEquals("sigh", st.getPrimitiveJavaObject(inner.getStructFieldData
        (midRow.get(1), inFields.get(1))));
    list = li.getList(readerInspector.getStructFieldData(row,
        fields.get(10)));
    assertEquals(3, list.size());
    assertEquals(100000000, in.get(inner.getStructFieldData(list.get(0),
        inFields.get(0))));
    assertEquals("cat", st.getPrimitiveJavaObject(inner.getStructFieldData
        (list.get(0), inFields.get(1))));
    assertEquals(-100000, in.get(inner.getStructFieldData(list.get(1),
        inFields.get(0))));
    assertEquals("in", st.getPrimitiveJavaObject(inner.getStructFieldData
        (list.get(1), inFields.get(1))));
    assertEquals(1234, in.get(inner.getStructFieldData(list.get(2),
        inFields.get(0))));
    assertEquals("hat", st.getPrimitiveJavaObject(inner.getStructFieldData
        (list.get(2), inFields.get(1))));
    map = ma.getMap(readerInspector.getStructFieldData(row,
        fields.get(11)));
    assertEquals(2, map.size());
    boolean[] found = new boolean[2];
    for(Object key: map.keySet()) {
      String str = mk.getPrimitiveJavaObject(key);
      if (str.equals("chani")) {
        assertEquals(false, found[0]);
        assertEquals(5, in.get(inner.getStructFieldData(map.get(key),
            inFields.get(0))));
        assertEquals(str, st.getPrimitiveJavaObject(
            inner.getStructFieldData(map.get(key), inFields.get(1))));
        found[0] = true;
      } else if (str.equals("mauddib")) {
        assertEquals(false, found[1]);
        assertEquals(1, in.get(inner.getStructFieldData(map.get(key),
            inFields.get(0))));
        assertEquals(str, st.getPrimitiveJavaObject(
            inner.getStructFieldData(map.get(key), inFields.get(1))));
        found[1] = true;
      } else {
        throw new IllegalArgumentException("Unknown key " + str);
      }
    }
    assertEquals(true, found[0]);
    assertEquals(true, found[1]);

    // handle the close up
    assertEquals(false, rows.hasNext());
    rows.close();
  }

  @Test
  public void testColumnProjection() throws Exception {
    TypeDescription schema = createInnerSchema();
    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .setSchema(schema)
                                         .stripeSize(1000)
                                         .compress(CompressionKind.NONE)
                                         .bufferSize(100)
                                         .rowIndexStride(1000));
    VectorizedRowBatch batch = schema.createRowBatch();
    Random r1 = new Random(1);
    Random r2 = new Random(2);
    int x;
    int minInt=0, maxInt=0;
    String y;
    String minStr = null, maxStr = null;
    batch.size = 1000;
    boolean first = true;
    for(int b=0; b < 21; ++b) {
      for(int r=0; r < 1000; ++r) {
        x = r1.nextInt();
        y = Long.toHexString(r2.nextLong());
        if (first || x < minInt) {
          minInt = x;
        }
        if (first || x > maxInt) {
          maxInt = x;
        }
        if (first || y.compareTo(minStr) < 0) {
          minStr = y;
        }
        if (first || y.compareTo(maxStr) > 0) {
          maxStr = y;
        }
        first = false;
        ((LongColumnVector) batch.cols[0]).vector[r] = x;
        ((BytesColumnVector) batch.cols[1]).setVal(r, y.getBytes());
      }
      writer.addRowBatch(batch);
    }
    writer.close();
    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));

    // check out the statistics
    ColumnStatistics[] stats = reader.getStatistics();
    assertEquals(3, stats.length);
    for(ColumnStatistics s: stats) {
      assertEquals(21000, s.getNumberOfValues());
      if (s instanceof IntegerColumnStatistics) {
        assertEquals(minInt, ((IntegerColumnStatistics) s).getMinimum());
        assertEquals(maxInt, ((IntegerColumnStatistics) s).getMaximum());
      } else if (s instanceof  StringColumnStatistics) {
        assertEquals(maxStr, ((StringColumnStatistics) s).getMaximum());
        assertEquals(minStr, ((StringColumnStatistics) s).getMinimum());
      }
    }

    // check out the types
    List<OrcProto.Type> types = reader.getTypes();
    assertEquals(3, types.size());
    assertEquals(OrcProto.Type.Kind.STRUCT, types.get(0).getKind());
    assertEquals(2, types.get(0).getSubtypesCount());
    assertEquals(1, types.get(0).getSubtypes(0));
    assertEquals(2, types.get(0).getSubtypes(1));
    assertEquals(OrcProto.Type.Kind.INT, types.get(1).getKind());
    assertEquals(0, types.get(1).getSubtypesCount());
    assertEquals(OrcProto.Type.Kind.STRING, types.get(2).getKind());
    assertEquals(0, types.get(2).getSubtypesCount());

    // read the contents and make sure they match
    RecordReader rows1 = reader.rows(new boolean[]{true, true, false});
    RecordReader rows2 = reader.rows(new boolean[]{true, false, true});
    r1 = new Random(1);
    r2 = new Random(2);
    OrcStruct row1 = null;
    OrcStruct row2 = null;
    for(int i = 0; i < 21000; ++i) {
      assertEquals(true, rows1.hasNext());
      assertEquals(true, rows2.hasNext());
      row1 = (OrcStruct) rows1.next(row1);
      row2 = (OrcStruct) rows2.next(row2);
      assertEquals(r1.nextInt(), ((IntWritable) row1.getFieldValue(0)).get());
      assertEquals(Long.toHexString(r2.nextLong()),
          row2.getFieldValue(1).toString());
    }
    assertEquals(false, rows1.hasNext());
    assertEquals(false, rows2.hasNext());
    rows1.close();
    rows2.close();
  }

  @Test
  public void testEmptyFile() throws Exception {
    TypeDescription schema = createBigRowSchema();
    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .setSchema(schema)
                                         .stripeSize(1000)
                                         .compress(CompressionKind.NONE)
                                         .bufferSize(100));
    writer.close();
    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    assertEquals(false, reader.rows().hasNext());
    assertEquals(CompressionKind.NONE, reader.getCompressionKind());
    assertEquals(0, reader.getNumberOfRows());
    assertEquals(0, reader.getCompressionSize());
    assertEquals(false, reader.getMetadataKeys().iterator().hasNext());
    assertEquals(3, reader.getContentLength());
    assertEquals(false, reader.getStripes().iterator().hasNext());
  }

  @Test
  public void metaData() throws Exception {
    TypeDescription schema = createBigRowSchema();
    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .stripeSize(1000)
            .compress(CompressionKind.NONE)
            .bufferSize(100));
    writer.addUserMetadata("my.meta", byteBuf(1, 2, 3, 4, 5, 6, 7, -1, -2, 127,
                                              -128));
    writer.addUserMetadata("clobber", byteBuf(1, 2, 3));
    writer.addUserMetadata("clobber", byteBuf(4, 3, 2, 1));
    ByteBuffer bigBuf = ByteBuffer.allocate(40000);
    Random random = new Random(0);
    random.nextBytes(bigBuf.array());
    writer.addUserMetadata("big", bigBuf);
    bigBuf.position(0);
    VectorizedRowBatch batch = schema.createRowBatch();
    batch.size = 1;
    setBigRow(batch, 0, true, (byte) 127, (short) 1024, 42,
        42L * 1024 * 1024 * 1024, (float) 3.1415, -2.713, null,
        null, null, null, null);
    writer.addRowBatch(batch);
    writer.addUserMetadata("clobber", byteBuf(5,7,11,13,17,19));
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    assertEquals(byteBuf(5,7,11,13,17,19), reader.getMetadataValue("clobber"));
    assertEquals(byteBuf(1,2,3,4,5,6,7,-1,-2,127,-128),
        reader.getMetadataValue("my.meta"));
    assertEquals(bigBuf, reader.getMetadataValue("big"));
    try {
      reader.getMetadataValue("unknown");
      assertTrue(false);
    } catch (IllegalArgumentException iae) {
      // PASS
    }
    int i = 0;
    for(String key: reader.getMetadataKeys()) {
      if ("my.meta".equals(key) ||
          "clobber".equals(key) ||
          "big".equals(key)) {
        i += 1;
      } else {
        throw new IllegalArgumentException("unknown key " + key);
      }
    }
    assertEquals(3, i);
    int numStripes = reader.getStripeStatistics().size();
    assertEquals(1, numStripes);
  }

  /**
   * Generate an ORC file with a range of dates and times.
   */
  public void createOrcDateFile(Path file, int minYear, int maxYear
                                ) throws IOException {
    TypeDescription schema = TypeDescription.createStruct()
        .addField("time", TypeDescription.createTimestamp())
        .addField("date", TypeDescription.createDate());
    Writer writer = OrcFile.createWriter(file,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .stripeSize(100000)
            .bufferSize(10000)
            .blockPadding(false));
    VectorizedRowBatch batch = schema.createRowBatch();
    batch.size = 1000;
    for (int year = minYear; year < maxYear; ++year) {
      for (int ms = 1000; ms < 2000; ++ms) {
        ((LongColumnVector) batch.cols[0]).vector[ms - 1000] =
            TimestampUtils.getTimeNanoSec(Timestamp.valueOf(year +
                "-05-05 12:34:56." + ms));
        ((LongColumnVector) batch.cols[1]).vector[ms - 1000] =
            new DateWritable(new Date(year - 1900, 11, 25)).getDays();
      }
      writer.addRowBatch(batch);
    }
    writer.close();
    Reader reader = OrcFile.createReader(file,
        OrcFile.readerOptions(conf));
    RecordReader rows = reader.rows();
    OrcStruct row = null;
    for (int year = minYear; year < maxYear; ++year) {
      for(int ms = 1000; ms < 2000; ++ms) {
        row = (OrcStruct) rows.next(row);
        assertEquals(new TimestampWritable
                (Timestamp.valueOf(year + "-05-05 12:34:56." + ms)),
            row.getFieldValue(0));
        assertEquals(new DateWritable(new Date(year - 1900, 11, 25)),
            row.getFieldValue(1));
      }
    }
  }

  @Test
  public void testDate1900() throws Exception {
    createOrcDateFile(testFilePath, 1900, 1970);
  }

  @Test
  public void testDate2038() throws Exception {
    createOrcDateFile(testFilePath, 2038, 2250);
  }

  private static void setUnion(VectorizedRowBatch batch, int rowId,
                               Timestamp ts, Integer tag, Integer i, String s,
                               HiveDecimalWritable dec) {
    UnionColumnVector union = (UnionColumnVector) batch.cols[1];
    if (ts != null) {
      ((LongColumnVector) batch.cols[0]).vector[rowId] =
          TimestampUtils.getTimeNanoSec(ts);
    } else {
      batch.cols[0].isNull[rowId] = true;
      batch.cols[0].noNulls = false;
    }
    if (tag != null) {
      union.tags[rowId] = tag;
      if (tag == 0) {
        if (i != null) {
          ((LongColumnVector) union.fields[tag]).vector[rowId] = i;
        } else {
          union.fields[tag].isNull[rowId] = true;
          union.fields[tag].noNulls = false;
        }
      } else if (tag == 1) {
        if (s != null) {
          ((BytesColumnVector) union.fields[tag]).setVal(rowId, s.getBytes());
        } else {
          union.fields[tag].isNull[rowId] = true;
          union.fields[tag].noNulls = false;
        }
      } else {
        throw new IllegalArgumentException("Bad tag " + tag);
      }
    } else {
      batch.cols[1].isNull[rowId] = true;
      batch.cols[1].noNulls = false;
    }
    if (dec != null) {
      ((DecimalColumnVector) batch.cols[2]).vector[rowId] = dec;
    } else {
      batch.cols[2].isNull[rowId] = true;
      batch.cols[2].noNulls = false;
    }
  }

  /**
     * We test union, timestamp, and decimal separately since we need to make the
     * object inspector manually. (The Hive reflection-based doesn't handle
     * them properly.)
     */
  @Test
  public void testUnionAndTimestamp() throws Exception {
    TypeDescription schema = TypeDescription.createStruct()
        .addField("time", TypeDescription.createTimestamp())
        .addField("union", TypeDescription.createUnion()
            .addUnionChild(TypeDescription.createInt())
            .addUnionChild(TypeDescription.createString()))
        .addField("decimal", TypeDescription.createDecimal()
            .withPrecision(38)
            .withScale(18));
    HiveDecimal maxValue = HiveDecimal.create("10000000000000000000");
    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .setSchema(schema)
                                         .stripeSize(1000)
                                         .compress(CompressionKind.NONE)
                                         .bufferSize(100)
                                         .blockPadding(false));
    VectorizedRowBatch batch = schema.createRowBatch();
    batch.size = 6;
    setUnion(batch, 0, Timestamp.valueOf("2000-03-12 15:00:00"), 0, 42, null,
             new HiveDecimalWritable("12345678.6547456"));
    setUnion(batch, 1, Timestamp.valueOf("2000-03-20 12:00:00.123456789"),
        1, null, "hello", new HiveDecimalWritable("-5643.234"));

    setUnion(batch, 2, null, null, null, null, null);
    setUnion(batch, 3, null, 0, null, null, null);
    setUnion(batch, 4, null, 1, null, null, null);

    setUnion(batch, 5, Timestamp.valueOf("1970-01-01 00:00:00"), 0, 200000,
        null, new HiveDecimalWritable("10000000000000000000"));
    writer.addRowBatch(batch);

    batch.reset();
    Random rand = new Random(42);
    for(int i=1970; i < 2038; ++i) {
      Timestamp ts = Timestamp.valueOf(i + "-05-05 12:34:56." + i);
      HiveDecimal dec =
          HiveDecimal.create(new BigInteger(64, rand), rand.nextInt(18));
      if ((i & 1) == 0) {
        setUnion(batch, batch.size++, ts, 0, i*i, null,
            new HiveDecimalWritable(dec));
      } else {
        setUnion(batch, batch.size++, ts, 1, null, Integer.toString(i*i),
            new HiveDecimalWritable(dec));
      }
      if (maxValue.compareTo(dec) < 0) {
        maxValue = dec;
      }
    }
    writer.addRowBatch(batch);
    batch.reset();

    // let's add a lot of constant rows to test the rle
    batch.size = 1000;
    for(int c=0; c < batch.cols.length; ++c) {
      batch.cols[c].setRepeating(true);
    }
    setUnion(batch, 0, null, 0, 1732050807, null, null);
    for(int i=0; i < 5; ++i) {
      writer.addRowBatch(batch);
    }

    batch.reset();
    batch.size = 3;
    setUnion(batch, 0, null, 0, 0, null, null);
    setUnion(batch, 1, null, 0, 10, null, null);
    setUnion(batch, 2, null, 0, 138, null, null);
    writer.addRowBatch(batch);
    writer.close();
    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));

    schema = writer.getSchema();
    assertEquals(5, schema.getMaximumId());
    boolean[] expected = new boolean[] {false, false, false, false, false, false};
    boolean[] included = OrcUtils.includeColumns("", schema);
    assertEquals(true, Arrays.equals(expected, included));

    expected = new boolean[] {false, true, false, false, false, true};
    included = OrcUtils.includeColumns("time,decimal", schema);
    assertEquals(true, Arrays.equals(expected, included));

    expected = new boolean[] {false, false, true, true, true, false};
    included = OrcUtils.includeColumns("union", schema);
    assertEquals(true, Arrays.equals(expected, included));

    assertEquals(false, reader.getMetadataKeys().iterator().hasNext());
    assertEquals(5077, reader.getNumberOfRows());
    DecimalColumnStatistics stats =
        (DecimalColumnStatistics) reader.getStatistics()[5];
    assertEquals(71, stats.getNumberOfValues());
    assertEquals(HiveDecimal.create("-5643.234"), stats.getMinimum());
    assertEquals(maxValue, stats.getMaximum());
    // TODO: fix this
//    assertEquals(null,stats.getSum());
    int stripeCount = 0;
    int rowCount = 0;
    long currentOffset = -1;
    for(StripeInformation stripe: reader.getStripes()) {
      stripeCount += 1;
      rowCount += stripe.getNumberOfRows();
      if (currentOffset < 0) {
        currentOffset = stripe.getOffset() + stripe.getLength();
      } else {
        assertEquals(currentOffset, stripe.getOffset());
        currentOffset += stripe.getLength();
      }
    }
    assertEquals(reader.getNumberOfRows(), rowCount);
    assertEquals(2, stripeCount);
    assertEquals(reader.getContentLength(), currentOffset);
    RecordReader rows = reader.rows();
    assertEquals(0, rows.getRowNumber());
    assertEquals(0.0, rows.getProgress(), 0.000001);
    assertEquals(true, rows.hasNext());
    OrcStruct row = (OrcStruct) rows.next(null);
    assertEquals(1, rows.getRowNumber());
    ObjectInspector inspector = reader.getObjectInspector();
    assertEquals("struct<time:timestamp,union:uniontype<int,string>,decimal:decimal(38,18)>",
        inspector.getTypeName());
    assertEquals(new TimestampWritable(Timestamp.valueOf("2000-03-12 15:00:00")),
        row.getFieldValue(0));
    OrcUnion union = (OrcUnion) row.getFieldValue(1);
    assertEquals(0, union.getTag());
    assertEquals(new IntWritable(42), union.getObject());
    assertEquals(new HiveDecimalWritable(HiveDecimal.create("12345678.6547456")),
        row.getFieldValue(2));
    row = (OrcStruct) rows.next(row);
    assertEquals(2, rows.getRowNumber());
    assertEquals(new TimestampWritable(Timestamp.valueOf("2000-03-20 12:00:00.123456789")),
        row.getFieldValue(0));
    assertEquals(1, union.getTag());
    assertEquals(new Text("hello"), union.getObject());
    assertEquals(new HiveDecimalWritable(HiveDecimal.create("-5643.234")),
        row.getFieldValue(2));
    row = (OrcStruct) rows.next(row);
    assertEquals(null, row.getFieldValue(0));
    assertEquals(null, row.getFieldValue(1));
    assertEquals(null, row.getFieldValue(2));
    row = (OrcStruct) rows.next(row);
    assertEquals(null, row.getFieldValue(0));
    union = (OrcUnion) row.getFieldValue(1);
    assertEquals(0, union.getTag());
    assertEquals(null, union.getObject());
    assertEquals(null, row.getFieldValue(2));
    row = (OrcStruct) rows.next(row);
    assertEquals(null, row.getFieldValue(0));
    assertEquals(1, union.getTag());
    assertEquals(null, union.getObject());
    assertEquals(null, row.getFieldValue(2));
    row = (OrcStruct) rows.next(row);
    assertEquals(new TimestampWritable(Timestamp.valueOf("1970-01-01 00:00:00")),
        row.getFieldValue(0));
    assertEquals(new IntWritable(200000), union.getObject());
    assertEquals(new HiveDecimalWritable(HiveDecimal.create("10000000000000000000")),
                 row.getFieldValue(2));
    rand = new Random(42);
    for(int i=1970; i < 2038; ++i) {
      row = (OrcStruct) rows.next(row);
      assertEquals(new TimestampWritable(Timestamp.valueOf(i + "-05-05 12:34:56." + i)),
          row.getFieldValue(0));
      if ((i & 1) == 0) {
        assertEquals(0, union.getTag());
        assertEquals(new IntWritable(i*i), union.getObject());
      } else {
        assertEquals(1, union.getTag());
        assertEquals(new Text(Integer.toString(i * i)), union.getObject());
      }
      assertEquals(new HiveDecimalWritable(HiveDecimal.create(new BigInteger(64, rand),
                                   rand.nextInt(18))), row.getFieldValue(2));
    }
    for(int i=0; i < 5000; ++i) {
      row = (OrcStruct) rows.next(row);
      assertEquals(new IntWritable(1732050807), union.getObject());
    }
    row = (OrcStruct) rows.next(row);
    assertEquals(new IntWritable(0), union.getObject());
    row = (OrcStruct) rows.next(row);
    assertEquals(new IntWritable(10), union.getObject());
    row = (OrcStruct) rows.next(row);
    assertEquals(new IntWritable(138), union.getObject());
    assertEquals(false, rows.hasNext());
    assertEquals(1.0, rows.getProgress(), 0.00001);
    assertEquals(reader.getNumberOfRows(), rows.getRowNumber());
    rows.seekToRow(1);
    row = (OrcStruct) rows.next(row);
    assertEquals(new TimestampWritable(Timestamp.valueOf("2000-03-20 12:00:00.123456789")),
        row.getFieldValue(0));
    assertEquals(1, union.getTag());
    assertEquals(new Text("hello"), union.getObject());
    assertEquals(new HiveDecimalWritable(HiveDecimal.create("-5643.234")), row.getFieldValue(2));
    rows.close();
  }

  /**
   * Read and write a randomly generated snappy file.
   * @throws Exception
   */
  @Test
  public void testSnappy() throws Exception {
    TypeDescription schema = createInnerSchema();
    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .setSchema(schema)
                                         .stripeSize(1000)
                                         .compress(CompressionKind.SNAPPY)
                                         .bufferSize(100));
    VectorizedRowBatch batch = schema.createRowBatch();
    Random rand = new Random(12);
    batch.size = 1000;
    for(int b=0; b < 10; ++b) {
      for (int r=0; r < 1000; ++r) {
        ((LongColumnVector) batch.cols[0]).vector[r] = rand.nextInt();
        ((BytesColumnVector) batch.cols[1]).setVal(r,
            Integer.toHexString(rand.nextInt()).getBytes());
      }
      writer.addRowBatch(batch);
    }
    writer.close();
    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    rand = new Random(12);
    OrcStruct row = null;
    for(int i=0; i < 10000; ++i) {
      assertEquals(true, rows.hasNext());
      row = (OrcStruct) rows.next(row);
      assertEquals(rand.nextInt(), ((IntWritable) row.getFieldValue(0)).get());
      assertEquals(Integer.toHexString(rand.nextInt()),
          row.getFieldValue(1).toString());
    }
    assertEquals(false, rows.hasNext());
    rows.close();
  }

  /**
   * Read and write a randomly generated snappy file.
   * @throws Exception
   */
  @Test
  public void testWithoutIndex() throws Exception {
    TypeDescription schema = createInnerSchema();
    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .setSchema(schema)
                                         .stripeSize(5000)
                                         .compress(CompressionKind.SNAPPY)
                                         .bufferSize(1000)
                                         .rowIndexStride(0));
    VectorizedRowBatch batch = schema.createRowBatch();
    Random rand = new Random(24);
    batch.size = 5;
    for(int c=0; c < batch.cols.length; ++c) {
      batch.cols[c].setRepeating(true);
    }
    for(int i=0; i < 10000; ++i) {
      ((LongColumnVector) batch.cols[0]).vector[0] = rand.nextInt();
      ((BytesColumnVector) batch.cols[1])
          .setVal(0, Integer.toBinaryString(rand.nextInt()).getBytes());
      writer.addRowBatch(batch);
    }
    writer.close();
    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    assertEquals(50000, reader.getNumberOfRows());
    assertEquals(0, reader.getRowIndexStride());
    StripeInformation stripe = reader.getStripes().iterator().next();
    assertEquals(true, stripe.getDataLength() != 0);
    assertEquals(0, stripe.getIndexLength());
    RecordReader rows = reader.rows();
    rand = new Random(24);
    OrcStruct row = null;
    for(int i=0; i < 10000; ++i) {
      int intVal = rand.nextInt();
      String strVal = Integer.toBinaryString(rand.nextInt());
      for(int j=0; j < 5; ++j) {
        assertEquals(true, rows.hasNext());
        row = (OrcStruct) rows.next(row);
        assertEquals(intVal, ((IntWritable) row.getFieldValue(0)).get());
        assertEquals(strVal, row.getFieldValue(1).toString());
      }
    }
    assertEquals(false, rows.hasNext());
    rows.close();
  }

  @Test
  public void testSeek() throws Exception {
    TypeDescription schema = createBigRowSchema();
    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .setSchema(schema)
                                         .stripeSize(200000)
                                         .bufferSize(65536)
                                         .rowIndexStride(1000));
    VectorizedRowBatch batch = schema.createRowBatch();
    Random rand = new Random(42);
    final int COUNT=32768;
    long[] intValues= new long[COUNT];
    double[] doubleValues = new double[COUNT];
    String[] stringValues = new String[COUNT];
    BytesWritable[] byteValues = new BytesWritable[COUNT];
    String[] words = new String[128];
    for(int i=0; i < words.length; ++i) {
      words[i] = Integer.toHexString(rand.nextInt());
    }
    for(int i=0; i < COUNT/2; ++i) {
      intValues[2*i] = rand.nextLong();
      intValues[2*i+1] = intValues[2*i];
      stringValues[2*i] = words[rand.nextInt(words.length)];
      stringValues[2*i+1] = stringValues[2*i];
    }
    for(int i=0; i < COUNT; ++i) {
      doubleValues[i] = rand.nextDouble();
      byte[] buf = new byte[20];
      rand.nextBytes(buf);
      byteValues[i] = new BytesWritable(buf);
    }
    for(int i=0; i < COUNT; ++i) {
      appendRandomRow(batch, intValues, doubleValues, stringValues,
          byteValues, words, i);
      if (batch.size == 1024) {
        writer.addRowBatch(batch);
        batch.reset();
      }
    }
    if (batch.size != 0) {
      writer.addRowBatch(batch);
    }
    writer.close();
    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    assertEquals(COUNT, reader.getNumberOfRows());
    RecordReader rows = reader.rows();
    // get the row index
    MetadataReader meta = ((RecordReaderImpl) rows).getMetadataReader();
    OrcIndex index =
        meta.readRowIndex(reader.getStripes().get(0), null, null, null, null,
            null);
    // check the primitive columns to make sure they have the right number of
    // items in the first row group
    for(int c=1; c < 9; ++c) {
      OrcProto.RowIndex colIndex = index.getRowGroupIndex()[c];
      assertEquals(1000,
          colIndex.getEntry(0).getStatistics().getNumberOfValues());
    }
    OrcStruct row = null;
    for(int i=COUNT-1; i >= 0; --i) {
      rows.seekToRow(i);
      row = (OrcStruct) rows.next(row);
      BigRow expected = createRandomRow(intValues, doubleValues,
          stringValues, byteValues, words, i);
      assertEquals(expected.boolean1.booleanValue(),
          ((BooleanWritable) row.getFieldValue(0)).get());
      assertEquals(expected.byte1.byteValue(),
          ((ByteWritable) row.getFieldValue(1)).get());
      assertEquals(expected.short1.shortValue(),
          ((ShortWritable) row.getFieldValue(2)).get());
      assertEquals(expected.int1.intValue(),
          ((IntWritable) row.getFieldValue(3)).get());
      assertEquals(expected.long1.longValue(),
          ((LongWritable) row.getFieldValue(4)).get());
      assertEquals(expected.float1,
          ((FloatWritable) row.getFieldValue(5)).get(), 0.0001);
      assertEquals(expected.double1,
          ((DoubleWritable) row.getFieldValue(6)).get(), 0.0001);
      assertEquals(expected.bytes1, row.getFieldValue(7));
      assertEquals(expected.string1, row.getFieldValue(8));
      List<InnerStruct> expectedList = expected.middle.list;
      List<OrcStruct> actualList =
          (List<OrcStruct>) ((OrcStruct) row.getFieldValue(9)).getFieldValue(0);
      compareList(expectedList, actualList, "middle list " + i);
      compareList(expected.list, (List<OrcStruct>) row.getFieldValue(10),
          "list " + i);
    }
    rows.close();
    Iterator<StripeInformation> stripeIterator =
      reader.getStripes().iterator();
    long offsetOfStripe2 = 0;
    long offsetOfStripe4 = 0;
    long lastRowOfStripe2 = 0;
    for(int i = 0; i < 5; ++i) {
      StripeInformation stripe = stripeIterator.next();
      if (i < 2) {
        lastRowOfStripe2 += stripe.getNumberOfRows();
      } else if (i == 2) {
        offsetOfStripe2 = stripe.getOffset();
        lastRowOfStripe2 += stripe.getNumberOfRows() - 1;
      } else if (i == 4) {
        offsetOfStripe4 = stripe.getOffset();
      }
    }
    boolean[] columns = new boolean[reader.getStatistics().length];
    columns[5] = true; // long colulmn
    columns[9] = true; // text column
    rows = reader.rowsOptions(new Reader.Options()
        .range(offsetOfStripe2, offsetOfStripe4 - offsetOfStripe2)
        .include(columns));
    rows.seekToRow(lastRowOfStripe2);
    for(int i = 0; i < 2; ++i) {
      row = (OrcStruct) rows.next(row);
      BigRow expected = createRandomRow(intValues, doubleValues,
                                        stringValues, byteValues, words,
                                        (int) (lastRowOfStripe2 + i));

      assertEquals(expected.long1.longValue(),
          ((LongWritable) row.getFieldValue(4)).get());
      assertEquals(expected.string1, row.getFieldValue(8));
    }
    rows.close();
  }

  private void compareInner(InnerStruct expect,
                            OrcStruct actual,
                            String context) throws Exception {
    if (expect == null || actual == null) {
      assertEquals(context, null, expect);
      assertEquals(context, null, actual);
    } else {
      assertEquals(context, expect.int1,
          ((IntWritable) actual.getFieldValue(0)).get());
      assertEquals(context, expect.string1, actual.getFieldValue(1));
    }
  }

  private void compareList(List<InnerStruct> expect,
                           List<OrcStruct> actual,
                           String context) throws Exception {
    assertEquals(context, expect.size(), actual.size());
    for(int j=0; j < expect.size(); ++j) {
      compareInner(expect.get(j), actual.get(j), context + " at " + j);
    }
  }

  private void appendRandomRow(VectorizedRowBatch batch,
                               long[] intValues, double[] doubleValues,
                               String[] stringValues,
                               BytesWritable[] byteValues,
                               String[] words, int i) {
    InnerStruct inner = new InnerStruct((int) intValues[i], stringValues[i]);
    InnerStruct inner2 = new InnerStruct((int) (intValues[i] >> 32),
        words[i % words.length] + "-x");
    setBigRow(batch, batch.size++, (intValues[i] & 1) == 0, (byte) intValues[i],
        (short) intValues[i], (int) intValues[i], intValues[i],
        (float) doubleValues[i], doubleValues[i], byteValues[i], stringValues[i],
        new MiddleStruct(inner, inner2), list(), map(inner, inner2));
  }

  private BigRow createRandomRow(long[] intValues, double[] doubleValues,
                                 String[] stringValues,
                                 BytesWritable[] byteValues,
                                 String[] words, int i) {
    InnerStruct inner = new InnerStruct((int) intValues[i], stringValues[i]);
    InnerStruct inner2 = new InnerStruct((int) (intValues[i] >> 32),
        words[i % words.length] + "-x");
    return new BigRow((intValues[i] & 1) == 0, (byte) intValues[i],
        (short) intValues[i], (int) intValues[i], intValues[i],
        (float) doubleValues[i], doubleValues[i], byteValues[i],stringValues[i],
        new MiddleStruct(inner, inner2), list(), map(inner,inner2));
  }

  private static class MyMemoryManager extends MemoryManager {
    final long totalSpace;
    double rate;
    Path path = null;
    long lastAllocation = 0;
    int rows = 0;
    Callback callback;

    MyMemoryManager(Configuration conf, long totalSpace, double rate) {
      super(conf);
      this.totalSpace = totalSpace;
      this.rate = rate;
    }

    @Override
    public void addWriter(Path path, long requestedAllocation,
                   Callback callback) {
      this.path = path;
      this.lastAllocation = requestedAllocation;
      this.callback = callback;
    }

    @Override
    public synchronized void removeWriter(Path path) {
      this.path = null;
      this.lastAllocation = 0;
    }

    @Override
    public long getTotalMemoryPool() {
      return totalSpace;
    }

    @Override
    public double getAllocationScale() {
      return rate;
    }

    @Override
    public void addedRow(int count) throws IOException {
      rows += count;
      if (rows % 100 == 0) {
        callback.checkMemory(rate);
      }
    }
  }

  @Test
  public void testMemoryManagementV11() throws Exception {
    TypeDescription schema = createInnerSchema();
    MyMemoryManager memory = new MyMemoryManager(conf, 10000, 0.1);
    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .compress(CompressionKind.NONE)
            .stripeSize(50000)
            .bufferSize(100)
            .rowIndexStride(0)
            .memory(memory)
            .version(OrcFile.Version.V_0_11));
    assertEquals(testFilePath, memory.path);
    VectorizedRowBatch batch = schema.createRowBatch();
    batch.size = 1;
    for(int i=0; i < 2500; ++i) {
      ((LongColumnVector) batch.cols[0]).vector[0] = i * 300;
      ((BytesColumnVector) batch.cols[1]).setVal(0,
          Integer.toHexString(10*i).getBytes());
      writer.addRowBatch(batch);
    }
    writer.close();
    assertEquals(null, memory.path);
    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    int i = 0;
    for(StripeInformation stripe: reader.getStripes()) {
      i += 1;
      assertTrue("stripe " + i + " is too long at " + stripe.getDataLength(),
          stripe.getDataLength() < 5000);
    }
    assertEquals(25, i);
    assertEquals(2500, reader.getNumberOfRows());
  }

  @Test
  public void testMemoryManagementV12() throws Exception {
    TypeDescription schema = createInnerSchema();
    MyMemoryManager memory = new MyMemoryManager(conf, 10000, 0.1);
    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .setSchema(schema)
                                         .compress(CompressionKind.NONE)
                                         .stripeSize(50000)
                                         .bufferSize(100)
                                         .rowIndexStride(0)
                                         .memory(memory)
                                         .version(OrcFile.Version.V_0_12));
    VectorizedRowBatch batch = schema.createRowBatch();
    assertEquals(testFilePath, memory.path);
    batch.size = 1;
    for(int i=0; i < 2500; ++i) {
      ((LongColumnVector) batch.cols[0]).vector[0] = i * 300;
      ((BytesColumnVector) batch.cols[1]).setVal(0,
          Integer.toHexString(10*i).getBytes());
      writer.addRowBatch(batch);
    }
    writer.close();
    assertEquals(null, memory.path);
    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    int i = 0;
    for(StripeInformation stripe: reader.getStripes()) {
      i += 1;
      assertTrue("stripe " + i + " is too long at " + stripe.getDataLength(),
          stripe.getDataLength() < 5000);
    }
    // with HIVE-7832, the dictionaries will be disabled after writing the first
    // stripe as there are too many distinct values. Hence only 3 stripes as
    // compared to 25 stripes in version 0.11 (above test case)
    assertEquals(3, i);
    assertEquals(2500, reader.getNumberOfRows());
  }

  @Test
  public void testPredicatePushdown() throws Exception {
    TypeDescription schema = createInnerSchema();
    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .stripeSize(400000L)
            .compress(CompressionKind.NONE)
            .bufferSize(500)
            .rowIndexStride(1000));
    VectorizedRowBatch batch = schema.createRowBatch();
    batch.ensureSize(3500);
    batch.size = 3500;
    for(int i=0; i < 3500; ++i) {
      ((LongColumnVector) batch.cols[0]).vector[i] = i * 300;
      ((BytesColumnVector) batch.cols[1]).setVal(i,
          Integer.toHexString(10*i).getBytes());
    }
    writer.addRowBatch(batch);
    writer.close();
    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    assertEquals(3500, reader.getNumberOfRows());

    SearchArgument sarg = SearchArgumentFactory.newBuilder()
        .startAnd()
          .startNot()
             .lessThan("int1", PredicateLeaf.Type.LONG, 300000L)
          .end()
          .lessThan("int1", PredicateLeaf.Type.LONG, 600000L)
        .end()
        .build();
    RecordReader rows = reader.rowsOptions(new Reader.Options()
        .range(0L, Long.MAX_VALUE)
        .include(new boolean[]{true, true, true})
        .searchArgument(sarg, new String[]{null, "int1", "string1"}));
    assertEquals(1000L, rows.getRowNumber());
    OrcStruct row = null;
    for(int i=1000; i < 2000; ++i) {
      assertTrue(rows.hasNext());
      row = (OrcStruct) rows.next(row);
      assertEquals(300 * i, ((IntWritable) row.getFieldValue(0)).get());
      assertEquals(Integer.toHexString(10*i), row.getFieldValue(1).toString());
    }
    assertTrue(!rows.hasNext());
    assertEquals(3500, rows.getRowNumber());

    // look through the file with no rows selected
    sarg = SearchArgumentFactory.newBuilder()
        .startAnd()
          .lessThan("int1", PredicateLeaf.Type.LONG, 0L)
        .end()
        .build();
    rows = reader.rowsOptions(new Reader.Options()
        .range(0L, Long.MAX_VALUE)
        .include(new boolean[]{true, true, true})
        .searchArgument(sarg, new String[]{null, "int1", "string1"}));
    assertEquals(3500L, rows.getRowNumber());
    assertTrue(!rows.hasNext());

    // select first 100 and last 100 rows
    sarg = SearchArgumentFactory.newBuilder()
        .startOr()
          .lessThan("int1", PredicateLeaf.Type.LONG, 300L * 100)
          .startNot()
            .lessThan("int1", PredicateLeaf.Type.LONG, 300L * 3400)
          .end()
        .end()
        .build();
    rows = reader.rowsOptions(new Reader.Options()
        .range(0L, Long.MAX_VALUE)
        .include(new boolean[]{true, true, true})
        .searchArgument(sarg, new String[]{null, "int1", "string1"}));
    row = null;
    for(int i=0; i < 1000; ++i) {
      assertTrue(rows.hasNext());
      assertEquals(i, rows.getRowNumber());
      row = (OrcStruct) rows.next(row);
      assertEquals(300 * i, ((IntWritable) row.getFieldValue(0)).get());
      assertEquals(Integer.toHexString(10*i), row.getFieldValue(1).toString());
    }
    for(int i=3000; i < 3500; ++i) {
      assertTrue(rows.hasNext());
      assertEquals(i, rows.getRowNumber());
      row = (OrcStruct) rows.next(row);
      assertEquals(300 * i, ((IntWritable) row.getFieldValue(0)).get());
      assertEquals(Integer.toHexString(10*i), row.getFieldValue(1).toString());
    }
    assertTrue(!rows.hasNext());
    assertEquals(3500, rows.getRowNumber());
  }

  private static String pad(String value, int length) {
    if (value.length() == length) {
      return value;
    } else if (value.length() > length) {
      return value.substring(0, length);
    } else {
      StringBuilder buf = new StringBuilder();
      buf.append(value);
      for(int i=0; i < length - value.length(); ++i) {
        buf.append(' ');
      }
      return buf.toString();
    }
  }

  /**
   * Test all of the types that have distinct ORC writers using the vectorized
   * writer with different combinations of repeating and null values.
   * @throws Exception
   */
  @Test
  public void testRepeating() throws Exception {
    // create a row type with each type that has a unique writer
    // really just folds short, int, and long together
    TypeDescription schema = TypeDescription.createStruct()
        .addField("bin", TypeDescription.createBinary())
        .addField("bool", TypeDescription.createBoolean())
        .addField("byte", TypeDescription.createByte())
        .addField("long", TypeDescription.createLong())
        .addField("float", TypeDescription.createFloat())
        .addField("double", TypeDescription.createDouble())
        .addField("date", TypeDescription.createDate())
        .addField("time", TypeDescription.createTimestamp())
        .addField("dec", TypeDescription.createDecimal()
            .withPrecision(20).withScale(6))
        .addField("string", TypeDescription.createString())
        .addField("char", TypeDescription.createChar().withMaxLength(10))
        .addField("vc", TypeDescription.createVarchar().withMaxLength(10))
        .addField("struct", TypeDescription.createStruct()
            .addField("sub1", TypeDescription.createInt()))
        .addField("union", TypeDescription.createUnion()
            .addUnionChild(TypeDescription.createString())
            .addUnionChild(TypeDescription.createInt()))
        .addField("list", TypeDescription
            .createList(TypeDescription.createInt()))
        .addField("map",
            TypeDescription.createMap(TypeDescription.createString(),
                TypeDescription.createString()));
    VectorizedRowBatch batch = schema.createRowBatch();
    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .rowIndexStride(1000));

    // write 1024 repeating nulls
    batch.size = 1024;
    for(int c = 0; c < batch.cols.length; ++c) {
      batch.cols[c].setRepeating(true);
      batch.cols[c].noNulls = false;
      batch.cols[c].isNull[0] = true;
    }
    writer.addRowBatch(batch);

    // write 1024 repeating non-null
    for(int c =0; c < batch.cols.length; ++c) {
      batch.cols[c].isNull[0] = false;
    }
    ((BytesColumnVector) batch.cols[0]).setVal(0, "Horton".getBytes());
    ((LongColumnVector) batch.cols[1]).vector[0] = 1;
    ((LongColumnVector) batch.cols[2]).vector[0] = 130;
    ((LongColumnVector) batch.cols[3]).vector[0] = 0x123456789abcdef0L;
    ((DoubleColumnVector) batch.cols[4]).vector[0] = 1.125;
    ((DoubleColumnVector) batch.cols[5]).vector[0] = 0.0009765625;
    ((LongColumnVector) batch.cols[6]).vector[0] =
        new DateWritable(new Date(111, 6, 1)).getDays();
    ((LongColumnVector) batch.cols[7]).vector[0] =
        TimestampUtils.getTimeNanoSec(new Timestamp(115, 9, 23, 10, 11, 59,
            999999999));
    ((DecimalColumnVector) batch.cols[8]).vector[0] =
        new HiveDecimalWritable("1.234567");
    ((BytesColumnVector) batch.cols[9]).setVal(0, "Echelon".getBytes());
    ((BytesColumnVector) batch.cols[10]).setVal(0, "Juggernaut".getBytes());
    ((BytesColumnVector) batch.cols[11]).setVal(0, "Dreadnaught".getBytes());
    ((LongColumnVector) ((StructColumnVector) batch.cols[12]).fields[0])
        .vector[0] = 123;
    ((UnionColumnVector) batch.cols[13]).tags[0] = 1;
    ((LongColumnVector) ((UnionColumnVector) batch.cols[13]).fields[1])
        .vector[0] = 1234;
    ((ListColumnVector) batch.cols[14]).offsets[0] = 0;
    ((ListColumnVector) batch.cols[14]).lengths[0] = 3;
    ((ListColumnVector) batch.cols[14]).child.isRepeating = true;
    ((LongColumnVector) ((ListColumnVector) batch.cols[14]).child).vector[0]
        = 31415;
    ((MapColumnVector) batch.cols[15]).offsets[0] = 0;
    ((MapColumnVector) batch.cols[15]).lengths[0] = 3;
    ((MapColumnVector) batch.cols[15]).values.isRepeating = true;
    ((BytesColumnVector) ((MapColumnVector) batch.cols[15]).keys)
        .setVal(0, "ORC".getBytes());
    ((BytesColumnVector) ((MapColumnVector) batch.cols[15]).keys)
        .setVal(1, "Hive".getBytes());
    ((BytesColumnVector) ((MapColumnVector) batch.cols[15]).keys)
        .setVal(2, "LLAP".getBytes());
    ((BytesColumnVector) ((MapColumnVector) batch.cols[15]).values)
        .setVal(0, "fast".getBytes());
    writer.addRowBatch(batch);

    // write 1024 null without repeat
    for(int c = 0; c < batch.cols.length; ++c) {
      batch.cols[c].setRepeating(false);
      batch.cols[c].noNulls = false;
      Arrays.fill(batch.cols[c].isNull, true);
    }
    writer.addRowBatch(batch);

    // add 1024 rows of non-null, non-repeating
    batch.reset();
    batch.size = 1024;
    ((ListColumnVector) batch.cols[14]).child.ensureSize(3 * 1024, false);
    ((MapColumnVector) batch.cols[15]).keys.ensureSize(3 * 1024, false);
    ((MapColumnVector) batch.cols[15]).values.ensureSize(3 * 1024, false);
    for(int r=0; r < 1024; ++r) {
      ((BytesColumnVector) batch.cols[0]).setVal(r,
          Integer.toHexString(r).getBytes());
      ((LongColumnVector) batch.cols[1]).vector[r] = r % 2;
      ((LongColumnVector) batch.cols[2]).vector[r] = (r % 255);
      ((LongColumnVector) batch.cols[3]).vector[r] = 31415L * r;
      ((DoubleColumnVector) batch.cols[4]).vector[r] = 1.125 * r;
      ((DoubleColumnVector) batch.cols[5]).vector[r] = 0.0009765625 * r;
      ((LongColumnVector) batch.cols[6]).vector[r] =
          new DateWritable(new Date(111, 6, 1)).getDays() + r;
      ((LongColumnVector) batch.cols[7]).vector[r] =
          TimestampUtils.getTimeNanoSec(new Timestamp(115, 9, 23, 10, 11, 59,
              999999999)) + r * 1000000000L;
      ((DecimalColumnVector) batch.cols[8]).vector[r] =
          new HiveDecimalWritable("1.234567");
      ((BytesColumnVector) batch.cols[9]).setVal(r,
          Integer.toString(r).getBytes());
      ((BytesColumnVector) batch.cols[10]).setVal(r,
          Integer.toHexString(r).getBytes());
      ((BytesColumnVector) batch.cols[11]).setVal(r,
          Integer.toHexString(r * 128).getBytes());
      ((LongColumnVector) ((StructColumnVector) batch.cols[12]).fields[0])
          .vector[r] = r + 13;
      ((UnionColumnVector) batch.cols[13]).tags[r] = 1;
      ((LongColumnVector) ((UnionColumnVector) batch.cols[13]).fields[1])
          .vector[r] = r + 42;
      ((ListColumnVector) batch.cols[14]).offsets[r] = 3 * r;
      ((ListColumnVector) batch.cols[14]).lengths[r] = 3;
      for(int i=0; i < 3; ++i) {
        ((LongColumnVector) ((ListColumnVector) batch.cols[14]).child)
            .vector[3 * r + i] = 31415 + i;
      }
      ((MapColumnVector) batch.cols[15]).offsets[r] = 3 * r;
      ((MapColumnVector) batch.cols[15]).lengths[r] = 3;
      for(int i=0; i < 3; ++i) {
        ((BytesColumnVector) ((MapColumnVector) batch.cols[15]).keys)
            .setVal(3 * r + i, Integer.toHexString(3 * r + i).getBytes());
        ((BytesColumnVector) ((MapColumnVector) batch.cols[15]).values)
            .setVal(3 * r + i, Integer.toString(3 * r + i).getBytes());
      }
    }
    writer.addRowBatch(batch);

    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));

    // check the stats
    ColumnStatistics[] stats = reader.getStatistics();
    assertEquals(4096, stats[0].getNumberOfValues());
    assertEquals(false, stats[0].hasNull());
    for(TypeDescription colType: schema.getChildren()) {
      assertEquals("count on " + colType.getId(),
          2048, stats[colType.getId()].getNumberOfValues());
      assertEquals("hasNull on " + colType.getId(),
          true, stats[colType.getId()].hasNull());
    }
    assertEquals(8944, ((BinaryColumnStatistics) stats[1]).getSum());
    assertEquals(1536, ((BooleanColumnStatistics) stats[2]).getTrueCount());
    assertEquals(512, ((BooleanColumnStatistics) stats[2]).getFalseCount());
    assertEquals(false, ((IntegerColumnStatistics) stats[4]).isSumDefined());
    assertEquals(0, ((IntegerColumnStatistics) stats[4]).getMinimum());
    assertEquals(0x123456789abcdef0L,
        ((IntegerColumnStatistics) stats[4]).getMaximum());
    assertEquals("0", ((StringColumnStatistics) stats[10]).getMinimum());
    assertEquals("Echelon", ((StringColumnStatistics) stats[10]).getMaximum());
    assertEquals(10154, ((StringColumnStatistics) stats[10]).getSum());
    assertEquals("0         ",
        ((StringColumnStatistics) stats[11]).getMinimum());
    assertEquals("ff        ",
        ((StringColumnStatistics) stats[11]).getMaximum());
    assertEquals(20480, ((StringColumnStatistics) stats[11]).getSum());
    assertEquals("0",
        ((StringColumnStatistics) stats[12]).getMinimum());
    assertEquals("ff80",
        ((StringColumnStatistics) stats[12]).getMaximum());
    assertEquals(14813, ((StringColumnStatistics) stats[12]).getSum());

    RecordReader rows = reader.rows();
    OrcStruct row = null;

    // read the 1024 nulls
    for(int r=0; r < 1024; ++r) {
      assertEquals(true, rows.hasNext());
      row = (OrcStruct) rows.next(row);
      for(int f=0; f < row.getNumFields(); ++f) {
        assertEquals("non-null on row " + r + " field " + f,
            null, row.getFieldValue(f));
      }
    }

    // read the 1024 repeat values
    for(int r=0; r < 1024; ++r) {
      assertEquals(true, rows.hasNext());
      row = (OrcStruct) rows.next(row);
      assertEquals("row " + r, "48 6f 72 74 6f 6e",
          row.getFieldValue(0).toString());
      assertEquals("row " + r, "true", row.getFieldValue(1).toString());
      assertEquals("row " + r, "-126", row.getFieldValue(2).toString());
      assertEquals("row " + r, "1311768467463790320",
          row.getFieldValue(3).toString());
      assertEquals("row " + r, "1.125", row.getFieldValue(4).toString());
      assertEquals("row " + r, "9.765625E-4", row.getFieldValue(5).toString());
      assertEquals("row " + r, "2011-07-01", row.getFieldValue(6).toString());
      assertEquals("row " + r, "2015-10-23 10:11:59.999999999",
          row.getFieldValue(7).toString());
      assertEquals("row " + r, "1.234567", row.getFieldValue(8).toString());
      assertEquals("row " + r, "Echelon", row.getFieldValue(9).toString());
      assertEquals("row " + r, "Juggernaut", row.getFieldValue(10).toString());
      assertEquals("row " + r, "Dreadnaugh", row.getFieldValue(11).toString());
      assertEquals("row " + r, "{123}", row.getFieldValue(12).toString());
      assertEquals("row " + r, "union(1, 1234)",
          row.getFieldValue(13).toString());
      assertEquals("row " + r, "[31415, 31415, 31415]",
          row.getFieldValue(14).toString());
      assertEquals("row " + r, "{ORC=fast, Hive=fast, LLAP=fast}",
          row.getFieldValue(15).toString());
    }

    // read the second set of 1024 nulls
    for(int r=0; r < 1024; ++r) {
      assertEquals(true, rows.hasNext());
      row = (OrcStruct) rows.next(row);
      for(int f=0; f < row.getNumFields(); ++f) {
        assertEquals("non-null on row " + r + " field " + f,
            null, row.getFieldValue(f));
      }
    }
    for(int r=0; r < 1024; ++r) {
      assertEquals(true, rows.hasNext());
      row = (OrcStruct) rows.next(row);
      byte[] hex = Integer.toHexString(r).getBytes();
      StringBuilder expected = new StringBuilder();
      for(int i=0; i < hex.length; ++i) {
        if (i != 0) {
          expected.append(' ');
        }
        expected.append(Integer.toHexString(hex[i]));
      }
      assertEquals("row " + r, expected.toString(),
          row.getFieldValue(0).toString());
      assertEquals("row " + r, r % 2 == 1 ? "true" : "false",
          row.getFieldValue(1).toString());
      assertEquals("row " + r, Integer.toString((byte) (r % 255)),
          row.getFieldValue(2).toString());
      assertEquals("row " + r, Long.toString(31415L * r),
          row.getFieldValue(3).toString());
      assertEquals("row " + r, Float.toString(1.125F * r),
          row.getFieldValue(4).toString());
      assertEquals("row " + r, Double.toString(0.0009765625 * r),
          row.getFieldValue(5).toString());
      assertEquals("row " + r, new Date(111, 6, 1 + r).toString(),
          row.getFieldValue(6).toString());
      assertEquals("row " + r,
          new Timestamp(115, 9, 23, 10, 11, 59 + r, 999999999).toString(),
          row.getFieldValue(7).toString());
      assertEquals("row " + r, "1.234567", row.getFieldValue(8).toString());
      assertEquals("row " + r, Integer.toString(r),
          row.getFieldValue(9).toString());
      assertEquals("row " + r, pad(Integer.toHexString(r), 10),
          row.getFieldValue(10).toString());
      assertEquals("row " + r, Integer.toHexString(r * 128),
          row.getFieldValue(11).toString());
      assertEquals("row " + r, "{" + Integer.toString(r + 13) + "}",
          row.getFieldValue(12).toString());
      assertEquals("row " + r, "union(1, " + Integer.toString(r + 42) + ")",
          row.getFieldValue(13).toString());
      assertEquals("row " + r, "[31415, 31416, 31417]",
          row.getFieldValue(14).toString());
      expected = new StringBuilder();
      expected.append('{');
      expected.append(Integer.toHexString(3 * r));
      expected.append('=');
      expected.append(3 * r);
      expected.append(", ");
      expected.append(Integer.toHexString(3 * r + 1));
      expected.append('=');
      expected.append(3 * r + 1);
      expected.append(", ");
      expected.append(Integer.toHexString(3 * r + 2));
      expected.append('=');
      expected.append(3 * r + 2);
      expected.append('}');
      assertEquals("row " + r, expected.toString(),
          row.getFieldValue(15).toString());
    }

    // should have no more rows
    assertEquals(false, rows.hasNext());
  }

  private static String makeString(BytesColumnVector vector, int row) {
    if (vector.isRepeating) {
      row = 0;
    }
    if (vector.noNulls || !vector.isNull[row]) {
      return new String(vector.vector[row], vector.start[row],
          vector.length[row]);
    } else {
      return null;
    }
  }

  /**
   * Test the char and varchar padding and truncation.
   * @throws Exception
   */
  @Test
  public void testStringPadding() throws Exception {
    TypeDescription schema = TypeDescription.createStruct()
        .addField("char", TypeDescription.createChar().withMaxLength(10))
        .addField("varchar", TypeDescription.createVarchar().withMaxLength(10));
    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema));
    VectorizedRowBatch batch = schema.createRowBatch();
    batch.size = 4;
    for(int c=0; c < batch.cols.length; ++c) {
      ((BytesColumnVector) batch.cols[c]).setVal(0, "".getBytes());
      ((BytesColumnVector) batch.cols[c]).setVal(1, "xyz".getBytes());
      ((BytesColumnVector) batch.cols[c]).setVal(2, "0123456789".getBytes());
      ((BytesColumnVector) batch.cols[c]).setVal(3,
          "0123456789abcdef".getBytes());
    }
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf));
    RecordReader rows = reader.rows();
    batch = rows.nextBatch(null);
    assertEquals(4, batch.size);
    // ORC currently trims the output strings. See HIVE-12286
    assertEquals("",
        makeString((BytesColumnVector) batch.cols[0], 0));
    assertEquals("xyz",
        makeString((BytesColumnVector) batch.cols[0], 1));
    assertEquals("0123456789",
        makeString((BytesColumnVector) batch.cols[0], 2));
    assertEquals("0123456789",
        makeString((BytesColumnVector) batch.cols[0], 3));
    assertEquals("",
        makeString((BytesColumnVector) batch.cols[1], 0));
    assertEquals("xyz",
        makeString((BytesColumnVector) batch.cols[1], 1));
    assertEquals("0123456789",
        makeString((BytesColumnVector) batch.cols[1], 2));
    assertEquals("0123456789",
        makeString((BytesColumnVector) batch.cols[1], 3));
  }

  /**
   * A test case that tests the case where you add a repeating batch
   * to a column that isn't using dictionary encoding.
   * @throws Exception
   */
  @Test
  public void testNonDictionaryRepeatingString() throws Exception {
    TypeDescription schema = TypeDescription.createStruct()
        .addField("str", TypeDescription.createString());
    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .rowIndexStride(1000));
    VectorizedRowBatch batch = schema.createRowBatch();
    batch.size = 1024;
    for(int r=0; r < batch.size; ++r) {
      ((BytesColumnVector) batch.cols[0]).setVal(r,
          Integer.toString(r * 10001).getBytes());
    }
    writer.addRowBatch(batch);
    batch.cols[0].isRepeating = true;
    ((BytesColumnVector) batch.cols[0]).setVal(0, "Halloween".getBytes());
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf));
    RecordReader rows = reader.rows();
    batch = rows.nextBatch(null);
    assertEquals(1024, batch.size);
    for(int r=0; r < 1024; ++r) {
      assertEquals(Integer.toString(r * 10001),
          makeString((BytesColumnVector) batch.cols[0], r));
    }
    batch = rows.nextBatch(batch);
    assertEquals(1024, batch.size);
    for(int r=0; r < 1024; ++r) {
      assertEquals("Halloween",
          makeString((BytesColumnVector) batch.cols[0], r));
    }
    assertEquals(false, rows.hasNext());
  }

  @Test
  public void testStructs() throws Exception {
    TypeDescription schema = TypeDescription.createStruct()
        .addField("struct", TypeDescription.createStruct()
            .addField("inner", TypeDescription.createLong()));
    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf).setSchema(schema));
    VectorizedRowBatch batch = schema.createRowBatch();
    batch.size = 1024;
    StructColumnVector outer = (StructColumnVector) batch.cols[0];
    outer.noNulls = false;
    for(int r=0; r < 1024; ++r) {
      if (r < 200 || (r >= 400 && r < 600) || r >= 800) {
        outer.isNull[r] = true;
      }
      ((LongColumnVector) outer.fields[0]).vector[r] = r;
    }
    writer.addRowBatch(batch);
    writer.close();
    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf));
    RecordReader rows = reader.rows();
    OrcStruct row = null;
    for(int r=0; r < 1024; ++r) {
      assertEquals(true, rows.hasNext());
      row = (OrcStruct) rows.next(row);
      OrcStruct inner = (OrcStruct) row.getFieldValue(0);
      if (r < 200 || (r >= 400 && r < 600) || r >= 800) {
        assertEquals("row " + r, null, inner);
      } else {
        assertEquals("row " + r, "{" + r + "}", inner.toString());
      }
    }
    assertEquals(false, rows.hasNext());
  }

  /**
   * Test Unions.
   * @throws Exception
   */
  @Test
  public void testUnions() throws Exception {
    TypeDescription schema = TypeDescription.createStruct()
        .addField("outer", TypeDescription.createUnion()
            .addUnionChild(TypeDescription.createInt())
            .addUnionChild(TypeDescription.createLong()));
    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf).setSchema(schema));
    VectorizedRowBatch batch = schema.createRowBatch();
    batch.size = 1024;
    UnionColumnVector outer = (UnionColumnVector) batch.cols[0];
    batch.cols[0].noNulls = false;
    for(int r=0; r < 1024; ++r) {
      if (r < 200) {
        outer.isNull[r] = true;
      } else if (r < 300) {
        outer.tags[r] = 0;
      } else if (r < 400) {
        outer.tags[r] = 1;
      } else if (r < 600) {
        outer.isNull[r] = true;
      } else if (r < 800) {
        outer.tags[r] = 1;
      } else if (r < 1000) {
        outer.isNull[r] = true;
      } else {
        outer.tags[r] = 1;
      }
      ((LongColumnVector) outer.fields[0]).vector[r] = r;
      ((LongColumnVector) outer.fields[1]).vector[r] = -r;
    }
    writer.addRowBatch(batch);
    writer.close();
    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf));
    RecordReader rows = reader.rows();
    OrcStruct row = null;
    for(int r=0; r < 1024; ++r) {
      assertEquals(true, rows.hasNext());
      row = (OrcStruct) rows.next(row);
      OrcUnion inner = (OrcUnion) row.getFieldValue(0);
      if (r < 200) {
        assertEquals("row " + r, null, inner);
      } else if (r < 300) {
        assertEquals("row " + r, "union(0, " + r +")", inner.toString());
      } else if (r < 400) {
        assertEquals("row " + r, "union(1, " + -r +")", inner.toString());
      } else if (r < 600) {
        assertEquals("row " + r, null, inner);
      } else if (r < 800) {
        assertEquals("row " + r, "union(1, " + -r +")", inner.toString());
      } else if (r < 1000) {
        assertEquals("row " + r, null, inner);
      } else {
        assertEquals("row " + r, "union(1, " + -r +")", inner.toString());
      }
    }
    assertEquals(false, rows.hasNext());
  }

  /**
   * Test lists and how they interact with the child column. In particular,
   * put nulls between back to back lists and then make some lists that
   * oper lap.
   * @throws Exception
   */
  @Test
  public void testLists() throws Exception {
    TypeDescription schema = TypeDescription.createStruct()
        .addField("list",
            TypeDescription.createList(TypeDescription.createLong()));
    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf).setSchema(schema));
    VectorizedRowBatch batch = schema.createRowBatch();
    batch.size = 1024;
    ListColumnVector list = (ListColumnVector) batch.cols[0];
    list.noNulls = false;
    for(int r=0; r < 1024; ++r) {
      if (r < 200) {
        list.isNull[r] = true;
      } else if (r < 300) {
        list.offsets[r] = r - 200;
        list.lengths[r] = 1;
      } else if (r < 400) {
        list.isNull[r] = true;
      } else if (r < 500) {
        list.offsets[r] = r - 300;
        list.lengths[r] = 1;
      } else if (r < 600) {
        list.isNull[r] = true;
      } else if (r < 700) {
        list.offsets[r] = r;
        list.lengths[r] = 2;
      } else {
        list.isNull[r] = true;
      }
      ((LongColumnVector) list.child).vector[r] = r * 10;
    }
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf));
    RecordReader rows = reader.rows();
    OrcStruct row = null;
    for(int r=0; r < 1024; ++r) {
      assertEquals(true, rows.hasNext());
      row = (OrcStruct) rows.next(row);
      List inner = (List) row.getFieldValue(0);
      if (r < 200) {
        assertEquals("row " + r, null, inner);
      } else if (r < 300) {
        assertEquals("row " + r, "[" + ((r - 200) * 10) + "]",
            inner.toString());
      } else if (r < 400) {
        assertEquals("row " + r, null, inner);
      } else if (r < 500) {
        assertEquals("row " + r, "[" + ((r - 300) * 10) + "]",
            inner.toString());
      } else if (r < 600) {
        assertEquals("row " + r, null, inner);
      } else if (r < 700) {
        assertEquals("row " + r, "[" + (10 * r) + ", " + (10 * (r + 1)) + "]",
            inner.toString());
      } else {
        assertEquals("row " + r, null, inner);
      }
    }
    assertEquals(false, rows.hasNext());
  }

  /**
   * Test maps and how they interact with the child column. In particular,
   * put nulls between back to back lists and then make some lists that
   * oper lap.
   * @throws Exception
   */
  @Test
  public void testMaps() throws Exception {
    TypeDescription schema = TypeDescription.createStruct()
        .addField("map",
            TypeDescription.createMap(TypeDescription.createLong(),
                TypeDescription.createLong()));
    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf).setSchema(schema));
    VectorizedRowBatch batch = schema.createRowBatch();
    batch.size = 1024;
    MapColumnVector map = (MapColumnVector) batch.cols[0];
    map.noNulls = false;
    for(int r=0; r < 1024; ++r) {
      if (r < 200) {
        map.isNull[r] = true;
      } else if (r < 300) {
        map.offsets[r] = r - 200;
        map.lengths[r] = 1;
      } else if (r < 400) {
        map.isNull[r] = true;
      } else if (r < 500) {
        map.offsets[r] = r - 300;
        map.lengths[r] = 1;
      } else if (r < 600) {
        map.isNull[r] = true;
      } else if (r < 700) {
        map.offsets[r] = r;
        map.lengths[r] = 2;
      } else {
        map.isNull[r] = true;
      }
      ((LongColumnVector) map.keys).vector[r] = r;
      ((LongColumnVector) map.values).vector[r] = r * 10;
    }
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf));
    RecordReader rows = reader.rows();
    OrcStruct row = null;
    for(int r=0; r < 1024; ++r) {
      assertEquals(true, rows.hasNext());
      row = (OrcStruct) rows.next(row);
      Map inner = (Map) row.getFieldValue(0);
      if (r < 200) {
        assertEquals("row " + r, null, inner);
      } else if (r < 300) {
        assertEquals("row " + r, "{" + (r - 200) + "=" + ((r - 200) * 10) + "}",
            inner.toString());
      } else if (r < 400) {
        assertEquals("row " + r, null, inner);
      } else if (r < 500) {
        assertEquals("row " + r, "{" + (r - 300) + "=" + ((r - 300) * 10) + "}",
            inner.toString());
      } else if (r < 600) {
        assertEquals("row " + r, null, inner);
      } else if (r < 700) {
        assertEquals("row " + r, "{" + r + "=" + (r * 10) + ", " +
                (r + 1) + "=" + (10 * (r + 1)) + "}",
            inner.toString());
      } else {
        assertEquals("row " + r, null, inner);
      }
    }
    assertEquals(false, rows.hasNext());
  }
}
