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

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
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
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Tests for the top level reader/streamFactory of ORC files.
 */
public class TestOrcFile {

  public static class SimpleStruct {
    BytesWritable bytes1;
    Text string1;

    SimpleStruct(BytesWritable b1, String s1) {
      this.bytes1 = b1;
      if(s1 == null) {
        this.string1 = null;
      } else {
        this.string1 = new Text(s1);
      }
    }
  }

  public static class InnerStruct {
    int int1;
    Text string1 = new Text();
    InnerStruct(int int1, String string1) {
      this.int1 = int1;
      this.string1.set(string1);
    }
  }

  public static class MiddleStruct {
    List<InnerStruct> list = new ArrayList<InnerStruct>();

    MiddleStruct(InnerStruct... items) {
      list.clear();
      for(InnerStruct item: items) {
        list.add(item);
      }
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
           List<InnerStruct> l2, Map<Text, InnerStruct> m2) {
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
      this.map = m2;
    }
  }

  private static InnerStruct inner(int i, String s) {
    return new InnerStruct(i, s);
  }

  private static Map<Text, InnerStruct> map(InnerStruct... items)  {
    Map<Text, InnerStruct> result = new HashMap<Text, InnerStruct>();
    for(InnerStruct i: items) {
      result.put(new Text(i.string1), i);
    }
    return result;
  }

  private static List<InnerStruct> list(InnerStruct... items) {
    List<InnerStruct> result = new ArrayList<InnerStruct>();
    for(InnerStruct s: items) {
      result.add(s);
    }
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

  private static ByteBuffer byteBuf(int... items) {
     ByteBuffer result = ByteBuffer.allocate(items.length);
    for(int item: items) {
      result.put((byte) item);
    }
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
    testFilePath = new Path(workDir, "TestOrcFile." +
        testCaseName.getMethodName() + ".orc");
    fs.delete(testFilePath, false);
  }

  @Test
  public void testReadFormat_0_11() throws Exception {
    Path oldFilePath = new Path(HiveTestUtils.getFileFromClasspath("orc-file-11-format.orc"));
    Reader reader = OrcFile.createReader(fs, oldFilePath);

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
    assertEquals("count: 7500 true: 3750", stats[1].toString());

    assertEquals(2048, ((IntegerColumnStatistics) stats[3]).getMaximum());
    assertEquals(1024, ((IntegerColumnStatistics) stats[3]).getMinimum());
    assertEquals(true, ((IntegerColumnStatistics) stats[3]).isSumDefined());
    assertEquals(11520000, ((IntegerColumnStatistics) stats[3]).getSum());
    assertEquals("count: 7500 min: 1024 max: 2048 sum: 11520000",
        stats[3].toString());

    assertEquals(Long.MAX_VALUE,
        ((IntegerColumnStatistics) stats[5]).getMaximum());
    assertEquals(Long.MAX_VALUE,
        ((IntegerColumnStatistics) stats[5]).getMinimum());
    assertEquals(false, ((IntegerColumnStatistics) stats[5]).isSumDefined());
    assertEquals(
        "count: 7500 min: 9223372036854775807 max: 9223372036854775807",
        stats[5].toString());

    assertEquals(-15.0, ((DoubleColumnStatistics) stats[7]).getMinimum());
    assertEquals(-5.0, ((DoubleColumnStatistics) stats[7]).getMaximum());
    assertEquals(-75000.0, ((DoubleColumnStatistics) stats[7]).getSum(),
        0.00001);
    assertEquals("count: 7500 min: -15.0 max: -5.0 sum: -75000.0",
        stats[7].toString());

    assertEquals("count: 7500 min: bye max: hi sum: 0", stats[9].toString());

    // check the inspectors
    StructObjectInspector readerInspector = (StructObjectInspector) reader
        .getObjectInspector();
    assertEquals(ObjectInspector.Category.STRUCT, readerInspector.getCategory());
    assertEquals("struct<boolean1:boolean,byte1:tinyint,short1:smallint,"
        + "int1:int,long1:bigint,float1:float,double1:double,bytes1:"
        + "binary,string1:string,middle:struct<list:array<struct<int1:int,"
        + "string1:string>>>,list:array<struct<int1:int,string1:string>>,"
        + "map:map<string,struct<int1:int,string1:string>>,ts:timestamp,"
        + "decimal1:decimal(65,30)>", readerInspector.getTypeName());
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
    RecordReader rows = reader.rows(null);
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
  public void testStringAndBinaryStatistics() throws Exception {

    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (SimpleStruct.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .inspector(inspector)
                                         .stripeSize(100000)
                                         .bufferSize(10000));
    writer.addRow(new SimpleStruct(bytes(0,1,2,3,4), "foo"));
    writer.addRow(new SimpleStruct(bytes(0,1,2,3), "bar"));
    writer.addRow(new SimpleStruct(bytes(0,1,2,3,4,5), null));
    writer.addRow(new SimpleStruct(null, "hi"));
    writer.close();
    Reader reader = OrcFile.createReader(fs, testFilePath);

    // check the stats
    ColumnStatistics[] stats = reader.getStatistics();
    assertEquals(4, stats[0].getNumberOfValues());
    assertEquals("count: 4", stats[0].toString());

    assertEquals(3, stats[1].getNumberOfValues());
    assertEquals(15, ((BinaryColumnStatistics) stats[1]).getSum());
    assertEquals("count: 3 sum: 15", stats[1].toString());

    assertEquals(3, stats[2].getNumberOfValues());
    assertEquals("bar", ((StringColumnStatistics) stats[2]).getMinimum());
    assertEquals("hi", ((StringColumnStatistics) stats[2]).getMaximum());
    assertEquals(8, ((StringColumnStatistics) stats[2]).getSum());
    assertEquals("count: 3 min: bar max: hi sum: 8",
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
    RecordReader rows = reader.rows(null);
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

    // check the contents of second row
    assertEquals(true, rows.hasNext());
    row = rows.next(row);
    assertEquals(bytes(0,1,2,3,4,5), bi.getPrimitiveWritableObject(
        readerInspector.getStructFieldData(row, fields.get(0))));
    assertNull(st.getPrimitiveJavaObject(readerInspector.
        getStructFieldData(row, fields.get(1))));

    // check the contents of second row
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
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (InnerStruct.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .inspector(inspector)
            .stripeSize(100000)
            .bufferSize(10000));
    for (int i = 0; i < 11000; i++) {
      if (i >= 5000) {
        if (i >= 10000) {
          writer.addRow(new InnerStruct(3, "three"));
        } else {
          writer.addRow(new InnerStruct(2, "two"));
        }
      } else {
        writer.addRow(new InnerStruct(1, "one"));
      }
    }

    writer.close();
    Reader reader = OrcFile.createReader(fs, testFilePath);
    Metadata metadata = reader.getMetadata();
    int numStripes = metadata.getStripeStatistics().size();
    assertEquals(3, numStripes);
    StripeStatistics ss1 = metadata.getStripeStatistics().get(0);
    StripeStatistics ss2 = metadata.getStripeStatistics().get(1);
    StripeStatistics ss3 = metadata.getStripeStatistics().get(2);
    assertEquals(4996, ss1.getColumnStatistics()[0].getNumberOfValues());
    assertEquals(5000, ss2.getColumnStatistics()[0].getNumberOfValues());
    assertEquals(1004, ss3.getColumnStatistics()[0].getNumberOfValues());

    assertEquals(4996, ((IntegerColumnStatistics)ss1.getColumnStatistics()[1]).getNumberOfValues());
    assertEquals(5000, ((IntegerColumnStatistics)ss2.getColumnStatistics()[1]).getNumberOfValues());
    assertEquals(1004, ((IntegerColumnStatistics)ss3.getColumnStatistics()[1]).getNumberOfValues());
    assertEquals(1, ((IntegerColumnStatistics)ss1.getColumnStatistics()[1]).getMinimum());
    assertEquals(1, ((IntegerColumnStatistics)ss2.getColumnStatistics()[1]).getMinimum());
    assertEquals(2, ((IntegerColumnStatistics)ss3.getColumnStatistics()[1]).getMinimum());
    assertEquals(1, ((IntegerColumnStatistics)ss1.getColumnStatistics()[1]).getMaximum());
    assertEquals(2, ((IntegerColumnStatistics)ss2.getColumnStatistics()[1]).getMaximum());
    assertEquals(3, ((IntegerColumnStatistics)ss3.getColumnStatistics()[1]).getMaximum());
    assertEquals(4996, ((IntegerColumnStatistics)ss1.getColumnStatistics()[1]).getSum());
    assertEquals(9996, ((IntegerColumnStatistics)ss2.getColumnStatistics()[1]).getSum());
    assertEquals(3008, ((IntegerColumnStatistics)ss3.getColumnStatistics()[1]).getSum());

    assertEquals(4996, ((StringColumnStatistics)ss1.getColumnStatistics()[2]).getNumberOfValues());
    assertEquals(5000, ((StringColumnStatistics)ss2.getColumnStatistics()[2]).getNumberOfValues());
    assertEquals(1004, ((StringColumnStatistics)ss3.getColumnStatistics()[2]).getNumberOfValues());
    assertEquals("one", ((StringColumnStatistics)ss1.getColumnStatistics()[2]).getMinimum());
    assertEquals("one", ((StringColumnStatistics)ss2.getColumnStatistics()[2]).getMinimum());
    assertEquals("three", ((StringColumnStatistics)ss3.getColumnStatistics()[2]).getMinimum());
    assertEquals("one", ((StringColumnStatistics)ss1.getColumnStatistics()[2]).getMaximum());
    assertEquals("two", ((StringColumnStatistics)ss2.getColumnStatistics()[2]).getMaximum());
    assertEquals("two", ((StringColumnStatistics)ss3.getColumnStatistics()[2]).getMaximum());
    assertEquals(14988, ((StringColumnStatistics)ss1.getColumnStatistics()[2]).getSum());
    assertEquals(15000, ((StringColumnStatistics)ss2.getColumnStatistics()[2]).getSum());
    assertEquals(5012, ((StringColumnStatistics)ss3.getColumnStatistics()[2]).getSum());
  }

  @Test
  public void test1() throws Exception {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (BigRow.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .inspector(inspector)
            .stripeSize(100000)
            .bufferSize(10000));
    writer.addRow(new BigRow(false, (byte) 1, (short) 1024, 65536,
        Long.MAX_VALUE, (float) 1.0, -15.0, bytes(0, 1, 2, 3, 4), "hi",
        new MiddleStruct(inner(1, "bye"), inner(2, "sigh")),
        list(inner(3, "good"), inner(4, "bad")),
        map()));
    writer.addRow(new BigRow(true, (byte) 100, (short) 2048, 65536,
        Long.MAX_VALUE, (float) 2.0, -5.0, bytes(), "bye",
        new MiddleStruct(inner(1, "bye"), inner(2, "sigh")),
        list(inner(100000000, "cat"), inner(-100000, "in"), inner(1234, "hat")),
        map(inner(5, "chani"), inner(1, "mauddib"))));
    writer.close();
    Reader reader = OrcFile.createReader(fs, testFilePath);

    Metadata metadata = reader.getMetadata();

    // check the stats
    ColumnStatistics[] stats = reader.getStatistics();
    assertEquals(2, stats[1].getNumberOfValues());
    assertEquals(1, ((BooleanColumnStatistics) stats[1]).getFalseCount());
    assertEquals(1, ((BooleanColumnStatistics) stats[1]).getTrueCount());
    assertEquals("count: 2 true: 1", stats[1].toString());

    assertEquals(2048, ((IntegerColumnStatistics) stats[3]).getMaximum());
    assertEquals(1024, ((IntegerColumnStatistics) stats[3]).getMinimum());
    assertEquals(true, ((IntegerColumnStatistics) stats[3]).isSumDefined());
    assertEquals(3072, ((IntegerColumnStatistics) stats[3]).getSum());
    assertEquals("count: 2 min: 1024 max: 2048 sum: 3072",
        stats[3].toString());

    StripeStatistics ss = metadata.getStripeStatistics().get(0);
    assertEquals(2, ss.getColumnStatistics()[0].getNumberOfValues());
    assertEquals(1, ((BooleanColumnStatistics) ss.getColumnStatistics()[1]).getTrueCount());
    assertEquals(1024, ((IntegerColumnStatistics) ss.getColumnStatistics()[3]).getMinimum());
    assertEquals(2048, ((IntegerColumnStatistics) ss.getColumnStatistics()[3]).getMaximum());
    assertEquals(3072, ((IntegerColumnStatistics) ss.getColumnStatistics()[3]).getSum());
    assertEquals(-15.0, ((DoubleColumnStatistics) stats[7]).getMinimum());
    assertEquals(-5.0, ((DoubleColumnStatistics) stats[7]).getMaximum());
    assertEquals(-20.0, ((DoubleColumnStatistics) stats[7]).getSum(), 0.00001);
    assertEquals("count: 2 min: -15.0 max: -5.0 sum: -20.0",
        stats[7].toString());

    assertEquals("count: 2 min: bye max: hi sum: 5", stats[9].toString());

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
    StructObjectInspector lc = (StructObjectInspector)
        li.getListElementObjectInspector();
    StringObjectInspector mk = (StringObjectInspector)
        ma.getMapKeyObjectInspector();
    StructObjectInspector mv = (StructObjectInspector)
        ma.getMapValueObjectInspector();
    RecordReader rows = reader.rows(null);
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
  public void columnProjection() throws Exception {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (InnerStruct.class,
              ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .inspector(inspector)
                                         .stripeSize(1000)
                                         .compress(CompressionKind.NONE)
                                         .bufferSize(100)
                                         .rowIndexStride(1000));
    Random r1 = new Random(1);
    Random r2 = new Random(2);
    int x;
    int minInt=0, maxInt=0;
    String y;
    String minStr = null, maxStr = null;
    for(int i=0; i < 21000; ++i) {
      x = r1.nextInt();
      y = Long.toHexString(r2.nextLong());
      if (i == 0 || x < minInt) {
        minInt = x;
      }
      if (i == 0 || x > maxInt) {
        maxInt = x;
      }
      if (i == 0 || y.compareTo(minStr) < 0) {
        minStr = y;
      }
      if (i == 0 || y.compareTo(maxStr) > 0) {
        maxStr = y;
      }
      writer.addRow(inner(x, y));
    }
    writer.close();
    Reader reader = OrcFile.createReader(fs, testFilePath);

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
  public void emptyFile() throws Exception {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (BigRow.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .inspector(inspector)
                                         .stripeSize(1000)
                                         .compress(CompressionKind.NONE)
                                         .bufferSize(100));
    writer.close();
    Reader reader = OrcFile.createReader(fs, testFilePath);
    assertEquals(false, reader.rows(null).hasNext());
    assertEquals(CompressionKind.NONE, reader.getCompression());
    assertEquals(0, reader.getNumberOfRows());
    assertEquals(0, reader.getCompressionSize());
    assertEquals(false, reader.getMetadataKeys().iterator().hasNext());
    assertEquals(3, reader.getContentLength());
    assertEquals(false, reader.getStripes().iterator().hasNext());
  }

  @Test
  public void metaData() throws Exception {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (BigRow.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .inspector(inspector)
                                         .stripeSize(1000)
                                         .compress(CompressionKind.NONE)
                                         .bufferSize(100));
    writer.addUserMetadata("my.meta", byteBuf(1, 2, 3, 4, 5, 6, 7, -1, -2, 127,
                                              -128));
    writer.addUserMetadata("clobber", byteBuf(1,2,3));
    writer.addUserMetadata("clobber", byteBuf(4,3,2,1));
    ByteBuffer bigBuf = ByteBuffer.allocate(40000);
    Random random = new Random(0);
    random.nextBytes(bigBuf.array());
    writer.addUserMetadata("big", bigBuf);
    bigBuf.position(0);
    writer.addRow(new BigRow(true, (byte) 127, (short) 1024, 42,
        42L * 1024 * 1024 * 1024, (float) 3.1415, -2.713, null,
        null, null, null, null));
    writer.addUserMetadata("clobber", byteBuf(5,7,11,13,17,19));
    writer.close();
    Reader reader = OrcFile.createReader(fs, testFilePath);
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
    Metadata metadata = reader.getMetadata();
    int numStripes = metadata.getStripeStatistics().size();
    assertEquals(1, numStripes);
  }

  /**
   * We test union, timestamp, and decimal separately since we need to make the
   * object inspector manually. (The Hive reflection-based doesn't handle
   * them properly.)
   */
  @Test
  public void testUnionAndTimestamp() throws Exception {
    List<OrcProto.Type> types = new ArrayList<OrcProto.Type>();
    types.add(OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.STRUCT).
        addFieldNames("time").addFieldNames("union").addFieldNames("decimal").
        addSubtypes(1).addSubtypes(2).addSubtypes(5).build());
    types.add(OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.TIMESTAMP).
        build());
    types.add(OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.UNION).
        addSubtypes(3).addSubtypes(4).build());
    types.add(OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.INT).
        build());
    types.add(OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.STRING).
        build());
    types.add(OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.DECIMAL).
        build());

    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = OrcStruct.createObjectInspector(0, types);
    }
    HiveDecimal maxValue = HiveDecimal.create("100000000000000000000");
    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .inspector(inspector)
                                         .stripeSize(1000)
                                         .compress(CompressionKind.NONE)
                                         .bufferSize(100)
                                         .blockPadding(false));
    OrcStruct row = new OrcStruct(3);
    OrcUnion union = new OrcUnion();
    row.setFieldValue(1, union);
    row.setFieldValue(0, Timestamp.valueOf("2000-03-12 15:00:00"));
    HiveDecimal value = HiveDecimal.create("12345678.6547456");
    row.setFieldValue(2, value);
    union.set((byte) 0, new IntWritable(42));
    writer.addRow(row);
    row.setFieldValue(0, Timestamp.valueOf("2000-03-20 12:00:00.123456789"));
    union.set((byte) 1, new Text("hello"));
    value = HiveDecimal.create("-5643.234");
    row.setFieldValue(2, value);
    writer.addRow(row);
    row.setFieldValue(0, null);
    row.setFieldValue(1, null);
    row.setFieldValue(2, null);
    writer.addRow(row);
    row.setFieldValue(1, union);
    union.set((byte) 0, null);
    writer.addRow(row);
    union.set((byte) 1, null);
    writer.addRow(row);
    union.set((byte) 0, new IntWritable(200000));
    row.setFieldValue(0, Timestamp.valueOf("1900-01-01 00:00:00"));
    value = HiveDecimal.create("100000000000000000000");
    row.setFieldValue(2, value);
    writer.addRow(row);
    Random rand = new Random(42);
    for(int i=1900; i < 2200; ++i) {
      row.setFieldValue(0, Timestamp.valueOf(i + "-05-05 12:34:56." + i));
      if ((i & 1) == 0) {
        union.set((byte) 0, new IntWritable(i*i));
      } else {
        union.set((byte) 1, new Text(new Integer(i*i).toString()));
      }
      value = HiveDecimal.create(new BigInteger(104, rand),
          rand.nextInt(28));
      row.setFieldValue(2, value);
      if (maxValue.compareTo(value) < 0) {
        maxValue = value;
      }
      writer.addRow(row);
    }
    // let's add a lot of constant rows to test the rle
    row.setFieldValue(0, null);
    union.set((byte) 0, new IntWritable(1732050807));
    row.setFieldValue(2, null);
    for(int i=0; i < 5000; ++i) {
      writer.addRow(row);
    }
    union.set((byte) 0, new IntWritable(0));
    writer.addRow(row);
    union.set((byte) 0, new IntWritable(10));
    writer.addRow(row);
    union.set((byte) 0, new IntWritable(138));
    writer.addRow(row);
    writer.close();
    Reader reader = OrcFile.createReader(fs, testFilePath);
    assertEquals(false, reader.getMetadataKeys().iterator().hasNext());
    assertEquals(5309, reader.getNumberOfRows());
    DecimalColumnStatistics stats =
        (DecimalColumnStatistics) reader.getStatistics()[5];
    assertEquals(303, stats.getNumberOfValues());
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
    RecordReader rows = reader.rows(null);
    assertEquals(0, rows.getRowNumber());
    assertEquals(0.0, rows.getProgress(), 0.000001);
    assertEquals(true, rows.hasNext());
    row = (OrcStruct) rows.next(null);
    assertEquals(1, rows.getRowNumber());
    inspector = reader.getObjectInspector();
    assertEquals("struct<time:timestamp,union:uniontype<int,string>,decimal:decimal(65,30)>",
        inspector.getTypeName());
    assertEquals(Timestamp.valueOf("2000-03-12 15:00:00"),
        row.getFieldValue(0));
    union = (OrcUnion) row.getFieldValue(1);
    assertEquals(0, union.getTag());
    assertEquals(new IntWritable(42), union.getObject());
    assertEquals(HiveDecimal.create("12345678.6547456"), row.getFieldValue(2));
    row = (OrcStruct) rows.next(row);
    assertEquals(2, rows.getRowNumber());
    assertEquals(Timestamp.valueOf("2000-03-20 12:00:00.123456789"),
        row.getFieldValue(0));
    assertEquals(1, union.getTag());
    assertEquals(new Text("hello"), union.getObject());
    assertEquals(HiveDecimal.create("-5643.234"), row.getFieldValue(2));
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
    assertEquals(Timestamp.valueOf("1900-01-01 00:00:00"),
        row.getFieldValue(0));
    assertEquals(new IntWritable(200000), union.getObject());
    assertEquals(HiveDecimal.create("100000000000000000000"),
                 row.getFieldValue(2));
    rand = new Random(42);
    for(int i=1900; i < 2200; ++i) {
      row = (OrcStruct) rows.next(row);
      assertEquals(Timestamp.valueOf(i + "-05-05 12:34:56." + i),
          row.getFieldValue(0));
      if ((i & 1) == 0) {
        assertEquals(0, union.getTag());
        assertEquals(new IntWritable(i*i), union.getObject());
      } else {
        assertEquals(1, union.getTag());
        assertEquals(new Text(new Integer(i*i).toString()), union.getObject());
      }
      assertEquals(HiveDecimal.create(new BigInteger(104, rand),
                                   rand.nextInt(28)), row.getFieldValue(2));
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
    assertEquals(Timestamp.valueOf("2000-03-20 12:00:00.123456789"),
        row.getFieldValue(0));
    assertEquals(1, union.getTag());
    assertEquals(new Text("hello"), union.getObject());
    assertEquals(HiveDecimal.create("-5643.234"), row.getFieldValue(2));
    rows.close();
  }

  /**
   * Read and write a randomly generated snappy file.
   * @throws Exception
   */
  @Test
  public void testSnappy() throws Exception {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (InnerStruct.class,
              ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .inspector(inspector)
                                         .stripeSize(1000)
                                         .compress(CompressionKind.SNAPPY)
                                         .bufferSize(100));
    Random rand = new Random(12);
    for(int i=0; i < 10000; ++i) {
      writer.addRow(new InnerStruct(rand.nextInt(),
          Integer.toHexString(rand.nextInt())));
    }
    writer.close();
    Reader reader = OrcFile.createReader(fs, testFilePath);
    RecordReader rows = reader.rows(null);
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
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (InnerStruct.class,
              ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .inspector(inspector)
                                         .stripeSize(5000)
                                         .compress(CompressionKind.SNAPPY)
                                         .bufferSize(1000)
                                         .rowIndexStride(0));
    Random rand = new Random(24);
    for(int i=0; i < 10000; ++i) {
      InnerStruct row = new InnerStruct(rand.nextInt(),
          Integer.toBinaryString(rand.nextInt()));
      for(int j=0; j< 5; ++j) {
        writer.addRow(row);
      }
    }
    writer.close();
    Reader reader = OrcFile.createReader(fs, testFilePath);
    assertEquals(50000, reader.getNumberOfRows());
    assertEquals(0, reader.getRowIndexStride());
    StripeInformation stripe = reader.getStripes().iterator().next();
    assertEquals(true, stripe.getDataLength() != 0);
    assertEquals(0, stripe.getIndexLength());
    RecordReader rows = reader.rows(null);
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
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (BigRow.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .inspector(inspector)
                                         .stripeSize(200000)
                                         .bufferSize(65536)
                                         .rowIndexStride(1000));
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
      writer.addRow(createRandomRow(intValues, doubleValues, stringValues,
          byteValues, words, i));
    }
    writer.close();
    writer = null;
    Reader reader = OrcFile.createReader(fs, testFilePath);
    assertEquals(COUNT, reader.getNumberOfRows());
    RecordReader rows = reader.rows(null);
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
      assertEquals(expected.float1.floatValue(),
          ((FloatWritable) row.getFieldValue(5)).get(), 0.0001);
      assertEquals(expected.double1.doubleValue(),
          ((DoubleWritable) row.getFieldValue(6)).get(), 0.0001);
      assertEquals(expected.bytes1, row.getFieldValue(7));
      assertEquals(expected.string1, row.getFieldValue(8));
      List<InnerStruct> expectedList = expected.middle.list;
      List<OrcStruct> actualList =
          (List) ((OrcStruct) row.getFieldValue(9)).getFieldValue(0);
      compareList(expectedList, actualList);
      compareList(expected.list, (List) row.getFieldValue(10));
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
    rows = reader.rows(offsetOfStripe2, offsetOfStripe4 - offsetOfStripe2,
                       columns);
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
                            OrcStruct actual) throws Exception {
    if (expect == null || actual == null) {
      assertEquals(expect, actual);
    } else {
      assertEquals(expect.int1, ((IntWritable) actual.getFieldValue(0)).get());
      assertEquals(expect.string1, actual.getFieldValue(1));
    }
  }

  private void compareList(List<InnerStruct> expect,
                           List<OrcStruct> actual) throws Exception {
    assertEquals(expect.size(), actual.size());
    for(int j=0; j < expect.size(); ++j) {
      compareInner(expect.get(j), actual.get(j));
    }
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
    MemoryManager.Callback callback;

    MyMemoryManager(Configuration conf, long totalSpace, double rate) {
      super(conf);
      this.totalSpace = totalSpace;
      this.rate = rate;
    }

    @Override
    void addWriter(Path path, long requestedAllocation,
                   MemoryManager.Callback callback) {
      this.path = path;
      this.lastAllocation = requestedAllocation;
      this.callback = callback;
    }

    @Override
    synchronized void removeWriter(Path path) {
      this.path = null;
      this.lastAllocation = 0;
    }

    @Override
    long getTotalMemoryPool() {
      return totalSpace;
    }

    @Override
    double getAllocationScale() {
      return rate;
    }

    @Override
    void addedRow() throws IOException {
      if (++rows % 100 == 0) {
        callback.checkMemory(rate);
      }
    }
  }

  @Test
  public void testMemoryManagement() throws Exception {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (InnerStruct.class,
              ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    MyMemoryManager memory = new MyMemoryManager(conf, 10000, 0.1);
    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .inspector(inspector)
                                         .compress(CompressionKind.NONE)
                                         .stripeSize(50000)
                                         .bufferSize(100)
                                         .rowIndexStride(0)
                                         .memory(memory));
    assertEquals(testFilePath, memory.path);
    for(int i=0; i < 2500; ++i) {
      writer.addRow(new InnerStruct(i*300, Integer.toHexString(10*i)));
    }
    writer.close();
    assertEquals(null, memory.path);
    Reader reader = OrcFile.createReader(fs, testFilePath);
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
  public void testPredicatePushdown() throws Exception {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (InnerStruct.class,
              ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    Writer writer = OrcFile.createWriter(fs, testFilePath, conf, inspector,
        400000L, CompressionKind.NONE, 500, 1000);
    for(int i=0; i < 3500; ++i) {
      writer.addRow(new InnerStruct(i*300, Integer.toHexString(10*i)));
    }
    writer.close();
    Reader reader = OrcFile.createReader(fs, testFilePath);
    assertEquals(3500, reader.getNumberOfRows());

    SearchArgument sarg = SearchArgument.FACTORY.newBuilder()
        .startAnd()
          .startNot()
             .lessThan("int1", 300000)
          .end()
          .lessThan("int1", 600000)
        .end()
        .build();
    RecordReader rows = reader.rows(0L, Long.MAX_VALUE,
        new boolean[]{true, true, true}, sarg,
        new String[]{null, "int1", "string1"});
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
    sarg = SearchArgument.FACTORY.newBuilder()
        .startAnd()
          .lessThan("int1", 0)
        .end()
        .build();
    rows = reader.rows(0L, Long.MAX_VALUE,
        new boolean[]{true, true, true}, sarg,
        new String[]{null, "int1", "string1"});
    assertEquals(3500L, rows.getRowNumber());
    assertTrue(!rows.hasNext());

    // select first 100 and last 100 rows
    sarg = SearchArgument.FACTORY.newBuilder()
        .startOr()
          .lessThan("int1", 300 * 100)
          .startNot()
            .lessThan("int1", 300 * 3400)
          .end()
        .end()
        .build();
    rows = reader.rows(0L, Long.MAX_VALUE,
        new boolean[]{true, true, true}, sarg,
        new String[]{null, "int1", "string1"});
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
}
