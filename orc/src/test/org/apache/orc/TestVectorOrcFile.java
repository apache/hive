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

package org.apache.orc;

import com.google.common.collect.Lists;

import junit.framework.Assert;
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
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.UnionColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.orc.impl.DataReaderProperties;
import org.apache.orc.impl.MemoryManager;
import org.apache.orc.impl.OrcIndex;
import org.apache.orc.impl.RecordReaderImpl;
import org.apache.orc.impl.RecordReaderUtils;
import org.apache.orc.tools.TestJsonFileDump;
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

import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the vectorized reader and writer for ORC files.
 */
public class TestVectorOrcFile {

  public static class InnerStruct {
    int int1;
    Text string1 = new Text();
    InnerStruct(int int1, Text string1) {
      this.int1 = int1;
      this.string1.set(string1);
    }
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
        new Path(TestJsonFileDump.getFileFromClasspath("orc-file-11-format.orc"));
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
    Assert.assertEquals(reader.getNumberOfRows(), rowCount);
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

    assertEquals(-15.0, ((DoubleColumnStatistics) stats[7]).getMinimum(), 0.0001);
    assertEquals(-5.0, ((DoubleColumnStatistics) stats[7]).getMaximum(), 0.0001);
    assertEquals(-75000.0, ((DoubleColumnStatistics) stats[7]).getSum(),
        0.00001);
    assertEquals("count: 7500 hasNull: true min: -15.0 max: -5.0 sum: -75000.0",
        stats[7].toString());

    assertEquals("count: 7500 hasNull: true min: bye max: hi sum: 0", stats[9].toString());

    // check the inspectors
    TypeDescription schema = reader.getSchema();
    assertEquals(TypeDescription.Category.STRUCT, schema.getCategory());
    assertEquals("struct<boolean1:boolean,byte1:tinyint,short1:smallint,"
        + "int1:int,long1:bigint,float1:float,double1:double,bytes1:"
        + "binary,string1:string,middle:struct<list:array<struct<int1:int,"
        + "string1:string>>>,list:array<struct<int1:int,string1:string>>,"
        + "map:map<string,struct<int1:int,string1:string>>,ts:timestamp,"
        + "decimal1:decimal(38,10)>", schema.toString());
    VectorizedRowBatch batch = schema.createRowBatch();

    RecordReader rows = reader.rows();
    Assert.assertEquals(true, rows.nextBatch(batch));
    assertEquals(1024, batch.size);

    // check the contents of the first row
    assertEquals(false, getBoolean(batch, 0));
    assertEquals(1, getByte(batch, 0));
    assertEquals(1024, getShort(batch, 0));
    assertEquals(65536, getInt(batch, 0));
    assertEquals(Long.MAX_VALUE, getLong(batch, 0));
    assertEquals(1.0, getFloat(batch, 0), 0.00001);
    assertEquals(-15.0, getDouble(batch, 0), 0.00001);
    assertEquals(bytes(0, 1, 2, 3, 4), getBinary(batch, 0));
    assertEquals("hi", getText(batch, 0).toString());

    StructColumnVector middle = (StructColumnVector) batch.cols[9];
    ListColumnVector midList = (ListColumnVector) middle.fields[0];
    StructColumnVector midListStruct = (StructColumnVector) midList.child;
    LongColumnVector midListInt = (LongColumnVector) midListStruct.fields[0];
    BytesColumnVector midListStr = (BytesColumnVector) midListStruct.fields[1];
    ListColumnVector list = (ListColumnVector) batch.cols[10];
    StructColumnVector listStruct = (StructColumnVector) list.child;
    LongColumnVector listInts = (LongColumnVector) listStruct.fields[0];
    BytesColumnVector listStrs = (BytesColumnVector) listStruct.fields[1];
    MapColumnVector map = (MapColumnVector) batch.cols[11];
    BytesColumnVector mapKey = (BytesColumnVector) map.keys;
    StructColumnVector mapValue = (StructColumnVector) map.values;
    LongColumnVector mapValueInts = (LongColumnVector) mapValue.fields[0];
    BytesColumnVector mapValueStrs = (BytesColumnVector) mapValue.fields[1];
    TimestampColumnVector timestamp = (TimestampColumnVector) batch.cols[12];
    DecimalColumnVector decs = (DecimalColumnVector) batch.cols[13];

    assertEquals(false, middle.isNull[0]);
    assertEquals(2, midList.lengths[0]);
    int start = (int) midList.offsets[0];
    assertEquals(1, midListInt.vector[start]);
    assertEquals("bye", midListStr.toString(start));
    assertEquals(2, midListInt.vector[start + 1]);
    assertEquals("sigh", midListStr.toString(start + 1));

    assertEquals(2, list.lengths[0]);
    start = (int) list.offsets[0];
    assertEquals(3, listInts.vector[start]);
    assertEquals("good", listStrs.toString(start));
    assertEquals(4, listInts.vector[start + 1]);
    assertEquals("bad", listStrs.toString(start + 1));
    assertEquals(0, map.lengths[0]);
    assertEquals(Timestamp.valueOf("2000-03-12 15:00:00"),
        timestamp.asScratchTimestamp(0));
    assertEquals(new HiveDecimalWritable(HiveDecimal.create("12345678.6547456")),
        decs.vector[0]);

    // check the contents of row 7499
    rows.seekToRow(7499);
    Assert.assertEquals(true, rows.nextBatch(batch));
    assertEquals(true, getBoolean(batch, 0));
    assertEquals(100, getByte(batch, 0));
    assertEquals(2048, getShort(batch, 0));
    assertEquals(65536, getInt(batch, 0));
    assertEquals(Long.MAX_VALUE, getLong(batch, 0));
    assertEquals(2.0, getFloat(batch, 0), 0.00001);
    assertEquals(-5.0, getDouble(batch, 0), 0.00001);
    assertEquals(bytes(), getBinary(batch, 0));
    assertEquals("bye", getText(batch, 0).toString());
    assertEquals(false, middle.isNull[0]);
    assertEquals(2, midList.lengths[0]);
    start = (int) midList.offsets[0];
    assertEquals(1, midListInt.vector[start]);
    assertEquals("bye", midListStr.toString(start));
    assertEquals(2, midListInt.vector[start + 1]);
    assertEquals("sigh", midListStr.toString(start + 1));
    assertEquals(3, list.lengths[0]);
    start = (int) list.offsets[0];
    assertEquals(100000000, listInts.vector[start]);
    assertEquals("cat", listStrs.toString(start));
    assertEquals(-100000, listInts.vector[start + 1]);
    assertEquals("in", listStrs.toString(start + 1));
    assertEquals(1234, listInts.vector[start + 2]);
    assertEquals("hat", listStrs.toString(start + 2));
    assertEquals(2, map.lengths[0]);
    start = (int) map.offsets[0];
    assertEquals("chani", mapKey.toString(start));
    assertEquals(5, mapValueInts.vector[start]);
    assertEquals("chani", mapValueStrs.toString(start));
    assertEquals("mauddib", mapKey.toString(start + 1));
    assertEquals(1, mapValueInts.vector[start + 1]);
    assertEquals("mauddib", mapValueStrs.toString(start + 1));
    assertEquals(Timestamp.valueOf("2000-03-12 15:00:01"),
        timestamp.asScratchTimestamp(0));
    assertEquals(new HiveDecimalWritable(HiveDecimal.create("12345678.6547457")),
        decs.vector[0]);

    // handle the close up
    Assert.assertEquals(false, rows.nextBatch(batch));
    rows.close();
  }

  @Test
  public void testTimestamp() throws Exception {
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
    TimestampColumnVector vec = new TimestampColumnVector(1024);
    batch.cols[0] = vec;
    batch.reset();
    batch.size = tslist.size();
    for (int i=0; i < tslist.size(); ++i) {
      Timestamp ts = tslist.get(i);
      vec.set(i, ts);
    }
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    batch = reader.getSchema().createRowBatch();
    TimestampColumnVector timestamps = (TimestampColumnVector) batch.cols[0];
    int idx = 0;
    while (rows.nextBatch(batch)) {
      for(int r=0; r < batch.size; ++r) {
        assertEquals(tslist.get(idx++).getNanos(),
            timestamps.asScratchTimestamp(r).getNanos());
      }
    }
    Assert.assertEquals(tslist.size(), rows.getRowNumber());
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
    batch = reader.getSchema().createRowBatch();
    BytesColumnVector bytes = (BytesColumnVector) batch.cols[0];
    BytesColumnVector strs = (BytesColumnVector) batch.cols[1];
    RecordReader rows = reader.rows();
    Assert.assertEquals(true, rows.nextBatch(batch));
    assertEquals(4, batch.size);

    // check the contents of the first row
    assertEquals(bytes(0,1,2,3,4), getBinary(bytes, 0));
    assertEquals("foo", strs.toString(0));

    // check the contents of second row
    assertEquals(bytes(0,1,2,3), getBinary(bytes, 1));
    assertEquals("bar", strs.toString(1));

    // check the contents of third row
    assertEquals(bytes(0,1,2,3,4,5), getBinary(bytes, 2));
    assertNull(strs.toString(2));

    // check the contents of fourth row
    assertNull(getBinary(bytes, 3));
    assertEquals("hi", strs.toString(3));

    // handle the close up
    Assert.assertEquals(false, rows.nextBatch(batch));
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

  private static void checkInner(StructColumnVector inner, int rowId,
                                 int rowInBatch, int i, String value) {
    assertEquals("row " + rowId, i,
        ((LongColumnVector) inner.fields[0]).vector[rowInBatch]);
    if (value != null) {
      assertEquals("row " + rowId, value,
          ((BytesColumnVector) inner.fields[1]).toString(rowInBatch));
    } else {
      assertEquals("row " + rowId, true, inner.fields[1].isNull[rowInBatch]);
      assertEquals("row " + rowId, false, inner.fields[1].noNulls);
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

  private static void checkInnerList(ListColumnVector list, int rowId,
                                     int rowInBatch, List<InnerStruct> value) {
    if (value != null) {
      assertEquals("row " + rowId, value.size(), list.lengths[rowInBatch]);
      int start = (int) list.offsets[rowInBatch];
      for (int i = 0; i < list.lengths[rowInBatch]; ++i) {
        InnerStruct inner = value.get(i);
        checkInner((StructColumnVector) list.child, rowId, i + start,
            inner.int1, inner.string1.toString());
      }
      list.childCount += value.size();
    } else {
      assertEquals("row " + rowId, true, list.isNull[rowInBatch]);
      assertEquals("row " + rowId, false, list.noNulls);
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

  private static void checkInnerMap(MapColumnVector map, int rowId,
                                    int rowInBatch,
                                    Map<String, InnerStruct> value) {
    if (value != null) {
      assertEquals("row " + rowId, value.size(), map.lengths[rowInBatch]);
      int offset = (int) map.offsets[rowInBatch];
      for(int i=0; i < value.size(); ++i) {
        String key = ((BytesColumnVector) map.keys).toString(offset + i);
        InnerStruct expected = value.get(key);
        checkInner((StructColumnVector) map.values, rowId, offset + i,
            expected.int1, expected.string1.toString());
      }
    } else {
      assertEquals("row " + rowId, true, map.isNull[rowId]);
      assertEquals("row " + rowId, false, map.noNulls);
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

  private static void checkMiddleStruct(StructColumnVector middle, int rowId,
                                        int rowInBatch, MiddleStruct value) {
    if (value != null) {
      checkInnerList((ListColumnVector) middle.fields[0], rowId, rowInBatch,
          value.list);
    } else {
      assertEquals("row " + rowId, true, middle.isNull[rowInBatch]);
      assertEquals("row " + rowId, false, middle.noNulls);
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

  private static void checkBigRow(VectorizedRowBatch batch,
                                  int rowInBatch,
                                  int rowId,
                                  boolean b1, byte b2, short s1,
                                  int i1, long l1, float f1,
                                  double d1, BytesWritable b3, String s2,
                                  MiddleStruct m1, List<InnerStruct> l2,
                                  Map<String, InnerStruct> m2) {
    assertEquals("row " + rowId, b1, getBoolean(batch, rowInBatch));
    assertEquals("row " + rowId, b2, getByte(batch, rowInBatch));
    assertEquals("row " + rowId, s1, getShort(batch, rowInBatch));
    assertEquals("row " + rowId, i1, getInt(batch, rowInBatch));
    assertEquals("row " + rowId, l1, getLong(batch, rowInBatch));
    assertEquals("row " + rowId, f1, getFloat(batch, rowInBatch), 0.0001);
    assertEquals("row " + rowId, d1, getDouble(batch, rowInBatch), 0.0001);
    if (b3 != null) {
      BytesColumnVector bytes = (BytesColumnVector) batch.cols[7];
      assertEquals("row " + rowId, b3.getLength(), bytes.length[rowInBatch]);
      for(int i=0; i < b3.getLength(); ++i) {
        assertEquals("row " + rowId + " byte " + i, b3.getBytes()[i],
            bytes.vector[rowInBatch][bytes.start[rowInBatch] + i]);
      }
    } else {
      assertEquals("row " + rowId, true, batch.cols[7].isNull[rowInBatch]);
      assertEquals("row " + rowId, false, batch.cols[7].noNulls);
    }
    if (s2 != null) {
      assertEquals("row " + rowId, s2, getText(batch, rowInBatch).toString());
    } else {
      assertEquals("row " + rowId, true, batch.cols[8].isNull[rowInBatch]);
      assertEquals("row " + rowId, false, batch.cols[8].noNulls);
    }
    checkMiddleStruct((StructColumnVector) batch.cols[9], rowId, rowInBatch,
        m1);
    checkInnerList((ListColumnVector) batch.cols[10], rowId, rowInBatch, l2);
    checkInnerMap((MapColumnVector) batch.cols[11], rowId, rowInBatch, m2);
  }

  private static boolean getBoolean(VectorizedRowBatch batch, int rowId) {
    return ((LongColumnVector) batch.cols[0]).vector[rowId] != 0;
  }

  private static byte getByte(VectorizedRowBatch batch, int rowId) {
    return (byte) ((LongColumnVector) batch.cols[1]).vector[rowId];
  }

  private static short getShort(VectorizedRowBatch batch, int rowId) {
    return (short) ((LongColumnVector) batch.cols[2]).vector[rowId];
  }

  private static int getInt(VectorizedRowBatch batch, int rowId) {
    return (int) ((LongColumnVector) batch.cols[3]).vector[rowId];
  }

  private static long getLong(VectorizedRowBatch batch, int rowId) {
    return ((LongColumnVector) batch.cols[4]).vector[rowId];
  }

  private static float getFloat(VectorizedRowBatch batch, int rowId) {
    return (float) ((DoubleColumnVector) batch.cols[5]).vector[rowId];
  }

  private static double getDouble(VectorizedRowBatch batch, int rowId) {
    return ((DoubleColumnVector) batch.cols[6]).vector[rowId];
  }

  private static BytesWritable getBinary(BytesColumnVector column, int rowId) {
    if (column.isRepeating) {
      rowId = 0;
    }
    if (column.noNulls || !column.isNull[rowId]) {
      return new BytesWritable(Arrays.copyOfRange(column.vector[rowId],
          column.start[rowId], column.start[rowId] + column.length[rowId]));
    } else {
      return null;
    }
  }

  private static BytesWritable getBinary(VectorizedRowBatch batch, int rowId) {
    return getBinary((BytesColumnVector) batch.cols[7], rowId);
  }

  private static Text getText(BytesColumnVector vector, int rowId) {
    if (vector.isRepeating) {
      rowId = 0;
    }
    if (vector.noNulls || !vector.isNull[rowId]) {
      return new Text(Arrays.copyOfRange(vector.vector[rowId],
          vector.start[rowId], vector.start[rowId] + vector.length[rowId]));
    } else {
      return null;
    }
  }

  private static Text getText(VectorizedRowBatch batch, int rowId) {
    return getText((BytesColumnVector) batch.cols[8], rowId);
  }

  private static InnerStruct getInner(StructColumnVector vector,
                                      int rowId) {
    return new InnerStruct(
        (int) ((LongColumnVector) vector.fields[0]).vector[rowId],
        getText((BytesColumnVector) vector.fields[1], rowId));
  }

  private static List<InnerStruct> getList(ListColumnVector cv,
                                           int rowId) {
    if (cv.isRepeating) {
      rowId = 0;
    }
    if (cv.noNulls || !cv.isNull[rowId]) {
      List<InnerStruct> result =
          new ArrayList<InnerStruct>((int) cv.lengths[rowId]);
      for(long i=cv.offsets[rowId];
          i < cv.offsets[rowId] + cv.lengths[rowId]; ++i) {
        result.add(getInner((StructColumnVector) cv.child, (int) i));
      }
      return result;
    } else {
      return null;
    }
  }

  private static List<InnerStruct> getMidList(VectorizedRowBatch batch,
                                              int rowId) {
    return getList((ListColumnVector) ((StructColumnVector) batch.cols[9])
        .fields[0], rowId);
  }

  private static List<InnerStruct> getList(VectorizedRowBatch batch,
                                           int rowId) {
    return getList((ListColumnVector) batch.cols[10], rowId);
  }

  private static Map<Text, InnerStruct> getMap(VectorizedRowBatch batch,
                                               int rowId) {
    MapColumnVector cv = (MapColumnVector) batch.cols[11];
    if (cv.isRepeating) {
      rowId = 0;
    }
    if (cv.noNulls || !cv.isNull[rowId]) {
      Map<Text, InnerStruct> result =
          new HashMap<Text, InnerStruct>((int) cv.lengths[rowId]);
      for(long i=cv.offsets[rowId];
          i < cv.offsets[rowId] + cv.lengths[rowId]; ++i) {
        result.put(getText((BytesColumnVector) cv.keys, (int) i),
            getInner((StructColumnVector) cv.values, (int) i));
      }
      return result;
    } else {
      return null;
    }
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
    assertEquals(-15.0, ((DoubleColumnStatistics) stats[7]).getMinimum(), 0.0001);
    assertEquals(-5.0, ((DoubleColumnStatistics) stats[7]).getMaximum(), 0.0001);
    assertEquals(-20.0, ((DoubleColumnStatistics) stats[7]).getSum(), 0.00001);
    assertEquals("count: 2 hasNull: false min: -15.0 max: -5.0 sum: -20.0",
        stats[7].toString());

    assertEquals("count: 2 hasNull: false min: bye max: hi sum: 5", stats[9].toString());

    // check the schema
    TypeDescription readerSchema = reader.getSchema();
    assertEquals(TypeDescription.Category.STRUCT, readerSchema.getCategory());
    assertEquals("struct<boolean1:boolean,byte1:tinyint,short1:smallint,"
        + "int1:int,long1:bigint,float1:float,double1:double,bytes1:"
        + "binary,string1:string,middle:struct<list:array<struct<int1:int,"
        + "string1:string>>>,list:array<struct<int1:int,string1:string>>,"
        + "map:map<string,struct<int1:int,string1:string>>>",
        readerSchema.toString());
    List<String> fieldNames = readerSchema.getFieldNames();
    List<TypeDescription> fieldTypes = readerSchema.getChildren();
    assertEquals("boolean1", fieldNames.get(0));
    assertEquals(TypeDescription.Category.BOOLEAN, fieldTypes.get(0).getCategory());
    assertEquals("byte1", fieldNames.get(1));
    assertEquals(TypeDescription.Category.BYTE, fieldTypes.get(1).getCategory());
    assertEquals("short1", fieldNames.get(2));
    assertEquals(TypeDescription.Category.SHORT, fieldTypes.get(2).getCategory());
    assertEquals("int1", fieldNames.get(3));
    assertEquals(TypeDescription.Category.INT, fieldTypes.get(3).getCategory());
    assertEquals("long1", fieldNames.get(4));
    assertEquals(TypeDescription.Category.LONG, fieldTypes.get(4).getCategory());
    assertEquals("float1", fieldNames.get(5));
    assertEquals(TypeDescription.Category.FLOAT, fieldTypes.get(5).getCategory());
    assertEquals("double1", fieldNames.get(6));
    assertEquals(TypeDescription.Category.DOUBLE, fieldTypes.get(6).getCategory());
    assertEquals("bytes1", fieldNames.get(7));
    assertEquals(TypeDescription.Category.BINARY, fieldTypes.get(7).getCategory());
    assertEquals("string1", fieldNames.get(8));
    assertEquals(TypeDescription.Category.STRING, fieldTypes.get(8).getCategory());
    assertEquals("middle", fieldNames.get(9));
    TypeDescription middle = fieldTypes.get(9);
    assertEquals(TypeDescription.Category.STRUCT, middle.getCategory());
    TypeDescription midList = middle.getChildren().get(0);
    assertEquals(TypeDescription.Category.LIST, midList.getCategory());
    TypeDescription inner = midList.getChildren().get(0);
    assertEquals(TypeDescription.Category.STRUCT, inner.getCategory());
    assertEquals("int1", inner.getFieldNames().get(0));
    assertEquals("string1", inner.getFieldNames().get(1));

    RecordReader rows = reader.rows();
    // create a new batch
    batch = readerSchema.createRowBatch();
    Assert.assertEquals(true, rows.nextBatch(batch));
    assertEquals(2, batch.size);
    Assert.assertEquals(false, rows.nextBatch(batch));

    // check the contents of the first row
    assertEquals(false, getBoolean(batch, 0));
    assertEquals(1, getByte(batch, 0));
    assertEquals(1024, getShort(batch, 0));
    assertEquals(65536, getInt(batch, 0));
    assertEquals(Long.MAX_VALUE, getLong(batch, 0));
    assertEquals(1.0, getFloat(batch, 0), 0.00001);
    assertEquals(-15.0, getDouble(batch, 0), 0.00001);
    assertEquals(bytes(0,1,2,3,4), getBinary(batch, 0));
    assertEquals("hi", getText(batch, 0).toString());
    List<InnerStruct> midRow = getMidList(batch, 0);
    assertNotNull(midRow);
    assertEquals(2, midRow.size());
    assertEquals(1, midRow.get(0).int1);
    assertEquals("bye", midRow.get(0).string1.toString());
    assertEquals(2, midRow.get(1).int1);
    assertEquals("sigh", midRow.get(1).string1.toString());
    List<InnerStruct> list = getList(batch, 0);
    assertEquals(2, list.size());
    assertEquals(3, list.get(0).int1);
    assertEquals("good", list.get(0).string1.toString());
    assertEquals(4, list.get(1).int1);
    assertEquals("bad", list.get(1).string1.toString());
    Map<Text, InnerStruct> map = getMap(batch, 0);
    assertEquals(0, map.size());

    // check the contents of second row
    assertEquals(true, getBoolean(batch, 1));
    assertEquals(100, getByte(batch, 1));
    assertEquals(2048, getShort(batch, 1));
    assertEquals(65536, getInt(batch, 1));
    assertEquals(Long.MAX_VALUE, getLong(batch, 1));
    assertEquals(2.0, getFloat(batch, 1), 0.00001);
    assertEquals(-5.0, getDouble(batch, 1), 0.00001);
    assertEquals(bytes(), getBinary(batch, 1));
    assertEquals("bye", getText(batch, 1).toString());
    midRow = getMidList(batch, 1);
    assertNotNull(midRow);
    assertEquals(2, midRow.size());
    assertEquals(1, midRow.get(0).int1);
    assertEquals("bye", midRow.get(0).string1.toString());
    assertEquals(2, midRow.get(1).int1);
    assertEquals("sigh", midRow.get(1).string1.toString());
    list = getList(batch, 1);
    assertEquals(3, list.size());
    assertEquals(100000000, list.get(0).int1);
    assertEquals("cat", list.get(0).string1.toString());
    assertEquals(-100000, list.get(1).int1);
    assertEquals("in", list.get(1).string1.toString());
    assertEquals(1234, list.get(2).int1);
    assertEquals("hat", list.get(2).string1.toString());
    map = getMap(batch, 1);
    assertEquals(2, map.size());
    InnerStruct value = map.get(new Text("chani"));
    assertEquals(5, value.int1);
    assertEquals("chani", value.string1.toString());
    value = map.get(new Text("mauddib"));
    assertEquals(1, value.int1);
    assertEquals("mauddib", value.string1.toString());

    // handle the close up
    Assert.assertEquals(false, rows.nextBatch(batch));
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
    TypeDescription type = reader.getSchema();
    assertEquals(TypeDescription.Category.STRUCT, type.getCategory());
    assertEquals(2, type.getChildren().size());
    TypeDescription type1 = type.getChildren().get(0);
    TypeDescription type2 = type.getChildren().get(1);
    assertEquals(TypeDescription.Category.INT, type1.getCategory());
    assertEquals(TypeDescription.Category.STRING, type2.getCategory());
    assertEquals("struct<int1:int,string1:string>", type.toString());

    // read the contents and make sure they match
    RecordReader rows1 = reader.rows(
        new Reader.Options().include(new boolean[]{true, true, false}));
    RecordReader rows2 = reader.rows(
        new Reader.Options().include(new boolean[]{true, false, true}));
    r1 = new Random(1);
    r2 = new Random(2);
    VectorizedRowBatch batch1 = reader.getSchema().createRowBatch(1000);
    VectorizedRowBatch batch2 = reader.getSchema().createRowBatch(1000);
    for(int i = 0; i < 21000; i += 1000) {
      Assert.assertEquals(true, rows1.nextBatch(batch1));
      Assert.assertEquals(true, rows2.nextBatch(batch2));
      assertEquals(1000, batch1.size);
      assertEquals(1000, batch2.size);
      for(int j=0; j < 1000; ++j) {
        assertEquals(r1.nextInt(),
            ((LongColumnVector) batch1.cols[0]).vector[j]);
        assertEquals(Long.toHexString(r2.nextLong()),
            ((BytesColumnVector) batch2.cols[1]).toString(j));
      }
    }
    Assert.assertEquals(false, rows1.nextBatch(batch1));
    Assert.assertEquals(false, rows2.nextBatch(batch2));
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
    VectorizedRowBatch batch = reader.getSchema().createRowBatch();
    Assert.assertEquals(false, reader.rows().nextBatch(batch));
    Assert.assertEquals(CompressionKind.NONE, reader.getCompressionKind());
    Assert.assertEquals(0, reader.getNumberOfRows());
    Assert.assertEquals(0, reader.getCompressionSize());
    Assert.assertEquals(false, reader.getMetadataKeys().iterator().hasNext());
    Assert.assertEquals(3, reader.getContentLength());
    Assert.assertEquals(false, reader.getStripes().iterator().hasNext());
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
    Assert.assertEquals(byteBuf(5, 7, 11, 13, 17, 19), reader.getMetadataValue("clobber"));
    Assert.assertEquals(byteBuf(1, 2, 3, 4, 5, 6, 7, -1, -2, 127, -128),
        reader.getMetadataValue("my.meta"));
    Assert.assertEquals(bigBuf, reader.getMetadataValue("big"));
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
        TimestampColumnVector timestampColVector = (TimestampColumnVector) batch.cols[0];
        timestampColVector.set(ms - 1000,
            Timestamp.valueOf(year +
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
    batch = reader.getSchema().createRowBatch(1000);
    TimestampColumnVector times = (TimestampColumnVector) batch.cols[0];
    LongColumnVector dates = (LongColumnVector) batch.cols[1];
    for (int year = minYear; year < maxYear; ++year) {
      rows.nextBatch(batch);
      assertEquals(1000, batch.size);
      for(int ms = 1000; ms < 2000; ++ms) {
        StringBuilder buffer = new StringBuilder();
        times.stringifyValue(buffer, ms - 1000);
        String expected = Integer.toString(year) + "-05-05 12:34:56.";
        // suppress the final zeros on the string by dividing by the largest
        // power of 10 that divides evenly.
        int roundedMs = ms;
        for(int round = 1000; round > 0; round /= 10) {
          if (ms % round == 0) {
            roundedMs = ms / round;
            break;
          }
        }
        expected += roundedMs;
        assertEquals(expected, buffer.toString());
        assertEquals(Integer.toString(year) + "-12-25",
            new DateWritable((int) dates.vector[ms - 1000]).toString());
      }
    }
    rows.nextBatch(batch);
    assertEquals(0, batch.size);
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
      TimestampColumnVector timestampColVector = (TimestampColumnVector) batch.cols[0];
      timestampColVector.set(rowId, ts);
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
    ((UnionColumnVector) batch.cols[1]).fields[0].isRepeating = true;
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

    Assert.assertEquals(false, reader.getMetadataKeys().iterator().hasNext());
    Assert.assertEquals(5077, reader.getNumberOfRows());
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
    Assert.assertEquals(reader.getNumberOfRows(), rowCount);
    assertEquals(2, stripeCount);
    Assert.assertEquals(reader.getContentLength(), currentOffset);
    RecordReader rows = reader.rows();
    Assert.assertEquals(0, rows.getRowNumber());
    Assert.assertEquals(0.0, rows.getProgress(), 0.000001);

    schema = reader.getSchema();
    batch = schema.createRowBatch(74);
    Assert.assertEquals(0, rows.getRowNumber());
    rows.nextBatch(batch);
    assertEquals(74, batch.size);
    Assert.assertEquals(74, rows.getRowNumber());
    TimestampColumnVector ts = (TimestampColumnVector) batch.cols[0];
    UnionColumnVector union = (UnionColumnVector) batch.cols[1];
    LongColumnVector longs = (LongColumnVector) union.fields[0];
    BytesColumnVector strs = (BytesColumnVector) union.fields[1];
    DecimalColumnVector decs = (DecimalColumnVector) batch.cols[2];

    assertEquals("struct<time:timestamp,union:uniontype<int,string>,decimal:decimal(38,18)>",
        schema.toString());
    assertEquals("2000-03-12 15:00:00.0", ts.asScratchTimestamp(0).toString());
    assertEquals(0, union.tags[0]);
    assertEquals(42, longs.vector[0]);
    assertEquals("12345678.6547456", decs.vector[0].toString());

    assertEquals("2000-03-20 12:00:00.123456789", ts.asScratchTimestamp(1).toString());
    assertEquals(1, union.tags[1]);
    assertEquals("hello", strs.toString(1));
    assertEquals("-5643.234", decs.vector[1].toString());

    assertEquals(false, ts.noNulls);
    assertEquals(false, union.noNulls);
    assertEquals(false, decs.noNulls);
    assertEquals(true, ts.isNull[2]);
    assertEquals(true, union.isNull[2]);
    assertEquals(true, decs.isNull[2]);

    assertEquals(true, ts.isNull[3]);
    assertEquals(false, union.isNull[3]);
    assertEquals(0, union.tags[3]);
    assertEquals(true, longs.isNull[3]);
    assertEquals(true, decs.isNull[3]);

    assertEquals(true, ts.isNull[4]);
    assertEquals(false, union.isNull[4]);
    assertEquals(1, union.tags[4]);
    assertEquals(true, strs.isNull[4]);
    assertEquals(true, decs.isNull[4]);

    assertEquals(false, ts.isNull[5]);
    assertEquals("1970-01-01 00:00:00.0", ts.asScratchTimestamp(5).toString());
    assertEquals(false, union.isNull[5]);
    assertEquals(0, union.tags[5]);
    assertEquals(false, longs.isNull[5]);
    assertEquals(200000, longs.vector[5]);
    assertEquals(false, decs.isNull[5]);
    assertEquals("10000000000000000000", decs.vector[5].toString());

    rand = new Random(42);
    for(int i=1970; i < 2038; ++i) {
      int row = 6 + i - 1970;
      assertEquals(Timestamp.valueOf(i + "-05-05 12:34:56." + i),
          ts.asScratchTimestamp(row));
      if ((i & 1) == 0) {
        assertEquals(0, union.tags[row]);
        assertEquals(i*i, longs.vector[row]);
      } else {
        assertEquals(1, union.tags[row]);
        assertEquals(Integer.toString(i * i), strs.toString(row));
      }
      assertEquals(new HiveDecimalWritable(HiveDecimal.create(new BigInteger(64, rand),
                                   rand.nextInt(18))), decs.vector[row]);
    }

    // rebuild the row batch, so that we can read by 1000 rows
    batch = schema.createRowBatch(1000);
    ts = (TimestampColumnVector) batch.cols[0];
    union = (UnionColumnVector) batch.cols[1];
    longs = (LongColumnVector) union.fields[0];
    strs = (BytesColumnVector) union.fields[1];
    decs = (DecimalColumnVector) batch.cols[2];

    for(int i=0; i < 5; ++i) {
      rows.nextBatch(batch);
      assertEquals("batch " + i, 1000, batch.size);
      assertEquals("batch " + i, false, union.isRepeating);
      assertEquals("batch " + i, true, union.noNulls);
      for(int r=0; r < batch.size; ++r) {
        assertEquals("bad tag at " + i + "." +r, 0, union.tags[r]);
      }
      assertEquals("batch " + i, true, longs.isRepeating);
      assertEquals("batch " + i, 1732050807, longs.vector[0]);
    }

    rows.nextBatch(batch);
    assertEquals(3, batch.size);
    assertEquals(0, union.tags[0]);
    assertEquals(0, longs.vector[0]);
    assertEquals(0, union.tags[1]);
    assertEquals(10, longs.vector[1]);
    assertEquals(0, union.tags[2]);
    assertEquals(138, longs.vector[2]);

    rows.nextBatch(batch);
    assertEquals(0, batch.size);
    Assert.assertEquals(1.0, rows.getProgress(), 0.00001);
    Assert.assertEquals(reader.getNumberOfRows(), rows.getRowNumber());
    rows.seekToRow(1);
    rows.nextBatch(batch);
    assertEquals(1000, batch.size);
    assertEquals(Timestamp.valueOf("2000-03-20 12:00:00.123456789"), ts.asScratchTimestamp(0));
    assertEquals(1, union.tags[0]);
    assertEquals("hello", strs.toString(0));
    assertEquals(new HiveDecimalWritable(HiveDecimal.create("-5643.234")), decs.vector[0]);
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
    Assert.assertEquals(CompressionKind.SNAPPY, reader.getCompressionKind());
    RecordReader rows = reader.rows();
    batch = reader.getSchema().createRowBatch(1000);
    rand = new Random(12);
    LongColumnVector longs = (LongColumnVector) batch.cols[0];
    BytesColumnVector strs = (BytesColumnVector) batch.cols[1];
    for(int b=0; b < 10; ++b) {
      rows.nextBatch(batch);
      assertEquals(1000, batch.size);
      for(int r=0; r < batch.size; ++r) {
        assertEquals(rand.nextInt(), longs.vector[r]);
        assertEquals(Integer.toHexString(rand.nextInt()), strs.toString(r));
      }
    }
    rows.nextBatch(batch);
    assertEquals(0, batch.size);
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
    Assert.assertEquals(50000, reader.getNumberOfRows());
    Assert.assertEquals(0, reader.getRowIndexStride());
    StripeInformation stripe = reader.getStripes().iterator().next();
    assertEquals(true, stripe.getDataLength() != 0);
    assertEquals(0, stripe.getIndexLength());
    RecordReader rows = reader.rows();
    rand = new Random(24);
    batch = reader.getSchema().createRowBatch(1000);
    LongColumnVector longs = (LongColumnVector) batch.cols[0];
    BytesColumnVector strs = (BytesColumnVector) batch.cols[1];
    for(int i=0; i < 50; ++i) {
      rows.nextBatch(batch);
      assertEquals("batch " + i, 1000, batch.size);
      for(int j=0; j < 200; ++j) {
        int intVal = rand.nextInt();
        String strVal = Integer.toBinaryString(rand.nextInt());
        for (int k = 0; k < 5; ++k) {
          assertEquals(intVal, longs.vector[j * 5 + k]);
          assertEquals(strVal, strs.toString(j * 5 + k));
        }
      }
    }
    rows.nextBatch(batch);
    assertEquals(0, batch.size);
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
    Assert.assertEquals(COUNT, reader.getNumberOfRows());
    RecordReader rows = reader.rows();
    // get the row index
    DataReader meta = RecordReaderUtils.createDefaultDataReader(
        DataReaderProperties.builder()
            .withBufferSize(reader.getCompressionSize())
            .withFileSystem(fs)
            .withPath(testFilePath)
            .withCompression(reader.getCompressionKind())
            .withTypeCount(reader.getSchema().getMaximumId() + 1)
            .withZeroCopy(false)
            .build());
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
    batch = reader.getSchema().createRowBatch();
    int nextRowInBatch = -1;
    for(int i=COUNT-1; i >= 0; --i, --nextRowInBatch) {
      // if we have consumed the previous batch read a new one
      if (nextRowInBatch < 0) {
        long base = Math.max(i - 1023, 0);
        rows.seekToRow(base);
        Assert.assertEquals("row " + i, true, rows.nextBatch(batch));
        nextRowInBatch = batch.size - 1;
      }
      checkRandomRow(batch, intValues, doubleValues,
          stringValues, byteValues, words, i, nextRowInBatch);
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
    rows = reader.rows(new Reader.Options()
        .range(offsetOfStripe2, offsetOfStripe4 - offsetOfStripe2)
        .include(columns));
    rows.seekToRow(lastRowOfStripe2);
    // we only want two rows
    batch = reader.getSchema().createRowBatch(2);
    Assert.assertEquals(true, rows.nextBatch(batch));
    assertEquals(1, batch.size);
    assertEquals(intValues[(int) lastRowOfStripe2], getLong(batch, 0));
    assertEquals(stringValues[(int) lastRowOfStripe2],
        getText(batch, 0).toString());
    Assert.assertEquals(true, rows.nextBatch(batch));
    assertEquals(intValues[(int) lastRowOfStripe2 + 1], getLong(batch, 0));
    assertEquals(stringValues[(int) lastRowOfStripe2 + 1],
        getText(batch, 0).toString());
    rows.close();
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

  private void checkRandomRow(VectorizedRowBatch batch,
                              long[] intValues, double[] doubleValues,
                              String[] stringValues,
                              BytesWritable[] byteValues,
                              String[] words, int i, int rowInBatch) {
    InnerStruct inner = new InnerStruct((int) intValues[i], stringValues[i]);
    InnerStruct inner2 = new InnerStruct((int) (intValues[i] >> 32),
        words[i % words.length] + "-x");
    checkBigRow(batch, rowInBatch, i, (intValues[i] & 1) == 0, (byte) intValues[i],
        (short) intValues[i], (int) intValues[i], intValues[i],
        (float) doubleValues[i], doubleValues[i], byteValues[i], stringValues[i],
        new MiddleStruct(inner, inner2), list(), map(inner, inner2));
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
    RecordReader rows = reader.rows(new Reader.Options()
        .range(0L, Long.MAX_VALUE)
        .include(new boolean[]{true, true, true})
        .searchArgument(sarg, new String[]{null, "int1", "string1"}));
    batch = reader.getSchema().createRowBatch(2000);
    LongColumnVector ints = (LongColumnVector) batch.cols[0];
    BytesColumnVector strs = (BytesColumnVector) batch.cols[1];

    Assert.assertEquals(1000L, rows.getRowNumber());
    Assert.assertEquals(true, rows.nextBatch(batch));
    assertEquals(1000, batch.size);

    for(int i=1000; i < 2000; ++i) {
      assertEquals(300 * i, ints.vector[i - 1000]);
      assertEquals(Integer.toHexString(10*i), strs.toString(i - 1000));
    }
    Assert.assertEquals(false, rows.nextBatch(batch));
    Assert.assertEquals(3500, rows.getRowNumber());

    // look through the file with no rows selected
    sarg = SearchArgumentFactory.newBuilder()
        .startAnd()
          .lessThan("int1", PredicateLeaf.Type.LONG, 0L)
        .end()
        .build();
    rows = reader.rows(new Reader.Options()
        .range(0L, Long.MAX_VALUE)
        .include(new boolean[]{true, true, true})
        .searchArgument(sarg, new String[]{null, "int1", "string1"}));
    Assert.assertEquals(3500L, rows.getRowNumber());
    assertTrue(!rows.nextBatch(batch));

    // select first 100 and last 100 rows
    sarg = SearchArgumentFactory.newBuilder()
        .startOr()
          .lessThan("int1", PredicateLeaf.Type.LONG, 300L * 100)
          .startNot()
            .lessThan("int1", PredicateLeaf.Type.LONG, 300L * 3400)
          .end()
        .end()
        .build();
    rows = reader.rows(new Reader.Options()
        .range(0L, Long.MAX_VALUE)
        .include(new boolean[]{true, true, true})
        .searchArgument(sarg, new String[]{null, "int1", "string1"}));
    Assert.assertEquals(0, rows.getRowNumber());
    Assert.assertEquals(true, rows.nextBatch(batch));
    assertEquals(1000, batch.size);
    Assert.assertEquals(3000, rows.getRowNumber());
    for(int i=0; i < 1000; ++i) {
      assertEquals(300 * i, ints.vector[i]);
      assertEquals(Integer.toHexString(10*i), strs.toString(i));
    }

    Assert.assertEquals(true, rows.nextBatch(batch));
    assertEquals(500, batch.size);
    Assert.assertEquals(3500, rows.getRowNumber());
    for(int i=3000; i < 3500; ++i) {
      assertEquals(300 * i, ints.vector[i - 3000]);
      assertEquals(Integer.toHexString(10*i), strs.toString(i - 3000));
    }
    Assert.assertEquals(false, rows.nextBatch(batch));
    Assert.assertEquals(3500, rows.getRowNumber());
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
    ((TimestampColumnVector) batch.cols[7]).set(0,
        new Timestamp(115, 9, 23, 10, 11, 59,
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

      Timestamp ts = new Timestamp(115, 9, 25, 10, 11, 59 + r, 999999999);
      ((TimestampColumnVector) batch.cols[7]).set(r, ts);
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
    batch = reader.getSchema().createRowBatch(1024);
    BytesColumnVector bins = (BytesColumnVector) batch.cols[0];
    LongColumnVector bools = (LongColumnVector) batch.cols[1];
    LongColumnVector bytes = (LongColumnVector) batch.cols[2];
    LongColumnVector longs = (LongColumnVector) batch.cols[3];
    DoubleColumnVector floats = (DoubleColumnVector) batch.cols[4];
    DoubleColumnVector doubles = (DoubleColumnVector) batch.cols[5];
    LongColumnVector dates = (LongColumnVector) batch.cols[6];
    TimestampColumnVector times = (TimestampColumnVector) batch.cols[7];
    DecimalColumnVector decs = (DecimalColumnVector) batch.cols[8];
    BytesColumnVector strs = (BytesColumnVector) batch.cols[9];
    BytesColumnVector chars = (BytesColumnVector) batch.cols[10];
    BytesColumnVector vcs = (BytesColumnVector) batch.cols[11];
    StructColumnVector structs = (StructColumnVector) batch.cols[12];
    UnionColumnVector unions = (UnionColumnVector) batch.cols[13];
    ListColumnVector lists = (ListColumnVector) batch.cols[14];
    MapColumnVector maps = (MapColumnVector) batch.cols[15];
    LongColumnVector structInts = (LongColumnVector) structs.fields[0];
    LongColumnVector unionInts = (LongColumnVector) unions.fields[1];
    LongColumnVector listInts = (LongColumnVector) lists.child;
    BytesColumnVector mapKeys = (BytesColumnVector) maps.keys;
    BytesColumnVector mapValues = (BytesColumnVector) maps.values;

    Assert.assertEquals(true, rows.nextBatch(batch));
    assertEquals(1024, batch.size);

    // read the 1024 nulls
    for(int f=0; f < batch.cols.length; ++f) {
      assertEquals("field " + f,
          true, batch.cols[f].isRepeating);
      assertEquals("field " + f,
          false, batch.cols[f].noNulls);
      assertEquals("field " + f,
          true, batch.cols[f].isNull[0]);
    }

    // read the 1024 repeat values
    Assert.assertEquals(true, rows.nextBatch(batch));
    assertEquals(1024, batch.size);
    for(int r=0; r < 1024; ++r) {
      assertEquals("row " + r, "Horton", bins.toString(r));
      assertEquals("row " + r, 1, bools.vector[r]);
      assertEquals("row " + r, -126, bytes.vector[r]);
      assertEquals("row " + r, 1311768467463790320L, longs.vector[r]);
      assertEquals("row " + r, 1.125, floats.vector[r], 0.00001);
      assertEquals("row " + r, 9.765625E-4, doubles.vector[r], 0.000001);
      assertEquals("row " + r, "2011-07-01",
          new DateWritable((int) dates.vector[r]).toString());
      assertEquals("row " + r, "2015-10-23 10:11:59.999999999",
          times.asScratchTimestamp(r).toString());
      assertEquals("row " + r, "1.234567", decs.vector[r].toString());
      assertEquals("row " + r, "Echelon", strs.toString(r));
      assertEquals("row " + r, "Juggernaut", chars.toString(r));
      assertEquals("row " + r, "Dreadnaugh", vcs.toString(r));
      assertEquals("row " + r, 123, structInts.vector[r]);
      assertEquals("row " + r, 1, unions.tags[r]);
      assertEquals("row " + r, 1234, unionInts.vector[r]);
      assertEquals("row " + r, 3, lists.lengths[r]);
      assertEquals("row " + r, true, listInts.isRepeating);
      assertEquals("row " + r, 31415, listInts.vector[0]);
      assertEquals("row " + r, 3, maps.lengths[r]);
      assertEquals("row " + r, "ORC", mapKeys.toString((int) maps.offsets[r]));
      assertEquals("row " + r, "Hive", mapKeys.toString((int) maps.offsets[r] + 1));
      assertEquals("row " + r, "LLAP", mapKeys.toString((int) maps.offsets[r] + 2));
      assertEquals("row " + r, "fast", mapValues.toString((int) maps.offsets[r]));
      assertEquals("row " + r, "fast", mapValues.toString((int) maps.offsets[r] + 1));
      assertEquals("row " + r, "fast", mapValues.toString((int) maps.offsets[r] + 2));
    }

    // read the second set of 1024 nulls
    Assert.assertEquals(true, rows.nextBatch(batch));
    assertEquals(1024, batch.size);
    for(int f=0; f < batch.cols.length; ++f) {
      assertEquals("field " + f,
          true, batch.cols[f].isRepeating);
      assertEquals("field " + f,
          false, batch.cols[f].noNulls);
      assertEquals("field " + f,
          true, batch.cols[f].isNull[0]);
    }

    Assert.assertEquals(true, rows.nextBatch(batch));
    assertEquals(1024, batch.size);
    for(int r=0; r < 1024; ++r) {
      String hex = Integer.toHexString(r);

      assertEquals("row " + r, hex, bins.toString(r));
      assertEquals("row " + r, r % 2 == 1 ? 1 : 0, bools.vector[r]);
      assertEquals("row " + r, (byte) (r % 255), bytes.vector[r]);
      assertEquals("row " + r, 31415L * r, longs.vector[r]);
      assertEquals("row " + r, 1.125F * r, floats.vector[r], 0.0001);
      assertEquals("row " + r, 0.0009765625 * r, doubles.vector[r], 0.000001);
      assertEquals("row " + r, new DateWritable(new Date(111, 6, 1 + r)),
          new DateWritable((int) dates.vector[r]));
      assertEquals("row " + r,
          new Timestamp(115, 9, 25, 10, 11, 59 + r, 999999999),
          times.asScratchTimestamp(r));
      assertEquals("row " + r, "1.234567", decs.vector[r].toString());
      assertEquals("row " + r, Integer.toString(r), strs.toString(r));
      assertEquals("row " + r, Integer.toHexString(r), chars.toString(r));
      assertEquals("row " + r, Integer.toHexString(r * 128), vcs.toString(r));
      assertEquals("row " + r, r + 13, structInts.vector[r]);
      assertEquals("row " + r, 1, unions.tags[r]);
      assertEquals("row " + r, r + 42, unionInts.vector[r]);
      assertEquals("row " + r, 3, lists.lengths[r]);
      assertEquals("row " + r, 31415, listInts.vector[(int) lists.offsets[r]]);
      assertEquals("row " + r, 31416, listInts.vector[(int) lists.offsets[r] + 1]);
      assertEquals("row " + r, 31417, listInts.vector[(int) lists.offsets[r] + 2]);
      assertEquals("row " + r, 3, maps.lengths[3]);
      assertEquals("row " + r, Integer.toHexString(3 * r), mapKeys.toString((int) maps.offsets[r]));
      assertEquals("row " + r, Integer.toString(3 * r), mapValues.toString((int) maps.offsets[r]));
      assertEquals("row " + r, Integer.toHexString(3 * r + 1), mapKeys.toString((int) maps.offsets[r] + 1));
      assertEquals("row " + r, Integer.toString(3 * r + 1), mapValues.toString((int) maps.offsets[r] + 1));
      assertEquals("row " + r, Integer.toHexString(3 * r + 2), mapKeys.toString((int) maps.offsets[r] + 2));
      assertEquals("row " + r, Integer.toString(3 * r + 2), mapValues.toString((int) maps.offsets[r] + 2));
    }

    // should have no more rows
    Assert.assertEquals(false, rows.nextBatch(batch));
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
    batch = reader.getSchema().createRowBatch();
    Assert.assertEquals(true, rows.nextBatch(batch));
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
    batch = reader.getSchema().createRowBatch();
    Assert.assertEquals(true, rows.nextBatch(batch));
    assertEquals(1024, batch.size);
    for(int r=0; r < 1024; ++r) {
      assertEquals(Integer.toString(r * 10001),
          makeString((BytesColumnVector) batch.cols[0], r));
    }
    Assert.assertEquals(true, rows.nextBatch(batch));
    assertEquals(1024, batch.size);
    for(int r=0; r < 1024; ++r) {
      assertEquals("Halloween",
          makeString((BytesColumnVector) batch.cols[0], r));
    }
    Assert.assertEquals(false, rows.nextBatch(batch));
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
    batch = reader.getSchema().createRowBatch();
    rows.nextBatch(batch);
    assertEquals(1024, batch.size);
    StructColumnVector inner = (StructColumnVector) batch.cols[0];
    LongColumnVector vec = (LongColumnVector) inner.fields[0];
    for(int r=0; r < 1024; ++r) {
      if (r < 200 || (r >= 400 && r < 600) || r >= 800) {
        assertEquals("row " + r, true, inner.isNull[r]);
      } else {
        assertEquals("row " + r, false, inner.isNull[r]);
        assertEquals("row " + r, r, vec.vector[r]);
      }
    }
    rows.nextBatch(batch);
    assertEquals(0, batch.size);
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
    batch = reader.getSchema().createRowBatch(1024);
    UnionColumnVector union = (UnionColumnVector) batch.cols[0];
    LongColumnVector ints = (LongColumnVector) union.fields[0];
    LongColumnVector longs = (LongColumnVector) union.fields[1];
    Assert.assertEquals(true, rows.nextBatch(batch));
    assertEquals(1024, batch.size);
    for(int r=0; r < 1024; ++r) {
      if (r < 200) {
        assertEquals("row " + r, true, union.isNull[r]);
      } else if (r < 300) {
        assertEquals("row " + r, false, union.isNull[r]);
        assertEquals("row " + r, 0, union.tags[r]);
        assertEquals("row " + r, r, ints.vector[r]);
      } else if (r < 400) {
        assertEquals("row " + r, false, union.isNull[r]);
        assertEquals("row " + r, 1, union.tags[r]);
        assertEquals("row " + r, -r, longs.vector[r]);
      } else if (r < 600) {
        assertEquals("row " + r, true, union.isNull[r]);
      } else if (r < 800) {
        assertEquals("row " + r, false, union.isNull[r]);
        assertEquals("row " + r, 1, union.tags[r]);
        assertEquals("row " + r, -r, longs.vector[r]);
      } else if (r < 1000) {
        assertEquals("row " + r, true, union.isNull[r]);
      } else {
        assertEquals("row " + r, false, union.isNull[r]);
        assertEquals("row " + r, 1, union.tags[r]);
        assertEquals("row " + r, -r, longs.vector[r]);
      }
    }
    Assert.assertEquals(false, rows.nextBatch(batch));
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
    batch = reader.getSchema().createRowBatch(1024);
    list = (ListColumnVector) batch.cols[0];
    rows.nextBatch(batch);
    assertEquals(1024, batch.size);
    for(int r=0; r < 1024; ++r) {
      StringBuilder actual = new StringBuilder();
      list.stringifyValue(actual, r);
      if (r < 200) {
        assertEquals("row " + r, "null", actual.toString());
      } else if (r < 300) {
        assertEquals("row " + r, "[" + ((r - 200) * 10) + "]",
            actual.toString());
      } else if (r < 400) {
        assertEquals("row " + r, "null", actual.toString());
      } else if (r < 500) {
        assertEquals("row " + r, "[" + ((r - 300) * 10) + "]",
            actual.toString());
      } else if (r < 600) {
        assertEquals("row " + r, "null", actual.toString());
      } else if (r < 700) {
        assertEquals("row " + r, "[" + (10 * r) + ", " + (10 * (r + 1)) + "]",
            actual.toString());
      } else {
        assertEquals("row " + r, "null", actual.toString());
      }
    }
    Assert.assertEquals(false, rows.nextBatch(batch));
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
    batch = reader.getSchema().createRowBatch();
    map = (MapColumnVector) batch.cols[0];
    rows.nextBatch(batch);
    assertEquals(1024, batch.size);
    for(int r=0; r < 1024; ++r) {
      StringBuilder buffer = new StringBuilder();
      map.stringifyValue(buffer, r);
      String actual = buffer.toString();
      if (r < 200) {
        assertEquals("row " + r, "null", actual);
      } else if (r < 300) {
        assertEquals("row " + r, "[{\"key\": " + (r - 200) +
                ", \"value\": " + ((r - 200) * 10) + "}]",
            actual);
      } else if (r < 400) {
        assertEquals("row " + r, "null", actual);
      } else if (r < 500) {
        assertEquals("row " + r, "[{\"key\": " + (r - 300) +
                ", \"value\": " + ((r - 300) * 10) + "}]", actual);
      } else if (r < 600) {
        assertEquals("row " + r, "null", actual);
      } else if (r < 700) {
        assertEquals("row " + r, "[{\"key\": " + r + ", \"value\": " + (r * 10)
                + "}, {\"key\": " + (r + 1) + ", \"value\": " + (10 * (r + 1))
                + "}]", actual);
      } else {
        assertEquals("row " + r, "null", actual);
      }
    }
    rows.nextBatch(batch);
    assertEquals(0, batch.size);
  }
}
