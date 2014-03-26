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
import static org.junit.Assert.assertNull;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import com.google.common.collect.Lists;

public class TestOrcNullOptimization {

  public static class MyStruct {
    Integer a;
    String b;
    Boolean c;
    List<InnerStruct> list = new ArrayList<InnerStruct>();

    public MyStruct(Integer a, String b, Boolean c, List<InnerStruct> l) {
      this.a = a;
      this.b = b;
      this.c = c;
      this.list = l;
    }
  }

  public static class InnerStruct {
    Integer z;

    public InnerStruct(int z) {
      this.z = z;
    }
  }

  Path workDir = new Path(System.getProperty("test.tmp.dir",
      "target" + File.separator + "test" + File.separator + "tmp"));

  Configuration conf;
  FileSystem fs;
  Path testFilePath;

  @Rule
  public TestName testCaseName = new TestName();

  @Before
  public void openFileSystem() throws Exception {
    conf = new Configuration();
    fs = FileSystem.getLocal(conf);
    testFilePath = new Path(workDir, "TestOrcNullOptimization." +
        testCaseName.getMethodName() + ".orc");
    fs.delete(testFilePath, false);
  }

  @Test
  public void testMultiStripeWithNull() throws Exception {
    ObjectInspector inspector;
    synchronized (TestOrcNullOptimization.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (MyStruct.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .inspector(inspector)
                                         .stripeSize(100000)
                                         .compress(CompressionKind.NONE)
                                         .bufferSize(10000));
    Random rand = new Random(100);
    writer.addRow(new MyStruct(null, null, true,
                               Lists.newArrayList(new InnerStruct(100))));
    for (int i = 2; i < 20000; i++) {
      writer.addRow(new MyStruct(rand.nextInt(1), "a", true, Lists
          .newArrayList(new InnerStruct(100))));
    }
    writer.addRow(new MyStruct(null, null, true,
                               Lists.newArrayList(new InnerStruct(100))));
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    // check the stats
    ColumnStatistics[] stats = reader.getStatistics();
    assertEquals(20000, reader.getNumberOfRows());
    assertEquals(20000, stats[0].getNumberOfValues());

    assertEquals(0, ((IntegerColumnStatistics) stats[1]).getMaximum());
    assertEquals(0, ((IntegerColumnStatistics) stats[1]).getMinimum());
    assertEquals(true, ((IntegerColumnStatistics) stats[1]).isSumDefined());
    assertEquals(0, ((IntegerColumnStatistics) stats[1]).getSum());
    assertEquals("count: 19998 min: 0 max: 0 sum: 0",
        stats[1].toString());

    assertEquals("a", ((StringColumnStatistics) stats[2]).getMaximum());
    assertEquals("a", ((StringColumnStatistics) stats[2]).getMinimum());
    assertEquals(19998, stats[2].getNumberOfValues());
    assertEquals("count: 19998 min: a max: a sum: 19998",
        stats[2].toString());

    // check the inspectors
    StructObjectInspector readerInspector =
        (StructObjectInspector) reader.getObjectInspector();
    assertEquals(ObjectInspector.Category.STRUCT,
        readerInspector.getCategory());
    assertEquals("struct<a:int,b:string,c:boolean,list:array<struct<z:int>>>",
        readerInspector.getTypeName());

    RecordReader rows = reader.rows();

    List<Boolean> expected = Lists.newArrayList();
    for (StripeInformation sinfo : reader.getStripes()) {
      expected.add(false);
    }
    // only the first and last stripe will have PRESENT stream
    expected.set(0, true);
    expected.set(expected.size() - 1, true);

    List<Boolean> got = Lists.newArrayList();
    // check if the strip footer contains PRESENT stream
    for (StripeInformation sinfo : reader.getStripes()) {
      OrcProto.StripeFooter sf =
        ((RecordReaderImpl) rows).readStripeFooter(sinfo);
      got.add(sf.toString().indexOf(OrcProto.Stream.Kind.PRESENT.toString())
              != -1);
    }
    assertEquals(expected, got);

    // row 1
    OrcStruct row = (OrcStruct) rows.next(null);
    assertNotNull(row);
    assertNull(row.getFieldValue(0));
    assertNull(row.getFieldValue(1));
    assertEquals(new BooleanWritable(true), row.getFieldValue(2));
    assertEquals(new IntWritable(100),
        ((OrcStruct) ((ArrayList<?>) row.getFieldValue(3)).get(0)).
                 getFieldValue(0));

    rows.seekToRow(19998);
    // last-1 row
    row = (OrcStruct) rows.next(null);
    assertNotNull(row);
    assertNotNull(row.getFieldValue(1));
    assertEquals(new IntWritable(0), row.getFieldValue(0));
    assertEquals(new BooleanWritable(true), row.getFieldValue(2));
    assertEquals(new IntWritable(100),
        ((OrcStruct) ((ArrayList<?>) row.getFieldValue(3)).get(0)).
                 getFieldValue(0));

    // last row
    row = (OrcStruct) rows.next(row);
    assertNotNull(row);
    assertNull(row.getFieldValue(0));
    assertNull(row.getFieldValue(1));
    assertEquals(new BooleanWritable(true), row.getFieldValue(2));
    assertEquals(new IntWritable(100),
        ((OrcStruct) ((ArrayList<?>) row.getFieldValue(3)).get(0)).
                 getFieldValue(0));

    rows.close();
  }

  @Test
  public void testMultiStripeWithoutNull() throws Exception {
    ObjectInspector inspector;
    synchronized (TestOrcNullOptimization.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (MyStruct.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .inspector(inspector)
                                         .stripeSize(100000)
                                         .compress(CompressionKind.NONE)
                                         .bufferSize(10000));
    Random rand = new Random(100);
    for (int i = 1; i < 20000; i++) {
      writer.addRow(new MyStruct(rand.nextInt(1), "a", true, Lists
          .newArrayList(new InnerStruct(100))));
    }
    writer.addRow(new MyStruct(0, "b", true,
                               Lists.newArrayList(new InnerStruct(100))));
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    // check the stats
    ColumnStatistics[] stats = reader.getStatistics();
    assertEquals(20000, reader.getNumberOfRows());
    assertEquals(20000, stats[0].getNumberOfValues());

    assertEquals(0, ((IntegerColumnStatistics) stats[1]).getMaximum());
    assertEquals(0, ((IntegerColumnStatistics) stats[1]).getMinimum());
    assertEquals(true, ((IntegerColumnStatistics) stats[1]).isSumDefined());
    assertEquals(0, ((IntegerColumnStatistics) stats[1]).getSum());
    assertEquals("count: 20000 min: 0 max: 0 sum: 0",
        stats[1].toString());

    assertEquals("b", ((StringColumnStatistics) stats[2]).getMaximum());
    assertEquals("a", ((StringColumnStatistics) stats[2]).getMinimum());
    assertEquals(20000, stats[2].getNumberOfValues());
    assertEquals("count: 20000 min: a max: b sum: 20000",
        stats[2].toString());

    // check the inspectors
    StructObjectInspector readerInspector =
        (StructObjectInspector) reader.getObjectInspector();
    assertEquals(ObjectInspector.Category.STRUCT,
        readerInspector.getCategory());
    assertEquals("struct<a:int,b:string,c:boolean,list:array<struct<z:int>>>",
        readerInspector.getTypeName());

    RecordReader rows = reader.rows();

    // none of the stripes will have PRESENT stream
    List<Boolean> expected = Lists.newArrayList();
    for (StripeInformation sinfo : reader.getStripes()) {
      expected.add(false);
    }

    List<Boolean> got = Lists.newArrayList();
    // check if the strip footer contains PRESENT stream
    for (StripeInformation sinfo : reader.getStripes()) {
      OrcProto.StripeFooter sf =
        ((RecordReaderImpl) rows).readStripeFooter(sinfo);
      got.add(sf.toString().indexOf(OrcProto.Stream.Kind.PRESENT.toString())
              != -1);
    }
    assertEquals(expected, got);

    rows.seekToRow(19998);
    // last-1 row
    OrcStruct row = (OrcStruct) rows.next(null);
    assertNotNull(row);
    assertNotNull(row.getFieldValue(1));
    assertEquals(new IntWritable(0), row.getFieldValue(0));
    assertEquals("a", row.getFieldValue(1).toString());
    assertEquals(new BooleanWritable(true), row.getFieldValue(2));
    assertEquals(new IntWritable(100),
                 ((OrcStruct) ((ArrayList<?>) row.getFieldValue(3)).get(0)).
                   getFieldValue(0));

    // last row
    row = (OrcStruct) rows.next(row);
    assertNotNull(row);
    assertNotNull(row.getFieldValue(0));
    assertNotNull(row.getFieldValue(1));
    assertEquals("b", row.getFieldValue(1).toString());
    assertEquals(new BooleanWritable(true), row.getFieldValue(2));
    assertEquals(new IntWritable(100),
                 ((OrcStruct) ((ArrayList<?>) row.getFieldValue(3)).get(0)).
                   getFieldValue(0));
    rows.close();
  }

  @Test
  public void testColumnsWithNullAndCompression() throws Exception {
    ObjectInspector inspector;
    synchronized (TestOrcNullOptimization.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (MyStruct.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .inspector(inspector)
                                         .stripeSize(100000)
                                         .bufferSize(10000));
    writer.addRow(new MyStruct(3, "a", true,
                               Lists.newArrayList(new InnerStruct(100))));
    writer.addRow(new MyStruct(null, "b", true,
                               Lists.newArrayList(new InnerStruct(100))));
    writer.addRow(new MyStruct(3, null, false,
                               Lists.newArrayList(new InnerStruct(100))));
    writer.addRow(new MyStruct(3, "d", true,
                               Lists.newArrayList(new InnerStruct(100))));
    writer.addRow(new MyStruct(2, "e", true,
                               Lists.newArrayList(new InnerStruct(100))));
    writer.addRow(new MyStruct(2, "f", true,
                               Lists.newArrayList(new InnerStruct(100))));
    writer.addRow(new MyStruct(2, "g", true,
                               Lists.newArrayList(new InnerStruct(100))));
    writer.addRow(new MyStruct(2, "h", true,
                               Lists.newArrayList(new InnerStruct(100))));
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    // check the stats
    ColumnStatistics[] stats = reader.getStatistics();
    assertEquals(8, reader.getNumberOfRows());
    assertEquals(8, stats[0].getNumberOfValues());

    assertEquals(3, ((IntegerColumnStatistics) stats[1]).getMaximum());
    assertEquals(2, ((IntegerColumnStatistics) stats[1]).getMinimum());
    assertEquals(true, ((IntegerColumnStatistics) stats[1]).isSumDefined());
    assertEquals(17, ((IntegerColumnStatistics) stats[1]).getSum());
    assertEquals("count: 7 min: 2 max: 3 sum: 17",
        stats[1].toString());

    assertEquals("h", ((StringColumnStatistics) stats[2]).getMaximum());
    assertEquals("a", ((StringColumnStatistics) stats[2]).getMinimum());
    assertEquals(7, stats[2].getNumberOfValues());
    assertEquals("count: 7 min: a max: h sum: 7",
        stats[2].toString());

    // check the inspectors
    StructObjectInspector readerInspector =
        (StructObjectInspector) reader.getObjectInspector();
    assertEquals(ObjectInspector.Category.STRUCT,
        readerInspector.getCategory());
    assertEquals("struct<a:int,b:string,c:boolean,list:array<struct<z:int>>>",
        readerInspector.getTypeName());

    RecordReader rows = reader.rows();
    // only the last strip will have PRESENT stream
    List<Boolean> expected = Lists.newArrayList();
    for (StripeInformation sinfo : reader.getStripes()) {
      expected.add(false);
    }
    expected.set(expected.size() - 1, true);

    List<Boolean> got = Lists.newArrayList();
    // check if the strip footer contains PRESENT stream
    for (StripeInformation sinfo : reader.getStripes()) {
      OrcProto.StripeFooter sf =
        ((RecordReaderImpl) rows).readStripeFooter(sinfo);
      got.add(sf.toString().indexOf(OrcProto.Stream.Kind.PRESENT.toString())
              != -1);
    }
    assertEquals(expected, got);

    // row 1
    OrcStruct row = (OrcStruct) rows.next(null);
    assertNotNull(row);
    assertEquals(new IntWritable(3), row.getFieldValue(0));
    assertEquals("a", row.getFieldValue(1).toString());
    assertEquals(new BooleanWritable(true), row.getFieldValue(2));
    assertEquals(new IntWritable(100),
        ((OrcStruct) ((ArrayList<?>) row.getFieldValue(3)).get(0)).
                 getFieldValue(0));

    // row 2
    row = (OrcStruct) rows.next(row);
    assertNotNull(row);
    assertNull(row.getFieldValue(0));
    assertEquals("b", row.getFieldValue(1).toString());
    assertEquals(new BooleanWritable(true), row.getFieldValue(2));
    assertEquals(new IntWritable(100),
        ((OrcStruct) ((ArrayList<?>) row.getFieldValue(3)).get(0)).
                 getFieldValue(0));

    // row 3
    row = (OrcStruct) rows.next(row);
    assertNotNull(row);
    assertNull(row.getFieldValue(1));
    assertEquals(new IntWritable(3), row.getFieldValue(0));
    assertEquals(new BooleanWritable(false), row.getFieldValue(2));
    assertEquals(new IntWritable(100),
                 ((OrcStruct) ((ArrayList<?>) row.getFieldValue(3)).get(0)).
                 getFieldValue(0));
    rows.close();
  }
}
