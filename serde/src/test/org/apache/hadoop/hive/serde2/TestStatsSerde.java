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

package org.apache.hadoop.hive.serde2;

import java.sql.Date;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.binarysortable.MyTestClass;
import org.apache.hadoop.hive.serde2.binarysortable.MyTestInnerStruct;
import org.apache.hadoop.hive.serde2.binarysortable.MyTestPrimitiveClass;
import org.apache.hadoop.hive.serde2.binarysortable.TestBinarySortableSerDe;
import org.apache.hadoop.hive.serde2.binarysortable.MyTestPrimitiveClass.ExtraTypeInfo;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

public class TestStatsSerde extends TestCase {

  public TestStatsSerde(String name) {
    super(name);
  }

  /**
   * Test LazySimpleSerDe
   */

  public void testLazySimpleSerDe() throws Throwable {
    try {
      // Create the SerDe
      System.out.println("test: testLazySimpleSerDe");
      LazySimpleSerDe serDe = new LazySimpleSerDe();
      Configuration conf = new Configuration();
      Properties tbl = createProperties();
      SerDeUtils.initializeSerDe(serDe, conf, tbl, null);

      // Data
      Text t = new Text("123\t456\t789\t1000\t5.3\thive and hadoop\t1.\tNULL");
      // Test
      deserializeAndSerializeLazySimple(serDe, t);
      System.out.println("test: testLazySimpleSerDe - OK");

    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }

  private void deserializeAndSerializeLazySimple(LazySimpleSerDe serDe, Text t)
      throws SerDeException {

    // Get the row structure
    StructObjectInspector oi = (StructObjectInspector) serDe
        .getObjectInspector();

    // Deserialize
    Object row = serDe.deserialize(t);
    assertEquals("serialized size correct after deserialization", serDe.getSerDeStats()
        .getRawDataSize(), t.getLength());

    // Serialize
    Text serializedText = (Text) serDe.serialize(row, oi);
    assertEquals("serialized size correct after serialization", serDe.getSerDeStats()
        .getRawDataSize(),
        serializedText.toString().length());
  }

  /**
   * Test LazyBinarySerDe
   */

  public void testLazyBinarySerDe() throws Throwable {
    try {
      System.out.println("test: testLazyBinarySerDe");
      int num = 1000;
      Random r = new Random(1234);
      MyTestClass rows[] = new MyTestClass[num];

      for (int i = 0; i < num; i++) {
        MyTestClass t = new MyTestClass();
        ExtraTypeInfo extraTypeInfo = new ExtraTypeInfo();
        t.randomFill(r, extraTypeInfo);
        rows[i] = t;
      }

      StructObjectInspector rowOI = (StructObjectInspector) ObjectInspectorFactory
          .getReflectionObjectInspector(MyTestClass.class,
              ObjectInspectorOptions.JAVA);

      String fieldNames = ObjectInspectorUtils.getFieldNames(rowOI);
      String fieldTypes = ObjectInspectorUtils.getFieldTypes(rowOI);

      Properties schema = new Properties();
      schema.setProperty(serdeConstants.LIST_COLUMNS, fieldNames);
      schema.setProperty(serdeConstants.LIST_COLUMN_TYPES, fieldTypes);

      LazyBinarySerDe serDe = new LazyBinarySerDe();
      SerDeUtils.initializeSerDe(serDe, new Configuration(), schema, null);

      deserializeAndSerializeLazyBinary(serDe, rows, rowOI);
      System.out.println("test: testLazyBinarySerDe - OK");

    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }

  private void deserializeAndSerializeLazyBinary(AbstractSerDe serDe, Object[] rows, ObjectInspector rowOI)
      throws Throwable {

    BytesWritable bytes[] = new BytesWritable[rows.length];
    int lenS = 0;
    int lenD = 0;

    for (int i = 0; i < rows.length; i++) {
      BytesWritable s = (BytesWritable) serDe.serialize(rows[i], rowOI);
      lenS += serDe.getSerDeStats().getRawDataSize();
      bytes[i] = new BytesWritable();
      bytes[i].set(s);
    }

    for (int i = 0; i < rows.length; i++) {
      serDe.deserialize(bytes[i]);
      lenD += serDe.getSerDeStats().getRawDataSize();
    }

    // serialized sizes after serialization and deserialization should be equal
    assertEquals(lenS, lenD);
    assertNotSame(0, lenS);
  }

  /**
   * Test ColumnarSerDe
   */

  public void testColumnarSerDe() throws Throwable {
    try {
      System.out.println("test: testColumnarSerde");
      // Create the SerDe
      ColumnarSerDe serDe = new ColumnarSerDe();
      Configuration conf = new Configuration();
      Properties tbl = createProperties();
      SerDeUtils.initializeSerDe(serDe, conf, tbl, null);

      // Data
      BytesRefArrayWritable braw = new BytesRefArrayWritable(8);
      String[] data = {"123", "456", "789", "1000", "5.3", "hive and hadoop", "1.", "NULL"};
      for (int i = 0; i < 8; i++) {
        braw.set(i, new BytesRefWritable(data[i].getBytes()));
      }
      // Test
      deserializeAndSerializeColumnar(serDe, braw, data);
      System.out.println("test: testColumnarSerde - OK");


    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }


  private void deserializeAndSerializeColumnar(ColumnarSerDe serDe, BytesRefArrayWritable t,
      String[] data) throws SerDeException {

    // Get the row structure
    StructObjectInspector oi = (StructObjectInspector) serDe
        .getObjectInspector();

    // Deserialize
    Object row = serDe.deserialize(t);
    int size = 0;
    for (int i = 0; i < data.length; i++) {
      size += data[i].length();
    }

    assertEquals("serialized size correct after deserialization", size, serDe.getSerDeStats()
        .getRawDataSize());
    assertNotSame(0, size);

    BytesRefArrayWritable serializedData = (BytesRefArrayWritable) serDe.serialize(row, oi);
    size = 0;
    for (int i = 0; i < serializedData.size(); i++) {
      size += serializedData.get(i).getLength();
    }

    assertEquals("serialized size correct after serialization", size, serDe.getSerDeStats()
        .getRawDataSize());
    assertNotSame(0, size);

  }

  private Properties createProperties() {
    Properties tbl = new Properties();

    // Set the configuration parameters
    tbl.setProperty(serdeConstants.SERIALIZATION_FORMAT, "9");
    tbl.setProperty("columns",
        "abyte,ashort,aint,along,adouble,astring,anullint,anullstring");
    tbl.setProperty("columns.types",
        "tinyint:smallint:int:bigint:double:string:int:string");
    tbl.setProperty(serdeConstants.SERIALIZATION_NULL_FORMAT, "NULL");
    return tbl;
  }




}
