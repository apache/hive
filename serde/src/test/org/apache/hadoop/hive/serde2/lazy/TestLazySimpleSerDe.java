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
package org.apache.hadoop.hive.serde2.lazy;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.binarysortable.MyTestClass;
import org.apache.hadoop.hive.serde2.binarysortable.MyTestPrimitiveClass.ExtraTypeInfo;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.SerDeParameters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

/**
 * TestLazySimpleSerDe.
 *
 */
public class TestLazySimpleSerDe extends TestCase {

  /**
   * Test the LazySimpleSerDe class.
   */
  public void testLazySimpleSerDe() throws Throwable {
    try {
      // Create the SerDe
      LazySimpleSerDe serDe = new LazySimpleSerDe();
      Configuration conf = new Configuration();
      Properties tbl = new Properties();
      tbl.setProperty(serdeConstants.SERIALIZATION_FORMAT, "9");
      tbl.setProperty("columns",
          "abyte,ashort,aint,along,adouble,astring,anullint,anullstring,aba");
      tbl.setProperty("columns.types",
          "tinyint:smallint:int:bigint:double:string:int:string:binary");
      tbl.setProperty(serdeConstants.SERIALIZATION_NULL_FORMAT, "NULL");
      SerDeUtils.initializeSerDe(serDe, conf, tbl, null);

      // Data
      Text t = new Text("123\t456\t789\t1000\t5.3\thive and hadoop\t1.\tNULL\t");
      t.append(new byte[]{(byte)Integer.parseInt("10111111", 2)}, 0, 1);
      StringBuilder sb = new StringBuilder("123\t456\t789\t1000\t5.3\thive and hadoop\t1\tNULL\t");
      String s = sb.append(new String(Base64.encodeBase64(new byte[]{(byte)Integer.parseInt("10111111", 2)}))).toString();
      Object[] expectedFieldsData = {new ByteWritable((byte) 123),
          new ShortWritable((short) 456), new IntWritable(789),
          new LongWritable(1000), new DoubleWritable(5.3),
          new Text("hive and hadoop"), new IntWritable(1), null, new BytesWritable(new byte[]{(byte)Integer.parseInt("10111111", 2)})};

      // Test
      deserializeAndSerialize(serDe, t, s, expectedFieldsData);

    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }

  /**
   * Test the LazySimpleSerDe class with LastColumnTakesRest option.
   */
  public void testLazySimpleSerDeLastColumnTakesRest() throws Throwable {
    try {
      // Create the SerDe
      LazySimpleSerDe serDe = new LazySimpleSerDe();
      Configuration conf = new Configuration();
      Properties tbl = createProperties();
      tbl.setProperty(serdeConstants.SERIALIZATION_LAST_COLUMN_TAKES_REST, "true");
      SerDeUtils.initializeSerDe(serDe, conf, tbl, null);

      // Data
      Text t = new Text("123\t456\t789\t1000\t5.3\thive and hadoop\t1.\ta\tb\t");
      String s = "123\t456\t789\t1000\t5.3\thive and hadoop\t1\ta\tb\t";
      Object[] expectedFieldsData = {new ByteWritable((byte) 123),
          new ShortWritable((short) 456), new IntWritable(789),
          new LongWritable(1000), new DoubleWritable(5.3),
          new Text("hive and hadoop"), new IntWritable(1), new Text("a\tb\t")};

      // Test
      deserializeAndSerialize(serDe, t, s, expectedFieldsData);

    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }

  /**
   * Test the LazySimpleSerDe class with extra columns.
   */
  public void testLazySimpleSerDeExtraColumns() throws Throwable {
    try {
      // Create the SerDe
      LazySimpleSerDe serDe = new LazySimpleSerDe();
      Configuration conf = new Configuration();
      Properties tbl = createProperties();
      SerDeUtils.initializeSerDe(serDe, conf, tbl, null);

      // Data
      Text t = new Text("123\t456\t789\t1000\t5.3\thive and hadoop\t1.\ta\tb\t");
      String s = "123\t456\t789\t1000\t5.3\thive and hadoop\t1\ta";
      Object[] expectedFieldsData = {new ByteWritable((byte) 123),
          new ShortWritable((short) 456), new IntWritable(789),
          new LongWritable(1000), new DoubleWritable(5.3),
          new Text("hive and hadoop"), new IntWritable(1), new Text("a")};

      // Test
      deserializeAndSerialize(serDe, t, s, expectedFieldsData);

    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }

  /**
   * Test the LazySimpleSerDe class with missing columns.
   */
  public void testLazySimpleSerDeMissingColumns() throws Throwable {
    try {
      // Create the SerDe
      LazySimpleSerDe serDe = new LazySimpleSerDe();
      Configuration conf = new Configuration();
      Properties tbl = createProperties();
      SerDeUtils.initializeSerDe(serDe, conf, tbl, null);

      // Data
      Text t = new Text("123\t456\t789\t1000\t5.3\t");
      String s = "123\t456\t789\t1000\t5.3\t\tNULL\tNULL";
      Object[] expectedFieldsData = {new ByteWritable((byte) 123),
          new ShortWritable((short) 456), new IntWritable(789),
          new LongWritable(1000), new DoubleWritable(5.3), new Text(""), null,
          null};

      // Test
      deserializeAndSerialize(serDe, t, s, expectedFieldsData);

    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }

  /**
   * Tests the deprecated usage of SerDeParameters.
   *
   */
  @Test
  @SuppressWarnings("deprecation")
  public void testSerDeParameters() throws SerDeException, IOException {
    // Setup
    LazySimpleSerDe serDe = new LazySimpleSerDe();
    Configuration conf = new Configuration();

    MyTestClass row = new MyTestClass();
    ExtraTypeInfo extraTypeInfo = new ExtraTypeInfo();
    row.randomFill(new Random(1234), extraTypeInfo);

    StructObjectInspector rowOI = (StructObjectInspector) ObjectInspectorFactory
        .getReflectionObjectInspector(MyTestClass.class,
            ObjectInspectorOptions.JAVA);

    String fieldNames = ObjectInspectorUtils.getFieldNames(rowOI);
    String fieldTypes = ObjectInspectorUtils.getFieldTypes(rowOI);

    Properties schema = new Properties();
    schema.setProperty(serdeConstants.LIST_COLUMNS, fieldNames);
    schema.setProperty(serdeConstants.LIST_COLUMN_TYPES, fieldTypes);

    SerDeUtils.initializeSerDe(serDe, conf, schema, null);
    SerDeParameters serdeParams = LazySimpleSerDe.initSerdeParams(conf, schema, "testSerdeName");

    // Test
    LazyStruct data = (LazyStruct)serializeAndDeserialize(row, rowOI, serDe, serdeParams);
    assertEquals((boolean)row.myBool, ((LazyBoolean)data.getField(0)).getWritableObject().get());
    assertEquals((int)row.myInt, ((LazyInteger)data.getField(3)).getWritableObject().get());
  }

  private Object serializeAndDeserialize(Object row,
      StructObjectInspector rowOI,
      LazySimpleSerDe serde,
      LazySerDeParameters serdeParams) throws IOException, SerDeException {
    ByteStream.Output serializeStream = new ByteStream.Output();
    LazySimpleSerDe.serialize(serializeStream, row, rowOI, serdeParams
        .getSeparators(), 0, serdeParams.getNullSequence(), serdeParams
        .isEscaped(), serdeParams.getEscapeChar(), serdeParams
        .getNeedsEscape());

    Text t = new Text(serializeStream.toByteArray());
    return serde.deserialize(t);
  }

  private void deserializeAndSerialize(LazySimpleSerDe serDe, Text t, String s,
      Object[] expectedFieldsData) throws SerDeException {
    // Get the row structure
    StructObjectInspector oi = (StructObjectInspector) serDe
        .getObjectInspector();
    List<? extends StructField> fieldRefs = oi.getAllStructFieldRefs();
    assertEquals(expectedFieldsData.length, fieldRefs.size());

    // Deserialize
    Object row = serDe.deserialize(t);
    for (int i = 0; i < fieldRefs.size(); i++) {
      Object fieldData = oi.getStructFieldData(row, fieldRefs.get(i));
      if (fieldData != null) {
        fieldData = ((LazyPrimitive) fieldData).getWritableObject();
      }
      assertEquals("Field " + i, expectedFieldsData[i], fieldData);
    }
    // Serialize
    assertEquals(Text.class, serDe.getSerializedClass());
    Text serializedText = (Text) serDe.serialize(row, oi);
    assertEquals("Serialized data", s, serializedText.toString());
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
