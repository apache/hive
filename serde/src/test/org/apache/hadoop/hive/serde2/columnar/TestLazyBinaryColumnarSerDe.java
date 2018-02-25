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

package org.apache.hadoop.hive.serde2.columnar;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.CrossMapEqualComparer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.SimpleMapEqualComparer;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.LongWritable;

public class TestLazyBinaryColumnarSerDe extends TestCase {

  private static class InnerStruct {
    public InnerStruct(Integer i, Long l) {
      mInt = i;
      mLong = l;
    }
    Integer mInt;
    Long mLong;
  }

  private static class OuterStruct {
    Byte mByte;
    Short mShort;
    Integer mInt;
    Long mLong;
    Float mFloat;
    Double mDouble;
    String mString;
    byte[] mBA;
    List<InnerStruct> mArray;
    Map<String, InnerStruct> mMap;
    InnerStruct mStruct;
  }

  public void testSerDe() throws SerDeException {
    StructObjectInspector oi = (StructObjectInspector) ObjectInspectorFactory
        .getReflectionObjectInspector(OuterStruct.class, ObjectInspectorOptions.JAVA);
    String cols = ObjectInspectorUtils.getFieldNames(oi);
    Properties props = new Properties();
    props.setProperty(serdeConstants.LIST_COLUMNS, cols);
    props.setProperty(serdeConstants.LIST_COLUMN_TYPES, ObjectInspectorUtils.getFieldTypes(oi));
    LazyBinaryColumnarSerDe serde = new LazyBinaryColumnarSerDe();
    SerDeUtils.initializeSerDe(serde, new Configuration(), props, null);

    OuterStruct outerStruct = new OuterStruct();
    outerStruct.mByte  = 1;
    outerStruct.mShort = 2;
    outerStruct.mInt = 3;
    outerStruct.mLong = 4l;
    outerStruct.mFloat = 5.01f;
    outerStruct.mDouble = 6.001d;
    outerStruct.mString = "seven";
    outerStruct.mBA =  new byte[]{'2'};
    InnerStruct is1 = new InnerStruct(8, 9l);
    InnerStruct is2 = new InnerStruct(10, 11l);
    outerStruct.mArray = new ArrayList<InnerStruct>(2);
    outerStruct.mArray.add(is1);
    outerStruct.mArray.add(is2);
    outerStruct.mMap = new TreeMap<String, InnerStruct>();
    outerStruct.mMap.put(new String("twelve"), new InnerStruct(13, 14l));
    outerStruct.mMap.put(new String("fifteen"), new InnerStruct(16, 17l));
    outerStruct.mStruct = new InnerStruct(18, 19l);
    BytesRefArrayWritable braw = (BytesRefArrayWritable) serde.serialize(outerStruct, oi);

    ObjectInspector out_oi = serde.getObjectInspector();
    Object out_o = serde.deserialize(braw);
    if (0 != ObjectInspectorUtils.compare(outerStruct, oi, out_o, out_oi, new CrossMapEqualComparer())) {
      System.out.println("expected = "
          + SerDeUtils.getJSONString(outerStruct, oi));
      System.out.println("actual = "
          + SerDeUtils.getJSONString(out_o, out_oi));
      fail("Deserialized object does not compare");
    }
  }

  public void testSerDeEmpties() throws SerDeException {
    StructObjectInspector oi = (StructObjectInspector) ObjectInspectorFactory
        .getReflectionObjectInspector(OuterStruct.class, ObjectInspectorOptions.JAVA);
    String cols = ObjectInspectorUtils.getFieldNames(oi);
    Properties props = new Properties();
    props.setProperty(serdeConstants.LIST_COLUMNS, cols);
    props.setProperty(serdeConstants.LIST_COLUMN_TYPES, ObjectInspectorUtils.getFieldTypes(oi));
    LazyBinaryColumnarSerDe serde = new LazyBinaryColumnarSerDe();
    SerDeUtils.initializeSerDe(serde, new Configuration(), props, null);

    OuterStruct outerStruct = new OuterStruct();
    outerStruct.mByte  = 101;
    outerStruct.mShort = 2002;
    outerStruct.mInt = 3003;
    outerStruct.mLong = 4004l;
    outerStruct.mFloat = 5005.01f;
    outerStruct.mDouble = 6006.001d;
    outerStruct.mString = "";
    outerStruct.mBA = new byte[]{'a'};
    outerStruct.mArray = new ArrayList<InnerStruct>();
    outerStruct.mMap = new TreeMap<String, InnerStruct>();
    outerStruct.mStruct = new InnerStruct(180018, 190019l);
    BytesRefArrayWritable braw = (BytesRefArrayWritable) serde.serialize(outerStruct, oi);

    ObjectInspector out_oi = serde.getObjectInspector();
    Object out_o = serde.deserialize(braw);
    if (0 != ObjectInspectorUtils.compare(outerStruct, oi, out_o, out_oi, new SimpleMapEqualComparer())) {
      System.out.println("expected = "
          + SerDeUtils.getJSONString(outerStruct, oi));
      System.out.println("actual = "
          + SerDeUtils.getJSONString(out_o, out_oi));
      fail("Deserialized object does not compare");
    }
  }

  public void testLazyBinaryColumnarSerDeWithEmptyBinary() throws SerDeException {
    StructObjectInspector oi = (StructObjectInspector) ObjectInspectorFactory
        .getReflectionObjectInspector(OuterStruct.class, ObjectInspectorOptions.JAVA);
    String cols = ObjectInspectorUtils.getFieldNames(oi);
    Properties props = new Properties();
    props.setProperty(serdeConstants.LIST_COLUMNS, cols);
    props.setProperty(serdeConstants.LIST_COLUMN_TYPES, ObjectInspectorUtils.getFieldTypes(oi));
    LazyBinaryColumnarSerDe serde = new LazyBinaryColumnarSerDe();
    SerDeUtils.initializeSerDe(serde, new Configuration(), props, null);

    OuterStruct outerStruct = new OuterStruct();
    outerStruct.mByte  = 101;
    outerStruct.mShort = 2002;
    outerStruct.mInt = 3003;
    outerStruct.mLong = 4004l;
    outerStruct.mFloat = 5005.01f;
    outerStruct.mDouble = 6006.001d;
    outerStruct.mString = "";
    outerStruct.mBA = new byte[]{};
    outerStruct.mArray = new ArrayList<InnerStruct>();
    outerStruct.mMap = new TreeMap<String, InnerStruct>();
    outerStruct.mStruct = new InnerStruct(180018, 190019l);
    try{
      serde.serialize(outerStruct, oi);
    }
    catch (RuntimeException re){
      assertEquals(re.getMessage(), "LazyBinaryColumnarSerde cannot serialize a non-null " +
                "zero length binary field. Consider using either LazyBinarySerde or ColumnarSerde.");
      return;
    }
    assert false;
  }

  public void testSerDeOuterNulls() throws SerDeException {
    StructObjectInspector oi = (StructObjectInspector) ObjectInspectorFactory
        .getReflectionObjectInspector(OuterStruct.class, ObjectInspectorOptions.JAVA);
    String cols = ObjectInspectorUtils.getFieldNames(oi);
    Properties props = new Properties();
    props.setProperty(serdeConstants.LIST_COLUMNS, cols);
    props.setProperty(serdeConstants.LIST_COLUMN_TYPES, ObjectInspectorUtils.getFieldTypes(oi));
    LazyBinaryColumnarSerDe serde = new LazyBinaryColumnarSerDe();
    SerDeUtils.initializeSerDe(serde, new Configuration(), props, null);

    OuterStruct outerStruct = new OuterStruct();
    BytesRefArrayWritable braw = (BytesRefArrayWritable) serde.serialize(outerStruct, oi);

    ObjectInspector out_oi = serde.getObjectInspector();
    Object out_o = serde.deserialize(braw);
    if (0 != ObjectInspectorUtils.compare(outerStruct, oi, out_o, out_oi, new SimpleMapEqualComparer())) {
      System.out.println("expected = "
          + SerDeUtils.getJSONString(outerStruct, oi));
      System.out.println("actual = "
          + SerDeUtils.getJSONString(out_o, out_oi));
      fail("Deserialized object does not compare");
    }
  }

  public void testSerDeInnerNulls() throws SerDeException {
    StructObjectInspector oi = (StructObjectInspector) ObjectInspectorFactory
        .getReflectionObjectInspector(OuterStruct.class, ObjectInspectorOptions.JAVA);
    String cols = ObjectInspectorUtils.getFieldNames(oi);
    Properties props = new Properties();
    props.setProperty(serdeConstants.LIST_COLUMNS, cols);
    props.setProperty(serdeConstants.LIST_COLUMN_TYPES, ObjectInspectorUtils.getFieldTypes(oi));
    LazyBinaryColumnarSerDe serde = new LazyBinaryColumnarSerDe();
    SerDeUtils.initializeSerDe(serde, new Configuration(), props, null);

    OuterStruct outerStruct = new OuterStruct();
    outerStruct.mByte  = 1;
    outerStruct.mShort = 2;
    outerStruct.mInt = 3;
    outerStruct.mLong = 4l;
    outerStruct.mFloat = 5.01f;
    outerStruct.mDouble = 6.001d;
    outerStruct.mString = "seven";
    outerStruct.mBA = new byte[]{'3'};
    InnerStruct is1 = new InnerStruct(null, 9l);
    InnerStruct is2 = new InnerStruct(10, null);
    outerStruct.mArray = new ArrayList<InnerStruct>(2);
    outerStruct.mArray.add(is1);
    outerStruct.mArray.add(is2);
    outerStruct.mMap = new HashMap<String, InnerStruct>();
    outerStruct.mMap.put(null, new InnerStruct(13, 14l));
    outerStruct.mMap.put(new String("fifteen"), null);
    outerStruct.mStruct = new InnerStruct(null, null);
    BytesRefArrayWritable braw = (BytesRefArrayWritable) serde.serialize(outerStruct, oi);

    ObjectInspector out_oi = serde.getObjectInspector();
    Object out_o = serde.deserialize(braw);
    if (0 != ObjectInspectorUtils.compare(outerStruct, oi, out_o, out_oi, new SimpleMapEqualComparer())) {
      System.out.println("expected = "
          + SerDeUtils.getJSONString(outerStruct, oi));
      System.out.println("actual = "
          + SerDeUtils.getJSONString(out_o, out_oi));
      fail("Deserialized object does not compare");
    }
  }

  private static class BeforeStruct {
    Long l1;
    Long l2;
  }

  private static class AfterStruct {
    Long l1;
    Long l2;
    Long l3;
  }

  /**
   * HIVE-5788
   * <p>
   * Background: in cases of "add column", table metadata changes but data does not.  Columns
   * missing from the data but which are required by metadata are interpreted as null.
   * <p>
   * This tests the use-case of altering columns of a table with already some data, then adding more data
   * in the new schema, and seeing if this serde can to read both types of data from the resultant table.
   * @throws SerDeException
   */
  public void testHandlingAlteredSchemas() throws SerDeException {
    StructObjectInspector oi = (StructObjectInspector) ObjectInspectorFactory
        .getReflectionObjectInspector(BeforeStruct.class,
            ObjectInspectorOptions.JAVA);
    String cols = ObjectInspectorUtils.getFieldNames(oi);
    Properties props = new Properties();
    props.setProperty(serdeConstants.LIST_COLUMNS, cols);
    props.setProperty(serdeConstants.LIST_COLUMN_TYPES,
        ObjectInspectorUtils.getFieldTypes(oi));

    // serialize some data in the schema before it is altered.
    LazyBinaryColumnarSerDe serde = new LazyBinaryColumnarSerDe();
    SerDeUtils.initializeSerDe(serde, new Configuration(), props, null);

    BeforeStruct bs1 = new BeforeStruct();
    bs1.l1 = 1L;
    bs1.l2 = 2L;
    BytesRefArrayWritable braw1 = (BytesRefArrayWritable) serde.serialize(bs1,
        oi);

    // alter table add column: change the metadata
    oi = (StructObjectInspector) ObjectInspectorFactory
        .getReflectionObjectInspector(AfterStruct.class,
            ObjectInspectorOptions.JAVA);
    cols = ObjectInspectorUtils.getFieldNames(oi);
    props = new Properties();
    props.setProperty(serdeConstants.LIST_COLUMNS, cols);
    props.setProperty(serdeConstants.LIST_COLUMN_TYPES,
        ObjectInspectorUtils.getFieldTypes(oi));
    serde = new LazyBinaryColumnarSerDe();
    SerDeUtils.initializeSerDe(serde, new Configuration(), props, null);

    // serialize some data in the schema after it is altered.
    AfterStruct as = new AfterStruct();
    as.l1 = 11L;
    as.l2 = 12L;
    as.l3 = 13L;
    BytesRefArrayWritable braw2 = (BytesRefArrayWritable) serde.serialize(as,
        oi);

    // fetch operator
    serde = new LazyBinaryColumnarSerDe();
    SerDeUtils.initializeSerDe(serde, new Configuration(), props, null);

    //fetch the row inserted before schema is altered and verify
    LazyBinaryColumnarStruct struct1 = (LazyBinaryColumnarStruct) serde
        .deserialize(braw1);
    oi = (StructObjectInspector) serde.getObjectInspector();
    List<Object> objs1 = oi.getStructFieldsDataAsList(struct1);
    Assert.assertEquals(((LongWritable) objs1.get(0)).get(), 1L);
    Assert.assertEquals(((LongWritable) objs1.get(1)).get(), 2L);
    Assert.assertNull(objs1.get(2));

    //fetch the row inserted after schema is altered and verify
    LazyBinaryColumnarStruct struct2 = (LazyBinaryColumnarStruct) serde
        .deserialize(braw2);
    List<Object> objs2 = struct2.getFieldsAsList();
    Assert.assertEquals(((LongWritable) objs2.get(0)).get(), 11L);
    Assert.assertEquals(((LongWritable) objs2.get(1)).get(), 12L);
    Assert.assertEquals(((LongWritable) objs2.get(2)).get(), 13L);
  }
}