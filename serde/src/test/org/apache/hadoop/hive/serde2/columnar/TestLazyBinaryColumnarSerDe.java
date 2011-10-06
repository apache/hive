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

package org.apache.hadoop.hive.serde2.columnar;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.objectinspector.CrossMapEqualComparer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.SimpleMapEqualComparer;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

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
    ByteArrayRef mBA;
    List<InnerStruct> mArray;
    Map<String, InnerStruct> mMap;
    InnerStruct mStruct;
  }

  public void testSerDe() throws SerDeException {
    StructObjectInspector oi = (StructObjectInspector) ObjectInspectorFactory
        .getReflectionObjectInspector(OuterStruct.class, ObjectInspectorOptions.JAVA);
    String cols = ObjectInspectorUtils.getFieldNames(oi);
    Properties props = new Properties();
    props.setProperty(Constants.LIST_COLUMNS, cols);
    props.setProperty(Constants.LIST_COLUMN_TYPES, ObjectInspectorUtils.getFieldTypes(oi));
    LazyBinaryColumnarSerDe serde = new LazyBinaryColumnarSerDe();
    serde.initialize(new Configuration(), props);

    OuterStruct outerStruct = new OuterStruct();
    outerStruct.mByte  = 1;
    outerStruct.mShort = 2;
    outerStruct.mInt = 3;
    outerStruct.mLong = 4l;
    outerStruct.mFloat = 5.01f;
    outerStruct.mDouble = 6.001d;
    outerStruct.mString = "seven";
    ByteArrayRef ba = new ByteArrayRef();
    ba.setData(new byte[]{'2'});
    outerStruct.mBA =  ba;
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
    props.setProperty(Constants.LIST_COLUMNS, cols);
    props.setProperty(Constants.LIST_COLUMN_TYPES, ObjectInspectorUtils.getFieldTypes(oi));
    LazyBinaryColumnarSerDe serde = new LazyBinaryColumnarSerDe();
    serde.initialize(new Configuration(), props);

    OuterStruct outerStruct = new OuterStruct();
    outerStruct.mByte  = 101;
    outerStruct.mShort = 2002;
    outerStruct.mInt = 3003;
    outerStruct.mLong = 4004l;
    outerStruct.mFloat = 5005.01f;
    outerStruct.mDouble = 6006.001d;
    outerStruct.mString = "";
    ByteArrayRef ba = new ByteArrayRef();
    ba.setData(new byte[]{'a'});
    outerStruct.mBA = ba;
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
    props.setProperty(Constants.LIST_COLUMNS, cols);
    props.setProperty(Constants.LIST_COLUMN_TYPES, ObjectInspectorUtils.getFieldTypes(oi));
    LazyBinaryColumnarSerDe serde = new LazyBinaryColumnarSerDe();
    serde.initialize(new Configuration(), props);

    OuterStruct outerStruct = new OuterStruct();
    outerStruct.mByte  = 101;
    outerStruct.mShort = 2002;
    outerStruct.mInt = 3003;
    outerStruct.mLong = 4004l;
    outerStruct.mFloat = 5005.01f;
    outerStruct.mDouble = 6006.001d;
    outerStruct.mString = "";
    ByteArrayRef ba = new ByteArrayRef();
    ba.setData(new byte[]{});
    outerStruct.mBA = ba;
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
    props.setProperty(Constants.LIST_COLUMNS, cols);
    props.setProperty(Constants.LIST_COLUMN_TYPES, ObjectInspectorUtils.getFieldTypes(oi));
    LazyBinaryColumnarSerDe serde = new LazyBinaryColumnarSerDe();
    serde.initialize(new Configuration(), props);

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
    props.setProperty(Constants.LIST_COLUMNS, cols);
    props.setProperty(Constants.LIST_COLUMN_TYPES, ObjectInspectorUtils.getFieldTypes(oi));
    LazyBinaryColumnarSerDe serde = new LazyBinaryColumnarSerDe();
    serde.initialize(new Configuration(), props);

    OuterStruct outerStruct = new OuterStruct();
    outerStruct.mByte  = 1;
    outerStruct.mShort = 2;
    outerStruct.mInt = 3;
    outerStruct.mLong = 4l;
    outerStruct.mFloat = 5.01f;
    outerStruct.mDouble = 6.001d;
    outerStruct.mString = "seven";
    ByteArrayRef ba = new ByteArrayRef();
    ba.setData(new byte[]{'3'});
    outerStruct.mBA = ba;
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


}
