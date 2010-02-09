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
package org.apache.hadoop.hive.serde2.objectinspector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import junit.framework.TestCase;

import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

/**
 * TestStandardObjectInspectors.
 *
 */
public class TestStandardObjectInspectors extends TestCase {

  void doTestStandardPrimitiveObjectInspector(Class<?> writableClass,
      Class<?> javaClass) throws Throwable {
    try {
      PrimitiveObjectInspector oi1 = PrimitiveObjectInspectorFactory
          .getPrimitiveWritableObjectInspector(PrimitiveObjectInspectorUtils
          .getTypeEntryFromPrimitiveWritableClass(writableClass).primitiveCategory);
      PrimitiveObjectInspector oi2 = PrimitiveObjectInspectorFactory
          .getPrimitiveWritableObjectInspector(PrimitiveObjectInspectorUtils
          .getTypeEntryFromPrimitiveWritableClass(writableClass).primitiveCategory);
      assertEquals(oi1, oi2);
      assertEquals(Category.PRIMITIVE, oi1.getCategory());
      assertEquals(writableClass, oi1.getPrimitiveWritableClass());
      assertEquals(javaClass, oi1.getJavaPrimitiveClass());
      // Cannot create NullWritable instances
      if (!NullWritable.class.equals(writableClass)) {
        assertEquals(writableClass, oi1.getPrimitiveWritableObject(
            writableClass.newInstance()).getClass());
        assertEquals(javaClass, oi1.getPrimitiveJavaObject(
            writableClass.newInstance()).getClass());
      }
      assertEquals(PrimitiveObjectInspectorUtils
          .getTypeNameFromPrimitiveWritable(writableClass), oi1.getTypeName());
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }

  public void testStandardPrimitiveObjectInspector() throws Throwable {
    try {
      doTestStandardPrimitiveObjectInspector(NullWritable.class, Void.class);
      doTestStandardPrimitiveObjectInspector(BooleanWritable.class,
          Boolean.class);
      doTestStandardPrimitiveObjectInspector(ByteWritable.class, Byte.class);
      doTestStandardPrimitiveObjectInspector(ShortWritable.class, Short.class);
      doTestStandardPrimitiveObjectInspector(IntWritable.class, Integer.class);
      doTestStandardPrimitiveObjectInspector(LongWritable.class, Long.class);
      doTestStandardPrimitiveObjectInspector(FloatWritable.class, Float.class);
      doTestStandardPrimitiveObjectInspector(DoubleWritable.class, Double.class);
      doTestStandardPrimitiveObjectInspector(Text.class, String.class);
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }

  void doTestJavaPrimitiveObjectInspector(Class<?> writableClass,
      Class<?> javaClass, Object javaObject) throws Throwable {
    try {
      PrimitiveObjectInspector oi1 = PrimitiveObjectInspectorFactory
          .getPrimitiveJavaObjectInspector(PrimitiveObjectInspectorUtils
          .getTypeEntryFromPrimitiveJavaClass(javaClass).primitiveCategory);
      PrimitiveObjectInspector oi2 = PrimitiveObjectInspectorFactory
          .getPrimitiveJavaObjectInspector(PrimitiveObjectInspectorUtils
          .getTypeEntryFromPrimitiveJavaClass(javaClass).primitiveCategory);
      assertEquals(oi1, oi2);
      assertEquals(Category.PRIMITIVE, oi1.getCategory());
      assertEquals(javaClass, oi1.getJavaPrimitiveClass());
      assertEquals(writableClass, oi1.getPrimitiveWritableClass());
      if (javaObject != null) {
        assertEquals(javaClass, oi1.getPrimitiveJavaObject(javaObject)
            .getClass());
        assertEquals(writableClass, oi1.getPrimitiveWritableObject(javaObject)
            .getClass());
      }

      assertEquals(PrimitiveObjectInspectorUtils
          .getTypeNameFromPrimitiveJava(javaClass), oi1.getTypeName());
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }

  public void testJavaPrimitiveObjectInspector() throws Throwable {
    try {
      doTestJavaPrimitiveObjectInspector(NullWritable.class, Void.class, null);
      doTestJavaPrimitiveObjectInspector(BooleanWritable.class, Boolean.class,
          true);
      doTestJavaPrimitiveObjectInspector(ByteWritable.class, Byte.class,
          (byte) 1);
      doTestJavaPrimitiveObjectInspector(ShortWritable.class, Short.class,
          (short) 1);
      doTestJavaPrimitiveObjectInspector(IntWritable.class, Integer.class, 1);
      doTestJavaPrimitiveObjectInspector(LongWritable.class, Long.class,
          (long) 1);
      doTestJavaPrimitiveObjectInspector(FloatWritable.class, Float.class,
          (float) 1);
      doTestJavaPrimitiveObjectInspector(DoubleWritable.class, Double.class,
          (double) 1);
      doTestJavaPrimitiveObjectInspector(Text.class, String.class, "a");

    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }

  public void testStandardListObjectInspector() throws Throwable {
    try {
      StandardListObjectInspector loi1 = ObjectInspectorFactory
          .getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
      StandardListObjectInspector loi2 = ObjectInspectorFactory
          .getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
      assertEquals(loi1, loi2);

      // metadata
      assertEquals(Category.LIST, loi1.getCategory());
      assertEquals(PrimitiveObjectInspectorFactory.javaIntObjectInspector, loi1
          .getListElementObjectInspector());

      // null
      assertNull("loi1.getList(null) should be null.", loi1.getList(null));
      assertEquals("loi1.getListLength(null) should be -1.", loi1
          .getListLength(null), -1);
      assertNull("loi1.getListElement(null, 0) should be null", loi1
          .getListElement(null, 0));
      assertNull("loi1.getListElement(null, 100) should be null", loi1
          .getListElement(null, 100));

      // ArrayList
      ArrayList<Integer> list = new ArrayList<Integer>();
      list.add(0);
      list.add(1);
      list.add(2);
      list.add(3);
      assertEquals(4, loi1.getList(list).size());
      assertEquals(4, loi1.getListLength(list));
      assertEquals(0, loi1.getListElement(list, 0));
      assertEquals(3, loi1.getListElement(list, 3));
      assertNull(loi1.getListElement(list, -1));
      assertNull(loi1.getListElement(list, 4));

      // Settable
      Object list4 = loi1.create(4);
      loi1.set(list4, 0, 0);
      loi1.set(list4, 1, 1);
      loi1.set(list4, 2, 2);
      loi1.set(list4, 3, 3);
      assertEquals(list, list4);

      loi1.resize(list4, 5);
      loi1.set(list4, 4, 4);
      loi1.resize(list4, 4);
      assertEquals(list, list4);

    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }

  }

  public void testStandardMapObjectInspector() throws Throwable {
    try {
      StandardMapObjectInspector moi1 = ObjectInspectorFactory
          .getStandardMapObjectInspector(
          PrimitiveObjectInspectorFactory.javaStringObjectInspector,
          PrimitiveObjectInspectorFactory.javaIntObjectInspector);
      StandardMapObjectInspector moi2 = ObjectInspectorFactory
          .getStandardMapObjectInspector(
          PrimitiveObjectInspectorFactory.javaStringObjectInspector,
          PrimitiveObjectInspectorFactory.javaIntObjectInspector);
      assertEquals(moi1, moi2);

      // metadata
      assertEquals(Category.MAP, moi1.getCategory());
      assertEquals(moi1.getMapKeyObjectInspector(),
          PrimitiveObjectInspectorFactory.javaStringObjectInspector);
      assertEquals(moi2.getMapValueObjectInspector(),
          PrimitiveObjectInspectorFactory.javaIntObjectInspector);

      // null
      assertNull(moi1.getMap(null));
      assertNull(moi1.getMapValueElement(null, null));
      assertNull(moi1.getMapValueElement(null, "nokey"));
      assertEquals(-1, moi1.getMapSize(null));
      assertEquals("map<"
          + PrimitiveObjectInspectorFactory.javaStringObjectInspector
          .getTypeName()
          + ","
          + PrimitiveObjectInspectorFactory.javaIntObjectInspector
          .getTypeName() + ">", moi1.getTypeName());

      // HashMap
      HashMap<String, Integer> map = new HashMap<String, Integer>();
      map.put("one", 1);
      map.put("two", 2);
      map.put("three", 3);
      assertEquals(map, moi1.getMap(map));
      assertEquals(3, moi1.getMapSize(map));
      assertEquals(1, moi1.getMapValueElement(map, "one"));
      assertEquals(2, moi1.getMapValueElement(map, "two"));
      assertEquals(3, moi1.getMapValueElement(map, "three"));
      assertNull(moi1.getMapValueElement(map, null));
      assertNull(moi1.getMapValueElement(map, "null"));

      // Settable
      Object map3 = moi1.create();
      moi1.put(map3, "one", 1);
      moi1.put(map3, "two", 2);
      moi1.put(map3, "three", 3);
      assertEquals(map, map3);
      moi1.clear(map3);
      assertEquals(0, moi1.getMapSize(map3));

    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }

  }

  @SuppressWarnings("unchecked")
  public void testStandardStructObjectInspector() throws Throwable {
    try {
      ArrayList<String> fieldNames = new ArrayList<String>();
      fieldNames.add("firstInteger");
      fieldNames.add("secondString");
      fieldNames.add("thirdBoolean");
      ArrayList<ObjectInspector> fieldObjectInspectors = new ArrayList<ObjectInspector>();
      fieldObjectInspectors
          .add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
      fieldObjectInspectors
          .add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
      fieldObjectInspectors
          .add(PrimitiveObjectInspectorFactory.javaBooleanObjectInspector);

      StandardStructObjectInspector soi1 = ObjectInspectorFactory
          .getStandardStructObjectInspector(fieldNames, fieldObjectInspectors);
      StandardStructObjectInspector soi2 = ObjectInspectorFactory
          .getStandardStructObjectInspector((ArrayList<String>) fieldNames
          .clone(), (ArrayList<ObjectInspector>) fieldObjectInspectors
          .clone());
      assertEquals(soi1, soi2);

      // metadata
      assertEquals(Category.STRUCT, soi1.getCategory());
      List<? extends StructField> fields = soi1.getAllStructFieldRefs();
      assertEquals(3, fields.size());
      for (int i = 0; i < 3; i++) {
        assertEquals(fieldNames.get(i).toLowerCase(), fields.get(i)
            .getFieldName());
        assertEquals(fieldObjectInspectors.get(i), fields.get(i)
            .getFieldObjectInspector());
      }
      assertEquals(fields.get(1), soi1.getStructFieldRef("secondString"));
      StringBuilder structTypeName = new StringBuilder();
      structTypeName.append("struct<");
      for (int i = 0; i < fields.size(); i++) {
        if (i > 0) {
          structTypeName.append(",");
        }
        structTypeName.append(fields.get(i).getFieldName());
        structTypeName.append(":");
        structTypeName.append(fields.get(i).getFieldObjectInspector()
            .getTypeName());
      }
      structTypeName.append(">");
      assertEquals(structTypeName.toString(), soi1.getTypeName());

      // null
      assertNull(soi1.getStructFieldData(null, fields.get(0)));
      assertNull(soi1.getStructFieldData(null, fields.get(1)));
      assertNull(soi1.getStructFieldData(null, fields.get(2)));
      assertNull(soi1.getStructFieldsDataAsList(null));

      // HashStruct
      ArrayList<Object> struct = new ArrayList<Object>(3);
      struct.add(1);
      struct.add("two");
      struct.add(true);

      assertEquals(1, soi1.getStructFieldData(struct, fields.get(0)));
      assertEquals("two", soi1.getStructFieldData(struct, fields.get(1)));
      assertEquals(true, soi1.getStructFieldData(struct, fields.get(2)));

      // Settable
      Object struct3 = soi1.create();
      System.out.println(struct3);
      soi1.setStructFieldData(struct3, fields.get(0), 1);
      soi1.setStructFieldData(struct3, fields.get(1), "two");
      soi1.setStructFieldData(struct3, fields.get(2), true);
      assertEquals(struct, struct3);

    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }

  }

}
