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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import junit.framework.TestCase;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.thrift.test.Complex;
import org.apache.hadoop.hive.serde2.thrift.test.IntString;
import org.apache.hadoop.hive.serde2.thrift.test.SetIntString;

/**
 * TestThriftObjectInspectors.
 *
 */
public class TestThriftObjectInspectors extends TestCase {

  public void testThriftObjectInspectors() throws Throwable {

    try {
      ObjectInspector oi1 = ObjectInspectorFactory
          .getReflectionObjectInspector(Complex.class,
          ObjectInspectorFactory.ObjectInspectorOptions.THRIFT);
      ObjectInspector oi2 = ObjectInspectorFactory
          .getReflectionObjectInspector(Complex.class,
          ObjectInspectorFactory.ObjectInspectorOptions.THRIFT);
      assertEquals(oi1, oi2);

      // metadata
      assertEquals(Category.STRUCT, oi1.getCategory());
      StructObjectInspector soi = (StructObjectInspector) oi1;
      List<? extends StructField> fields = soi.getAllStructFieldRefs();
      assertEquals(10, fields.size());
      assertEquals(fields.get(0), soi.getStructFieldRef("aint"));

      // null
      for (int i = 0; i < fields.size(); i++) {
        assertNull(soi.getStructFieldData(null, fields.get(i)));
      }

      // real object
      Complex c = new Complex();
      c.setAint(1);
      c.setAString("test");
      List<Integer> c2 = Arrays.asList(new Integer[] {1, 2, 3});
      c.setLint(c2);
      List<String> c3 = Arrays.asList(new String[] {"one", "two"});
      c.setLString(c3);
      List<IntString> c4 = new ArrayList<IntString>();
      c.setLintString(c4);
      c.setMStringString(null);
      c.setAttributes(null);
      c.setUnionField1(null);
      c.setUnionField2(null);
      c.setUnionField3(null);

      assertEquals(1, soi.getStructFieldData(c, fields.get(0)));
      assertEquals("test", soi.getStructFieldData(c, fields.get(1)));
      assertEquals(c2, soi.getStructFieldData(c, fields.get(2)));
      assertEquals(c3, soi.getStructFieldData(c, fields.get(3)));
      assertEquals(c4, soi.getStructFieldData(c, fields.get(4)));
      assertNull(soi.getStructFieldData(c, fields.get(5)));
      assertNull(soi.getStructFieldData(c, fields.get(6)));
      assertNull(soi.getStructFieldData(c, fields.get(7)));
      assertNull(soi.getStructFieldData(c, fields.get(8)));
      assertNull(soi.getStructFieldData(c, fields.get(9)));

      ArrayList<Object> cfields = new ArrayList<Object>();
      for (int i = 0; i < 10; i++) {
        cfields.add(soi.getStructFieldData(c, fields.get(i)));
      }
      assertEquals(cfields, soi.getStructFieldsDataAsList(c));

      // sub fields
      assertEquals(PrimitiveObjectInspectorFactory.javaIntObjectInspector,
          fields.get(0).getFieldObjectInspector());
      assertEquals(PrimitiveObjectInspectorFactory.javaStringObjectInspector,
          fields.get(1).getFieldObjectInspector());
      assertEquals(
          ObjectInspectorFactory
          .getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaIntObjectInspector),
          fields.get(2).getFieldObjectInspector());
      assertEquals(
          ObjectInspectorFactory
              .getStandardListObjectInspector(PrimitiveObjectInspectorFactory
                  .javaStringObjectInspector),
          fields.get(3).getFieldObjectInspector());
      assertEquals(ObjectInspectorFactory
          .getStandardListObjectInspector(ObjectInspectorFactory
          .getReflectionObjectInspector(IntString.class,
          ObjectInspectorFactory.ObjectInspectorOptions.THRIFT)),
          fields.get(4).getFieldObjectInspector());
      assertEquals(ObjectInspectorFactory.getStandardMapObjectInspector(
          PrimitiveObjectInspectorFactory.javaStringObjectInspector,
          PrimitiveObjectInspectorFactory.javaStringObjectInspector), fields
          .get(5).getFieldObjectInspector());
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }

  @SuppressWarnings("unchecked")
  public void testThriftSetObjectInspector() throws Throwable {

    try {
      ObjectInspector oi1 = ObjectInspectorFactory
          .getReflectionObjectInspector(SetIntString.class,
          ObjectInspectorFactory.ObjectInspectorOptions.THRIFT);
      ObjectInspector oi2 = ObjectInspectorFactory
          .getReflectionObjectInspector(SetIntString.class,
          ObjectInspectorFactory.ObjectInspectorOptions.THRIFT);
      assertEquals(oi1, oi2);

      // metadata
      assertEquals(Category.STRUCT, oi1.getCategory());
      StructObjectInspector soi = (StructObjectInspector) oi1;
      List<? extends StructField> fields = soi.getAllStructFieldRefs();
      assertEquals(2, fields.size());
      assertEquals(fields.get(0), soi.getStructFieldRef("sIntString"));
      assertEquals(fields.get(1), soi.getStructFieldRef("aString"));

      // null
      for (int i = 0; i < fields.size(); i++) {
        assertNull(soi.getStructFieldData(null, fields.get(i)));
      }

      // real object
      IntString s1 = new IntString();
      s1.setMyint(1);
      s1.setMyString("test");
      s1.setUnderscore_int(2);

      Set<IntString> set1 = new HashSet<IntString>();
      set1.add(s1);

      SetIntString s = new SetIntString();
      s.setSIntString(set1);
      s.setAString("setString");

      assertEquals(set1, soi.getStructFieldData(s, fields.get(0)));
      assertEquals("setString", soi.getStructFieldData(s, fields.get(1)));

      // sub fields
      assertEquals(
          ObjectInspectorFactory
          .getStandardListObjectInspector(ObjectInspectorFactory
              .getReflectionObjectInspector(IntString.class,
                  ObjectInspectorFactory.ObjectInspectorOptions.THRIFT)),
          fields.get(0).getFieldObjectInspector());
      assertEquals(PrimitiveObjectInspectorFactory.javaStringObjectInspector,
          fields.get(1).getFieldObjectInspector());

      // compare set fields
      ListObjectInspector loi = (ListObjectInspector) fields.get(0).getFieldObjectInspector();
      assertEquals(1, loi.getListLength(set1));
      List<IntString> list = (List<IntString>) loi.getList(set1);
      assertEquals(1, list.size());
      s1 = (IntString) loi.getListElement(list, 0);
      assertEquals(1, s1.getMyint());
      assertEquals("test", s1.getMyString());
      assertEquals(2, s1.getUnderscore_int());
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }
}
