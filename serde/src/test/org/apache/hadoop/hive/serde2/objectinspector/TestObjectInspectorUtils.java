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
import java.util.List;

import junit.framework.TestCase;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.thrift.test.Complex;
import org.apache.hadoop.hive.serde2.thrift.test.IntString;

/**
 * TestObjectInspectorUtils.
 *
 */
public class TestObjectInspectorUtils extends TestCase {

  public void testCompareFloatingNumberSignedZero() {
    PrimitiveObjectInspector doubleOI = PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.DOUBLE);

    Double d1 = Double.valueOf("0.0");
    Double d2 = Double.valueOf("-0.0");
    assertEquals(0, ObjectInspectorUtils.compare(d1, doubleOI, d2, doubleOI));
    assertEquals(0, ObjectInspectorUtils.compare(d2, doubleOI, d1, doubleOI));
    assertEquals(0, ObjectInspectorUtils.compare(d1, doubleOI, d1, doubleOI));
    assertEquals(0, ObjectInspectorUtils.compare(d2, doubleOI, d2, doubleOI));

    PrimitiveObjectInspector floatOI = PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.FLOAT);

    Float f1 = Float.valueOf("0.0");
    Float f2 = Float.valueOf("-0.0");
    assertEquals(0, ObjectInspectorUtils.compare(f1, floatOI, f2, floatOI));
    assertEquals(0, ObjectInspectorUtils.compare(f2, floatOI, f1, floatOI));
    assertEquals(0, ObjectInspectorUtils.compare(f1, floatOI, f1, floatOI));
    assertEquals(0, ObjectInspectorUtils.compare(f2, floatOI, f2, floatOI));
  }

  public void testObjectInspectorUtils() throws Throwable {
    try {
      ObjectInspector oi1 = ObjectInspectorFactory
          .getReflectionObjectInspector(Complex.class,
          ObjectInspectorFactory.ObjectInspectorOptions.THRIFT);

      // metadata
      assertEquals(Category.STRUCT, oi1.getCategory());
      // standard ObjectInspector
      StructObjectInspector soi = (StructObjectInspector) ObjectInspectorUtils
          .getStandardObjectInspector(oi1);
      List<? extends StructField> fields = soi.getAllStructFieldRefs();
      assertEquals(10, fields.size());
      assertEquals(fields.get(0), soi.getStructFieldRef("aint"));

      // null
      for (int i = 0; i < fields.size(); i++) {
        assertNull(soi.getStructFieldData(null, fields.get(i)));
      }

      // real object
      Complex cc = new Complex();
      cc.setAint(1);
      cc.setAString("test");
      List<Integer> c2 = Arrays.asList(new Integer[] {1, 2, 3});
      cc.setLint(c2);
      List<String> c3 = Arrays.asList(new String[] {"one", "two"});
      cc.setLString(c3);
      List<IntString> c4 = new ArrayList<IntString>();
      cc.setLintString(c4);
      cc.setMStringString(null);
      // standard object
      Object c = ObjectInspectorUtils.copyToStandardObject(cc, oi1);

      assertEquals(1, soi.getStructFieldData(c, fields.get(0)));
      assertEquals("test", soi.getStructFieldData(c, fields.get(1)));
      assertEquals(c2, soi.getStructFieldData(c, fields.get(2)));
      assertEquals(c3, soi.getStructFieldData(c, fields.get(3)));
      assertEquals(c4, soi.getStructFieldData(c, fields.get(4)));
      assertNull(soi.getStructFieldData(c, fields.get(5)));
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
              .getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector),
          fields.get(3).getFieldObjectInspector());
      assertEquals(ObjectInspectorUtils
          .getStandardObjectInspector(ObjectInspectorFactory
          .getStandardListObjectInspector(ObjectInspectorFactory
          .getReflectionObjectInspector(IntString.class,
          ObjectInspectorFactory.ObjectInspectorOptions.THRIFT))),
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
  public void testBucketIdGeneration() {
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

    StandardStructObjectInspector soi1 = ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldObjectInspectors);
    ArrayList<Object> struct = new ArrayList<Object>(3);
    struct.add(1);
    struct.add("two");
    struct.add(true);

    int hashCode = ObjectInspectorUtils.getBucketHashCode(struct.toArray(), fieldObjectInspectors.toArray(new ObjectInspector[fieldObjectInspectors.size()]));
    assertEquals("", 3574518, hashCode);
    int bucketId = ObjectInspectorUtils.getBucketNumber(struct.toArray(), fieldObjectInspectors.toArray(new ObjectInspector[fieldObjectInspectors.size()]), 16);
    assertEquals("", 6, bucketId);
    assertEquals("", bucketId, ObjectInspectorUtils.getBucketNumber(hashCode, 16));
  }
}
