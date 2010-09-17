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
import org.apache.hadoop.hive.serde2.proto.test.Complexpb.Complex;
import org.apache.hadoop.hive.serde2.proto.test.Complexpb.IntString;

/**
 * TestProtocolBuffersObjectInspectors.
 *
 */
public class TestProtocolBuffersObjectInspectors extends TestCase {

  public void testProtocolBuffersObjectInspectors() throws Throwable {

    try {
      ObjectInspector oi1 = ObjectInspectorFactory
          .getReflectionObjectInspector(Complex.class,
          ObjectInspectorFactory.ObjectInspectorOptions.PROTOCOL_BUFFERS);
      ObjectInspector oi2 = ObjectInspectorFactory
          .getReflectionObjectInspector(Complex.class,
          ObjectInspectorFactory.ObjectInspectorOptions.PROTOCOL_BUFFERS);
      assertEquals(oi1, oi2);

      // metadata
      assertEquals(Category.STRUCT, oi1.getCategory());
      StructObjectInspector soi = (StructObjectInspector) oi1;
      List<? extends StructField> fields = soi.getAllStructFieldRefs();
      assertEquals(5, fields.size());
      assertEquals(fields.get(0), soi.getStructFieldRef("aint_"));

      // null
      for (int i = 0; i < fields.size(); i++) {
        assertNull(soi.getStructFieldData(null, fields.get(i)));
      }

      // real object
      List<Integer> c2 = Arrays.asList(new Integer[] {1, 2, 3});
      List<String> c3 = Arrays.asList(new String[] {"one", "two"});
      List<IntString> c4 = new ArrayList<IntString>();
      Complex c = new Complex(1, "test", c2, c3, c4);

      assertEquals(1, soi.getStructFieldData(c, fields.get(0)));
      assertEquals("test", soi.getStructFieldData(c, fields.get(1)));
      assertEquals(c2, soi.getStructFieldData(c, fields.get(2)));
      assertEquals(c3, soi.getStructFieldData(c, fields.get(3)));
      assertEquals(c4, soi.getStructFieldData(c, fields.get(4)));
      ArrayList<Object> cfields = new ArrayList<Object>();
      for (int i = 0; i < 5; i++) {
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
          ObjectInspectorFactory.ObjectInspectorOptions.PROTOCOL_BUFFERS)),
          fields.get(4).getFieldObjectInspector());
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }
}
