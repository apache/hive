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
package org.apache.hadoop.hive.serde2.binarysortable;

import java.util.HashMap;
import java.util.Properties;
import java.util.Random;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.binarysortable.MyTestPrimitiveClass.ExtraTypeInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.BytesWritable;

import junit.framework.TestCase;

/**
 * TestBinarySortableSerDe.
 *
 */
public class TestBinarySortableSerDe extends TestCase {

  private static final String DECIMAL_CHARS = "0123456789";

  public static HashMap<String, String> makeHashMap(String... params) {
    HashMap<String, String> r = new HashMap<String, String>();
    for (int i = 0; i < params.length; i += 2) {
      r.put(params[i], params[i + 1]);
    }
    return r;
  }

  public static String hexString(BytesWritable bytes) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < bytes.getSize(); i++) {
      byte b = bytes.get()[i];
      int v = (b < 0 ? 256 + b : b);
      sb.append(String.format("x%02x", v));
    }
    return sb.toString();
  }

  public static AbstractSerDe getSerDe(String fieldNames, String fieldTypes, String order, String nullOrder)
      throws Throwable {
    Properties schema = new Properties();
    schema.setProperty(serdeConstants.LIST_COLUMNS, fieldNames);
    schema.setProperty(serdeConstants.LIST_COLUMN_TYPES, fieldTypes);
    schema.setProperty(serdeConstants.SERIALIZATION_SORT_ORDER, order);
    schema.setProperty(serdeConstants.SERIALIZATION_NULL_SORT_ORDER, nullOrder);

    BinarySortableSerDe serde = new BinarySortableSerDe();
    SerDeUtils.initializeSerDe(serde, new Configuration(), schema, null);
    return serde;
  }

  private void testBinarySortableSerDe(Object[] rows, ObjectInspector rowOI,
      AbstractSerDe serde, boolean ascending) throws Throwable {

    ObjectInspector serdeOI = serde.getObjectInspector();

    // Try to serialize
    BytesWritable bytes[] = new BytesWritable[rows.length];
    for (int i = 0; i < rows.length; i++) {
      BytesWritable s = (BytesWritable) serde.serialize(rows[i], rowOI);
      bytes[i] = new BytesWritable();
      bytes[i].set(s);
      if (i > 0) {
        int compareResult = bytes[i - 1].compareTo(bytes[i]);
        if ((compareResult < 0 && !ascending)
            || (compareResult > 0 && ascending)) {
          System.out.println("Test failed in "
              + (ascending ? "ascending" : "descending") + " order with "
              + (i - 1) + " and " + i);
          System.out.println("serialized data [" + (i - 1) + "] = "
              + hexString(bytes[i - 1]));
          System.out.println("serialized data [" + i + "] = "
              + hexString(bytes[i]));
          System.out.println("deserialized data [" + (i - 1) + " = "
              + SerDeUtils.getJSONString(rows[i - 1], rowOI));
          System.out.println("deserialized data [" + i + " = "
              + SerDeUtils.getJSONString(rows[i], rowOI));
          fail("Sort order of serialized " + (i - 1) + " and " + i
              + " are reversed!");
        }
      }
    }

    // Try to deserialize
    Object[] deserialized = new Object[rows.length];
    for (int i = 0; i < rows.length; i++) {
      deserialized[i] = serde.deserialize(bytes[i]);
      if (0 != ObjectInspectorUtils.compare(rows[i], rowOI, deserialized[i],
          serdeOI)) {
        System.out.println("structs[" + i + "] = "
            + SerDeUtils.getJSONString(rows[i], rowOI));
        System.out.println("deserialized[" + i + "] = "
            + SerDeUtils.getJSONString(deserialized[i], serdeOI));
        System.out.println("serialized[" + i + "] = " + hexString(bytes[i]));
        assertEquals(rows[i], deserialized[i]);
      }
    }
  }

  public static void sort(Object[] structs, ObjectInspector oi) {
    for (int i = 0; i < structs.length; i++) {
      for (int j = i + 1; j < structs.length; j++) {
        if (ObjectInspectorUtils.compare(structs[i], oi, structs[j], oi) > 0) {
          Object t = structs[i];
          structs[i] = structs[j];
          structs[j] = t;
        }
      }
    }
  }

  public void testBinarySortableSerDe() throws Throwable {
    try {

      System.out.println("Beginning Test testBinarySortableSerDe:");

      int num = 1000;
      Random r = new Random(1234);
      MyTestClass rows[] = new MyTestClass[num];

      int i;
      // First try non-random values
      for (i = 0; i < MyTestClass.nrDecimal.length; i++) {
        MyTestClass t = new MyTestClass();
        t.nonRandomFill(i);
        rows[i] = t;
      }

      for ( ; i < num; i++) {
        MyTestClass t = new MyTestClass();
        ExtraTypeInfo extraTypeInfo = new ExtraTypeInfo();
        t.randomFill(r, extraTypeInfo);
        rows[i] = t;
      }

      StructObjectInspector rowOI = (StructObjectInspector) ObjectInspectorFactory
          .getReflectionObjectInspector(MyTestClass.class,
          ObjectInspectorOptions.JAVA);
      sort(rows, rowOI);

      String fieldNames = ObjectInspectorUtils.getFieldNames(rowOI);
      String fieldTypes = ObjectInspectorUtils.getFieldTypes(rowOI);

      String order;
      order = StringUtils.leftPad("", MyTestClass.fieldCount, '+');
      String nullOrder;
      nullOrder = StringUtils.leftPad("", MyTestClass.fieldCount, 'a');
      testBinarySortableSerDe(rows, rowOI, getSerDe(fieldNames, fieldTypes,
          order, nullOrder), true);
      order = StringUtils.leftPad("", MyTestClass.fieldCount, '-');
      nullOrder = StringUtils.leftPad("", MyTestClass.fieldCount, 'z');
      testBinarySortableSerDe(rows, rowOI, getSerDe(fieldNames, fieldTypes,
          order, nullOrder), false);

      System.out.println("Test testTBinarySortableProtocol passed!");
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }

}
