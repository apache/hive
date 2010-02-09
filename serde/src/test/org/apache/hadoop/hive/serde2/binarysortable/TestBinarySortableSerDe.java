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
package org.apache.hadoop.hive.serde2.binarysortable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions;
import org.apache.hadoop.io.BytesWritable;

/**
 * TestBinarySortableSerDe.
 *
 */
public class TestBinarySortableSerDe extends TestCase {

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

  private SerDe getSerDe(String fieldNames, String fieldTypes, String order)
      throws Throwable {
    Properties schema = new Properties();
    schema.setProperty(Constants.LIST_COLUMNS, fieldNames);
    schema.setProperty(Constants.LIST_COLUMN_TYPES, fieldTypes);
    schema.setProperty(Constants.SERIALIZATION_SORT_ORDER, order);

    BinarySortableSerDe serde = new BinarySortableSerDe();
    serde.initialize(new Configuration(), schema);
    return serde;
  }

  private void testBinarySortableSerDe(Object[] rows, ObjectInspector rowOI,
      SerDe serde, boolean ascending) throws Throwable {

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

  private void sort(Object[] structs, ObjectInspector oi) {
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

  public static String getRandString(Random r) {
    int length = r.nextInt(10);
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < length; i++) {
      sb.append((char) (r.nextInt(128)));
    }
    return sb.toString();
  }

  public static List<Integer> getRandIntegerArray(Random r) {
    int length = r.nextInt(10);
    ArrayList<Integer> result = new ArrayList<Integer>(length);
    for (int i = 0; i < length; i++) {
      result.add(r.nextInt(128));
    }
    return result;
  }

  public void testBinarySortableSerDe() throws Throwable {
    try {

      System.out.println("Beginning Test testBinarySortableSerDe:");

      int num = 1000;
      Random r = new Random(1234);
      MyTestClass rows[] = new MyTestClass[num];

      for (int i = 0; i < num; i++) {
        int randField = r.nextInt(10);
        MyTestClass t = new MyTestClass();
        t.myByte = randField > 0 ? null : Byte.valueOf((byte) r.nextInt());
        t.myShort = randField > 1 ? null : Short.valueOf((short) r.nextInt());
        t.myInt = randField > 2 ? null : Integer.valueOf(r.nextInt());
        t.myLong = randField > 3 ? null : Long.valueOf(r.nextLong());
        t.myFloat = randField > 4 ? null : Float
            .valueOf(r.nextFloat() * 10 - 5);
        t.myDouble = randField > 5 ? null : Double
            .valueOf(r.nextDouble() * 10 - 5);
        t.myString = randField > 6 ? null : getRandString(r);
        t.myStruct = randField > 7 ? null : new MyTestInnerStruct(
            r.nextInt(5) - 2, r.nextInt(5) - 2);
        t.myList = randField > 8 ? null : getRandIntegerArray(r);
        rows[i] = t;
      }

      StructObjectInspector rowOI = (StructObjectInspector) ObjectInspectorFactory
          .getReflectionObjectInspector(MyTestClass.class,
          ObjectInspectorOptions.JAVA);
      sort(rows, rowOI);

      String fieldNames = ObjectInspectorUtils.getFieldNames(rowOI);
      String fieldTypes = ObjectInspectorUtils.getFieldTypes(rowOI);

      testBinarySortableSerDe(rows, rowOI, getSerDe(fieldNames, fieldTypes,
          "++++++++++"), true);
      testBinarySortableSerDe(rows, rowOI, getSerDe(fieldNames, fieldTypes,
          "----------"), false);

      System.out.println("Test testTBinarySortableProtocol passed!");
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }

}
