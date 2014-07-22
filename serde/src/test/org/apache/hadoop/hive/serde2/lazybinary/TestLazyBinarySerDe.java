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
package org.apache.hadoop.hive.serde2.lazybinary;

import java.sql.Date;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.binarysortable.MyTestClass;
import org.apache.hadoop.hive.serde2.binarysortable.MyTestInnerStruct;
import org.apache.hadoop.hive.serde2.binarysortable.TestBinarySortableSerDe;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazy.LazyBinary;
import org.apache.hadoop.hive.serde2.lazy.LazyFactory;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.AbstractPrimitiveLazyObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyPrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.lazybinary.objectinspector.LazyBinaryMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaBinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableBinaryObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BytesWritable;

/**
 * TestLazyBinarySerDe.
 *
 */
public class TestLazyBinarySerDe extends TestCase {

  /**
   * Generate a random struct array.
   *
   * @param r
   *          random number generator
   * @return an struct array
   */
  static List<MyTestInnerStruct> getRandStructArray(Random r) {
    int length = r.nextInt(10);
    ArrayList<MyTestInnerStruct> result = new ArrayList<MyTestInnerStruct>(
        length);
    for (int i = 0; i < length; i++) {
      MyTestInnerStruct ti = new MyTestInnerStruct(r.nextInt(), r.nextInt());
      result.add(ti);
    }
    return result;
  }

  /**
   * Initialize the LazyBinarySerDe.
   *
   * @param fieldNames
   *          table field names
   * @param fieldTypes
   *          table field types
   * @return the initialized LazyBinarySerDe
   * @throws Throwable
   */
  private SerDe getSerDe(String fieldNames, String fieldTypes) throws Throwable {
    Properties schema = new Properties();
    schema.setProperty(serdeConstants.LIST_COLUMNS, fieldNames);
    schema.setProperty(serdeConstants.LIST_COLUMN_TYPES, fieldTypes);

    LazyBinarySerDe serde = new LazyBinarySerDe();
    SerDeUtils.initializeSerDe(serde, new Configuration(), schema, null);
    return serde;
  }

  /**
   * Test the LazyBinarySerDe.
   *
   * @param rows
   *          array of structs to be serialized
   * @param rowOI
   *          array of struct object inspectors
   * @param serde
   *          the serde
   * @throws Throwable
   */
  private void testLazyBinarySerDe(Object[] rows, ObjectInspector rowOI,
      SerDe serde) throws Throwable {

    ObjectInspector serdeOI = serde.getObjectInspector();

    // Try to serialize
    BytesWritable bytes[] = new BytesWritable[rows.length];
    for (int i = 0; i < rows.length; i++) {
      BytesWritable s = (BytesWritable) serde.serialize(rows[i], rowOI);
      bytes[i] = new BytesWritable();
      bytes[i].set(s);
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
        System.out.println("serialized[" + i + "] = "
            + TestBinarySortableSerDe.hexString(bytes[i]));
        assertEquals(rows[i], deserialized[i]);
      }
    }
  }

  /**
   * Compare two structs that have different number of fields. We just compare
   * the first few common fields, ignoring the fields existing in one struct but
   * not the other.
   *
   * @see ObjectInspectorUtils#compare(Object, ObjectInspector, Object,
   *      ObjectInspector)
   */
  int compareDiffSizedStructs(Object o1, ObjectInspector oi1, Object o2,
      ObjectInspector oi2) {
    StructObjectInspector soi1 = (StructObjectInspector) oi1;
    StructObjectInspector soi2 = (StructObjectInspector) oi2;
    List<? extends StructField> fields1 = soi1.getAllStructFieldRefs();
    List<? extends StructField> fields2 = soi2.getAllStructFieldRefs();
    int minimum = Math.min(fields1.size(), fields2.size());
    for (int i = 0; i < minimum; i++) {
      int result = ObjectInspectorUtils.compare(soi1.getStructFieldData(o1,
          fields1.get(i)), fields1.get(i).getFieldObjectInspector(), soi2
          .getStructFieldData(o2, fields2.get(i)), fields2.get(i)
          .getFieldObjectInspector());
      if (result != 0) {
        return result;
      }
    }
    return 0;
  }

  /**
   * Test shorter schema deserialization where a bigger struct is serialized and
   * it is then deserialized with a smaller struct. Here the serialized struct
   * has 10 fields and we deserialized to a struct of 9 fields.
   */
  private void testShorterSchemaDeserialization(Random r) throws Throwable {

    StructObjectInspector rowOI1 = (StructObjectInspector) ObjectInspectorFactory
        .getReflectionObjectInspector(MyTestClassBigger.class,
        ObjectInspectorOptions.JAVA);
    String fieldNames1 = ObjectInspectorUtils.getFieldNames(rowOI1);
    String fieldTypes1 = ObjectInspectorUtils.getFieldTypes(rowOI1);
    SerDe serde1 = getSerDe(fieldNames1, fieldTypes1);
    serde1.getObjectInspector();

    StructObjectInspector rowOI2 = (StructObjectInspector) ObjectInspectorFactory
        .getReflectionObjectInspector(MyTestClass.class,
        ObjectInspectorOptions.JAVA);
    String fieldNames2 = ObjectInspectorUtils.getFieldNames(rowOI2);
    String fieldTypes2 = ObjectInspectorUtils.getFieldTypes(rowOI2);
    SerDe serde2 = getSerDe(fieldNames2, fieldTypes2);
    ObjectInspector serdeOI2 = serde2.getObjectInspector();

    int num = 100;
    for (int itest = 0; itest < num; itest++) {
      int randField = r.nextInt(11);
      Byte b = randField > 0 ? null : Byte.valueOf((byte) r.nextInt());
      Short s = randField > 1 ? null : Short.valueOf((short) r.nextInt());
      Integer n = randField > 2 ? null : Integer.valueOf(r.nextInt());
      Long l = randField > 3 ? null : Long.valueOf(r.nextLong());
      Float f = randField > 4 ? null : Float.valueOf(r.nextFloat());
      Double d = randField > 5 ? null : Double.valueOf(r.nextDouble());
      String st = randField > 6 ? null : TestBinarySortableSerDe
          .getRandString(r);
      HiveDecimal bd = randField > 7 ? null : TestBinarySortableSerDe.getRandHiveDecimal(r);
      Date date = randField > 8 ? null : TestBinarySortableSerDe.getRandDate(r);
      MyTestInnerStruct is = randField > 9 ? null : new MyTestInnerStruct(r
          .nextInt(5) - 2, r.nextInt(5) - 2);
      List<Integer> li = randField > 10 ? null : TestBinarySortableSerDe
          .getRandIntegerArray(r);
      byte[] ba  = TestBinarySortableSerDe.getRandBA(r, itest);
      Map<String, List<MyTestInnerStruct>> mp = new HashMap<String, List<MyTestInnerStruct>>();
      String key = TestBinarySortableSerDe.getRandString(r);
      List<MyTestInnerStruct> value = randField > 9 ? null
          : getRandStructArray(r);
      mp.put(key, value);
      String key1 = TestBinarySortableSerDe.getRandString(r);
      mp.put(key1, null);
      String key2 = TestBinarySortableSerDe.getRandString(r);
      List<MyTestInnerStruct> value2 = getRandStructArray(r);
      mp.put(key2, value2);

      MyTestClassBigger input = new MyTestClassBigger(b, s, n, l, f, d, st, bd, date, is,
          li, ba, mp);
      BytesWritable bw = (BytesWritable) serde1.serialize(input, rowOI1);
      Object output = serde2.deserialize(bw);

      if (0 != compareDiffSizedStructs(input, rowOI1, output, serdeOI2)) {
        System.out.println("structs      = "
            + SerDeUtils.getJSONString(input, rowOI1));
        System.out.println("deserialized = "
            + SerDeUtils.getJSONString(output, serdeOI2));
        System.out.println("serialized   = "
            + TestBinarySortableSerDe.hexString(bw));
        assertEquals(input, output);
      }
    }
  }

  /**
   * Test shorter schema deserialization where a bigger struct is serialized and
   * it is then deserialized with a smaller struct. Here the serialized struct
   * has 9 fields and we deserialized to a struct of 8 fields.
   */
  private void testShorterSchemaDeserialization1(Random r) throws Throwable {

    StructObjectInspector rowOI1 = (StructObjectInspector) ObjectInspectorFactory
        .getReflectionObjectInspector(MyTestClass.class,
        ObjectInspectorOptions.JAVA);
    String fieldNames1 = ObjectInspectorUtils.getFieldNames(rowOI1);
    String fieldTypes1 = ObjectInspectorUtils.getFieldTypes(rowOI1);
    SerDe serde1 = getSerDe(fieldNames1, fieldTypes1);
    serde1.getObjectInspector();

    StructObjectInspector rowOI2 = (StructObjectInspector) ObjectInspectorFactory
        .getReflectionObjectInspector(MyTestClassSmaller.class,
        ObjectInspectorOptions.JAVA);
    String fieldNames2 = ObjectInspectorUtils.getFieldNames(rowOI2);
    String fieldTypes2 = ObjectInspectorUtils.getFieldTypes(rowOI2);
    SerDe serde2 = getSerDe(fieldNames2, fieldTypes2);
    ObjectInspector serdeOI2 = serde2.getObjectInspector();

    int num = 100;
    for (int itest = 0; itest < num; itest++) {
      int randField = r.nextInt(12);
      Byte b = randField > 0 ? null : Byte.valueOf((byte) r.nextInt());
      Short s = randField > 1 ? null : Short.valueOf((short) r.nextInt());
      Integer n = randField > 2 ? null : Integer.valueOf(r.nextInt());
      Long l = randField > 3 ? null : Long.valueOf(r.nextLong());
      Float f = randField > 4 ? null : Float.valueOf(r.nextFloat());
      Double d = randField > 5 ? null : Double.valueOf(r.nextDouble());
      String st = randField > 6 ? null : TestBinarySortableSerDe
          .getRandString(r);
      HiveDecimal bd = randField > 7 ? null : TestBinarySortableSerDe.getRandHiveDecimal(r);
      Date date = randField > 8 ? null : TestBinarySortableSerDe.getRandDate(r);
      MyTestInnerStruct is = randField > 9 ? null : new MyTestInnerStruct(r
          .nextInt(5) - 2, r.nextInt(5) - 2);
      List<Integer> li = randField > 10 ? null : TestBinarySortableSerDe
          .getRandIntegerArray(r);
      byte[] ba = TestBinarySortableSerDe.getRandBA(r, itest);
      MyTestClass input = new MyTestClass(b, s, n, l, f, d, st, bd, date, is, li, ba);
      BytesWritable bw = (BytesWritable) serde1.serialize(input, rowOI1);
      Object output = serde2.deserialize(bw);

      if (0 != compareDiffSizedStructs(input, rowOI1, output, serdeOI2)) {
        System.out.println("structs      = "
            + SerDeUtils.getJSONString(input, rowOI1));
        System.out.println("deserialized = "
            + SerDeUtils.getJSONString(output, serdeOI2));
        System.out.println("serialized   = "
            + TestBinarySortableSerDe.hexString(bw));
        assertEquals(input, output);
      }
    }
  }

  /**
   * Test longer schema deserialization where a smaller struct is serialized and
   * it is then deserialized with a bigger struct Here the serialized struct has
   * 9 fields and we deserialized to a struct of 10 fields.
   */
  void testLongerSchemaDeserialization(Random r) throws Throwable {

    StructObjectInspector rowOI1 = (StructObjectInspector) ObjectInspectorFactory
        .getReflectionObjectInspector(MyTestClass.class,
        ObjectInspectorOptions.JAVA);
    String fieldNames1 = ObjectInspectorUtils.getFieldNames(rowOI1);
    String fieldTypes1 = ObjectInspectorUtils.getFieldTypes(rowOI1);
    SerDe serde1 = getSerDe(fieldNames1, fieldTypes1);
    serde1.getObjectInspector();

    StructObjectInspector rowOI2 = (StructObjectInspector) ObjectInspectorFactory
        .getReflectionObjectInspector(MyTestClassBigger.class,
        ObjectInspectorOptions.JAVA);
    String fieldNames2 = ObjectInspectorUtils.getFieldNames(rowOI2);
    String fieldTypes2 = ObjectInspectorUtils.getFieldTypes(rowOI2);
    SerDe serde2 = getSerDe(fieldNames2, fieldTypes2);
    ObjectInspector serdeOI2 = serde2.getObjectInspector();

    int num = 100;
    for (int itest = 0; itest < num; itest++) {
      int randField = r.nextInt(12);
      Byte b = randField > 0 ? null : Byte.valueOf((byte) r.nextInt());
      Short s = randField > 1 ? null : Short.valueOf((short) r.nextInt());
      Integer n = randField > 2 ? null : Integer.valueOf(r.nextInt());
      Long l = randField > 3 ? null : Long.valueOf(r.nextLong());
      Float f = randField > 4 ? null : Float.valueOf(r.nextFloat());
      Double d = randField > 5 ? null : Double.valueOf(r.nextDouble());
      String st = randField > 6 ? null : TestBinarySortableSerDe
          .getRandString(r);
      HiveDecimal bd = randField > 7 ? null : TestBinarySortableSerDe.getRandHiveDecimal(r);
      Date date = randField > 8 ? null : TestBinarySortableSerDe.getRandDate(r);
      MyTestInnerStruct is = randField > 9 ? null : new MyTestInnerStruct(r
          .nextInt(5) - 2, r.nextInt(5) - 2);
      List<Integer> li = randField > 10 ? null : TestBinarySortableSerDe
          .getRandIntegerArray(r);
      byte[] ba = TestBinarySortableSerDe.getRandBA(r, itest);
      MyTestClass input = new MyTestClass(b, s, n, l, f, d, st, bd, date, is, li,ba);
      BytesWritable bw = (BytesWritable) serde1.serialize(input, rowOI1);
      Object output = serde2.deserialize(bw);

      if (0 != compareDiffSizedStructs(input, rowOI1, output, serdeOI2)) {
        System.out.println("structs      = "
            + SerDeUtils.getJSONString(input, rowOI1));
        System.out.println("deserialized = "
            + SerDeUtils.getJSONString(output, serdeOI2));
        System.out.println("serialized   = "
            + TestBinarySortableSerDe.hexString(bw));
        assertEquals(input, output);
      }
    }
  }

  /**
   * Test longer schema deserialization where a smaller struct is serialized and
   * it is then deserialized with a bigger struct Here the serialized struct has
   * 8 fields and we deserialized to a struct of 9 fields.
   */
  void testLongerSchemaDeserialization1(Random r) throws Throwable {

    StructObjectInspector rowOI1 = (StructObjectInspector) ObjectInspectorFactory
        .getReflectionObjectInspector(MyTestClassSmaller.class,
        ObjectInspectorOptions.JAVA);
    String fieldNames1 = ObjectInspectorUtils.getFieldNames(rowOI1);
    String fieldTypes1 = ObjectInspectorUtils.getFieldTypes(rowOI1);
    SerDe serde1 = getSerDe(fieldNames1, fieldTypes1);
    serde1.getObjectInspector();

    StructObjectInspector rowOI2 = (StructObjectInspector) ObjectInspectorFactory
        .getReflectionObjectInspector(MyTestClass.class,
        ObjectInspectorOptions.JAVA);
    String fieldNames2 = ObjectInspectorUtils.getFieldNames(rowOI2);
    String fieldTypes2 = ObjectInspectorUtils.getFieldTypes(rowOI2);
    SerDe serde2 = getSerDe(fieldNames2, fieldTypes2);
    ObjectInspector serdeOI2 = serde2.getObjectInspector();

    int num = 100;
    for (int itest = 0; itest < num; itest++) {
      int randField = r.nextInt(9);
      Byte b = randField > 0 ? null : Byte.valueOf((byte) r.nextInt());
      Short s = randField > 1 ? null : Short.valueOf((short) r.nextInt());
      Integer n = randField > 2 ? null : Integer.valueOf(r.nextInt());
      Long l = randField > 3 ? null : Long.valueOf(r.nextLong());
      Float f = randField > 4 ? null : Float.valueOf(r.nextFloat());
      Double d = randField > 5 ? null : Double.valueOf(r.nextDouble());
      String st = randField > 6 ? null : TestBinarySortableSerDe
          .getRandString(r);
      HiveDecimal bd = randField > 7 ? null : TestBinarySortableSerDe.getRandHiveDecimal(r);
      Date date = randField > 7 ? null : TestBinarySortableSerDe.getRandDate(r);
      MyTestInnerStruct is = randField > 7 ? null : new MyTestInnerStruct(r
          .nextInt(5) - 2, r.nextInt(5) - 2);

      MyTestClassSmaller input = new MyTestClassSmaller(b, s, n, l, f, d, st, bd, date,
          is);
      BytesWritable bw = (BytesWritable) serde1.serialize(input, rowOI1);
      Object output = serde2.deserialize(bw);

      if (0 != compareDiffSizedStructs(input, rowOI1, output, serdeOI2)) {
        System.out.println("structs      = "
            + SerDeUtils.getJSONString(input, rowOI1));
        System.out.println("deserialized = "
            + SerDeUtils.getJSONString(output, serdeOI2));
        System.out.println("serialized   = "
            + TestBinarySortableSerDe.hexString(bw));
        assertEquals(input, output);
      }
    }
  }

  void testLazyBinaryMap(Random r) throws Throwable {

    StructObjectInspector rowOI = (StructObjectInspector) ObjectInspectorFactory
        .getReflectionObjectInspector(MyTestClassBigger.class,
        ObjectInspectorOptions.JAVA);
    String fieldNames = ObjectInspectorUtils.getFieldNames(rowOI);
    String fieldTypes = ObjectInspectorUtils.getFieldTypes(rowOI);
    SerDe serde = getSerDe(fieldNames, fieldTypes);
    ObjectInspector serdeOI = serde.getObjectInspector();

    StructObjectInspector soi1 = (StructObjectInspector) serdeOI;
    List<? extends StructField> fields1 = soi1.getAllStructFieldRefs();
    LazyBinaryMapObjectInspector lazympoi = (LazyBinaryMapObjectInspector) fields1
        .get(12).getFieldObjectInspector();
    ObjectInspector lazympkeyoi = lazympoi.getMapKeyObjectInspector();
    ObjectInspector lazympvalueoi = lazympoi.getMapValueObjectInspector();

    StructObjectInspector soi2 = rowOI;
    List<? extends StructField> fields2 = soi2.getAllStructFieldRefs();
    MapObjectInspector inputmpoi = (MapObjectInspector) fields2.get(12)
        .getFieldObjectInspector();
    ObjectInspector inputmpkeyoi = inputmpoi.getMapKeyObjectInspector();
    ObjectInspector inputmpvalueoi = inputmpoi.getMapValueObjectInspector();

    int num = 100;
    for (int testi = 0; testi < num; testi++) {

      Map<String, List<MyTestInnerStruct>> mp = new LinkedHashMap<String, List<MyTestInnerStruct>>();

      int randFields = r.nextInt(10);
      for (int i = 0; i < randFields; i++) {
        String key = TestBinarySortableSerDe.getRandString(r);
        int randField = r.nextInt(10);
        List<MyTestInnerStruct> value = randField > 4 ? null
            : getRandStructArray(r);
        mp.put(key, value);
      }

      MyTestClassBigger input = new MyTestClassBigger(null, null, null, null,
						      null, null, null, null, null, null, null, null, mp);
      BytesWritable bw = (BytesWritable) serde.serialize(input, rowOI);
      Object output = serde.deserialize(bw);
      Object lazyobj = soi1.getStructFieldData(output, fields1.get(12));
      Map<?, ?> outputmp = lazympoi.getMap(lazyobj);

      if (outputmp.size() != mp.size()) {
        throw new RuntimeException("Map size changed from " + mp.size()
            + " to " + outputmp.size() + " after serialization!");
      }

      for (Map.Entry<?, ?> entryinput : mp.entrySet()) {
        boolean bEqual = false;
        for (Map.Entry<?, ?> entryoutput : outputmp.entrySet()) {
          // find the same key
          if (0 == ObjectInspectorUtils.compare(entryoutput.getKey(),
              lazympkeyoi, entryinput.getKey(), inputmpkeyoi)) {
            if (0 != ObjectInspectorUtils.compare(entryoutput.getValue(),
                lazympvalueoi, entryinput.getValue(), inputmpvalueoi)) {
              assertEquals(entryoutput.getValue(), entryinput.getValue());
            } else {
              bEqual = true;
            }
            break;
          }
        }
        if (!bEqual) {
          throw new RuntimeException(
              "Could not find matched key in deserialized map : "
              + entryinput.getKey());
        }
      }
    }
  }

  /**
   * The test entrance function.
   *
   * @throws Throwable
   */
  public void testLazyBinarySerDe() throws Throwable {
    try {

      System.out.println("Beginning Test TestLazyBinarySerDe:");

      // generate the data
      int num = 1000;
      Random r = new Random(1234);
      MyTestClass rows[] = new MyTestClass[num];
      for (int i = 0; i < num; i++) {
        int randField = r.nextInt(12);
        Byte b = randField > 0 ? null : Byte.valueOf((byte) r.nextInt());
        Short s = randField > 1 ? null : Short.valueOf((short) r.nextInt());
        Integer n = randField > 2 ? null : Integer.valueOf(r.nextInt());
        Long l = randField > 3 ? null : Long.valueOf(r.nextLong());
        Float f = randField > 4 ? null : Float.valueOf(r.nextFloat());
        Double d = randField > 5 ? null : Double.valueOf(r.nextDouble());
        String st = randField > 6 ? null : TestBinarySortableSerDe
            .getRandString(r);
        HiveDecimal bd = randField > 7 ? null : TestBinarySortableSerDe.getRandHiveDecimal(r);
        Date date = randField > 8 ? null : TestBinarySortableSerDe.getRandDate(r);
        MyTestInnerStruct is = randField > 9 ? null : new MyTestInnerStruct(r
            .nextInt(5) - 2, r.nextInt(5) - 2);
        List<Integer> li = randField > 10 ? null : TestBinarySortableSerDe
            .getRandIntegerArray(r);
        byte[] ba = TestBinarySortableSerDe.getRandBA(r, i);
        MyTestClass t = new MyTestClass(b, s, n, l, f, d, st, bd, date, is, li, ba);
        rows[i] = t;
      }

      StructObjectInspector rowOI = (StructObjectInspector) ObjectInspectorFactory
          .getReflectionObjectInspector(MyTestClass.class,
          ObjectInspectorOptions.JAVA);

      String fieldNames = ObjectInspectorUtils.getFieldNames(rowOI);
      String fieldTypes = ObjectInspectorUtils.getFieldTypes(rowOI);

      // call the tests
      // 1/ test LazyBinarySerDe
      testLazyBinarySerDe(rows, rowOI, getSerDe(fieldNames, fieldTypes));
      // 2/ test LazyBinaryMap
      testLazyBinaryMap(r);
      // 3/ test serialization and deserialization with different schemas
      testShorterSchemaDeserialization(r);
      // 4/ test serialization and deserialization with different schemas
      testLongerSchemaDeserialization(r);
      // 5/ test serialization and deserialization with different schemas
      testShorterSchemaDeserialization1(r);
      // 6/ test serialization and deserialization with different schemas
      testLongerSchemaDeserialization1(r);

      System.out.println("Test TestLazyBinarySerDe passed!");
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }

  private final  byte[] inpBArray = {'1','\u0001','3','4'};
  private BytesWritable getInputBytesWritable() {
    //create input BytesWritable. This would have capacity greater than length)
    BytesWritable bW = new BytesWritable();
    bW.set(inpBArray, 0, inpBArray.length);
    return bW;
  }

  /**
   * Test to see if byte[] with correct contents is generated by
   * JavaBinaryObjectInspector from input BytesWritable
   * @throws Throwable
   */
  public void testJavaBinaryObjectInspector() throws Throwable {
    BytesWritable bW = getInputBytesWritable();

    //create JavaBinaryObjectInspector
    JavaBinaryObjectInspector binInspector =
        PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;

    //convert BytesWritable to byte][
    byte[] outBARef = binInspector.set(null, bW);

    assertTrue("compare input and output BAs",
        Arrays.equals(inpBArray, outBARef));
  }


  /**
   * Test to see if byte[] with correct contents is generated by
   * WritableBinaryObjectInspector from input BytesWritable
   * @throws Throwable
   */
  public void testWritableBinaryObjectInspector() throws Throwable {
    BytesWritable bW = getInputBytesWritable();

    //test WritableBinaryObjectInspector
    WritableBinaryObjectInspector writableBinInsp =
        PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;

    //convert BytesWritable to byte[]
    byte[] outBARef = writableBinInsp.getPrimitiveJavaObject(bW);

    assertTrue("compare input and output BAs",
        Arrays.equals(inpBArray, outBARef));
  }

  /**
   * Test to see if byte[] with correct contents is generated by
   * LazyBinaryObjectInspector from input BytesWritable
   * @throws Throwable
   */
  public void testLazyBinaryObjectInspector() throws Throwable {

    //create input ByteArrayRef
    ByteArrayRef inpBARef = new ByteArrayRef();
    inpBARef.setData(inpBArray);

    AbstractPrimitiveLazyObjectInspector<?> binInspector = LazyPrimitiveObjectInspectorFactory
    .getLazyObjectInspector(TypeInfoFactory.binaryTypeInfo, false, (byte)0);

    //create LazyBinary initialed with inputBA
    LazyBinary lazyBin = (LazyBinary) LazyFactory.createLazyObject(binInspector);
    lazyBin.init(inpBARef, 0, inpBArray.length);

    //use inspector to get a byte[] out of LazyBinary
    byte[] outBARef = (byte[]) binInspector.getPrimitiveJavaObject(lazyBin);

    assertTrue("compare input and output BAs",
        Arrays.equals(inpBArray, outBARef));

  }

}
