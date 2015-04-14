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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.VerifyFast;
import org.apache.hadoop.hive.serde2.binarysortable.MyTestPrimitiveClass;
import org.apache.hadoop.hive.serde2.binarysortable.MyTestPrimitiveClass.ExtraTypeInfo;
import org.apache.hadoop.hive.serde2.binarysortable.fast.BinarySortableDeserializeRead;
import org.apache.hadoop.hive.serde2.lazybinary.fast.LazyBinaryDeserializeRead;
import org.apache.hadoop.hive.serde2.lazybinary.fast.LazyBinarySerializeWrite;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BytesWritable;

public class TestLazyBinaryFast extends TestCase {

  private void testLazyBinaryFast(MyTestPrimitiveClass[] myTestPrimitiveClasses, SerDe[] serdes, StructObjectInspector[] rowOIs,
      PrimitiveTypeInfo[][] primitiveTypeInfosArray) throws Throwable {

    LazyBinarySerializeWrite lazyBinarySerializeWrite = new LazyBinarySerializeWrite(MyTestPrimitiveClass.primitiveCount);

    // Try to serialize
    BytesWritable serializeWriteBytes[] = new BytesWritable[myTestPrimitiveClasses.length];
    for (int i = 0; i < myTestPrimitiveClasses.length; i++) {
      MyTestPrimitiveClass t = myTestPrimitiveClasses[i];
      Output output = new Output();
      lazyBinarySerializeWrite.set(output);

      for (int index = 0; index < MyTestPrimitiveClass.primitiveCount; index++) {
        Object object = t.getPrimitiveObject(index);
        PrimitiveCategory primitiveCategory = t.getPrimitiveCategory(index);
        VerifyFast.serializeWrite(lazyBinarySerializeWrite, primitiveCategory, object);
      }

      BytesWritable bytesWritable = new BytesWritable();
      bytesWritable.set(output.getData(), 0, output.getLength());
      serializeWriteBytes[i] = bytesWritable;
    }

    // Try to deserialize
    for (int i = 0; i < myTestPrimitiveClasses.length; i++) {
      MyTestPrimitiveClass t = myTestPrimitiveClasses[i];
      PrimitiveTypeInfo[] primitiveTypeInfos = primitiveTypeInfosArray[i];
      LazyBinaryDeserializeRead lazyBinaryDeserializeRead = 
              new LazyBinaryDeserializeRead(primitiveTypeInfos);

      BytesWritable bytesWritable = serializeWriteBytes[i];
      lazyBinaryDeserializeRead.set(bytesWritable.getBytes(), 0, bytesWritable.getLength());

      for (int index = 0; index < MyTestPrimitiveClass.primitiveCount; index++) {
        Object object = t.getPrimitiveObject(index);
        PrimitiveCategory primitiveCategory = t.getPrimitiveCategory(index);
        VerifyFast.verifyDeserializeRead(lazyBinaryDeserializeRead, primitiveTypeInfos[index], object);
      }
      lazyBinaryDeserializeRead.extraFieldsCheck();
      TestCase.assertTrue(!lazyBinaryDeserializeRead.readBeyondConfiguredFieldsWarned());
      TestCase.assertTrue(!lazyBinaryDeserializeRead.readBeyondBufferRangeWarned());
      TestCase.assertTrue(!lazyBinaryDeserializeRead.bufferRangeHasExtraDataWarned());
    }

    // Try to deserialize using SerDe class our Writable row objects created by SerializeWrite.
    for (int i = 0; i < myTestPrimitiveClasses.length; i++) {
      BytesWritable bytesWritable = serializeWriteBytes[i];
      LazyBinaryStruct lazyBinaryStruct = (LazyBinaryStruct) serdes[i].deserialize(bytesWritable);

      MyTestPrimitiveClass t = myTestPrimitiveClasses[i];
      PrimitiveTypeInfo[] primitiveTypeInfos = primitiveTypeInfosArray[i];

      for (int index = 0; index < MyTestPrimitiveClass.primitiveCount; index++) {
        PrimitiveTypeInfo primitiveTypeInfo = primitiveTypeInfos[index];
        Object expected = t.getPrimitiveWritableObject(index, primitiveTypeInfo);
        Object object = lazyBinaryStruct.getField(index);
        if (expected == null || object == null) {
          if (expected != null || object != null) {
            fail("SerDe deserialized NULL column mismatch");
          }
        } else {
          if (!object.equals(expected)) {
            fail("SerDe deserialized value does not match");
          }
        }
      }
    }

    // One Writable per row.
    BytesWritable serdeBytes[] = new BytesWritable[myTestPrimitiveClasses.length];
  
    // Serialize using the SerDe, then below deserialize using DeserializeRead.
    Object[] row = new Object[MyTestPrimitiveClass.primitiveCount];
    for (int i = 0; i < myTestPrimitiveClasses.length; i++) {
      MyTestPrimitiveClass t = myTestPrimitiveClasses[i];
      PrimitiveTypeInfo[] primitiveTypeInfos = primitiveTypeInfosArray[i];

      // LazyBinary seems to work better with an row object array instead of a Java object...
      for (int index = 0; index < MyTestPrimitiveClass.primitiveCount; index++) {
        Object object = t.getPrimitiveWritableObject(index, primitiveTypeInfos[index]);
        row[index] = object;
      }

      BytesWritable serialized = (BytesWritable) serdes[i].serialize(row, rowOIs[i]);
      BytesWritable bytesWritable = new BytesWritable();
      bytesWritable.set(serialized);
      byte[] bytes1 = Arrays.copyOfRange(bytesWritable.getBytes(), 0, bytesWritable.getLength());

      byte[] bytes2 = Arrays.copyOfRange(serializeWriteBytes[i].getBytes(), 0, serializeWriteBytes[i].getLength());
      if (!Arrays.equals(bytes1, bytes2)) {
        fail("SerializeWrite and SerDe serialization does not match");
      }
      serdeBytes[i] = bytesWritable;
    }

    // Try to deserialize using DeserializeRead our Writable row objects created by SerDe.
    for (int i = 0; i < myTestPrimitiveClasses.length; i++) {
      MyTestPrimitiveClass t = myTestPrimitiveClasses[i];
      PrimitiveTypeInfo[] primitiveTypeInfos = primitiveTypeInfosArray[i];
      LazyBinaryDeserializeRead lazyBinaryDeserializeRead = 
              new LazyBinaryDeserializeRead(primitiveTypeInfos);

      BytesWritable bytesWritable = serdeBytes[i];
      lazyBinaryDeserializeRead.set(bytesWritable.getBytes(), 0, bytesWritable.getLength());

      for (int index = 0; index < MyTestPrimitiveClass.primitiveCount; index++) {
        Object object = t.getPrimitiveObject(index);
        VerifyFast.verifyDeserializeRead(lazyBinaryDeserializeRead, primitiveTypeInfos[index], object);
      }
      lazyBinaryDeserializeRead.extraFieldsCheck();
      TestCase.assertTrue(!lazyBinaryDeserializeRead.readBeyondConfiguredFieldsWarned());
      TestCase.assertTrue(!lazyBinaryDeserializeRead.readBeyondBufferRangeWarned());
      TestCase.assertTrue(!lazyBinaryDeserializeRead.bufferRangeHasExtraDataWarned());
    }
  }

  public void testLazyBinaryFast() throws Throwable {
    try {

      int num = 1000;
      Random r = new Random(1234);
      MyTestPrimitiveClass[] rows = new MyTestPrimitiveClass[num];
      PrimitiveTypeInfo[][] primitiveTypeInfosArray = new PrimitiveTypeInfo[num][];
      for (int i = 0; i < num; i++) {
        int randField = r.nextInt(MyTestPrimitiveClass.primitiveCount);
        MyTestPrimitiveClass t = new MyTestPrimitiveClass();
        int field = 0;
        ExtraTypeInfo extraTypeInfo = new ExtraTypeInfo();
        t.randomFill(r, randField, field, extraTypeInfo);
        PrimitiveTypeInfo[] primitiveTypeInfos = MyTestPrimitiveClass.getPrimitiveTypeInfos(extraTypeInfo);
        rows[i] = t;
        primitiveTypeInfosArray[i] = primitiveTypeInfos;
      }

      // To get the specific type information for CHAR and VARCHAR, seems like we need an
      // inspector and SerDe per row...
      StructObjectInspector[] rowOIs = new StructObjectInspector[num];
      SerDe[] serdes = new SerDe[num];
      for (int i = 0; i < num; i++) {
        MyTestPrimitiveClass t = rows[i];

        StructObjectInspector rowOI = t.getRowInspector(primitiveTypeInfosArray[i]);

        String fieldNames = ObjectInspectorUtils.getFieldNames(rowOI);
        String fieldTypes = ObjectInspectorUtils.getFieldTypes(rowOI);

        rowOIs[i] = rowOI;
        serdes[i] = TestLazyBinarySerDe.getSerDe(fieldNames, fieldTypes);
      }

      testLazyBinaryFast(rows, serdes, rowOIs, primitiveTypeInfosArray);
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }
}