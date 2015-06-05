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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.VerifyFast;
import org.apache.hadoop.hive.serde2.binarysortable.MyTestPrimitiveClass.ExtraTypeInfo;
import org.apache.hadoop.hive.serde2.binarysortable.fast.BinarySortableDeserializeRead;
import org.apache.hadoop.hive.serde2.binarysortable.fast.BinarySortableSerializeWrite;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.io.BytesWritable;

public class TestBinarySortableFast extends TestCase {

  private void testBinarySortableFast(MyTestPrimitiveClass[] myTestPrimitiveClasses,
          boolean[] columnSortOrderIsDesc, SerDe serde, StructObjectInspector rowOI, boolean ascending,
          Map<Object, PrimitiveTypeInfo[]> primitiveTypeInfoMap) throws Throwable {

    BinarySortableSerializeWrite binarySortableSerializeWrite = new BinarySortableSerializeWrite(columnSortOrderIsDesc);

    // Try to serialize

    // One Writable per row.
    BytesWritable serializeWriteBytes[] = new BytesWritable[myTestPrimitiveClasses.length];
  
    int[][] perFieldWriteLengthsArray = new int[myTestPrimitiveClasses.length][];
    for (int i = 0; i < myTestPrimitiveClasses.length; i++) {
      MyTestPrimitiveClass t = myTestPrimitiveClasses[i];
      Output output = new Output();
      binarySortableSerializeWrite.set(output);

      int[] perFieldWriteLengths = new int[MyTestPrimitiveClass.primitiveCount];
      for (int index = 0; index < MyTestPrimitiveClass.primitiveCount; index++) {
        Object object = t.getPrimitiveObject(index);
        PrimitiveCategory primitiveCategory = t.getPrimitiveCategory(index);
        VerifyFast.serializeWrite(binarySortableSerializeWrite, primitiveCategory, object);
        perFieldWriteLengths[index] = output.getLength();
      }
      perFieldWriteLengthsArray[i] = perFieldWriteLengths;

      BytesWritable bytesWritable = new BytesWritable();
      bytesWritable.set(output.getData(), 0, output.getLength());
      serializeWriteBytes[i] = bytesWritable;
      if (i > 0) {
        int compareResult = serializeWriteBytes[i - 1].compareTo(serializeWriteBytes[i]);
        if ((compareResult < 0 && !ascending)
            || (compareResult > 0 && ascending)) {
          System.out.println("Test failed in "
              + (ascending ? "ascending" : "descending") + " order with "
              + (i - 1) + " and " + i);
          System.out.println("serialized data [" + (i - 1) + "] = "
              + TestBinarySortableSerDe.hexString(serializeWriteBytes[i - 1]));
          System.out.println("serialized data [" + i + "] = "
              + TestBinarySortableSerDe.hexString(serializeWriteBytes[i]));
          fail("Sort order of serialized " + (i - 1) + " and " + i
              + " are reversed!");
        }
      }
    }


    // Try to deserialize using DeserializeRead our Writable row objects created by SerializeWrite.
    for (int i = 0; i < myTestPrimitiveClasses.length; i++) {
      MyTestPrimitiveClass t = myTestPrimitiveClasses[i];
      PrimitiveTypeInfo[] primitiveTypeInfos = primitiveTypeInfoMap.get(t);
      BinarySortableDeserializeRead binarySortableDeserializeRead = 
              new BinarySortableDeserializeRead(primitiveTypeInfos, columnSortOrderIsDesc);

      BytesWritable bytesWritable = serializeWriteBytes[i];
      binarySortableDeserializeRead.set(bytesWritable.getBytes(), 0, bytesWritable.getLength());

      for (int index = 0; index < MyTestPrimitiveClass.primitiveCount; index++) {
        Object object = t.getPrimitiveObject(index);
        VerifyFast.verifyDeserializeRead(binarySortableDeserializeRead, primitiveTypeInfos[index], object);
      }
      binarySortableDeserializeRead.extraFieldsCheck();
      TestCase.assertTrue(!binarySortableDeserializeRead.readBeyondConfiguredFieldsWarned());
      TestCase.assertTrue(!binarySortableDeserializeRead.readBeyondBufferRangeWarned());
      TestCase.assertTrue(!binarySortableDeserializeRead.bufferRangeHasExtraDataWarned());
    }

    // Try to deserialize using SerDe class our Writable row objects created by SerializeWrite.
    for (int i = 0; i < myTestPrimitiveClasses.length; i++) {
      BytesWritable bytesWritable = serializeWriteBytes[i];
      List<Object> deserializedRow = (List<Object>) serde.deserialize(bytesWritable);

      MyTestPrimitiveClass t = myTestPrimitiveClasses[i];
      PrimitiveTypeInfo[] primitiveTypeInfos = primitiveTypeInfoMap.get(t);
      for (int index = 0; index < MyTestPrimitiveClass.primitiveCount; index++) {
        Object expected = t.getPrimitiveWritableObject(index, primitiveTypeInfos[index]);
        Object object = deserializedRow.get(index);
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
    for (int i = 0; i < myTestPrimitiveClasses.length; i++) {
      MyTestPrimitiveClass t = myTestPrimitiveClasses[i];

      // Since SerDe reuses memory, we will need to make a copy.
      BytesWritable serialized = (BytesWritable) serde.serialize(t, rowOI);
      BytesWritable bytesWritable = new BytesWritable();
      bytesWritable.set(serialized);
      byte[] serDeOutput = Arrays.copyOfRange(bytesWritable.getBytes(), 0, bytesWritable.getLength());

      byte[] serializeWriteExpected = Arrays.copyOfRange(serializeWriteBytes[i].getBytes(), 0, serializeWriteBytes[i].getLength());
      if (!Arrays.equals(serDeOutput, serializeWriteExpected)) {
        int mismatchPos = -1;
        if (serDeOutput.length != serializeWriteExpected.length) {
          for (int b = 0; b < Math.min(serDeOutput.length, serializeWriteExpected.length); b++) {
            if (serDeOutput[b] != serializeWriteExpected[b]) {
              mismatchPos = b;
              break;
            }
          }
          fail("Different byte array lengths: serDeOutput.length " + serDeOutput.length + ", serializeWriteExpected.length " + serializeWriteExpected.length +
                  " mismatchPos " + mismatchPos + " perFieldWriteLengths " + Arrays.toString(perFieldWriteLengthsArray[i]));
        }
        for (int b = 0; b < serDeOutput.length; b++) {
          if (serDeOutput[b] != serializeWriteExpected[b]) {
            fail("SerializeWrite and SerDe serialization does not match at position " + b);
          }
        }
      }
      serdeBytes[i] = bytesWritable;
    }

    // Try to deserialize using DeserializeRead our Writable row objects created by SerDe.
    for (int i = 0; i < myTestPrimitiveClasses.length; i++) {
      MyTestPrimitiveClass t = myTestPrimitiveClasses[i];
      PrimitiveTypeInfo[] primitiveTypeInfos = primitiveTypeInfoMap.get(t);
      BinarySortableDeserializeRead binarySortableDeserializeRead = 
              new BinarySortableDeserializeRead(primitiveTypeInfos, columnSortOrderIsDesc);

      BytesWritable bytesWritable = serdeBytes[i];
      binarySortableDeserializeRead.set(bytesWritable.getBytes(), 0, bytesWritable.getLength());

      for (int index = 0; index < MyTestPrimitiveClass.primitiveCount; index++) {
        Object object = t.getPrimitiveObject(index);
        VerifyFast.verifyDeserializeRead(binarySortableDeserializeRead, primitiveTypeInfos[index], object);
      }
      binarySortableDeserializeRead.extraFieldsCheck();
      TestCase.assertTrue(!binarySortableDeserializeRead.readBeyondConfiguredFieldsWarned());
      TestCase.assertTrue(!binarySortableDeserializeRead.readBeyondBufferRangeWarned());
      TestCase.assertTrue(!binarySortableDeserializeRead.bufferRangeHasExtraDataWarned());
    }
  }

  public void testBinarySortableFast() throws Throwable {
    try {

      int num = 1000;
      Random r = new Random(1234);
      MyTestPrimitiveClass myTestPrimitiveClasses[] = new MyTestPrimitiveClass[num];
      // Need a map because we sort.
      Map<Object, PrimitiveTypeInfo[]> primitiveTypeInfoMap = new HashMap<Object, PrimitiveTypeInfo[]>();

      int i;
      // First try non-random values
      for (i = 0; i < MyTestClass.nrDecimal.length; i++) {
        MyTestPrimitiveClass t = new MyTestPrimitiveClass();
        ExtraTypeInfo extraTypeInfo = new ExtraTypeInfo();
        t.nonRandomFill(i, extraTypeInfo);
        myTestPrimitiveClasses[i] = t;
        PrimitiveTypeInfo[] primitiveTypeInfos = MyTestPrimitiveClass.getPrimitiveTypeInfos(extraTypeInfo);
        primitiveTypeInfoMap.put(t, primitiveTypeInfos);
      }

      for ( ; i < num; i++) {
        int randField = r.nextInt(MyTestPrimitiveClass.primitiveCount);
        MyTestPrimitiveClass t = new MyTestPrimitiveClass();
        int field = 0;
        ExtraTypeInfo extraTypeInfo = new ExtraTypeInfo();
        t.randomFill(r, randField, field, extraTypeInfo);
        myTestPrimitiveClasses[i] = t;
        PrimitiveTypeInfo[] primitiveTypeInfos = MyTestPrimitiveClass.getPrimitiveTypeInfos(extraTypeInfo);
        primitiveTypeInfoMap.put(t, primitiveTypeInfos);
      }

      StructObjectInspector rowOI = (StructObjectInspector) ObjectInspectorFactory
          .getReflectionObjectInspector(MyTestPrimitiveClass.class,
          ObjectInspectorOptions.JAVA);

      TestBinarySortableSerDe.sort(myTestPrimitiveClasses, rowOI);

      String fieldNames = ObjectInspectorUtils.getFieldNames(rowOI);
      String fieldTypes = ObjectInspectorUtils.getFieldTypes(rowOI);
      String order;
      order = StringUtils.leftPad("", MyTestPrimitiveClass.primitiveCount, '+');
      SerDe serde_ascending = TestBinarySortableSerDe.getSerDe(fieldNames, fieldTypes, order);
      order = StringUtils.leftPad("", MyTestPrimitiveClass.primitiveCount, '-');
      SerDe serde_descending = TestBinarySortableSerDe.getSerDe(fieldNames, fieldTypes, order);

      boolean[] columnSortOrderIsDesc = new boolean[MyTestPrimitiveClass.primitiveCount];
      Arrays.fill(columnSortOrderIsDesc, false);
      testBinarySortableFast(myTestPrimitiveClasses, columnSortOrderIsDesc, serde_ascending, rowOI, true, primitiveTypeInfoMap);
      Arrays.fill(columnSortOrderIsDesc, true);
      testBinarySortableFast(myTestPrimitiveClasses, columnSortOrderIsDesc, serde_descending, rowOI, false, primitiveTypeInfoMap);
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }
}