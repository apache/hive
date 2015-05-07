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
package org.apache.hadoop.hive.serde2.lazy;

import java.util.Arrays;
import java.util.Properties;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.VerifyFast;
import org.apache.hadoop.hive.serde2.binarysortable.MyTestPrimitiveClass;
import org.apache.hadoop.hive.serde2.binarysortable.MyTestPrimitiveClass.ExtraTypeInfo;
import org.apache.hadoop.hive.serde2.lazy.fast.LazySimpleDeserializeRead;
import org.apache.hadoop.hive.serde2.lazy.fast.LazySimpleSerializeWrite;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

import junit.framework.TestCase;

public class TestLazySimpleFast extends TestCase {

  private void testLazySimpleFast(MyTestPrimitiveClass[] myTestPrimitiveClasses, LazySimpleSerDe[] serdes,
      StructObjectInspector[] rowOIs, byte separator, LazySerDeParameters[] serdeParams,
      PrimitiveTypeInfo[][] primitiveTypeInfosArray) throws Throwable {


    // Try to serialize
    BytesWritable serializeWriteBytes[] = new BytesWritable[myTestPrimitiveClasses.length];
    for (int i = 0; i < myTestPrimitiveClasses.length; i++) {
      MyTestPrimitiveClass t = myTestPrimitiveClasses[i];
      Output output = new Output();

      LazySimpleSerializeWrite lazySimpleSerializeWrite = 
          new LazySimpleSerializeWrite(MyTestPrimitiveClass.primitiveCount,
              separator, serdeParams[i]);

      lazySimpleSerializeWrite.set(output);

      for (int index = 0; index < MyTestPrimitiveClass.primitiveCount; index++) {
        Object object = t.getPrimitiveObject(index);
        PrimitiveCategory primitiveCategory = t.getPrimitiveCategory(index);
        VerifyFast.serializeWrite(lazySimpleSerializeWrite, primitiveCategory, object);
      }

      BytesWritable bytesWritable = new BytesWritable();
      bytesWritable.set(output.getData(), 0, output.getLength());
      serializeWriteBytes[i] = bytesWritable;
    }

    // Try to deserialize
    for (int i = 0; i < myTestPrimitiveClasses.length; i++) {
      MyTestPrimitiveClass t = myTestPrimitiveClasses[i];
      PrimitiveTypeInfo[] primitiveTypeInfos = primitiveTypeInfosArray[i];
      LazySimpleDeserializeRead lazySimpleDeserializeRead = 
              new LazySimpleDeserializeRead(primitiveTypeInfos,
                  separator, serdeParams[i]);

      BytesWritable bytesWritable = serializeWriteBytes[i];
      byte[] bytes = bytesWritable.getBytes();
      int length = bytesWritable.getLength();
      lazySimpleDeserializeRead.set(bytes, 0, length);

      char[] chars = new char[length];
      for (int c = 0; c < chars.length; c++) {
        chars[c] = (char) (bytes[c] & 0xFF);
      }

      for (int index = 0; index < MyTestPrimitiveClass.primitiveCount; index++) {
        Object object = t.getPrimitiveObject(index);
        PrimitiveCategory primitiveCategory = t.getPrimitiveCategory(index);
        VerifyFast.verifyDeserializeRead(lazySimpleDeserializeRead, primitiveTypeInfos[index], object);
      }
      lazySimpleDeserializeRead.extraFieldsCheck();
      TestCase.assertTrue(!lazySimpleDeserializeRead.readBeyondConfiguredFieldsWarned());
      TestCase.assertTrue(!lazySimpleDeserializeRead.readBeyondBufferRangeWarned());
      TestCase.assertTrue(!lazySimpleDeserializeRead.bufferRangeHasExtraDataWarned());
    }

    // Try to deserialize using SerDe class our Writable row objects created by SerializeWrite.
    for (int i = 0; i < myTestPrimitiveClasses.length; i++) {
      BytesWritable bytesWritable = serializeWriteBytes[i];
      LazyStruct lazySimpleStruct = (LazyStruct) serdes[i].deserialize(bytesWritable);

      MyTestPrimitiveClass t = myTestPrimitiveClasses[i];
      PrimitiveTypeInfo[] primitiveTypeInfos = primitiveTypeInfosArray[i];

      for (int index = 0; index < MyTestPrimitiveClass.primitiveCount; index++) {
        PrimitiveTypeInfo primitiveTypeInfo = primitiveTypeInfos[index];
        Object expected = t.getPrimitiveWritableObject(index, primitiveTypeInfo);
        LazyPrimitive lazyPrimitive = (LazyPrimitive) lazySimpleStruct.getField(index);
        Object object;
        if (lazyPrimitive != null) {
          object = lazyPrimitive.getWritableObject();
        } else {
          object = null;
        }
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
    byte[][] serdeBytes = new byte[myTestPrimitiveClasses.length][];
  
    // Serialize using the SerDe, then below deserialize using DeserializeRead.
    Object[] row = new Object[MyTestPrimitiveClass.primitiveCount];
    for (int i = 0; i < myTestPrimitiveClasses.length; i++) {
      MyTestPrimitiveClass t = myTestPrimitiveClasses[i];
      PrimitiveTypeInfo[] primitiveTypeInfos = primitiveTypeInfosArray[i];

      // LazySimple seems to work better with an row object array instead of a Java object...
      for (int index = 0; index < MyTestPrimitiveClass.primitiveCount; index++) {
        Object object = t.getPrimitiveWritableObject(index, primitiveTypeInfos[index]);
        row[index] = object;
      }

      Text serialized = (Text) serdes[i].serialize(row, rowOIs[i]);
      byte[] bytes1 = Arrays.copyOfRange(serialized.getBytes(), 0, serialized.getLength());

      byte[] bytes2 = Arrays.copyOfRange(serializeWriteBytes[i].getBytes(), 0, serializeWriteBytes[i].getLength());
      if (!Arrays.equals(bytes1, bytes2)) {
        fail("SerializeWrite and SerDe serialization does not match");
      }
      serdeBytes[i] = copyBytes(serialized);
    }

    // Try to deserialize using DeserializeRead our Writable row objects created by SerDe.
    for (int i = 0; i < myTestPrimitiveClasses.length; i++) {
      MyTestPrimitiveClass t = myTestPrimitiveClasses[i];
      PrimitiveTypeInfo[] primitiveTypeInfos = primitiveTypeInfosArray[i];
      LazySimpleDeserializeRead lazySimpleDeserializeRead = 
              new LazySimpleDeserializeRead(primitiveTypeInfos,
                  separator, serdeParams[i]);

      byte[] bytes = serdeBytes[i];
      lazySimpleDeserializeRead.set(bytes, 0, bytes.length);

      for (int index = 0; index < MyTestPrimitiveClass.primitiveCount; index++) {
        Object object = t.getPrimitiveObject(index);
        VerifyFast.verifyDeserializeRead(lazySimpleDeserializeRead, primitiveTypeInfos[index], object);
      }
      lazySimpleDeserializeRead.extraFieldsCheck();
      TestCase.assertTrue(!lazySimpleDeserializeRead.readBeyondConfiguredFieldsWarned());
      TestCase.assertTrue(!lazySimpleDeserializeRead.readBeyondBufferRangeWarned());
      TestCase.assertTrue(!lazySimpleDeserializeRead.bufferRangeHasExtraDataWarned());
    }
  }

  private byte[] copyBytes(Text serialized) {
    byte[] result = new byte[serialized.getLength()];
    System.arraycopy(serialized.getBytes(), 0, result, 0, serialized.getLength());
    return result;
  }

  private Properties createProperties(String fieldNames, String fieldTypes) {
    Properties tbl = new Properties();

    // Set the configuration parameters
    tbl.setProperty(serdeConstants.SERIALIZATION_FORMAT, "9");
    
    tbl.setProperty("columns", fieldNames);
    tbl.setProperty("columns.types", fieldTypes);

    tbl.setProperty(serdeConstants.SERIALIZATION_NULL_FORMAT, "NULL");

    return tbl;
  }

  private LazySimpleSerDe getSerDe(String fieldNames, String fieldTypes) throws SerDeException {
    // Create the SerDe
    LazySimpleSerDe serDe = new LazySimpleSerDe();
    Configuration conf = new Configuration();
    Properties tbl = createProperties(fieldNames, fieldTypes);
    SerDeUtils.initializeSerDe(serDe, conf, tbl, null);
    return serDe;
  }

  private LazySerDeParameters getSerDeParams(String fieldNames, String fieldTypes) throws SerDeException {
    Configuration conf = new Configuration();
    Properties tbl = createProperties(fieldNames, fieldTypes);
    return new LazySerDeParameters(conf, tbl, LazySimpleSerDe.class.getName());
  }

  public void testLazySimpleFast() throws Throwable {
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
      LazySimpleSerDe[] serdes = new LazySimpleSerDe[num];
      LazySerDeParameters[] serdeParams = new LazySerDeParameters[num];
      for (int i = 0; i < num; i++) {
        MyTestPrimitiveClass t = rows[i];

        StructObjectInspector rowOI = t.getRowInspector(primitiveTypeInfosArray[i]);

        String fieldNames = ObjectInspectorUtils.getFieldNames(rowOI);
        String fieldTypes = ObjectInspectorUtils.getFieldTypes(rowOI);

        rowOIs[i] = rowOI;
        serdes[i] = getSerDe(fieldNames, fieldTypes);
        serdeParams[i] = getSerDeParams(fieldNames, fieldTypes);
      }

      byte separator = (byte) '\t';
      testLazySimpleFast(rows, serdes, rowOIs, separator, serdeParams, primitiveTypeInfosArray);
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }
}