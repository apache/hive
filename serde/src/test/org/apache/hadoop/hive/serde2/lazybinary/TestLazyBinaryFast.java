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

import java.util.Arrays;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerdeRandomRowSource;
import org.apache.hadoop.hive.serde2.VerifyFast;
import org.apache.hadoop.hive.serde2.binarysortable.MyTestClass;
import org.apache.hadoop.hive.serde2.lazybinary.fast.LazyBinaryDeserializeRead;
import org.apache.hadoop.hive.serde2.lazybinary.fast.LazyBinarySerializeWrite;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;

public class TestLazyBinaryFast extends TestCase {

  private void testLazyBinaryFast(
      SerdeRandomRowSource source, Object[][] rows,
      AbstractSerDe serde, StructObjectInspector rowOI,
      AbstractSerDe serde_fewer, StructObjectInspector writeRowOI,
      PrimitiveTypeInfo[] primitiveTypeInfos,
      boolean useIncludeColumns, boolean doWriteFewerColumns, Random r) throws Throwable {

    int rowCount = rows.length;
    int columnCount = primitiveTypeInfos.length;

    boolean[] columnsToInclude = null;
    if (useIncludeColumns) {
      columnsToInclude = new boolean[columnCount];
      for (int i = 0; i < columnCount; i++) {
        columnsToInclude[i] = r.nextBoolean();
      }
    }

    int writeColumnCount = columnCount;
    PrimitiveTypeInfo[] writePrimitiveTypeInfos = primitiveTypeInfos;
    if (doWriteFewerColumns) {
      writeColumnCount = writeRowOI.getAllStructFieldRefs().size();
      writePrimitiveTypeInfos = Arrays.copyOf(primitiveTypeInfos, writeColumnCount);
    }

    LazyBinarySerializeWrite lazyBinarySerializeWrite =
        new LazyBinarySerializeWrite(writeColumnCount);

    // Try to serialize
    BytesWritable serializeWriteBytes[] = new BytesWritable[rowCount];
    for (int i = 0; i < rowCount; i++) {
      Object[] row = rows[i];
      Output output = new Output();
      lazyBinarySerializeWrite.set(output);

      for (int index = 0; index < writeColumnCount; index++) {

        Writable writable = (Writable) row[index];

        VerifyFast.serializeWrite(lazyBinarySerializeWrite, primitiveTypeInfos[index], writable);
      }

      BytesWritable bytesWritable = new BytesWritable();
      bytesWritable.set(output.getData(), 0, output.getLength());
      serializeWriteBytes[i] = bytesWritable;
    }

    // Try to deserialize
    for (int i = 0; i < rowCount; i++) {
      Object[] row = rows[i];

      // Specifying the right type info length tells LazyBinaryDeserializeRead which is the last
      // column.
      LazyBinaryDeserializeRead lazyBinaryDeserializeRead =
              new LazyBinaryDeserializeRead(
                  writePrimitiveTypeInfos,
                  /* useExternalBuffer */ false);

      BytesWritable bytesWritable = serializeWriteBytes[i];
      lazyBinaryDeserializeRead.set(bytesWritable.getBytes(), 0, bytesWritable.getLength());

      for (int index = 0; index < columnCount; index++) {
        if (useIncludeColumns && !columnsToInclude[index]) {
          lazyBinaryDeserializeRead.skipNextField();
        } else if (index >= writeColumnCount) {
          // Should come back a null.
          VerifyFast.verifyDeserializeRead(lazyBinaryDeserializeRead, primitiveTypeInfos[index], null);
        } else {
          Writable writable = (Writable) row[index];
          VerifyFast.verifyDeserializeRead(lazyBinaryDeserializeRead, primitiveTypeInfos[index], writable);
        }
      }
      if (writeColumnCount == columnCount) {
        TestCase.assertTrue(lazyBinaryDeserializeRead.isEndOfInputReached());
      }
    }

    // Try to deserialize using SerDe class our Writable row objects created by SerializeWrite.
    for (int i = 0; i < rowCount; i++) {
      BytesWritable bytesWritable = serializeWriteBytes[i];
      LazyBinaryStruct lazyBinaryStruct;
      if (doWriteFewerColumns) {
        lazyBinaryStruct = (LazyBinaryStruct) serde_fewer.deserialize(bytesWritable);
      } else {
        lazyBinaryStruct = (LazyBinaryStruct) serde.deserialize(bytesWritable);
      }

      Object[] row = rows[i];

      for (int index = 0; index < writeColumnCount; index++) {
        PrimitiveTypeInfo primitiveTypeInfo = primitiveTypeInfos[index];
        Writable writable = (Writable) row[index];
        Object object = lazyBinaryStruct.getField(index);
        if (writable == null || object == null) {
          if (writable != null || object != null) {
            fail("SerDe deserialized NULL column mismatch");
          }
        } else {
          if (!object.equals(writable)) {
            fail("SerDe deserialized value does not match");
          }
        }
      }
    }

    // One Writable per row.
    BytesWritable serdeBytes[] = new BytesWritable[rowCount];

    // Serialize using the SerDe, then below deserialize using DeserializeRead.
    Object[] serdeRow = new Object[writeColumnCount];
    for (int i = 0; i < rowCount; i++) {
      Object[] row = rows[i];

      // LazyBinary seems to work better with an row object array instead of a Java object...
      for (int index = 0; index < writeColumnCount; index++) {
        serdeRow[index] = row[index];
      }

      BytesWritable serialized;
      if (doWriteFewerColumns) {
        serialized = (BytesWritable) serde_fewer.serialize(serdeRow, writeRowOI);
      } else {
        serialized = (BytesWritable) serde.serialize(serdeRow, rowOI);
      }

      BytesWritable bytesWritable =
          new BytesWritable(
              Arrays.copyOfRange(serialized.getBytes(), 0, serialized.getLength()));
      byte[] bytes1 = bytesWritable.getBytes();

      BytesWritable lazySerializedWriteBytes = serializeWriteBytes[i];
      byte[] bytes2 = Arrays.copyOfRange(lazySerializedWriteBytes.getBytes(), 0, lazySerializedWriteBytes.getLength());
      if (bytes1.length != bytes2.length) {
        fail("SerializeWrite length " + bytes2.length + " and " +
              "SerDe serialization length " + bytes1.length +
              " do not match (" + Arrays.toString(primitiveTypeInfos) + ")");
      }
      if (!Arrays.equals(bytes1, bytes2)) {
        fail("SerializeWrite and SerDe serialization does not match (" + Arrays.toString(primitiveTypeInfos) + ")");
      }
      serdeBytes[i] = bytesWritable;
    }

    // Try to deserialize using DeserializeRead our Writable row objects created by SerDe.
    for (int i = 0; i < rowCount; i++) {
      Object[] row = rows[i];

      // When doWriteFewerColumns, try to read more fields than exist in buffer.
      LazyBinaryDeserializeRead lazyBinaryDeserializeRead =
              new LazyBinaryDeserializeRead(
                  primitiveTypeInfos,
                  /* useExternalBuffer */ false);

      BytesWritable bytesWritable = serdeBytes[i];
      lazyBinaryDeserializeRead.set(bytesWritable.getBytes(), 0, bytesWritable.getLength());

      for (int index = 0; index < columnCount; index++) {
        if (useIncludeColumns && !columnsToInclude[index]) {
          lazyBinaryDeserializeRead.skipNextField();
        } else if (index >= writeColumnCount) {
          // Should come back a null.
          VerifyFast.verifyDeserializeRead(lazyBinaryDeserializeRead, primitiveTypeInfos[index], null);
        } else {
          Writable writable = (Writable) row[index];
          VerifyFast.verifyDeserializeRead(lazyBinaryDeserializeRead, primitiveTypeInfos[index], writable);
        }
      }
      if (writeColumnCount == columnCount) {
        TestCase.assertTrue(lazyBinaryDeserializeRead.isEndOfInputReached());
      }
    }
  }

  public void testLazyBinaryFastCase(int caseNum, boolean doNonRandomFill, Random r) throws Throwable {

    SerdeRandomRowSource source = new SerdeRandomRowSource();
    source.init(r);

    int rowCount = 1000;
    Object[][] rows = source.randomRows(rowCount);

    if (doNonRandomFill) {
      MyTestClass.nonRandomRowFill(rows, source.primitiveCategories());
    }

    StructObjectInspector rowStructObjectInspector = source.rowStructObjectInspector();

    PrimitiveTypeInfo[] primitiveTypeInfos = source.primitiveTypeInfos();
    int columnCount = primitiveTypeInfos.length;

    int writeColumnCount = columnCount;
    StructObjectInspector writeRowStructObjectInspector = rowStructObjectInspector;
    boolean doWriteFewerColumns = r.nextBoolean();
    if (doWriteFewerColumns) {
      writeColumnCount = 1 + r.nextInt(columnCount);
      if (writeColumnCount == columnCount) {
        doWriteFewerColumns = false;
      } else {
        writeRowStructObjectInspector = source.partialRowStructObjectInspector(writeColumnCount);
      }
    }

    String fieldNames = ObjectInspectorUtils.getFieldNames(rowStructObjectInspector);
    String fieldTypes = ObjectInspectorUtils.getFieldTypes(rowStructObjectInspector);

    AbstractSerDe serde = TestLazyBinarySerDe.getSerDe(fieldNames, fieldTypes);

    AbstractSerDe serde_fewer = null;
    if (doWriteFewerColumns) {
      String partialFieldNames = ObjectInspectorUtils.getFieldNames(writeRowStructObjectInspector);
      String partialFieldTypes = ObjectInspectorUtils.getFieldTypes(writeRowStructObjectInspector);

        serde_fewer = TestLazyBinarySerDe.getSerDe(partialFieldNames, partialFieldTypes);;
    }

    testLazyBinaryFast(
        source, rows,
        serde, rowStructObjectInspector,
        serde_fewer, writeRowStructObjectInspector,
        primitiveTypeInfos,
        /* useIncludeColumns */ false, /* doWriteFewerColumns */ false, r);

    testLazyBinaryFast(
        source, rows,
        serde, rowStructObjectInspector,
        serde_fewer, writeRowStructObjectInspector,
        primitiveTypeInfos,
        /* useIncludeColumns */ true, /* doWriteFewerColumns */ false, r);

    /*
     * Can the LazyBinary format really tolerate writing fewer columns?
     */
    // if (doWriteFewerColumns) {
    //   testLazyBinaryFast(
    //       source, rows,
    //       serde, rowStructObjectInspector,
    //       serde_fewer, writeRowStructObjectInspector,
    //       primitiveTypeInfos,
    //       /* useIncludeColumns */ false, /* doWriteFewerColumns */ true, r);

    //   testLazyBinaryFast(
    //       source, rows,
    //       serde, rowStructObjectInspector,
    //       serde_fewer, writeRowStructObjectInspector,
    //       primitiveTypeInfos,
    //       /* useIncludeColumns */ true, /* doWriteFewerColumns */ true, r);
    // }
  }

  public void testLazyBinaryFast() throws Throwable {

    try {
      Random r = new Random(35790);

      int caseNum = 0;
      for (int i = 0; i < 10; i++) {
        testLazyBinaryFastCase(caseNum, (i % 2 == 0), r);
        caseNum++;
      }

    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }
}