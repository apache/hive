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
package org.apache.hadoop.hive.serde2.lazybinary;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;



import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerdeRandomRowSource;
import org.apache.hadoop.hive.serde2.VerifyFast;
import org.apache.hadoop.hive.serde2.binarysortable.MyTestClass;
import org.apache.hadoop.hive.serde2.lazy.VerifyLazy;
import org.apache.hadoop.hive.serde2.lazybinary.fast.LazyBinaryDeserializeRead;
import org.apache.hadoop.hive.serde2.lazybinary.fast.LazyBinarySerializeWrite;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObject;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Test;

/**
 * TestLazyBinaryFast.
 */
public class TestLazyBinaryFast {

  private void testLazyBinaryFast(
      SerdeRandomRowSource source, Object[][] rows,
      AbstractSerDe serde, StructObjectInspector rowOI,
      AbstractSerDe serde_fewer, StructObjectInspector writeRowOI,
      TypeInfo[] typeInfos,
      boolean useIncludeColumns, boolean doWriteFewerColumns, Random r) throws Throwable {

    int rowCount = rows.length;
    int columnCount = typeInfos.length;
    boolean[] columnsToInclude = null;

    if (useIncludeColumns) {
      columnsToInclude = new boolean[columnCount];
      for (int i = 0; i < columnCount; i++) {
        columnsToInclude[i] = r.nextBoolean();
      }
    }

    int writeColumnCount = columnCount;
    TypeInfo[] writeTypeInfos = typeInfos;
    if (doWriteFewerColumns) {
      writeColumnCount = writeRowOI.getAllStructFieldRefs().size();
      writeTypeInfos = Arrays.copyOf(typeInfos, writeColumnCount);
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
        VerifyFast.serializeWrite(lazyBinarySerializeWrite, typeInfos[index], row[index]);
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
                  writeTypeInfos,
                  /* useExternalBuffer */ false);

      BytesWritable bytesWritable = serializeWriteBytes[i];
      lazyBinaryDeserializeRead.set(bytesWritable.getBytes(), 0, bytesWritable.getLength());

      for (int index = 0; index < columnCount; index++) {
        if (useIncludeColumns && !columnsToInclude[index]) {
          lazyBinaryDeserializeRead.skipNextField();
        } else if (index >= writeColumnCount) {
          // Should come back a null.
          VerifyFast.verifyDeserializeRead(lazyBinaryDeserializeRead, typeInfos[index], null);
        } else {
          verifyRead(lazyBinaryDeserializeRead, typeInfos[index], row[index]);
        }
      }
      if (writeColumnCount == columnCount) {
        assertTrue(lazyBinaryDeserializeRead.isEndOfInputReached());
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
        TypeInfo typeInfo = typeInfos[index];
        Object object = lazyBinaryStruct.getField(index);
        if (row[index] == null || object == null) {
          if (row[index] != null || object != null) {
            fail("SerDe deserialized NULL column mismatch");
          }
        } else {
          if (!VerifyLazy.lazyCompare(typeInfo, object, row[index])) {
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
              " do not match (" + Arrays.toString(typeInfos) + ")");
      }
      if (!Arrays.equals(bytes1, bytes2)) {
        fail("SerializeWrite and SerDe serialization does not match (" + Arrays.toString(typeInfos) + ")");
      }
      serdeBytes[i] = bytesWritable;
    }

    // Try to deserialize using DeserializeRead our Writable row objects created by SerDe.
    for (int i = 0; i < rowCount; i++) {
      Object[] row = rows[i];

      // When doWriteFewerColumns, try to read more fields than exist in buffer.
      LazyBinaryDeserializeRead lazyBinaryDeserializeRead =
              new LazyBinaryDeserializeRead(
                  typeInfos,
                  /* useExternalBuffer */ false);

      BytesWritable bytesWritable = serdeBytes[i];
      lazyBinaryDeserializeRead.set(bytesWritable.getBytes(), 0, bytesWritable.getLength());

      for (int index = 0; index < columnCount; index++) {
        if (useIncludeColumns && !columnsToInclude[index]) {
          lazyBinaryDeserializeRead.skipNextField();
        } else if (index >= writeColumnCount) {
          // Should come back a null.
          VerifyFast.verifyDeserializeRead(lazyBinaryDeserializeRead, typeInfos[index], null);
        } else {
          verifyRead(lazyBinaryDeserializeRead, typeInfos[index], row[index]);
        }
      }
      if (writeColumnCount == columnCount) {
        assertTrue(lazyBinaryDeserializeRead.isEndOfInputReached());
      }
    }
  }

  private void verifyRead(LazyBinaryDeserializeRead lazyBinaryDeserializeRead,
      TypeInfo typeInfo, Object expectedObject) throws IOException {
    if (typeInfo.getCategory() == ObjectInspector.Category.PRIMITIVE) {
      VerifyFast.verifyDeserializeRead(lazyBinaryDeserializeRead, typeInfo, expectedObject);
    } else {
      Object complexFieldObj = VerifyFast.deserializeReadComplexType(lazyBinaryDeserializeRead, typeInfo);
      if (expectedObject == null) {
        if (complexFieldObj != null) {
          fail("Field reports not null but object is null (class " + complexFieldObj.getClass().getName() +
              ", " + complexFieldObj.toString() + ")");
        }
      } else {
        if (complexFieldObj == null) {
          // It's hard to distinguish a union with null from a null union.
          if (expectedObject instanceof UnionObject) {
            UnionObject expectedUnion = (UnionObject) expectedObject;
            if (expectedUnion.getObject() == null) {
              return;
            }
          }
          fail("Field reports null but object is not null (class " + expectedObject.getClass().getName() +
              ", " + expectedObject.toString() + ")");
        }
      }
      if (!VerifyLazy.lazyCompare(typeInfo, complexFieldObj, expectedObject)) {
        fail("Comparison failed typeInfo " + typeInfo.toString());
      }
    }
  }

  public void testLazyBinaryFastCase(
      int caseNum, boolean doNonRandomFill, Random r, SerdeRandomRowSource.SupportedTypes supportedTypes, int depth)
      throws Throwable {

    SerdeRandomRowSource source = new SerdeRandomRowSource();

    source.init(r, supportedTypes, depth);

    int rowCount = 100;
    Object[][] rows = source.randomRows(rowCount);

    if (doNonRandomFill) {
      MyTestClass.nonRandomRowFill(rows, source.primitiveCategories());
    }

    StructObjectInspector rowStructObjectInspector = source.rowStructObjectInspector();

    TypeInfo[] typeInfos = source.typeInfos();
    int columnCount = typeInfos.length;

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

    TestLazyBinarySerDe testLazyBinarySerDe = new TestLazyBinarySerDe();
    AbstractSerDe serde = testLazyBinarySerDe.getSerDe(fieldNames, fieldTypes);

    AbstractSerDe serde_fewer = null;
    if (doWriteFewerColumns) {
      String partialFieldNames = ObjectInspectorUtils.getFieldNames(writeRowStructObjectInspector);
      String partialFieldTypes = ObjectInspectorUtils.getFieldTypes(writeRowStructObjectInspector);

        serde_fewer = testLazyBinarySerDe.getSerDe(partialFieldNames, partialFieldTypes);;
    }

    testLazyBinaryFast(
        source, rows,
        serde, rowStructObjectInspector,
        serde_fewer, writeRowStructObjectInspector,
        typeInfos,
        /* useIncludeColumns */ false, /* doWriteFewerColumns */ false, r);

    testLazyBinaryFast(
        source, rows,
        serde, rowStructObjectInspector,
        serde_fewer, writeRowStructObjectInspector,
        typeInfos,
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

  private void testLazyBinaryFast(SerdeRandomRowSource.SupportedTypes supportedTypes, int depth) throws Throwable {
    try {
      Random r = new Random(9983);

      int caseNum = 0;
      for (int i = 0; i < 10; i++) {
        testLazyBinaryFastCase(caseNum, (i % 2 == 0), r, supportedTypes, depth);
        caseNum++;
      }

    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  public void testLazyBinaryFastPrimitive() throws Throwable {
    testLazyBinaryFast(SerdeRandomRowSource.SupportedTypes.PRIMITIVE, 0);
  }

  @Test
  public void testLazyBinaryFastComplexDepthOne() throws Throwable {
    testLazyBinaryFast(SerdeRandomRowSource.SupportedTypes.ALL, 1);
  }

  @Test
  public void testLazyBinaryFastComplexDepthFour() throws Throwable {
    testLazyBinaryFast(SerdeRandomRowSource.SupportedTypes.ALL, 4);
  }
}
