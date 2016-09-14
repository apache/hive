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
import org.apache.hadoop.hive.serde2.SerdeRandomRowSource;
import org.apache.hadoop.hive.serde2.VerifyFast;
import org.apache.hadoop.hive.serde2.binarysortable.MyTestClass;
import org.apache.hadoop.hive.serde2.lazy.fast.LazySimpleDeserializeRead;
import org.apache.hadoop.hive.serde2.lazy.fast.LazySimpleSerializeWrite;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import junit.framework.TestCase;

public class TestLazySimpleFast extends TestCase {

  private void testLazySimpleFast(
    SerdeRandomRowSource source, Object[][] rows,
    LazySimpleSerDe serde, StructObjectInspector rowOI,
    LazySimpleSerDe serde_fewer, StructObjectInspector writeRowOI,
    byte separator, LazySerDeParameters serdeParams, LazySerDeParameters serdeParams_fewer,
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

    // Try to serialize
    BytesWritable serializeWriteBytes[] = new BytesWritable[rowCount];
    for (int i = 0; i < rowCount; i++) {
      Object[] row = rows[i];
      Output output = new Output();

      LazySimpleSerializeWrite lazySimpleSerializeWrite =
          new LazySimpleSerializeWrite(columnCount,
              separator, serdeParams);

      lazySimpleSerializeWrite.set(output);

      for (int index = 0; index < columnCount; index++) {

        Writable writable = (Writable) row[index];

        VerifyFast.serializeWrite(lazySimpleSerializeWrite, primitiveTypeInfos[index], writable);
      }

      BytesWritable bytesWritable = new BytesWritable();
      bytesWritable.set(output.getData(), 0, output.getLength());
      serializeWriteBytes[i] = bytesWritable;
    }

    // Try to deserialize
    for (int i = 0; i < rowCount; i++) {
      Object[] row = rows[i];
      LazySimpleDeserializeRead lazySimpleDeserializeRead =
              new LazySimpleDeserializeRead(
                  writePrimitiveTypeInfos,
                  /* useExternalBuffer */ false,
                  separator, serdeParams);

      BytesWritable bytesWritable = serializeWriteBytes[i];
      byte[] bytes = bytesWritable.getBytes();
      int length = bytesWritable.getLength();
      lazySimpleDeserializeRead.set(bytes, 0, length);

      char[] chars = new char[length];
      for (int c = 0; c < chars.length; c++) {
        chars[c] = (char) (bytes[c] & 0xFF);
      }

      for (int index = 0; index < columnCount; index++) {
        if (useIncludeColumns && !columnsToInclude[index]) {
          lazySimpleDeserializeRead.skipNextField();
        } else if (index >= writeColumnCount) {
          // Should come back a null.
          VerifyFast.verifyDeserializeRead(lazySimpleDeserializeRead, primitiveTypeInfos[index], null);
        } else {
          Writable writable = (Writable) row[index];
          VerifyFast.verifyDeserializeRead(lazySimpleDeserializeRead, primitiveTypeInfos[index], writable);
        }
      }
      if (writeColumnCount == columnCount) {
        TestCase.assertTrue(lazySimpleDeserializeRead.isEndOfInputReached());
      }
    }

    // Try to deserialize using SerDe class our Writable row objects created by SerializeWrite.
    for (int i = 0; i < rowCount; i++) {
      BytesWritable bytesWritable = serializeWriteBytes[i];
      LazyStruct lazySimpleStruct = (LazyStruct) serde.deserialize(bytesWritable);

      Object[] row = rows[i];

      for (int index = 0; index < columnCount; index++) {
        PrimitiveTypeInfo primitiveTypeInfo = primitiveTypeInfos[index];
        Writable writable = (Writable) row[index];
        LazyPrimitive lazyPrimitive = (LazyPrimitive) lazySimpleStruct.getField(index);
        Object object;
        if (lazyPrimitive != null) {
          object = lazyPrimitive.getWritableObject();
        } else {
          object = null;
        }
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
    byte[][] serdeBytes = new byte[rowCount][];

    // Serialize using the SerDe, then below deserialize using DeserializeRead.
    Object[] serdeRow = new Object[columnCount];
    for (int i = 0; i < rowCount; i++) {
      Object[] row = rows[i];

      // LazySimple seems to work better with an row object array instead of a Java object...
      for (int index = 0; index < columnCount; index++) {
        serdeRow[index] = row[index];
      }

      Text serialized = (Text) serde.serialize(serdeRow, rowOI);
      byte[] bytes1 = Arrays.copyOfRange(serialized.getBytes(), 0, serialized.getLength());

      byte[] bytes2 = Arrays.copyOfRange(serializeWriteBytes[i].getBytes(), 0, serializeWriteBytes[i].getLength());
      if (!Arrays.equals(bytes1, bytes2)) {
        fail("SerializeWrite and SerDe serialization does not match");
      }
      serdeBytes[i] = copyBytes(serialized);
    }

    // Try to deserialize using DeserializeRead our Writable row objects created by SerDe.
    for (int i = 0; i < rowCount; i++) {
      Object[] row = rows[i];

      LazySimpleDeserializeRead lazySimpleDeserializeRead =
              new LazySimpleDeserializeRead(
                  writePrimitiveTypeInfos,
                  /* useExternalBuffer */ false,
                  separator, serdeParams);

      byte[] bytes = serdeBytes[i];
      lazySimpleDeserializeRead.set(bytes, 0, bytes.length);

      for (int index = 0; index < columnCount; index++) {
        if (useIncludeColumns && !columnsToInclude[index]) {
          lazySimpleDeserializeRead.skipNextField();
        } else if (index >= writeColumnCount) {
          // Should come back a null.
          VerifyFast.verifyDeserializeRead(lazySimpleDeserializeRead, primitiveTypeInfos[index], null);
        } else {
          Writable writable = (Writable) row[index];
          VerifyFast.verifyDeserializeRead(lazySimpleDeserializeRead, primitiveTypeInfos[index], writable);
        }
      }
      if (writeColumnCount == columnCount) {
        TestCase.assertTrue(lazySimpleDeserializeRead.isEndOfInputReached());
      }
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

  public void testLazySimpleFastCase(int caseNum, boolean doNonRandomFill, Random r)
      throws Throwable {

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

    LazySimpleSerDe serde = getSerDe(fieldNames, fieldTypes);
    LazySerDeParameters serdeParams = getSerDeParams(fieldNames, fieldTypes);

    LazySimpleSerDe serde_fewer = null;
    LazySerDeParameters serdeParams_fewer = null;
    if (doWriteFewerColumns) {
      String partialFieldNames = ObjectInspectorUtils.getFieldNames(writeRowStructObjectInspector);
      String partialFieldTypes = ObjectInspectorUtils.getFieldTypes(writeRowStructObjectInspector);

        serde_fewer = getSerDe(fieldNames, fieldTypes);
        serdeParams_fewer = getSerDeParams(partialFieldNames, partialFieldTypes);
    }

    byte separator = (byte) '\t';
    testLazySimpleFast(
        source, rows,
        serde, rowStructObjectInspector,
        serde_fewer, writeRowStructObjectInspector,
        separator, serdeParams, serdeParams_fewer, primitiveTypeInfos,
        /* useIncludeColumns */ false, /* doWriteFewerColumns */ false, r);

    testLazySimpleFast(
        source, rows,
        serde, rowStructObjectInspector,
        serde_fewer, writeRowStructObjectInspector,
        separator, serdeParams, serdeParams_fewer, primitiveTypeInfos,
        /* useIncludeColumns */ true, /* doWriteFewerColumns */ false, r);

    if (doWriteFewerColumns) {
      testLazySimpleFast(
          source, rows,
          serde, rowStructObjectInspector,
          serde_fewer, writeRowStructObjectInspector,
          separator, serdeParams, serdeParams_fewer, primitiveTypeInfos,
          /* useIncludeColumns */ false, /* doWriteFewerColumns */ true, r);

      testLazySimpleFast(
          source, rows,
          serde, rowStructObjectInspector,
          serde_fewer, writeRowStructObjectInspector,
          separator, serdeParams, serdeParams_fewer, primitiveTypeInfos,
          /* useIncludeColumns */ true, /* doWriteFewerColumns */ true, r);
    }
  }

  public void testLazySimpleFast() throws Throwable {

    try {
      Random r = new Random(35790);

      int caseNum = 0;
      for (int i = 0; i < 10; i++) {
        testLazySimpleFastCase(caseNum, (i % 2 == 0), r);
        caseNum++;
      }

    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }
}