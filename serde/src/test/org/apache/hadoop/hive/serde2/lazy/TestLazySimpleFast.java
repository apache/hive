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
package org.apache.hadoop.hive.serde2.lazy;

import java.io.IOException;
import java.util.ArrayList;
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
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObject;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

import junit.framework.TestCase;

public class TestLazySimpleFast extends TestCase {

  private void testLazySimpleFast(
    SerdeRandomRowSource source, Object[][] rows,
    LazySimpleSerDe serde, StructObjectInspector rowOI,
    LazySimpleSerDe serde_fewer, StructObjectInspector writeRowOI,
    LazySerDeParameters serdeParams, LazySerDeParameters serdeParams_fewer,
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

    // Try to serialize
    BytesWritable serializeWriteBytes[] = new BytesWritable[rowCount];
    for (int i = 0; i < rowCount; i++) {
      Object[] row = rows[i];
      Output output = new Output();

      LazySimpleSerializeWrite lazySimpleSerializeWrite =
          new LazySimpleSerializeWrite(columnCount, serdeParams);

      lazySimpleSerializeWrite.set(output);

      for (int index = 0; index < columnCount; index++) {
        VerifyFast.serializeWrite(lazySimpleSerializeWrite, typeInfos[index], row[index]);
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
                  writeTypeInfos,
                  /* useExternalBuffer */ false,
                  serdeParams);

      BytesWritable bytesWritable = serializeWriteBytes[i];
      byte[] bytes = bytesWritable.getBytes();
      int length = bytesWritable.getLength();
      lazySimpleDeserializeRead.set(bytes, 0, length);

      for (int index = 0; index < columnCount; index++) {
        if (useIncludeColumns && !columnsToInclude[index]) {
          lazySimpleDeserializeRead.skipNextField();
        } else if (index >= writeColumnCount) {
          // Should come back a null.
          verifyReadNull(lazySimpleDeserializeRead, typeInfos[index]);
        } else {
          Object expectedObject = row[index];
          verifyRead(lazySimpleDeserializeRead, typeInfos[index], expectedObject);
        }
      }
      if (writeColumnCount == columnCount) {
        TestCase.assertTrue(lazySimpleDeserializeRead.isEndOfInputReached());
      }
    }

    // Try to deserialize using SerDe class our Writable row objects created by SerializeWrite.
    for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
      BytesWritable bytesWritable = serializeWriteBytes[rowIndex];
      LazyStruct lazySimpleStruct = (LazyStruct) serde.deserialize(bytesWritable);

      Object[] row = rows[rowIndex];

      for (int index = 0; index < columnCount; index++) {
        TypeInfo typeInfo = typeInfos[index];
        Object expectedObject = row[index];
        Object object = lazySimpleStruct.getField(index);
        if (expectedObject == null || object == null) {
          if (expectedObject != null || object != null) {
            fail("SerDe deserialized NULL column mismatch");
          }
        } else {
          if (!VerifyLazy.lazyCompare(typeInfo, object, expectedObject)) {
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
                  writeTypeInfos,
                  /* useExternalBuffer */ false,
                  serdeParams);

      byte[] bytes = serdeBytes[i];
      lazySimpleDeserializeRead.set(bytes, 0, bytes.length);

      for (int index = 0; index < columnCount; index++) {
        if (useIncludeColumns && !columnsToInclude[index]) {
          lazySimpleDeserializeRead.skipNextField();
        } else if (index >= writeColumnCount) {
          // Should come back a null.
          verifyReadNull(lazySimpleDeserializeRead, typeInfos[index]);
        } else {
          Object expectedObject = row[index];
          verifyRead(lazySimpleDeserializeRead, typeInfos[index], expectedObject);
        }
      }
      if (writeColumnCount == columnCount) {
        TestCase.assertTrue(lazySimpleDeserializeRead.isEndOfInputReached());
      }
    }
  }

  private void verifyReadNull(LazySimpleDeserializeRead lazySimpleDeserializeRead,
      TypeInfo typeInfo) throws IOException {
    if (typeInfo.getCategory() == Category.PRIMITIVE) {
      VerifyFast.verifyDeserializeRead(lazySimpleDeserializeRead, typeInfo, null);
    } else {
      Object complexFieldObj = VerifyFast.deserializeReadComplexType(lazySimpleDeserializeRead, typeInfo);
      if (complexFieldObj != null) {
        TestCase.fail("Field report not null but object is null");
      }
    }
  }

  private void verifyRead(LazySimpleDeserializeRead lazySimpleDeserializeRead,
      TypeInfo typeInfo, Object expectedObject) throws IOException {
    if (typeInfo.getCategory() == Category.PRIMITIVE) {
      VerifyFast.verifyDeserializeRead(lazySimpleDeserializeRead, typeInfo, expectedObject);
    } else {
      Object complexFieldObj = VerifyFast.deserializeReadComplexType(lazySimpleDeserializeRead, typeInfo);
      if (expectedObject == null) {
        if (complexFieldObj != null) {
          TestCase.fail("Field reports not null but object is null (class " + complexFieldObj.getClass().getName() + ", " + complexFieldObj.toString() + ")");
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
          TestCase.fail("Field reports null but object is not null (class " + expectedObject.getClass().getName() + ", " + expectedObject.toString() + ")");
        }
      }
      if (!VerifyLazy.lazyCompare(typeInfo, complexFieldObj, expectedObject)) {
        TestCase.fail("Comparision failed typeInfo " + typeInfo.toString());
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

  private LazySerDeParameters getSerDeParams(String fieldNames, String fieldTypes,
      byte[] separators) throws SerDeException {
    Configuration conf = new Configuration();
    Properties tbl = createProperties(fieldNames, fieldTypes);
    LazySerDeParameters lazySerDeParams = new LazySerDeParameters(conf, tbl, LazySimpleSerDe.class.getName());
    for (int i = 0; i < separators.length; i++) {
      lazySerDeParams.setSeparator(i, separators[i]);
    }
    return lazySerDeParams;
  }

  public void testLazySimpleFastCase(
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

    // Use different separator values.
    byte[] separators = new byte[] {(byte) 9, (byte) 2, (byte) 3, (byte) 4, (byte) 5, (byte) 6, (byte) 7, (byte) 8};

    LazySimpleSerDe serde = getSerDe(fieldNames, fieldTypes);
    LazySerDeParameters serdeParams = getSerDeParams(fieldNames, fieldTypes, separators);

    LazySimpleSerDe serde_fewer = null;
    LazySerDeParameters serdeParams_fewer = null;
    if (doWriteFewerColumns) {
      String partialFieldNames = ObjectInspectorUtils.getFieldNames(writeRowStructObjectInspector);
      String partialFieldTypes = ObjectInspectorUtils.getFieldTypes(writeRowStructObjectInspector);

        serde_fewer = getSerDe(fieldNames, fieldTypes);
        serdeParams_fewer = getSerDeParams(partialFieldNames, partialFieldTypes, separators);
    }


    testLazySimpleFast(
        source, rows,
        serde, rowStructObjectInspector,
        serde_fewer, writeRowStructObjectInspector,
        serdeParams, serdeParams_fewer, typeInfos,
        /* useIncludeColumns */ false, /* doWriteFewerColumns */ false, r);

    testLazySimpleFast(
        source, rows,
        serde, rowStructObjectInspector,
        serde_fewer, writeRowStructObjectInspector,
        serdeParams, serdeParams_fewer, typeInfos,
        /* useIncludeColumns */ true, /* doWriteFewerColumns */ false, r);

    if (doWriteFewerColumns) {
      testLazySimpleFast(
          source, rows,
          serde, rowStructObjectInspector,
          serde_fewer, writeRowStructObjectInspector,
          serdeParams, serdeParams_fewer, typeInfos,
          /* useIncludeColumns */ false, /* doWriteFewerColumns */ true, r);

      testLazySimpleFast(
          source, rows,
          serde, rowStructObjectInspector,
          serde_fewer, writeRowStructObjectInspector,
          serdeParams, serdeParams_fewer, typeInfos,
          /* useIncludeColumns */ true, /* doWriteFewerColumns */ true, r);
    }
  }

  public void testLazySimpleFast(SerdeRandomRowSource.SupportedTypes supportedTypes, int depth) throws Throwable {

    try {
      Random r = new Random(8322);

      int caseNum = 0;
      for (int i = 0; i < 20; i++) {
        testLazySimpleFastCase(caseNum, (i % 2 == 0), r, supportedTypes, depth);
        caseNum++;
      }

    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }

  public void testLazyBinarySimplePrimitive() throws Throwable {
    testLazySimpleFast(SerdeRandomRowSource.SupportedTypes.PRIMITIVE, 0);
  }

  public void testLazyBinarySimpleComplexDepthOne() throws Throwable {
    testLazySimpleFast(SerdeRandomRowSource.SupportedTypes.ALL, 1);
  }

  public void testLazyBinarySimpleComplexDepthFour() throws Throwable {
    testLazySimpleFast(SerdeRandomRowSource.SupportedTypes.ALL, 4);
  }
}