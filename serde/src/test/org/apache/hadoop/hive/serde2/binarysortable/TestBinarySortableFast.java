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

import java.io.IOException;
import java.util.ArrayList;
import java.io.EOFException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerdeRandomRowSource;
import org.apache.hadoop.hive.serde2.VerifyFast;
import org.apache.hadoop.hive.serde2.binarysortable.fast.BinarySortableDeserializeRead;
import org.apache.hadoop.hive.serde2.binarysortable.fast.BinarySortableSerializeWrite;
import org.apache.hadoop.hive.serde2.lazy.VerifyLazy;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObject;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.BytesWritable;

import junit.framework.TestCase;
import org.junit.Assert;

public class TestBinarySortableFast extends TestCase {

  private static String debugDetailedReadPositionString;
  private static StackTraceElement[] debugStackTrace;

  private void testBinarySortableFast(
          SerdeRandomRowSource source, Object[][] rows,
          boolean[] columnSortOrderIsDesc, byte[] columnNullMarker, byte[] columnNotNullMarker,
          AbstractSerDe serde, StructObjectInspector rowOI,
          AbstractSerDe serde_fewer, StructObjectInspector writeRowOI,
          boolean ascending, TypeInfo[] typeInfos,
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
    if (doWriteFewerColumns) {
      writeColumnCount = writeRowOI.getAllStructFieldRefs().size();
    }

    BinarySortableSerializeWrite binarySortableSerializeWrite =
        new BinarySortableSerializeWrite(columnSortOrderIsDesc, columnNullMarker, columnNotNullMarker);

    // Try to serialize

    // One Writable per row.
    BytesWritable serializeWriteBytes[] = new BytesWritable[rowCount];

    int[][] perFieldWriteLengthsArray = new int[rowCount][];
    for (int i = 0; i < rowCount; i++) {
      Object[] row = rows[i];
      Output output = new Output();
      binarySortableSerializeWrite.set(output);

      int[] perFieldWriteLengths = new int[columnCount];
      for (int index = 0; index < writeColumnCount; index++) {
        VerifyFast.serializeWrite(binarySortableSerializeWrite, typeInfos[index], row[index]);
        perFieldWriteLengths[index] = output.getLength();
      }
      perFieldWriteLengthsArray[i] = perFieldWriteLengths;

      BytesWritable bytesWritable = new BytesWritable();
      bytesWritable.set(output.getData(), 0, output.getLength());
      serializeWriteBytes[i] = bytesWritable;
      if (i > 0) {
        BytesWritable previousBytesWritable = serializeWriteBytes[i - 1];
        int compareResult = previousBytesWritable.compareTo(bytesWritable);
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
    for (int i = 0; i < rowCount; i++) {
      Object[] row = rows[i];
      BinarySortableDeserializeRead binarySortableDeserializeRead =
              new BinarySortableDeserializeRead(
                  typeInfos,
                  /* useExternalBuffer */ false,
                  columnSortOrderIsDesc,
                  columnNullMarker,
                  columnNotNullMarker);

      BytesWritable bytesWritable = serializeWriteBytes[i];
      binarySortableDeserializeRead.set(
          bytesWritable.getBytes(), 0, bytesWritable.getLength());

      for (int index = 0; index < columnCount; index++) {
        if (useIncludeColumns && !columnsToInclude[index]) {
          binarySortableDeserializeRead.skipNextField();
        } else if (index >= writeColumnCount) {
          // Should come back a null.
          VerifyFast.verifyDeserializeRead(binarySortableDeserializeRead, typeInfos[index], null);
        } else {
          verifyRead(binarySortableDeserializeRead, typeInfos[index], row[index]);
        }
      }
      if (writeColumnCount == columnCount) {
        TestCase.assertTrue(binarySortableDeserializeRead.isEndOfInputReached());
      }

      /*
       * Clip off one byte and expect to get an EOFException on the write field.
       */
      BinarySortableDeserializeRead binarySortableDeserializeRead2 =
          new BinarySortableDeserializeRead(
              typeInfos,
              /* useExternalBuffer */ false,
              columnSortOrderIsDesc,
              columnNullMarker,
              columnNotNullMarker);

      binarySortableDeserializeRead2.set(
          bytesWritable.getBytes(), 0, bytesWritable.getLength() - 1);  // One fewer byte.

      for (int index = 0; index < writeColumnCount; index++) {
        if (index == writeColumnCount - 1) {
          boolean threw = false;
          try {
            verifyRead(binarySortableDeserializeRead2, typeInfos[index], row[index]);
          } catch (EOFException e) {
//          debugDetailedReadPositionString = binarySortableDeserializeRead2.getDetailedReadPositionString();
//          debugStackTrace = e.getStackTrace();
            threw = true;
          }

          if (!threw && row[index] != null) {
            Assert.fail();
          }
        } else {
          if (useIncludeColumns && !columnsToInclude[index]) {
            binarySortableDeserializeRead2.skipNextField();
          } else {
            verifyRead(binarySortableDeserializeRead2, typeInfos[index], row[index]);
          }
        }
      }

    }

    // Try to deserialize using SerDe class our Writable row objects created by SerializeWrite.
    for (int i = 0; i < rowCount; i++) {
      BytesWritable bytesWritable = serializeWriteBytes[i];

      // Note that regular SerDe doesn't tolerate fewer columns.
      List<Object> deserializedRow;
      if (doWriteFewerColumns) {
        deserializedRow = (List<Object>) serde_fewer.deserialize(bytesWritable);
      } else {
        deserializedRow = (List<Object>) serde.deserialize(bytesWritable);
      }

      Object[] row = rows[i];
      for (int index = 0; index < writeColumnCount; index++) {
        Object expected = row[index];
        Object object = deserializedRow.get(index);
        if (expected == null || object == null) {
          if (expected != null || object != null) {
            fail("SerDe deserialized NULL column mismatch");
          }
        } else {
          if (!object.equals(expected)) {
            fail("SerDe deserialized value does not match (expected " +
              expected.getClass().getName() + " " +
              expected.toString() + ", actual " +
              object.getClass().getName() + " " +
              object.toString() + ")");
          }
        }
      }
    }

    // One Writable per row.
    BytesWritable serdeBytes[] = new BytesWritable[rowCount];

    // Serialize using the SerDe, then below deserialize using DeserializeRead.
    for (int i = 0; i < rowCount; i++) {
      Object[] row = rows[i];

      // Since SerDe reuses memory, we will need to make a copy.
      BytesWritable serialized;
      if (doWriteFewerColumns) {
        serialized = (BytesWritable) serde_fewer.serialize(row, rowOI);
      } else {
        serialized = (BytesWritable) serde.serialize(row, rowOI);;
      }
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
        List<Integer> differentPositions = new ArrayList();
        for (int b = 0; b < serDeOutput.length; b++) {
          if (serDeOutput[b] != serializeWriteExpected[b]) {
            differentPositions.add(b);
          }
        }
        if (differentPositions.size() > 0) {
          List<String> serializeWriteExpectedFields = new ArrayList<String>();
          List<String> serDeFields = new ArrayList<String>();
          int f = 0;
          int lastBegin = 0;
          for (int b = 0; b < serDeOutput.length; b++) {
            int writeLength = perFieldWriteLengthsArray[i][f];
            if (b + 1 == writeLength) {
              serializeWriteExpectedFields.add(
                  displayBytes(serializeWriteExpected, lastBegin, writeLength - lastBegin));
              serDeFields.add(
                  displayBytes(serDeOutput, lastBegin, writeLength - lastBegin));
              f++;
              lastBegin = b + 1;
            }
          }
          fail("SerializeWrite and SerDe serialization does not match at positions " + differentPositions.toString() +
              "\n(SerializeWrite: " +
                  serializeWriteExpectedFields.toString() +
              "\nSerDe: " +
                  serDeFields.toString() +
              "\nperFieldWriteLengths " + Arrays.toString(perFieldWriteLengthsArray[i]) +
              "\nprimitiveTypeInfos " + Arrays.toString(typeInfos) +
              "\nrow " + Arrays.toString(row));
        }
      }
      serdeBytes[i] = bytesWritable;
    }

    // Try to deserialize using DeserializeRead our Writable row objects created by SerDe.
    for (int i = 0; i < rowCount; i++) {
      Object[] row = rows[i];
      BinarySortableDeserializeRead binarySortableDeserializeRead =
              new BinarySortableDeserializeRead(
                  typeInfos,
                  /* useExternalBuffer */ false,
                  columnSortOrderIsDesc,
                  columnNullMarker,
                  columnNotNullMarker);


      BytesWritable bytesWritable = serdeBytes[i];
      binarySortableDeserializeRead.set(bytesWritable.getBytes(), 0, bytesWritable.getLength());

      for (int index = 0; index < columnCount; index++) {
        if (useIncludeColumns && !columnsToInclude[index]) {
          binarySortableDeserializeRead.skipNextField();
        } else if (index >= writeColumnCount) {
          // Should come back a null.
          verifyRead(binarySortableDeserializeRead, typeInfos[index], null);
        } else {
          verifyRead(binarySortableDeserializeRead, typeInfos[index], row[index]);
        }
      }
      if (writeColumnCount == columnCount) {
        TestCase.assertTrue(binarySortableDeserializeRead.isEndOfInputReached());
      }
    }
  }

  private void verifyRead(BinarySortableDeserializeRead binarySortableDeserializeRead,
      TypeInfo typeInfo, Object expectedObject) throws IOException {
    if (typeInfo.getCategory() == ObjectInspector.Category.PRIMITIVE) {
      VerifyFast.verifyDeserializeRead(binarySortableDeserializeRead, typeInfo, expectedObject);
    } else {
      Object complexFieldObj = VerifyFast.deserializeReadComplexType(binarySortableDeserializeRead, typeInfo);
      if (expectedObject == null) {
        if (complexFieldObj != null) {
          TestCase.fail("Field reports not null but object is null (class " + complexFieldObj.getClass().getName() +
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
          TestCase.fail("Field reports null but object is not null (class " + expectedObject.getClass().getName() +
              ", " + expectedObject.toString() + ")");
        }
      }
      if (!VerifyLazy.lazyCompare(typeInfo, complexFieldObj, expectedObject)) {
        TestCase.fail("Comparision failed typeInfo " + typeInfo.toString());
      }
    }
  }

  private void testBinarySortableFastCase(
      int caseNum, boolean doNonRandomFill, Random r, SerdeRandomRowSource.SupportedTypes supportedTypes, int depth)
      throws Throwable {

    SerdeRandomRowSource source = new SerdeRandomRowSource();

    // UNDONE: Until Fast BinarySortable supports complex types -- disable.
    source.init(r, supportedTypes, depth);

    int rowCount = 1000;
    Object[][] rows = source.randomRows(rowCount);

    if (doNonRandomFill) {
      MyTestClass.nonRandomRowFill(rows, source.primitiveCategories());
    }

    // We need to operate on sorted data to fully test BinarySortable.
    source.sort(rows);

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
    String order;
    order = StringUtils.leftPad("", columnCount, '+');
    String nullOrder;
    nullOrder = StringUtils.leftPad("", columnCount, 'a');
    AbstractSerDe serde_ascending = TestBinarySortableSerDe.getSerDe(fieldNames, fieldTypes, order, nullOrder);

    AbstractSerDe serde_ascending_fewer = null;
    if (doWriteFewerColumns) {
      String partialFieldNames = ObjectInspectorUtils.getFieldNames(writeRowStructObjectInspector);
      String partialFieldTypes = ObjectInspectorUtils.getFieldTypes(writeRowStructObjectInspector);

      serde_ascending_fewer = TestBinarySortableSerDe.getSerDe(partialFieldNames, partialFieldTypes, order, nullOrder);
    }

    order = StringUtils.leftPad("", columnCount, '-');
    nullOrder = StringUtils.leftPad("", columnCount, 'z');
    AbstractSerDe serde_descending = TestBinarySortableSerDe.getSerDe(fieldNames, fieldTypes, order, nullOrder);

    AbstractSerDe serde_descending_fewer = null;
    if (doWriteFewerColumns) {
      String partialFieldNames = ObjectInspectorUtils.getFieldNames(writeRowStructObjectInspector);
      String partialFieldTypes = ObjectInspectorUtils.getFieldTypes(writeRowStructObjectInspector);

      serde_descending_fewer = TestBinarySortableSerDe.getSerDe(partialFieldNames, partialFieldTypes, order, nullOrder);
    }

    boolean[] columnSortOrderIsDesc = new boolean[columnCount];
    Arrays.fill(columnSortOrderIsDesc, false);

    byte[] columnNullMarker = new byte[columnCount];
    Arrays.fill(columnNullMarker, BinarySortableSerDe.ZERO);
    byte[] columnNotNullMarker = new byte[columnCount];
    Arrays.fill(columnNotNullMarker, BinarySortableSerDe.ONE);

    /*
     * Acending.
     */
    testBinarySortableFast(source, rows,
        columnSortOrderIsDesc, columnNullMarker, columnNotNullMarker,
        serde_ascending, rowStructObjectInspector,
        serde_ascending_fewer, writeRowStructObjectInspector,
        /* ascending */ true, typeInfos,
        /* useIncludeColumns */ false, /* doWriteFewerColumns */ false, r);

    testBinarySortableFast(source, rows,
        columnSortOrderIsDesc, columnNullMarker, columnNotNullMarker,
        serde_ascending, rowStructObjectInspector,
        serde_ascending_fewer, writeRowStructObjectInspector,
        /* ascending */ true, typeInfos,
        /* useIncludeColumns */ true, /* doWriteFewerColumns */ false, r);

    if (doWriteFewerColumns) {
      testBinarySortableFast(source, rows,
          columnSortOrderIsDesc, columnNullMarker, columnNotNullMarker,
          serde_ascending, rowStructObjectInspector,
          serde_ascending_fewer, writeRowStructObjectInspector,
          /* ascending */ true, typeInfos,
          /* useIncludeColumns */ false, /* doWriteFewerColumns */ true, r);

      testBinarySortableFast(source, rows,
          columnSortOrderIsDesc, columnNullMarker, columnNotNullMarker,
          serde_ascending, rowStructObjectInspector,
          serde_ascending_fewer, writeRowStructObjectInspector,
          /* ascending */ true, typeInfos,
          /* useIncludeColumns */ true, /* doWriteFewerColumns */ true, r);
    }

    /*
     * Descending.
     */
    Arrays.fill(columnSortOrderIsDesc, true);

    testBinarySortableFast(source, rows,
        columnSortOrderIsDesc, columnNullMarker, columnNotNullMarker,
        serde_descending, rowStructObjectInspector,
        serde_ascending_fewer, writeRowStructObjectInspector,
        /* ascending */ false, typeInfos,
        /* useIncludeColumns */ false, /* doWriteFewerColumns */ false, r);

    testBinarySortableFast(source, rows,
        columnSortOrderIsDesc, columnNullMarker, columnNotNullMarker,
        serde_descending, rowStructObjectInspector,
        serde_ascending_fewer, writeRowStructObjectInspector,
        /* ascending */ false, typeInfos,
        /* useIncludeColumns */ true, /* doWriteFewerColumns */ false, r);

    if (doWriteFewerColumns) {
      testBinarySortableFast(source, rows,
          columnSortOrderIsDesc, columnNullMarker, columnNotNullMarker,
          serde_descending, rowStructObjectInspector,
          serde_descending_fewer, writeRowStructObjectInspector,
          /* ascending */ false, typeInfos,
          /* useIncludeColumns */ false, /* doWriteFewerColumns */ true, r);

      testBinarySortableFast(source, rows,
          columnSortOrderIsDesc, columnNullMarker, columnNotNullMarker,
          serde_descending, rowStructObjectInspector,
          serde_descending_fewer, writeRowStructObjectInspector,
          /* ascending */ false, typeInfos,
          /* useIncludeColumns */ true, /* doWriteFewerColumns */ true, r);
    }

  }

  public void testBinarySortableFast(SerdeRandomRowSource.SupportedTypes supportedTypes, int depth) throws Throwable {

    try {
      Random r = new Random(35790);

      int caseNum = 0;
      for (int i = 0; i < 10; i++) {
        testBinarySortableFastCase(caseNum, (i % 2 == 0), r, supportedTypes, depth);
        caseNum++;
      }

    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }

  public void testBinarySortableFastPrimitive() throws Throwable {
    testBinarySortableFast(SerdeRandomRowSource.SupportedTypes.PRIMITIVE, 0);
  }

  public void testBinarySortableFastComplexDepthOne() throws Throwable {
    testBinarySortableFast(SerdeRandomRowSource.SupportedTypes.ALL_EXCEPT_MAP, 1);
  }

  public void testBinarySortableFastComplexDepthFour() throws Throwable {
    testBinarySortableFast(SerdeRandomRowSource.SupportedTypes.ALL_EXCEPT_MAP, 4);
  }

  private static String displayBytes(byte[] bytes, int start, int length) {
    StringBuilder sb = new StringBuilder();
    for (int i = start; i < start + length; i++) {
      sb.append(String.format("\\%03d", (int) (bytes[i] & 0xff)));
    }
    return sb.toString();
  }
}