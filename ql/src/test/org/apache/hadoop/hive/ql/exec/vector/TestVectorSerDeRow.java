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

package org.apache.hadoop.hive.ql.exec.vector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hadoop.hive.serde2.binarysortable.BinarySortableSerDe;
import org.apache.hadoop.hive.serde2.binarysortable.fast.BinarySortableDeserializeRead;
import org.apache.hadoop.hive.serde2.binarysortable.fast.BinarySortableSerializeWrite;
import org.apache.hadoop.hive.serde2.fast.DeserializeRead;
import org.apache.hadoop.hive.serde2.lazy.LazySerDeParameters;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.lazy.VerifyLazy;
import org.apache.hadoop.hive.serde2.lazy.fast.LazySimpleDeserializeRead;
import org.apache.hadoop.hive.serde2.lazy.fast.LazySimpleSerializeWrite;
import org.apache.hadoop.hive.serde2.lazy.fast.StringToDouble;
import org.apache.hadoop.hive.serde2.lazybinary.fast.LazyBinaryDeserializeRead;
import org.apache.hadoop.hive.serde2.lazybinary.fast.LazyBinarySerializeWrite;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObject;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.fast.SerializeWrite;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.io.Text;

import junit.framework.TestCase;

/**
 * Unit test for the vectorized serialize and deserialize row.
 */
public class TestVectorSerDeRow extends TestCase {

  public static enum SerializationType {
    NONE,
    BINARY_SORTABLE,
    LAZY_BINARY,
    LAZY_SIMPLE
  }

  private void verifyRead(
      DeserializeRead deserializeRead, TypeInfo typeInfo, Object expectedObject) throws IOException {

    if (typeInfo.getCategory() == ObjectInspector.Category.PRIMITIVE) {
      VectorVerifyFast.verifyDeserializeRead(deserializeRead, typeInfo, expectedObject);
    } else {
      Object complexFieldObj = VectorVerifyFast.deserializeReadComplexType(deserializeRead, typeInfo);
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

  void deserializeAndVerify(
      Output output, DeserializeRead deserializeRead,
      VectorRandomRowSource source, Object[] expectedRow)
      throws HiveException, IOException {

    deserializeRead.set(output.getData(),  0, output.getLength());
    TypeInfo[] typeInfos = source.typeInfos();
    for (int i = 0; i < typeInfos.length; i++) {
      Object expected = expectedRow[i];
      TypeInfo typeInfo = typeInfos[i];
      verifyRead(deserializeRead, typeInfo, expected);
    }
    TestCase.assertTrue(deserializeRead.isEndOfInputReached());
  }

  void serializeBatch(
      VectorizedRowBatch batch, VectorSerializeRow vectorSerializeRow,
      DeserializeRead deserializeRead, VectorRandomRowSource source, Object[][] randomRows,
      int firstRandomRowIndex) throws HiveException, IOException {

    Output output = new Output();
    for (int i = 0; i < batch.size; i++) {
      output.reset();
      vectorSerializeRow.setOutput(output);
      vectorSerializeRow.serializeWrite(batch, i);
      Object[] expectedRow = randomRows[firstRandomRowIndex + i];

      byte[] bytes = output.getData();
      int length = output.getLength();
      char[] chars = new char[length];
      for (int c = 0; c < chars.length; c++) {
        chars[c] = (char) (bytes[c] & 0xFF);
      }

      deserializeAndVerify(output, deserializeRead, source, expectedRow);
    }
  }

  void testVectorSerializeRow(Random r, SerializationType serializationType)
      throws HiveException, IOException, SerDeException {

    for (int i = 0; i < 20; i++) {
      innerTestVectorSerializeRow(r, serializationType);
    }
  }

  void innerTestVectorSerializeRow(
      Random r, SerializationType serializationType)
      throws HiveException, IOException, SerDeException {

    String[] emptyScratchTypeNames = new String[0];

    VectorRandomRowSource source = new VectorRandomRowSource();

    // FUTURE: try NULLs and UNICODE.
    source.init(
        r, VectorRandomRowSource.SupportedTypes.ALL, 4,
        /* allowNulls */ false, /* isUnicodeOk */ false);

    VectorizedRowBatchCtx batchContext = new VectorizedRowBatchCtx();
    batchContext.init(source.rowStructObjectInspector(), emptyScratchTypeNames);
    VectorizedRowBatch batch = batchContext.createVectorizedRowBatch();

    VectorAssignRow vectorAssignRow = new VectorAssignRow();
    vectorAssignRow.init(source.typeNames());

    int fieldCount = source.typeNames().size();
    DeserializeRead deserializeRead;
    SerializeWrite serializeWrite;
    switch (serializationType) {
    case BINARY_SORTABLE:
      deserializeRead = new BinarySortableDeserializeRead(source.typeInfos(), /* useExternalBuffer */ false);
      serializeWrite = new BinarySortableSerializeWrite(fieldCount);
      break;
    case LAZY_BINARY:
      deserializeRead = new LazyBinaryDeserializeRead(source.typeInfos(), /* useExternalBuffer */ false);
      serializeWrite = new LazyBinarySerializeWrite(fieldCount);
      break;
    case LAZY_SIMPLE:
      {
        StructObjectInspector rowObjectInspector = source.rowStructObjectInspector();
        // Use different separator values.
        byte[] separators = new byte[] {(byte) 9, (byte) 2, (byte) 3, (byte) 4, (byte) 5, (byte) 6, (byte) 7, (byte) 8};
        LazySerDeParameters lazySerDeParams = getSerDeParams(rowObjectInspector, separators);
        deserializeRead =
            new LazySimpleDeserializeRead(
                source.typeInfos(),
                /* useExternalBuffer */ false,
                lazySerDeParams);
        serializeWrite = new LazySimpleSerializeWrite(fieldCount, lazySerDeParams);
      }
      break;
    default:
      throw new Error("Unknown serialization type " + serializationType);
    }
    VectorSerializeRow vectorSerializeRow = new VectorSerializeRow(serializeWrite);
    vectorSerializeRow.init(source.typeNames());

    Object[][] randomRows = source.randomRows(2000);
    int firstRandomRowIndex = 0;
    for (int i = 0; i < randomRows.length; i++) {
      Object[] row = randomRows[i];

      vectorAssignRow.assignRow(batch, batch.size, row);
      batch.size++;
      if (batch.size == batch.DEFAULT_SIZE) {
        serializeBatch(batch, vectorSerializeRow, deserializeRead, source, randomRows, firstRandomRowIndex);
        firstRandomRowIndex = i + 1;
        batch.reset();
      }
    }
    if (batch.size > 0) {
      serializeBatch(batch, vectorSerializeRow, deserializeRead, source, randomRows, firstRandomRowIndex);
    }
  }

  private String getDifferenceInfo(Object actualRow, Object expectedRow) {
    if (actualRow instanceof List && expectedRow instanceof List) {
      List<Object> actualList = (List) actualRow;
      final int actualSize = actualList.size();
      List<Object> expectedList = (List) expectedRow;
      final int expectedSize = expectedList.size();
      if (actualSize != expectedSize) {
        return "Actual size " + actualSize + ", expected size " + expectedSize;
      }
      for (int i = 0; i < actualSize; i++) {
        Object actualObject = actualList.get(i);
        Object expecedObject = expectedList.get(i);
        if (!actualObject.equals(expecedObject)) {
          return "Column " + i + " is different";
        }
      }
    } else {
      if (!actualRow.equals(expectedRow)) {
        return "Object is different";
      }
    }
    return "Actual and expected row are the same";
  }

  private String getObjectDisplayString(Object object) {
    StringBuilder sb = new StringBuilder();

    if (object == null) {
      sb.append("NULL");
    } else if (object instanceof Text ||
               object instanceof HiveChar || object instanceof HiveCharWritable ||
               object instanceof HiveVarchar || object instanceof HiveVarcharWritable) {
        final String string;
        if (object instanceof Text) {
          Text text = (Text) object;
          string = text.toString();
        } else if (object instanceof HiveChar) {
          HiveChar hiveChar = (HiveChar) object;
          string = hiveChar.getStrippedValue();
        } else if (object instanceof HiveCharWritable) {
          HiveChar hiveChar = ((HiveCharWritable) object).getHiveChar();
          string = hiveChar.getStrippedValue();
        } else if (object instanceof HiveVarchar) {
          HiveVarchar hiveVarchar = (HiveVarchar) object;
          string = hiveVarchar.getValue();
        } else if (object instanceof HiveVarcharWritable) {
          HiveVarchar hiveVarchar = ((HiveVarcharWritable) object).getHiveVarchar();
          string = hiveVarchar.getValue();
        } else {
          throw new RuntimeException("Unexpected");
        }

        byte[] bytes = string.getBytes();
        final int byteLength = bytes.length;

        sb.append("'");
        sb.append(string);
        sb.append("' (byte length ");
        sb.append(bytes.length);
        sb.append(", string length ");
        sb.append(string.length());
        sb.append(", bytes ");
        sb.append(VectorizedBatchUtil.displayBytes(bytes, 0, byteLength));
        sb.append(")");
    } else {
      sb.append(object.toString());
    }
    return sb.toString();
  }

  private String getRowDisplayString(Object row) {
    StringBuilder sb = new StringBuilder();
    if (row instanceof List) {
      List<Object> list = (List) row;
      final int size = list.size();
      boolean isFirst = true;
      for (int i = 0; i < size; i++) {
        if (isFirst) {
          isFirst = false;
        } else {
          sb.append(", ");
        }
        Object object = list.get(i);
        sb.append(getObjectDisplayString(object));
      }
    } else {
      sb.append(getObjectDisplayString(row));
    }
    return sb.toString();
  }

  void examineBatch(VectorizedRowBatch batch, VectorExtractRow vectorExtractRow,
      TypeInfo[] typeInfos, Object[][] randomRows, int firstRandomRowIndex,
      String title) {

    int rowSize = vectorExtractRow.getCount();
    Object[] row = new Object[rowSize];
    for (int i = 0; i < batch.size; i++) {
      vectorExtractRow.extractRow(batch, i, row);

      Object[] expectedRow = randomRows[firstRandomRowIndex + i];

      for (int c = 0; c < rowSize; c++) {
        Object rowObj = row[c];
        Object expectedObj = expectedRow[c];
        if (rowObj == null) {
          if (expectedObj == null) {
            continue;
          }
          fail("Unexpected NULL from extractRow.  Expected class " +
              typeInfos[c].getCategory() + " value " + expectedObj +
              " batch index " + i + " firstRandomRowIndex " + firstRandomRowIndex);
        }
        if (!rowObj.equals(expectedObj)) {
          String actualValueString = getRowDisplayString(rowObj);
          String expectedValueString = getRowDisplayString(expectedObj);
          String differentInfoString = getDifferenceInfo(row, expectedObj);
          fail("Row " + (firstRandomRowIndex + i) + " and column " + c + " mismatch (" +
              typeInfos[c].getCategory() + " actual value '" + actualValueString + "'" +
              " and expected value '" + expectedValueString + "')" +
              " difference info " + differentInfoString +
              " typeInfos " + Arrays.toString(typeInfos) +
              " title " + title);
        }
      }
    }
  }

  private Output serializeRow(Object[] row, VectorRandomRowSource source,
      SerializeWrite serializeWrite) throws HiveException, IOException {
    Output output = new Output();
    serializeWrite.set(output);
    TypeInfo[] typeInfos = source.typeInfos();

    for (int i = 0; i < typeInfos.length; i++) {
      VectorVerifyFast.serializeWrite(serializeWrite, typeInfos[i], row[i]);
    }
    return output;
  }

  private void addToProperties(Properties tbl, String fieldNames, String fieldTypes) {
    // Set the configuration parameters
    tbl.setProperty(serdeConstants.SERIALIZATION_FORMAT, "9");

    tbl.setProperty("columns", fieldNames);
    tbl.setProperty("columns.types", fieldTypes);

    tbl.setProperty(serdeConstants.SERIALIZATION_NULL_FORMAT, "\\N");
  }

  private LazySerDeParameters getSerDeParams(
      StructObjectInspector rowObjectInspector, byte[] separators) throws SerDeException {
    return getSerDeParams(new Configuration(), new Properties(), rowObjectInspector, separators);
  }

  private LazySerDeParameters getSerDeParams(
      Configuration conf, Properties tbl, StructObjectInspector rowObjectInspector,
      byte[] separators) throws SerDeException {

    String fieldNames = ObjectInspectorUtils.getFieldNames(rowObjectInspector);
    String fieldTypes = ObjectInspectorUtils.getFieldTypes(rowObjectInspector);
    addToProperties(tbl, fieldNames, fieldTypes);
    LazySerDeParameters lazySerDeParams = new LazySerDeParameters(conf, tbl, LazySimpleSerDe.class.getName());
    for (int i = 0; i < separators.length; i++) {
      lazySerDeParams.setSeparator(i, separators[i]);
    }
    return lazySerDeParams;
  }

  void testVectorDeserializeRow(
      Random r, SerializationType serializationType,
      boolean alternate1, boolean alternate2, boolean useExternalBuffer)
      throws HiveException, IOException, SerDeException {

    for (int i = 0; i < 20; i++) {
      innerTestVectorDeserializeRow(
          r, i,serializationType, alternate1, alternate2, useExternalBuffer);
    }
  }

  void innerTestVectorDeserializeRow(
      Random r, int iteration,
      SerializationType serializationType,
      boolean alternate1, boolean alternate2, boolean useExternalBuffer)
      throws HiveException, IOException, SerDeException {

    String title = "serializationType: " + serializationType + ", iteration " + iteration;

    String[] emptyScratchTypeNames = new String[0];

    VectorRandomRowSource source = new VectorRandomRowSource();

    // FUTURE: try NULLs and UNICODE.
    source.init(
        r, VectorRandomRowSource.SupportedTypes.ALL, 4,
        /* allowNulls */ false, /* isUnicodeOk */ false);

    VectorizedRowBatchCtx batchContext = new VectorizedRowBatchCtx();
    batchContext.init(source.rowStructObjectInspector(), emptyScratchTypeNames);
    VectorizedRowBatch batch = batchContext.createVectorizedRowBatch();

    // junk the destination for the 1st pass
    for (ColumnVector cv : batch.cols) {
      Arrays.fill(cv.isNull, true);
    }

    TypeInfo[] typeInfos = source.typeInfos();
    int fieldCount = source.typeNames().size();
    DeserializeRead deserializeRead;
    SerializeWrite serializeWrite;
    switch (serializationType) {
    case BINARY_SORTABLE:
      boolean useColumnSortOrderIsDesc = alternate1;
      if (!useColumnSortOrderIsDesc) {
        deserializeRead = new BinarySortableDeserializeRead(source.typeInfos(), useExternalBuffer);
        serializeWrite = new BinarySortableSerializeWrite(fieldCount);
      } else {
        boolean[] columnSortOrderIsDesc = new boolean[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
          columnSortOrderIsDesc[i] = r.nextBoolean();
        }

        byte[] columnNullMarker = new byte[fieldCount];
        byte[] columnNotNullMarker = new byte[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
          if (columnSortOrderIsDesc[i]) {
            // Descending
            // Null last (default for descending order)
            columnNullMarker[i] = BinarySortableSerDe.ZERO;
            columnNotNullMarker[i] = BinarySortableSerDe.ONE;
          } else {
            // Ascending
            // Null first (default for ascending order)
            columnNullMarker[i] = BinarySortableSerDe.ZERO;
            columnNotNullMarker[i] = BinarySortableSerDe.ONE;
          }
        }
        serializeWrite = new BinarySortableSerializeWrite(columnSortOrderIsDesc, columnNullMarker, columnNotNullMarker);
        deserializeRead = new BinarySortableDeserializeRead(source.typeInfos(), useExternalBuffer,
            columnSortOrderIsDesc, columnNullMarker, columnNotNullMarker);

      }
      boolean useBinarySortableCharsNeedingEscape = alternate2;
      if (useBinarySortableCharsNeedingEscape) {
        source.addBinarySortableAlphabets();
      }
      break;
    case LAZY_BINARY:
      deserializeRead = new LazyBinaryDeserializeRead(source.typeInfos(), useExternalBuffer);
      serializeWrite = new LazyBinarySerializeWrite(fieldCount);
      break;
    case LAZY_SIMPLE:
      {
        StructObjectInspector rowObjectInspector = source.rowStructObjectInspector();
        Configuration conf = new Configuration();
        Properties tbl = new Properties();
        tbl.setProperty(serdeConstants.FIELD_DELIM, "\t");
        tbl.setProperty(serdeConstants.LINE_DELIM, "\n");
        byte separator = (byte) '\t';
        boolean useLazySimpleEscapes = alternate1;
        if (useLazySimpleEscapes) {
          tbl.setProperty(serdeConstants.QUOTE_CHAR, "'");
          String escapeString = "\\";
          tbl.setProperty(serdeConstants.ESCAPE_CHAR, escapeString);
        }

        LazySerDeParameters lazySerDeParams =
            getSerDeParams(conf, tbl, rowObjectInspector, new byte[] { separator });

        if (useLazySimpleEscapes) {
          // LazySimple seems to throw away everything but \n and \r.
          boolean[] needsEscape = lazySerDeParams.getNeedsEscape();
          StringBuilder sb = new StringBuilder();
          if (needsEscape['\n']) {
            sb.append('\n');
          }
          if (needsEscape['\r']) {
            sb.append('\r');
          }
          // for (int i = 0; i < needsEscape.length; i++) {
          //  if (needsEscape[i]) {
          //    sb.append((char) i);
          //  }
          // }
          String needsEscapeStr = sb.toString();
          if (needsEscapeStr.length() > 0) {
            source.addEscapables(needsEscapeStr);
          }
        }
        deserializeRead =
            new LazySimpleDeserializeRead(source.typeInfos(), useExternalBuffer, lazySerDeParams);
        serializeWrite = new LazySimpleSerializeWrite(fieldCount, lazySerDeParams);
      }
      break;
    default:
      throw new Error("Unknown serialization type " + serializationType);
    }
    VectorDeserializeRow vectorDeserializeRow = new VectorDeserializeRow(deserializeRead);
    vectorDeserializeRow.init();

    // junk the destination for the 1st pass
    for (ColumnVector cv : batch.cols) {
      Arrays.fill(cv.isNull, true);
      cv.noNulls = false;
    }

    VectorExtractRow vectorExtractRow = new VectorExtractRow();
    vectorExtractRow.init(source.typeNames());

    Object[][] randomRows = source.randomRows(2000);
    int firstRandomRowIndex = 0;
    for (int i = 0; i < randomRows.length; i++) {
      Object[] row = randomRows[i];

      Output output = serializeRow(row, source, serializeWrite);
      vectorDeserializeRow.setBytes(output.getData(), 0, output.getLength());
      try {
        vectorDeserializeRow.deserialize(batch, batch.size);
      } catch (Exception e) {
        throw new HiveException(
            "\nDeserializeRead details: " +
                vectorDeserializeRow.getDetailedReadPositionString(),
            e);
      }
      batch.size++;
      if (batch.size == batch.DEFAULT_SIZE) {
        examineBatch(
            batch, vectorExtractRow, typeInfos, randomRows, firstRandomRowIndex,
            title);
        firstRandomRowIndex = i + 1;
        batch.reset();
      }
    }
    if (batch.size > 0) {
      examineBatch(
          batch, vectorExtractRow, typeInfos, randomRows, firstRandomRowIndex,
          title);
    }
  }

  public void testVectorBinarySortableSerializeRow() throws Throwable {
    Random r = new Random(8732);
    testVectorSerializeRow(r, SerializationType.BINARY_SORTABLE);
  }

  public void testVectorLazyBinarySerializeRow() throws Throwable {
    Random r = new Random(8732);
    testVectorSerializeRow(r, SerializationType.LAZY_BINARY);
  }

  public void testVectorLazySimpleSerializeRow() throws Throwable {
    Random r = new Random(8732);
    testVectorSerializeRow(r, SerializationType.LAZY_SIMPLE);
  }
 
  public void testVectorBinarySortableDeserializeRow() throws Throwable {
    Random r = new Random(8732);
    testVectorDeserializeRow(r,
        SerializationType.BINARY_SORTABLE,
        /* alternate1 = useColumnSortOrderIsDesc */ false,
        /* alternate2 = useBinarySortableCharsNeedingEscape */ false,
        /* useExternalBuffer */ false);

    testVectorDeserializeRow(r,
        SerializationType.BINARY_SORTABLE,
        /* alternate1 = useColumnSortOrderIsDesc */ true,
        /* alternate2 = useBinarySortableCharsNeedingEscape */ false,
        /* useExternalBuffer */ false);

    testVectorDeserializeRow(r,
        SerializationType.BINARY_SORTABLE,
        /* alternate1 = useColumnSortOrderIsDesc */ false,
        /* alternate2 = useBinarySortableCharsNeedingEscape */ false,
        /* useExternalBuffer */ true);

    testVectorDeserializeRow(r,
        SerializationType.BINARY_SORTABLE,
        /* alternate1 = useColumnSortOrderIsDesc */ true,
        /* alternate2 = useBinarySortableCharsNeedingEscape */ false,
        /* useExternalBuffer */ true);

    testVectorDeserializeRow(r,
        SerializationType.BINARY_SORTABLE,
        /* alternate1 = useColumnSortOrderIsDesc */ false,
        /* alternate2 = useBinarySortableCharsNeedingEscape */ true,
        /* useExternalBuffer */ false);

    testVectorDeserializeRow(r,
        SerializationType.BINARY_SORTABLE,
        /* alternate1 = useColumnSortOrderIsDesc */ true,
        /* alternate2 = useBinarySortableCharsNeedingEscape */ true,
        /* useExternalBuffer */ false);

    testVectorDeserializeRow(r,
        SerializationType.BINARY_SORTABLE,
        /* alternate1 = useColumnSortOrderIsDesc */ false,
        /* alternate2 = useBinarySortableCharsNeedingEscape */ true,
        /* useExternalBuffer */ true);

    testVectorDeserializeRow(r,
        SerializationType.BINARY_SORTABLE,
        /* alternate1 = useColumnSortOrderIsDesc */ true,
        /* alternate2 = useBinarySortableCharsNeedingEscape */ true,
        /* useExternalBuffer */ true);
  }

  public void testVectorLazyBinaryDeserializeRow() throws Throwable {
    Random r = new Random(8732);
    testVectorDeserializeRow(r,
        SerializationType.LAZY_BINARY,
        /* alternate1 = unused */ false,
        /* alternate2 = unused */ false,
        /* useExternalBuffer */ false);

    testVectorDeserializeRow(r,
        SerializationType.LAZY_BINARY,
        /* alternate1 = unused */ false,
        /* alternate2 = unused */ false,
        /* useExternalBuffer */ true);
  }

  public void testVectorLazySimpleDeserializeRow() throws Throwable {
    Random r = new Random(8732);
    testVectorDeserializeRow(r,
        SerializationType.LAZY_SIMPLE,
        /* alternate1 = useLazySimpleEscapes */ false,
        /* alternate2 = unused */ false,
        /* useExternalBuffer */ false);

    testVectorDeserializeRow(r,
        SerializationType.LAZY_SIMPLE,
        /* alternate1 = useLazySimpleEscapes */ false,
        /* alternate2 = unused */ false,
        /* useExternalBuffer */ true);

    testVectorDeserializeRow(r,
        SerializationType.LAZY_SIMPLE,
        /* alternate1 = useLazySimpleEscapes */ true,
        /* alternate2 = unused */ false,
        /* useExternalBuffer */ false);

    testVectorDeserializeRow(r,
        SerializationType.LAZY_SIMPLE,
        /* alternate1 = useLazySimpleEscapes */ true,
        /* alternate2 = unused */ false,
        /* useExternalBuffer */ true);
  }
}