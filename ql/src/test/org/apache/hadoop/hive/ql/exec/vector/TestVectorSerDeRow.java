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

package org.apache.hadoop.hive.ql.exec.vector;

import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.OpenCSVSerde;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalDayTimeWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalYearMonthWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hadoop.hive.serde2.binarysortable.BinarySortableSerDe;
import org.apache.hadoop.hive.serde2.binarysortable.fast.BinarySortableDeserializeRead;
import org.apache.hadoop.hive.serde2.binarysortable.fast.BinarySortableSerializeWrite;
import org.apache.hadoop.hive.serde2.fast.DeserializeRead;
import org.apache.hadoop.hive.serde2.lazy.LazySerDeParameters;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.lazy.fast.LazySimpleDeserializeRead;
import org.apache.hadoop.hive.serde2.lazy.fast.LazySimpleSerializeWrite;
import org.apache.hadoop.hive.serde2.lazybinary.fast.LazyBinaryDeserializeRead;
import org.apache.hadoop.hive.serde2.lazybinary.fast.LazyBinarySerializeWrite;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hadoop.hive.serde2.fast.SerializeWrite;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import com.google.common.base.Charsets;

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

  void deserializeAndVerify(Output output, DeserializeRead deserializeRead,
              VectorRandomRowSource source, Object[] expectedRow)
              throws HiveException, IOException {
    deserializeRead.set(output.getData(),  0, output.getLength());
    PrimitiveCategory[] primitiveCategories = source.primitiveCategories();
    for (int i = 0; i < primitiveCategories.length; i++) {
      Object expected = expectedRow[i];
      PrimitiveCategory primitiveCategory = primitiveCategories[i];
      PrimitiveTypeInfo primitiveTypeInfo = source.primitiveTypeInfos()[i];
      if (!deserializeRead.readNextField()) {
        throw new HiveException("Unexpected NULL when reading primitiveCategory " + primitiveCategory +
            " expected (" + expected.getClass().getName() + ", " + expected.toString() + ") " +
            " deserializeRead " + deserializeRead.getClass().getName());
      }
      switch (primitiveCategory) {
      case BOOLEAN:
        {
          Boolean value = deserializeRead.currentBoolean;
          BooleanWritable expectedWritable = (BooleanWritable) expected;
          if (!value.equals(expectedWritable.get())) {
            TestCase.fail("Boolean field mismatch (expected " + expected + " found " + value + ")");
          }
        }
        break;
      case BYTE:
        {
          Byte value = deserializeRead.currentByte;
          ByteWritable expectedWritable = (ByteWritable) expected;
          if (!value.equals(expectedWritable.get())) {
            TestCase.fail("Byte field mismatch (expected " + (int) expected + " found " + (int) value + ")");
          }
        }
        break;
      case SHORT:
        {
          Short value = deserializeRead.currentShort;
          ShortWritable expectedWritable = (ShortWritable) expected;
          if (!value.equals(expectedWritable.get())) {
            TestCase.fail("Short field mismatch (expected " + expected + " found " + value + ")");
          }
        }
        break;
      case INT:
        {
          Integer value = deserializeRead.currentInt;
          IntWritable expectedWritable = (IntWritable) expected;
          if (!value.equals(expectedWritable.get())) {
            TestCase.fail("Int field mismatch (expected " + expected + " found " + value + ")");
          }
        }
        break;
      case LONG:
        {
          Long value = deserializeRead.currentLong;
          LongWritable expectedWritable = (LongWritable) expected;
          if (!value.equals(expectedWritable.get())) {
            TestCase.fail("Long field mismatch (expected " + expected + " found " + value + ")");
          }
        }
        break;
      case DATE:
        {
          DateWritable value = deserializeRead.currentDateWritable;
          DateWritable expectedWritable = (DateWritable) expected;
          if (!value.equals(expectedWritable)) {
            TestCase.fail("Date field mismatch (expected " + expected.toString() + " found " + value.toString() + ")");
          }
        }
        break;
      case FLOAT:
        {
          Float value = deserializeRead.currentFloat;
          FloatWritable expectedWritable = (FloatWritable) expected;
          if (!value.equals(expectedWritable.get())) {
            TestCase.fail("Float field mismatch (expected " + expected + " found " + value + ")");
          }
        }
        break;
      case DOUBLE:
        {
          Double value = deserializeRead.currentDouble;
          DoubleWritable expectedWritable = (DoubleWritable) expected;
          if (!value.equals(expectedWritable.get())) {
            TestCase.fail("Double field mismatch (expected " + expected + " found " + value + ")");
          }
        }
        break;
      case STRING:
      case CHAR:
      case VARCHAR:
      case BINARY:
        {
          byte[] stringBytes =
              Arrays.copyOfRange(
                  deserializeRead.currentBytes,
                  deserializeRead.currentBytesStart,
                  deserializeRead.currentBytesStart + deserializeRead.currentBytesLength);

          Text text = new Text(stringBytes);
          String string = text.toString();

          switch (primitiveCategory) {
          case STRING:
            {
              Text expectedWritable = (Text) expected;
              if (!string.equals(expectedWritable.toString())) {
                TestCase.fail("String field mismatch (expected '" + expectedWritable.toString() + "' found '" + string + "')");
              }
            }
            break;
          case CHAR:
            {
              HiveChar hiveChar = new HiveChar(string, ((CharTypeInfo) primitiveTypeInfo).getLength());

              HiveCharWritable expectedWritable = (HiveCharWritable) expected;
              if (!hiveChar.equals(expectedWritable.getHiveChar())) {
                TestCase.fail("Char field mismatch (expected '" + expectedWritable.getHiveChar() + "' found '" + hiveChar + "')");
              }
            }
            break;
          case VARCHAR:
            {
              HiveVarchar hiveVarchar = new HiveVarchar(string, ((VarcharTypeInfo) primitiveTypeInfo).getLength());
              HiveVarcharWritable expectedWritable = (HiveVarcharWritable) expected;
              if (!hiveVarchar.equals(expectedWritable.getHiveVarchar())) {
                TestCase.fail("Varchar field mismatch (expected '" + expectedWritable.getHiveVarchar() + "' found '" + hiveVarchar + "')");
              }
            }
            break;
          case BINARY:
            {
               BytesWritable expectedWritable = (BytesWritable) expected;
              if (stringBytes.length != expectedWritable.getLength()){
                TestCase.fail("Byte Array field mismatch (expected " + expected + " found " + stringBytes + ")");
              }
              byte[] expectedBytes = expectedWritable.getBytes();
              for (int b = 0; b < stringBytes.length; b++) {
                if (stringBytes[b] != expectedBytes[b]) {
                  TestCase.fail("Byte Array field mismatch (expected " + expected + " found " + stringBytes + ")");
                }
              }
            }
            break;
          default:
            throw new HiveException("Unexpected primitive category " + primitiveCategory);
          }
        }
        break;
      case DECIMAL:
        {
          HiveDecimal value = deserializeRead.currentHiveDecimalWritable.getHiveDecimal();
          if (value == null) {
            TestCase.fail("Decimal field evaluated to NULL");
          }
          HiveDecimalWritable expectedWritable = (HiveDecimalWritable) expected;
          if (!value.equals(expectedWritable.getHiveDecimal())) {
            DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) primitiveTypeInfo;
            int precision = decimalTypeInfo.getPrecision();
            int scale = decimalTypeInfo.getScale();
            TestCase.fail("Decimal field mismatch (expected " + expectedWritable.getHiveDecimal() + " found " + value.toString() + ") precision " + precision + ", scale " + scale);
          }
        }
        break;
    case TIMESTAMP:
      {
        Timestamp value = deserializeRead.currentTimestampWritable.getTimestamp();
        TimestampWritable expectedWritable = (TimestampWritable) expected;
        if (!value.equals(expectedWritable.getTimestamp())) {
          TestCase.fail("Timestamp field mismatch (expected " + expectedWritable.getTimestamp() + " found " + value.toString() + ")");
        }
      }
      break;
    case INTERVAL_YEAR_MONTH:
      {
        HiveIntervalYearMonth value = deserializeRead.currentHiveIntervalYearMonthWritable.getHiveIntervalYearMonth();
        HiveIntervalYearMonthWritable expectedWritable = (HiveIntervalYearMonthWritable) expected;
        HiveIntervalYearMonth expectedValue = expectedWritable.getHiveIntervalYearMonth();
        if (!value.equals(expectedValue)) {
          TestCase.fail("HiveIntervalYearMonth field mismatch (expected " + expectedValue + " found " + value.toString() + ")");
        }
      }
      break;
    case INTERVAL_DAY_TIME:
      {
        HiveIntervalDayTime value = deserializeRead.currentHiveIntervalDayTimeWritable.getHiveIntervalDayTime();
        HiveIntervalDayTimeWritable expectedWritable = (HiveIntervalDayTimeWritable) expected;
        HiveIntervalDayTime expectedValue = expectedWritable.getHiveIntervalDayTime();
        if (!value.equals(expectedValue)) {
          TestCase.fail("HiveIntervalDayTime field mismatch (expected " + expectedValue + " found " + value.toString() + ")");
        }
      }
      break;

    default:
      throw new HiveException("Unexpected primitive category " + primitiveCategory);
    }
    }
    TestCase.assertTrue(deserializeRead.isEndOfInputReached());
  }

  void serializeBatch(VectorizedRowBatch batch, VectorSerializeRow vectorSerializeRow,
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

    String[] emptyScratchTypeNames = new String[0];

    VectorRandomRowSource source = new VectorRandomRowSource();
    source.init(r);

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
      deserializeRead = new BinarySortableDeserializeRead(source.primitiveTypeInfos(), /* useExternalBuffer */ false);
      serializeWrite = new BinarySortableSerializeWrite(fieldCount);
      break;
    case LAZY_BINARY:
      deserializeRead = new LazyBinaryDeserializeRead(source.primitiveTypeInfos(), /* useExternalBuffer */ false);
      serializeWrite = new LazyBinarySerializeWrite(fieldCount);
      break;
    case LAZY_SIMPLE:
      {
        StructObjectInspector rowObjectInspector = source.rowStructObjectInspector();
        LazySerDeParameters lazySerDeParams = getSerDeParams(rowObjectInspector);
        byte separator = (byte) '\t';
        deserializeRead = new LazySimpleDeserializeRead(source.primitiveTypeInfos(), /* useExternalBuffer */ false,
            separator, lazySerDeParams);
        serializeWrite = new LazySimpleSerializeWrite(fieldCount,
            separator, lazySerDeParams);
      }
      break;
    default:
      throw new Error("Unknown serialization type " + serializationType);
    }
    VectorSerializeRow vectorSerializeRow = new VectorSerializeRow(serializeWrite);
    vectorSerializeRow.init(source.typeNames());

    Object[][] randomRows = source.randomRows(100000);
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

  void examineBatch(VectorizedRowBatch batch, VectorExtractRow vectorExtractRow,
      PrimitiveTypeInfo[] primitiveTypeInfos, Object[][] randomRows, int firstRandomRowIndex ) {

    int rowSize = vectorExtractRow.getCount();
    Object[] row = new Object[rowSize];
    for (int i = 0; i < batch.size; i++) {
      vectorExtractRow.extractRow(batch, i, row);

      Object[] expectedRow = randomRows[firstRandomRowIndex + i];

      for (int c = 0; c < rowSize; c++) {
        Object rowObj = row[c];
        Object expectedObj = expectedRow[c];
        if (rowObj == null) {
          fail("Unexpected NULL from extractRow.  Expected class " +
              expectedObj.getClass().getName() + " value " + expectedObj.toString() +
              " batch index " + i + " firstRandomRowIndex " + firstRandomRowIndex);
        }
        if (!rowObj.equals(expectedObj)) {
          fail("Row " + (firstRandomRowIndex + i) + " and column " + c + " mismatch (" + primitiveTypeInfos[c].getPrimitiveCategory() + " actual value " + rowObj + " and expected value " + expectedObj + ")");
        }
      }
    }
  }

  private Output serializeRow(Object[] row, VectorRandomRowSource source,
      SerializeWrite serializeWrite) throws HiveException, IOException {
    Output output = new Output();
    serializeWrite.set(output);
    PrimitiveTypeInfo[] primitiveTypeInfos = source.primitiveTypeInfos();
    for (int i = 0; i < primitiveTypeInfos.length; i++) {
      Object object = row[i];
      PrimitiveCategory primitiveCategory = primitiveTypeInfos[i].getPrimitiveCategory();
      switch (primitiveCategory) {
      case BOOLEAN:
        {
          BooleanWritable expectedWritable = (BooleanWritable) object;
          boolean value = expectedWritable.get();
          serializeWrite.writeBoolean(value);
        }
        break;
      case BYTE:
        {
          ByteWritable expectedWritable = (ByteWritable) object;
          byte value = expectedWritable.get();
          serializeWrite.writeByte(value);
        }
        break;
      case SHORT:
        {
          ShortWritable expectedWritable = (ShortWritable) object;
          short value = expectedWritable.get();
          serializeWrite.writeShort(value);
        }
        break;
      case INT:
        {
          IntWritable expectedWritable = (IntWritable) object;
          int value = expectedWritable.get();
          serializeWrite.writeInt(value);
        }
        break;
      case LONG:
        {
          LongWritable expectedWritable = (LongWritable) object;
          long value = expectedWritable.get();
          serializeWrite.writeLong(value);
          }
        break;
      case DATE:
        {
          DateWritable expectedWritable = (DateWritable) object;
          Date value = expectedWritable.get();
          serializeWrite.writeDate(value);
        }
        break;
      case FLOAT:
        {
          FloatWritable expectedWritable = (FloatWritable) object;
          float value = expectedWritable.get();
          serializeWrite.writeFloat(value);
        }
        break;
      case DOUBLE:
        {
          DoubleWritable expectedWritable = (DoubleWritable) object;
          double value = expectedWritable.get();
          serializeWrite.writeDouble(value);
        }
        break;
      case STRING:
        {
          Text text = (Text) object;
          serializeWrite.writeString(text.getBytes(), 0, text.getLength());
        }
        break;
      case CHAR:
        {
          HiveCharWritable expectedWritable = (HiveCharWritable) object;
          HiveChar value = expectedWritable.getHiveChar();
          serializeWrite.writeHiveChar(value);
        }
        break;
      case VARCHAR:
        {
          HiveVarcharWritable expectedWritable = (HiveVarcharWritable) object;
          HiveVarchar value = expectedWritable.getHiveVarchar();
          serializeWrite.writeHiveVarchar(value);
        }
        break;
      case BINARY:
        {
          BytesWritable expectedWritable = (BytesWritable) object;
          byte[] bytes = expectedWritable.getBytes();
          int length = expectedWritable.getLength();
          serializeWrite.writeBinary(bytes, 0, length);
        }
        break;
      case TIMESTAMP:
        {
          TimestampWritable expectedWritable = (TimestampWritable) object;
          Timestamp value = expectedWritable.getTimestamp();
          serializeWrite.writeTimestamp(value);
        }
        break;
      case INTERVAL_YEAR_MONTH:
        {
          HiveIntervalYearMonthWritable expectedWritable = (HiveIntervalYearMonthWritable) object;
          HiveIntervalYearMonth value = expectedWritable.getHiveIntervalYearMonth();
          serializeWrite.writeHiveIntervalYearMonth(value);
        }
        break;
      case INTERVAL_DAY_TIME:
        {
          HiveIntervalDayTimeWritable expectedWritable = (HiveIntervalDayTimeWritable) object;
          HiveIntervalDayTime value = expectedWritable.getHiveIntervalDayTime();
          serializeWrite.writeHiveIntervalDayTime(value);
        }
        break;
      case DECIMAL:
        {
          HiveDecimalWritable expectedWritable = (HiveDecimalWritable) object;
          HiveDecimal value = expectedWritable.getHiveDecimal();
          serializeWrite.writeHiveDecimal(value, ((DecimalTypeInfo)primitiveTypeInfos[i]).scale());
        }
        break;
      default:
        throw new HiveException("Unexpected primitive category " + primitiveCategory);
      }
    }
    return output;
  }

  private void addToProperties(Properties tbl, String fieldNames, String fieldTypes) {
    // Set the configuration parameters
    tbl.setProperty(serdeConstants.SERIALIZATION_FORMAT, "9");

    tbl.setProperty("columns", fieldNames);
    tbl.setProperty("columns.types", fieldTypes);

    tbl.setProperty(serdeConstants.SERIALIZATION_NULL_FORMAT, "NULL");
  }

  private LazySerDeParameters getSerDeParams( StructObjectInspector rowObjectInspector) throws SerDeException {
    return getSerDeParams(new Configuration(), new Properties(), rowObjectInspector);
  }

  private LazySerDeParameters getSerDeParams(Configuration conf, Properties tbl, StructObjectInspector rowObjectInspector) throws SerDeException {
    String fieldNames = ObjectInspectorUtils.getFieldNames(rowObjectInspector);
    String fieldTypes = ObjectInspectorUtils.getFieldTypes(rowObjectInspector);
    addToProperties(tbl, fieldNames, fieldTypes);
    return new LazySerDeParameters(conf, tbl, LazySimpleSerDe.class.getName());
  }

  void testVectorDeserializeRow(Random r, SerializationType serializationType,
      boolean alternate1, boolean alternate2,
      boolean useExternalBuffer)
          throws HiveException, IOException, SerDeException {

    String[] emptyScratchTypeNames = new String[0];

    VectorRandomRowSource source = new VectorRandomRowSource();
    source.init(r);

    VectorizedRowBatchCtx batchContext = new VectorizedRowBatchCtx();
    batchContext.init(source.rowStructObjectInspector(), emptyScratchTypeNames);
    VectorizedRowBatch batch = batchContext.createVectorizedRowBatch();

    // junk the destination for the 1st pass
    for (ColumnVector cv : batch.cols) {
      Arrays.fill(cv.isNull, true);
    }

    PrimitiveTypeInfo[] primitiveTypeInfos = source.primitiveTypeInfos();
    int fieldCount = source.typeNames().size();
    DeserializeRead deserializeRead;
    SerializeWrite serializeWrite;
    switch (serializationType) {
    case BINARY_SORTABLE:
      boolean useColumnSortOrderIsDesc = alternate1;
      if (!useColumnSortOrderIsDesc) {
        deserializeRead = new BinarySortableDeserializeRead(source.primitiveTypeInfos(), useExternalBuffer);
        serializeWrite = new BinarySortableSerializeWrite(fieldCount);
      } else {
        boolean[] columnSortOrderIsDesc = new boolean[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
          columnSortOrderIsDesc[i] = r.nextBoolean();
        }
        deserializeRead = new BinarySortableDeserializeRead(source.primitiveTypeInfos(), useExternalBuffer,
            columnSortOrderIsDesc);

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
      }
      boolean useBinarySortableCharsNeedingEscape = alternate2;
      if (useBinarySortableCharsNeedingEscape) {
        source.addBinarySortableAlphabets();
      }
      break;
    case LAZY_BINARY:
      deserializeRead = new LazyBinaryDeserializeRead(source.primitiveTypeInfos(), useExternalBuffer);
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

        LazySerDeParameters lazySerDeParams = getSerDeParams(conf, tbl, rowObjectInspector);

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
        deserializeRead = new LazySimpleDeserializeRead(source.primitiveTypeInfos(), useExternalBuffer,
            separator, lazySerDeParams);
        serializeWrite = new LazySimpleSerializeWrite(fieldCount,
            separator, lazySerDeParams);
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

    Object[][] randomRows = source.randomRows(100000);
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
        examineBatch(batch, vectorExtractRow, primitiveTypeInfos, randomRows, firstRandomRowIndex);
        firstRandomRowIndex = i + 1;
        batch.reset();
      }
    }
    if (batch.size > 0) {
      examineBatch(batch, vectorExtractRow, primitiveTypeInfos, randomRows, firstRandomRowIndex);
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