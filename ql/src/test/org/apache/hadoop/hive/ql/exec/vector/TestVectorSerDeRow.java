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
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
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
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableDateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableDoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableFloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableIntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableLongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
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
              RandomRowObjectSource source, Object[] expectedRow)
              throws HiveException, IOException {
    deserializeRead.set(output.getData(),  0, output.getLength());
    PrimitiveCategory[] primitiveCategories = source.primitiveCategories();
    for (int i = 0; i < primitiveCategories.length; i++) {
      Object expected = expectedRow[i];
      PrimitiveCategory primitiveCategory = primitiveCategories[i];
      PrimitiveTypeInfo primitiveTypeInfo = source.primitiveTypeInfos()[i];
      if (deserializeRead.readCheckNull()) {
        throw new HiveException("Unexpected NULL");
      }
      switch (primitiveCategory) {
      case BOOLEAN:
        {
          Boolean value = deserializeRead.readBoolean();
          BooleanWritable expectedWritable = (BooleanWritable) expected;
          if (!value.equals(expectedWritable.get())) {
            TestCase.fail("Boolean field mismatch (expected " + expected + " found " + value + ")");
          }
        }
        break;
      case BYTE:
        {
          Byte value = deserializeRead.readByte();
          ByteWritable expectedWritable = (ByteWritable) expected;
          if (!value.equals(expectedWritable.get())) {
            TestCase.fail("Byte field mismatch (expected " + (int) expected + " found " + (int) value + ")");
          }
        }
        break;
      case SHORT:
        {
          Short value = deserializeRead.readShort();
          ShortWritable expectedWritable = (ShortWritable) expected;
          if (!value.equals(expectedWritable.get())) {
            TestCase.fail("Short field mismatch (expected " + expected + " found " + value + ")");
          }
        }
        break;
      case INT:
        {
          Integer value = deserializeRead.readInt();
          IntWritable expectedWritable = (IntWritable) expected;
          if (!value.equals(expectedWritable.get())) {
            TestCase.fail("Int field mismatch (expected " + expected + " found " + value + ")");
          }
        }
        break;
      case LONG:
        {
          Long value = deserializeRead.readLong();
          LongWritable expectedWritable = (LongWritable) expected;
          if (!value.equals(expectedWritable.get())) {
            TestCase.fail("Long field mismatch (expected " + expected + " found " + value + ")");
          }
        }
        break;
      case DATE:
        {
          DeserializeRead.ReadDateResults readDateResults = deserializeRead.createReadDateResults();
          deserializeRead.readDate(readDateResults);
          Date value = readDateResults.getDate();
          DateWritable expectedWritable = (DateWritable) expected;
          if (!value.equals(expectedWritable.get())) {
            TestCase.fail("Date field mismatch (expected " + expected.toString() + " found " + value.toString() + ")");
          }
        }
        break;
      case FLOAT:
        {
          Float value = deserializeRead.readFloat();
          FloatWritable expectedWritable = (FloatWritable) expected;
          if (!value.equals(expectedWritable.get())) {
            TestCase.fail("Float field mismatch (expected " + expected + " found " + value + ")");
          }
        }
        break;
      case DOUBLE:
        {
          Double value = deserializeRead.readDouble();
          DoubleWritable expectedWritable = (DoubleWritable) expected;
          if (!value.equals(expectedWritable.get())) {
            TestCase.fail("Double field mismatch (expected " + expected + " found " + value + ")");
          }
        }
        break;
      case STRING:
        {
          DeserializeRead.ReadStringResults readStringResults = deserializeRead.createReadStringResults();
          deserializeRead.readString(readStringResults);

          char[] charsBuffer = new char[readStringResults.bytes.length];
          for (int c = 0; c < charsBuffer.length; c++) {
            charsBuffer[c] = (char) (readStringResults.bytes[c] & 0xFF);
          }

          byte[] stringBytes = Arrays.copyOfRange(readStringResults.bytes, readStringResults.start, readStringResults.start + readStringResults.length);

          char[] charsRange = new char[stringBytes.length];
          for (int c = 0; c < charsRange.length; c++) {
            charsRange[c] = (char) (stringBytes[c] & 0xFF);
          }

          Text text = new Text(stringBytes);
          String value = text.toString();
          Text expectedWritable = (Text) expected;
          if (!value.equals(expectedWritable.toString())) {
            TestCase.fail("String field mismatch (expected '" + expectedWritable.toString() + "' found '" + value + "')");
          }
        }
        break;
      case CHAR:
        {
          DeserializeRead.ReadHiveCharResults readHiveCharResults = deserializeRead.createReadHiveCharResults();
          deserializeRead.readHiveChar(readHiveCharResults);
          HiveChar hiveChar = readHiveCharResults.getHiveChar();
          HiveCharWritable expectedWritable = (HiveCharWritable) expected;
          if (!hiveChar.equals(expectedWritable.getHiveChar())) {
            TestCase.fail("Char field mismatch (expected '" + expectedWritable.getHiveChar() + "' found '" + hiveChar + "')");
          }
        }
        break;
      case VARCHAR:
        {
          DeserializeRead.ReadHiveVarcharResults readHiveVarcharResults = deserializeRead.createReadHiveVarcharResults();
          deserializeRead.readHiveVarchar(readHiveVarcharResults);
          HiveVarchar hiveVarchar = readHiveVarcharResults.getHiveVarchar();
          HiveVarcharWritable expectedWritable = (HiveVarcharWritable) expected;
          if (!hiveVarchar.equals(expectedWritable.getHiveVarchar())) {
            TestCase.fail("Varchar field mismatch (expected '" + expectedWritable.getHiveVarchar() + "' found '" + hiveVarchar + "')");
          }
        }
        break;
      case DECIMAL:
        {
          DeserializeRead.ReadDecimalResults readDecimalResults = deserializeRead.createReadDecimalResults();
          deserializeRead.readHiveDecimal(readDecimalResults);
          HiveDecimal value = readDecimalResults.getHiveDecimal();
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
        DeserializeRead.ReadTimestampResults readTimestampResults = deserializeRead.createReadTimestampResults();
        deserializeRead.readTimestamp(readTimestampResults);
        Timestamp value = readTimestampResults.getTimestamp();
        TimestampWritable expectedWritable = (TimestampWritable) expected;
        if (!value.equals(expectedWritable.getTimestamp())) {
          TestCase.fail("Timestamp field mismatch (expected " + expectedWritable.getTimestamp() + " found " + value.toString() + ")");
        }
      }
      break;
    case INTERVAL_YEAR_MONTH:
      {
        DeserializeRead.ReadIntervalYearMonthResults readIntervalYearMonthResults = deserializeRead.createReadIntervalYearMonthResults();
        deserializeRead.readIntervalYearMonth(readIntervalYearMonthResults);
        HiveIntervalYearMonth value = readIntervalYearMonthResults.getHiveIntervalYearMonth();
        HiveIntervalYearMonthWritable expectedWritable = (HiveIntervalYearMonthWritable) expected;
        HiveIntervalYearMonth expectedValue = expectedWritable.getHiveIntervalYearMonth();
        if (!value.equals(expectedValue)) {
          TestCase.fail("HiveIntervalYearMonth field mismatch (expected " + expectedValue + " found " + value.toString() + ")");
        }
      }
      break;
    case INTERVAL_DAY_TIME:
      {
        DeserializeRead.ReadIntervalDayTimeResults readIntervalDayTimeResults = deserializeRead.createReadIntervalDayTimeResults();
        deserializeRead.readIntervalDayTime(readIntervalDayTimeResults);
        HiveIntervalDayTime value = readIntervalDayTimeResults.getHiveIntervalDayTime();
        HiveIntervalDayTimeWritable expectedWritable = (HiveIntervalDayTimeWritable) expected;
        HiveIntervalDayTime expectedValue = expectedWritable.getHiveIntervalDayTime();
        if (!value.equals(expectedValue)) {
          TestCase.fail("HiveIntervalDayTime field mismatch (expected " + expectedValue + " found " + value.toString() + ")");
        }
      }
      break;
    case BINARY:
      {
        DeserializeRead.ReadBinaryResults readBinaryResults = deserializeRead.createReadBinaryResults();
        deserializeRead.readBinary(readBinaryResults);
        byte[] byteArray = Arrays.copyOfRange(readBinaryResults.bytes, readBinaryResults.start, readBinaryResults.start + readBinaryResults.length);
        BytesWritable expectedWritable = (BytesWritable) expected;
        if (byteArray.length != expectedWritable.getLength()){
          TestCase.fail("Byte Array field mismatch (expected " + expected + " found " + byteArray + ")");
        }
        byte[] expectedBytes = expectedWritable.getBytes();
        for (int b = 0; b < byteArray.length; b++) {
          if (byteArray[b] != expectedBytes[b]) {
            TestCase.fail("Byte Array field mismatch (expected " + expected + " found " + byteArray + ")");
          }
        }
      }
      break;
      default:
        throw new HiveException("Unexpected primitive category " + primitiveCategory);
      }
    }
    deserializeRead.extraFieldsCheck();
    TestCase.assertTrue(!deserializeRead.readBeyondConfiguredFieldsWarned());
    TestCase.assertTrue(!deserializeRead.readBeyondBufferRangeWarned());
    TestCase.assertTrue(!deserializeRead.bufferRangeHasExtraDataWarned());
  }

  void serializeBatch(VectorizedRowBatch batch, VectorSerializeRow vectorSerializeRow,
           DeserializeRead deserializeRead, RandomRowObjectSource source, Object[][] randomRows,
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

  void testVectorSerializeRow(int caseNum, Random r, SerializationType serializationType) throws HiveException, IOException, SerDeException {

    Map<Integer, String> emptyScratchMap = new HashMap<Integer, String>();

    RandomRowObjectSource source = new RandomRowObjectSource();
    source.init(r);

    VectorizedRowBatchCtx batchContext = new VectorizedRowBatchCtx();
    batchContext.init(emptyScratchMap, source.rowStructObjectInspector());
    VectorizedRowBatch batch = batchContext.createVectorizedRowBatch();

    VectorAssignRowSameBatch vectorAssignRow = new VectorAssignRowSameBatch();
    vectorAssignRow.init(source.typeNames());
    vectorAssignRow.setOneBatch(batch);

    int fieldCount = source.typeNames().size();
    DeserializeRead deserializeRead;
    SerializeWrite serializeWrite;
    switch (serializationType) {
    case BINARY_SORTABLE:
      deserializeRead = new BinarySortableDeserializeRead(source.primitiveTypeInfos());
      serializeWrite = new BinarySortableSerializeWrite(fieldCount);
      break;
    case LAZY_BINARY:
      deserializeRead = new LazyBinaryDeserializeRead(source.primitiveTypeInfos());
      serializeWrite = new LazyBinarySerializeWrite(fieldCount);
      break;
    case LAZY_SIMPLE:
      {
        StructObjectInspector rowObjectInspector = source.rowStructObjectInspector();
        LazySerDeParameters lazySerDeParams = getSerDeParams(rowObjectInspector);
        byte separator = (byte) '\t';
        deserializeRead = new LazySimpleDeserializeRead(source.primitiveTypeInfos(),
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

      vectorAssignRow.assignRow(batch.size, row);
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

  void examineBatch(VectorizedRowBatch batch, VectorExtractRowSameBatch vectorExtractRow,
      Object[][] randomRows, int firstRandomRowIndex ) {

    int rowSize = vectorExtractRow.getCount();
    Object[] row = new Object[rowSize];
    for (int i = 0; i < batch.size; i++) {
      vectorExtractRow.extractRow(i, row);

      Object[] expectedRow = randomRows[firstRandomRowIndex + i];

      for (int c = 0; c < rowSize; c++) {
        if (row[c] == null) {
          fail("Unexpected NULL from extractRow");
        }
        if (!row[c].equals(expectedRow[c])) {
          fail("Row " + (firstRandomRowIndex + i) + " and column " + c + " mismatch");
        }
      }
    }
  }

  private Output serializeRow(Object[] row, RandomRowObjectSource source, SerializeWrite serializeWrite) throws HiveException, IOException {
    Output output = new Output();
    serializeWrite.set(output);
    PrimitiveCategory[] primitiveCategories = source.primitiveCategories();
    for (int i = 0; i < primitiveCategories.length; i++) {
      Object object = row[i];
      PrimitiveCategory primitiveCategory = primitiveCategories[i];
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
          serializeWrite.writeHiveDecimal(value);
        }
        break;
      default:
        throw new HiveException("Unexpected primitive category " + primitiveCategory);
      }
    }
    return output;
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

  private LazySerDeParameters getSerDeParams(StructObjectInspector rowObjectInspector) throws SerDeException {
    String fieldNames = ObjectInspectorUtils.getFieldNames(rowObjectInspector);
    String fieldTypes = ObjectInspectorUtils.getFieldTypes(rowObjectInspector);
    Configuration conf = new Configuration();
    Properties tbl = createProperties(fieldNames, fieldTypes);
    return new LazySerDeParameters(conf, tbl, LazySimpleSerDe.class.getName());
  }

  void testVectorDeserializeRow(int caseNum, Random r, SerializationType serializationType) throws HiveException, IOException, SerDeException {

    Map<Integer, String> emptyScratchMap = new HashMap<Integer, String>();

    RandomRowObjectSource source = new RandomRowObjectSource();
    source.init(r);

    VectorizedRowBatchCtx batchContext = new VectorizedRowBatchCtx();
    batchContext.init(emptyScratchMap, source.rowStructObjectInspector());
    VectorizedRowBatch batch = batchContext.createVectorizedRowBatch();

    int fieldCount = source.typeNames().size();
    DeserializeRead deserializeRead;
    SerializeWrite serializeWrite;
    switch (serializationType) {
    case BINARY_SORTABLE:
      deserializeRead = new BinarySortableDeserializeRead(source.primitiveTypeInfos());
      serializeWrite = new BinarySortableSerializeWrite(fieldCount);
      break;
    case LAZY_BINARY:
      deserializeRead = new LazyBinaryDeserializeRead(source.primitiveTypeInfos());
      serializeWrite = new LazyBinarySerializeWrite(fieldCount);
      break;
    case LAZY_SIMPLE:
      {
        StructObjectInspector rowObjectInspector = source.rowStructObjectInspector();
        LazySerDeParameters lazySerDeParams = getSerDeParams(rowObjectInspector);
        byte separator = (byte) '\t';
        deserializeRead = new LazySimpleDeserializeRead(source.primitiveTypeInfos(),
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

    VectorExtractRowSameBatch vectorExtractRow = new VectorExtractRowSameBatch();
    vectorExtractRow.init(source.typeNames());
    vectorExtractRow.setOneBatch(batch);

    Object[][] randomRows = source.randomRows(100000);
    int firstRandomRowIndex = 0;
    for (int i = 0; i < randomRows.length; i++) {
      Object[] row = randomRows[i];

      Output output = serializeRow(row, source, serializeWrite);
      vectorDeserializeRow.setBytes(output.getData(), 0, output.getLength());
      vectorDeserializeRow.deserializeByValue(batch, batch.size);
      batch.size++;
      if (batch.size == batch.DEFAULT_SIZE) {
        examineBatch(batch, vectorExtractRow, randomRows, firstRandomRowIndex);
        firstRandomRowIndex = i + 1;
        batch.reset();
      }
    }
    if (batch.size > 0) {
      examineBatch(batch, vectorExtractRow, randomRows, firstRandomRowIndex);
    }
  }

  public void testVectorSerDeRow() throws Throwable {

  try {
    Random r = new Random(5678);
    for (int c = 0; c < 10; c++) {
      testVectorSerializeRow(c, r, SerializationType.BINARY_SORTABLE);
    }
    for (int c = 0; c < 10; c++) {
      testVectorSerializeRow(c, r, SerializationType.LAZY_BINARY);
    }
    for (int c = 0; c < 10; c++) {
      testVectorSerializeRow(c, r, SerializationType.LAZY_SIMPLE);
    }

    for (int c = 0; c < 10; c++) {
      testVectorDeserializeRow(c, r, SerializationType.BINARY_SORTABLE);
    }
    for (int c = 0; c < 10; c++) {
      testVectorDeserializeRow(c, r, SerializationType.LAZY_BINARY);
    }
    for (int c = 0; c < 10; c++) {
      testVectorDeserializeRow(c, r, SerializationType.LAZY_SIMPLE);
    }


  } catch (Throwable e) {
    e.printStackTrace();
    throw e;
  }
  }
}