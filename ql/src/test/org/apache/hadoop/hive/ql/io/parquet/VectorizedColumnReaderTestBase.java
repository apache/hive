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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.ql.io.parquet;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.CalendarUtils;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.common.type.DataTypePhysicalVariation;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DateColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.Decimal64ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.parquet.read.DataWritableReadSupport;
import org.apache.hadoop.hive.ql.io.parquet.serde.ArrayWritableObjectInspector;
import org.apache.hadoop.hive.ql.io.parquet.timestamp.NanoTime;
import org.apache.hadoop.hive.ql.io.parquet.timestamp.NanoTimeUtils;
import org.apache.hadoop.hive.ql.io.parquet.vector.VectorizedParquetRecordReader;
import org.apache.hadoop.hive.ql.io.parquet.vector.VectorizedPrimitiveColumnReader;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.BaseCharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TimeZone;

import static junit.framework.Assert.assertTrue;
import static junit.framework.TestCase.assertFalse;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0;
import static org.apache.parquet.hadoop.api.ReadSupport.PARQUET_READ_SCHEMA;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.GZIP;
import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class VectorizedColumnReaderTestBase {

  protected final static int nElements = 2500;
  protected final static int UNIQUE_NUM = 10;
  protected final static int NULL_FREQUENCY = 13;

  protected final static Configuration conf = new Configuration();
  protected final static Path file = new Path("target/test/TestParquetVectorReader/testParquetFile");

  protected static final MessageType schema = parseMessageType(
      "message hive_schema { "
          + "required int32 int32_field; "
          + "required int64 int64_field; "
          + "required int96 int96_field; "
          + "required double double_field; "
          + "required float float_field; "
          + "required boolean boolean_field; "
          + "required fixed_len_byte_array(3) flba_field; "
          + "optional fixed_len_byte_array(1) some_null_field; "
          + "optional fixed_len_byte_array(1) all_null_field; "
          + "required binary binary_field; "
          + "optional binary binary_field_some_null; "
          + "required binary value (DECIMAL(5,2)); "
          + "required group struct_field {"
          + "  required int32 a;\n"
          + "  required double b;\n"
          + "}\n"
          + "optional group nested_struct_field {"
          + "  optional group nsf {"
          + "    optional int32 c;\n"
          + "    optional int32 d;\n"
          + "  }\n"
          + "  optional double e;\n"
          + "}\n"
          + "optional group struct_field_some_null {"
          + "  optional int32 f;\n"
          + "  optional double g;\n"
          + "}\n"
          + "optional group map_field (MAP) {\n"
          + "  repeated group map (MAP_KEY_VALUE) {\n"
          + "    required binary key;\n"
          + "    optional binary value;\n"
          + "  }\n"
          + "}\n"
          + "optional group array_list (LIST) {\n"
          + "  repeated group bag {\n"
          + "    optional int32 array_element;\n"
          + "  }\n"
          + "}\n"
          + "repeated int32 list_int32_field;"
          + "repeated int64 list_int64_field;"
          + "repeated double list_double_field;"
          + "repeated float list_float_field;"
          + "repeated boolean list_boolean_field;"
          + "repeated fixed_len_byte_array(3) list_byte_array_field;"
          + "repeated binary list_binary_field;"
          + "repeated binary list_decimal_field (DECIMAL(5,2));"
          + "repeated binary list_binary_field_for_repeat_test;"
          + "repeated int32 list_int32_field_for_repeat_test;"
          + "repeated group map_int32 (MAP_KEY_VALUE) {\n"
          + "  required int32 key;\n"
          + "  optional int32 value;\n"
          + "}\n"
          + "repeated group map_int64 (MAP_KEY_VALUE) {\n"
          + "  required int64 key;\n"
          + "  optional int64 value;\n"
          + "}\n"
          + "repeated group map_double (MAP_KEY_VALUE) {\n"
          + "  required double key;\n"
          + "  optional double value;\n"
          + "}\n"
          + "repeated group map_float (MAP_KEY_VALUE) {\n"
          + "  required float key;\n"
          + "  optional float value;\n"
          + "}\n"
          + "repeated group map_binary (MAP_KEY_VALUE) {\n"
          + "  required binary key;\n"
          + "  optional binary value;\n"
          + "}\n"
          + "repeated group map_decimal (MAP_KEY_VALUE) {\n"
          + "  required binary key (DECIMAL(5,2));\n"
          + "  optional binary value (DECIMAL(5,2));\n"
          + "}\n"
          + "repeated group map_int32_for_repeat_test (MAP_KEY_VALUE) {\n"
          + "  required int32 key;\n"
          + "  optional int32 value;\n"
          + "}\n"
          + "} ");

  protected static void removeFile() throws IOException {
    FileSystem fs = file.getFileSystem(conf);
    if (fs.exists(file)) {
      fs.delete(file, true);
    }
  }

  protected static ParquetWriter<Group> initWriterFromFile() throws IOException {
    GroupWriteSupport.setSchema(schema, conf);
    return new ParquetWriter<>(
        file,
        new GroupWriteSupport(),
        GZIP, 1024 * 1024, 1024, 1024 * 1024,
        true, false, PARQUET_1_0, conf);
  }

  protected static int getIntValue(
      boolean isDictionaryEncoding,
      int index) {
    return isDictionaryEncoding ? index % UNIQUE_NUM : index;
  }

  protected static double getDoubleValue(
      boolean isDictionaryEncoding,
      int index) {
    return isDictionaryEncoding ? index % UNIQUE_NUM : index;
  }

  protected static long getLongValue(
      boolean isDictionaryEncoding,
      int index) {
    return isDictionaryEncoding ? (long) 2 * index % UNIQUE_NUM : (long) 2 * index;
  }

  protected static float getFloatValue(
      boolean isDictionaryEncoding,
      int index) {
    return (float) (isDictionaryEncoding ? index % UNIQUE_NUM * 2.0 : index * 2.0);
  }

  protected static boolean getBooleanValue(
      float index) {
    return (index % 2 == 0);
  }

  protected static NanoTime getNanoTime(int index) {
    Timestamp ts = new Timestamp();
    ts.setTimeInMillis(index);
    return NanoTimeUtils.getNanoTime(ts, TimeZone.getDefault().toZoneId(), false);
  }

  protected static HiveDecimal getDecimal(
      boolean isDictionaryEncoding,
      int index) {
    int decimalVal = index % 100;
    String decimalStr = (decimalVal < 10) ? "0" + String.valueOf(decimalVal) : String
        .valueOf(decimalVal);
    int intVal = (isDictionaryEncoding) ? index % UNIQUE_NUM : index / 100;
    String d = String.valueOf(intVal) + "." + decimalStr;
    return HiveDecimal.create(d);
  }

  protected static Binary getTimestamp(
      boolean isDictionaryEncoding,
      int index) {
    NanoTime s = isDictionaryEncoding ? getNanoTime(index % UNIQUE_NUM) : getNanoTime(index);
    return s.toBinary();
  }

  protected static String getStr(
      boolean isDictionaryEncoding,
      int index) {
    int binaryLen = isDictionaryEncoding ? index % UNIQUE_NUM : index;
    String v = "";
    while (binaryLen > 0) {
      char t = (char) ('a' + binaryLen % 26);
      binaryLen /= 26;
      v = t + v;
    }
    return v;
  }

  protected static Binary getBinaryValue(
      boolean isDictionaryEncoding,
      int index) {
    return Binary.fromString(getStr(isDictionaryEncoding, index));
  }

  protected static boolean isNull(int index) {
    return (index % NULL_FREQUENCY == 0);
  }

  public static VectorizedParquetRecordReader createTestParquetReader(String schemaString, Configuration conf)
      throws IOException, InterruptedException, HiveException {
    return createTestParquetReader(schemaString, conf, null);
  }

  public static VectorizedParquetRecordReader createTestParquetReader(String schemaString, Configuration conf,
      DataTypePhysicalVariation[] rowDataTypePhysicalVariations)
      throws IOException, InterruptedException, HiveException {
    return createTestParquetReader(schemaString, conf, rowDataTypePhysicalVariations, file);
  }

  public static VectorizedParquetRecordReader createTestParquetReader(String schemaString, Configuration conf,
      DataTypePhysicalVariation[] rowDataTypePhysicalVariations, Path inputFile)
      throws IOException, InterruptedException, HiveException {
    conf.set(PARQUET_READ_SCHEMA, schemaString);
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, true);
    HiveConf.setVar(conf, HiveConf.ConfVars.PLAN, "//tmp");
    Job vectorJob = new Job(conf, "read vector");
    ParquetInputFormat.setInputPaths(vectorJob, inputFile);
    initialVectorizedRowBatchCtx(conf, rowDataTypePhysicalVariations);
    return new VectorizedParquetRecordReader(getFileSplit(vectorJob, inputFile), new JobConf(conf));
  }

  protected static FileSplit getFileSplit(Job vectorJob) throws IOException, InterruptedException {
    return getFileSplit(vectorJob, file);
  }

  protected static FileSplit getFileSplit(Job vectorJob, Path inputFile)
      throws IOException, InterruptedException {
    ParquetInputFormat parquetInputFormat = new ParquetInputFormat(GroupReadSupport.class);
    InputSplit split = (InputSplit) parquetInputFormat.getSplits(vectorJob).get(0);
    FileSplit fsplit = new FileSplit(inputFile, 0L, split.getLength(), split.getLocations());
    return fsplit;
  }

  protected static void writeData(ParquetWriter<Group> writer, boolean isDictionaryEncoding) throws IOException {
    SimpleGroupFactory f = new SimpleGroupFactory(schema);
    for (int i = 0; i < nElements; i++) {
      boolean isNull = isNull(i);
      int intVal = getIntValue(isDictionaryEncoding, i);
      long longVal = getLongValue(isDictionaryEncoding, i);
      Binary timeStamp = getTimestamp(isDictionaryEncoding, i);
      HiveDecimal decimalVal = getDecimal(isDictionaryEncoding, i).setScale(2);
      double doubleVal = getDoubleValue(isDictionaryEncoding, i);
      float floatVal = getFloatValue(isDictionaryEncoding, i);
      boolean booleanVal = getBooleanValue(i);
      Binary binary = getBinaryValue(isDictionaryEncoding, i);
      Group group = f.newGroup()
          .append("int32_field", intVal)
          .append("int64_field", longVal)
          .append("int96_field", timeStamp)
          .append("double_field", doubleVal)
          .append("float_field", floatVal)
          .append("boolean_field", booleanVal)
          .append("flba_field", "abc");

      if (!isNull) {
        group.append("some_null_field", "x");
      }

      group.append("binary_field", binary);

      if (!isNull) {
        group.append("binary_field_some_null", binary);
      }

      HiveDecimalWritable w = new HiveDecimalWritable(decimalVal);
      group.append("value", Binary.fromConstantByteArray(w.getInternalStorage()));

      group.addGroup("struct_field")
          .append("a", intVal)
          .append("b", doubleVal);

      Group g = group.addGroup("nested_struct_field");

      g.addGroup("nsf").append("c", intVal).append("d", intVal);
      g.append("e", doubleVal);

      if (i % 2 != 0 || i % 3 != 0) {
        Group structFieldWithNulls = group.addGroup("struct_field_some_null");
        if (i % 2 != 0) {
          structFieldWithNulls.append("f", intVal);
        }
        if (i % 3 != 0) {
          structFieldWithNulls.append("g", doubleVal);
        }
      }
      Group mapGroup = group.addGroup("map_field");
      if (i % 13 != 1) {
        mapGroup.addGroup("map").append("key", binary).append("value", "abc");
      } else {
        mapGroup.addGroup("map").append("key", binary);
      }

      Group arrayGroup = group.addGroup("array_list");
      for (int j = 0; j < i % 4; j++) {
        arrayGroup.addGroup("bag").append("array_element", intVal);
      }

      writer.write(group);
    }
    writer.close();
  }

  protected static void initialVectorizedRowBatchCtx(Configuration conf) throws HiveException {
    initialVectorizedRowBatchCtx(conf, null);
  }

  protected static void initialVectorizedRowBatchCtx(Configuration conf,
      DataTypePhysicalVariation[] rowDataTypePhysicalVariations) throws HiveException {
    MapWork mapWork = new MapWork();
    VectorizedRowBatchCtx rbCtx = new VectorizedRowBatchCtx();
    rbCtx.init(createStructObjectInspector(conf), new String[0]);
    if (rowDataTypePhysicalVariations != null) {
      rbCtx.setRowDataTypePhysicalVariations(rowDataTypePhysicalVariations);
    }
    mapWork.setVectorMode(true);
    mapWork.setVectorizedRowBatchCtx(rbCtx);
    Utilities.setMapWork(conf, mapWork);
  }

  /**
   * Verifies the Decimal64 read path: when the decimal column is tagged DECIMAL_64 (as the
   * vectorizer does once {@code MapredParquetInputFormat} advertises it), the reader must fill a
   * {@link Decimal64ColumnVector} (long-backed) with the correct unscaled values.
   */
  protected void decimal64Read(boolean isDictionaryEncoding) throws Exception {
    Configuration readerConf = new Configuration();
    readerConf.set(IOConstants.COLUMNS, "value");
    readerConf.set(IOConstants.COLUMNS_TYPES, "decimal(5,2)");
    readerConf.setBoolean(ColumnProjectionUtils.READ_ALL_COLUMNS, false);
    readerConf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, "0");
    VectorizedParquetRecordReader reader = createTestParquetReader(
        "message hive_schema { required value (DECIMAL(5,2));}", readerConf,
        new DataTypePhysicalVariation[] { DataTypePhysicalVariation.DECIMAL_64 });
    VectorizedRowBatch previous = reader.createValue();
    try {
      int c = 0;
      while (reader.next(NullWritable.get(), previous)) {
        assertTrue("expected Decimal64ColumnVector but got " + previous.cols[0].getClass().getSimpleName(),
            previous.cols[0] instanceof Decimal64ColumnVector);
        Decimal64ColumnVector vector = (Decimal64ColumnVector) previous.cols[0];
        assertTrue(vector.noNulls);
        assertEquals((short) 5, vector.precision);
        assertEquals((short) 2, vector.scale);
        for (int i = 0; i < vector.vector.length; i++) {
          if (c == nElements) {
            break;
          }
          long expected =
              new HiveDecimalWritable(getDecimal(isDictionaryEncoding, c).setScale(2)).serialize64(2);
          assertEquals("Check failed at pos " + c, expected, vector.vector[i]);
          assertFalse(vector.isNull[i]);
          c++;
        }
      }
      assertEquals(nElements, c);
    } finally {
      reader.close();
    }
  }

  // Unscaled values (scale=2) used by the INT32/INT64-backed Decimal64 tests. The element at
  // DECIMAL64_NULL_INDEX is ignored because those rows are written as null. 1001 -> 10.01,
  // 1234 -> 12.34, -550 -> -5.50, 9999 -> 99.99. The Decimal64 vector must hold the full unscaled
  // long; at scale 2 that equals the literal value, which is what the tests assert.
  private static final long[] DECIMAL64_UNSCALED = { 1001L, 1234L, -550L, 0L, 9999L };
  private static final int DECIMAL64_NULL_INDEX = 3;
  private static final int DECIMAL64_ROWS = 200;
  // Dedicated file so the per-test write never clobbers the class-level {@link #file} fixture that
  // other @Test methods read via the @BeforeClass writeData(); JUnit @Test ordering is undefined.
  private static final Path DECIMAL64_FILE =
      new Path(System.getProperty("java.io.tmpdir"), "testDecimal64ParquetFile");

  /**
   * Writes a parquet file with a single DECIMAL(5,2) column physically stored as the given
   * primitive ({@link PrimitiveTypeName#INT32} or {@link PrimitiveTypeName#INT64}), cycling
   * through the unscaled values in {@link #DECIMAL64_UNSCALED} (index {@link #DECIMAL64_NULL_INDEX}
   * is written as null), then reads it back tagged DECIMAL_64 and asserts the long-backed
   * {@link Decimal64ColumnVector} holds the correct unscaled longs (NOT the truncated/zero values
   * the buggy reader produced).
   * <p>
   * Runs with {@code dictionaryEncoding} both true and false so that BOTH reader paths are
   * exercised: with dictionary encoding on (few distinct values, huge dictionary page) Parquet
   * dictionary-encodes the column and the read goes through {@code decodeDictionaryIds}; with it off
   * every value is plain-encoded and the read goes through {@code readDecimal64}. Both populate the
   * {@link Decimal64ColumnVector} via {@link Decimal64ColumnVector#set(int, byte[], int)}.
   */
  protected void decimal64ReadFromPrimitive(PrimitiveTypeName physical, boolean dictionaryEncoding)
      throws Exception {
    final int scale = 2;
    final int precision = 5;
    MessageType writeSchema = Types.buildMessage()
        .optional(physical).as(LogicalTypeAnnotation.decimalType(scale, precision)).named("value")
        .named("hive_schema");

    FileSystem fs = DECIMAL64_FILE.getFileSystem(conf);
    if (fs.exists(DECIMAL64_FILE)) {
      fs.delete(DECIMAL64_FILE, true);
    }
    GroupWriteSupport.setSchema(writeSchema, conf);
    try (ParquetWriter<Group> writer = new ParquetWriter<>(
        DECIMAL64_FILE, new GroupWriteSupport(), GZIP, 1024 * 1024, 1024, 1024 * 1024,
        dictionaryEncoding, false, PARQUET_1_0, conf)) {
      SimpleGroupFactory f = new SimpleGroupFactory(writeSchema);
      for (int i = 0; i < DECIMAL64_ROWS; i++) {
        int idx = i % DECIMAL64_UNSCALED.length;
        Group group = f.newGroup();
        if (idx != DECIMAL64_NULL_INDEX) {
          if (physical == PrimitiveTypeName.INT32) {
            group.append("value", (int) DECIMAL64_UNSCALED[idx]);
          } else {
            group.append("value", DECIMAL64_UNSCALED[idx]);
          }
        }
        writer.write(group);
      }
    }

    Configuration readerConf = new Configuration();
    readerConf.set(IOConstants.COLUMNS, "value");
    readerConf.set(IOConstants.COLUMNS_TYPES, "decimal(" + precision + "," + scale + ")");
    readerConf.setBoolean(ColumnProjectionUtils.READ_ALL_COLUMNS, false);
    readerConf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, "0");
    VectorizedParquetRecordReader reader = createTestParquetReader(
        writeSchema.toString(), readerConf,
        new DataTypePhysicalVariation[] { DataTypePhysicalVariation.DECIMAL_64 }, DECIMAL64_FILE);
    VectorizedRowBatch previous = reader.createValue();
    String label = physical + (dictionaryEncoding ? " (dict)" : " (plain)");
    try {
      int c = 0;
      boolean sawNull = false;
      while (reader.next(NullWritable.get(), previous)) {
        assertTrue("expected Decimal64ColumnVector but got " + previous.cols[0].getClass().getSimpleName(),
            previous.cols[0] instanceof Decimal64ColumnVector);
        Decimal64ColumnVector vector = (Decimal64ColumnVector) previous.cols[0];
        assertEquals((short) precision, vector.precision);
        assertEquals((short) scale, vector.scale);
        for (int i = 0; i < previous.size; i++) {
          if (c == DECIMAL64_ROWS) {
            break;
          }
          int idx = c % DECIMAL64_UNSCALED.length;
          if (idx == DECIMAL64_NULL_INDEX) {
            assertTrue("Expected null at pos " + c + " for " + label, vector.isNull[i]);
            sawNull = true;
          } else {
            assertFalse("Unexpected null at pos " + c + " for " + label, vector.isNull[i]);
            // A Decimal64 vector must hold the FULL unscaled long: 10.01 -> 1001. For scale 2 the
            // expected unscaled long is exactly DECIMAL64_UNSCALED[idx].
            assertEquals("Decimal64 must keep the full unscaled value at pos " + c + " for " + label,
                DECIMAL64_UNSCALED[idx], vector.vector[i]);
          }
          c++;
        }
      }
      assertEquals("Did not read all rows for " + label, DECIMAL64_ROWS, c);
      assertTrue("Null row was never exercised for " + label, sawNull);
    } finally {
      reader.close();
      if (fs.exists(DECIMAL64_FILE)) {
        fs.delete(DECIMAL64_FILE, true);
      }
    }
  }

  protected void decimal64ReadInt32() throws Exception {
    decimal64ReadFromPrimitive(PrimitiveTypeName.INT32, true);
    decimal64ReadFromPrimitive(PrimitiveTypeName.INT32, false);
  }

  protected void decimal64ReadInt64() throws Exception {
    decimal64ReadFromPrimitive(PrimitiveTypeName.INT64, true);
    decimal64ReadFromPrimitive(PrimitiveTypeName.INT64, false);
  }

  /**
   * Gate test for the Decimal64 identity fast path. When the Parquet file scale differs from the Hive
   * table scale (schema evolution), the fast path must NOT engage: each value must be rescaled
   * (rounded, or NULLed when it no longer fits) exactly as HiveDecimal would -- never copied verbatim,
   * which would be off by a power of ten. Writes DECIMAL(9,4), reads as DECIMAL(6,2), and asserts every
   * result against a HiveDecimal oracle. (The {@code decimal64Read*} tests cover the file==table-scale
   * identity fast path; this covers the mismatched-scale fallback and proves the gate.)
   */
  protected void decimal64ReadScaleEvolution(PrimitiveTypeName physical, boolean dictionaryEncoding)
      throws Exception {
    final int fileScale = 4;
    final int filePrecision = 9;
    final int readScale = 2;
    final int readPrecision = 6;
    // Unscaled values at fileScale=4 (index DECIMAL64_NULL_INDEX is written as null):
    //  1234567 -> 123.4567 (rounds to 123.46), 100 -> 0.0100, -98765 -> -9.8765 (-> -9.88),
    //  99999999 -> 9999.9999 (rounds to 10000.00 -> exceeds DECIMAL(6,2) -> NULL).
    long[] unscaled = { 1234567L, 100L, -98765L, 0L, 99999999L };

    FileSystem fs = DECIMAL64_FILE.getFileSystem(conf);
    if (fs.exists(DECIMAL64_FILE)) {
      fs.delete(DECIMAL64_FILE, true);
    }
    MessageType writeSchema = Types.buildMessage()
        .optional(physical).as(LogicalTypeAnnotation.decimalType(fileScale, filePrecision)).named("value")
        .named("hive_schema");
    GroupWriteSupport.setSchema(writeSchema, conf);
    try (ParquetWriter<Group> writer = new ParquetWriter<>(
        DECIMAL64_FILE, new GroupWriteSupport(), GZIP, 1024 * 1024, 1024, 1024 * 1024,
        dictionaryEncoding, false, PARQUET_1_0, conf)) {
      SimpleGroupFactory f = new SimpleGroupFactory(writeSchema);
      for (int i = 0; i < DECIMAL64_ROWS; i++) {
        int idx = i % unscaled.length;
        Group group = f.newGroup();
        if (idx != DECIMAL64_NULL_INDEX) {
          if (physical == PrimitiveTypeName.INT32) {
            group.append("value", (int) unscaled[idx]);
          } else {
            group.append("value", unscaled[idx]);
          }
        }
        writer.write(group);
      }
    }

    Configuration readerConf = new Configuration();
    readerConf.set(IOConstants.COLUMNS, "value");
    readerConf.set(IOConstants.COLUMNS_TYPES, "decimal(" + readPrecision + "," + readScale + ")");
    readerConf.setBoolean(ColumnProjectionUtils.READ_ALL_COLUMNS, false);
    readerConf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, "0");
    VectorizedParquetRecordReader reader = createTestParquetReader(
        writeSchema.toString(), readerConf,
        new DataTypePhysicalVariation[] { DataTypePhysicalVariation.DECIMAL_64 }, DECIMAL64_FILE);
    VectorizedRowBatch previous = reader.createValue();
    String label = physical + (dictionaryEncoding ? " (dict)" : " (plain)") + " scale-evolution";
    HiveDecimalWritable oracle = new HiveDecimalWritable();
    try {
      int c = 0;
      while (reader.next(NullWritable.get(), previous)) {
        Decimal64ColumnVector vector = (Decimal64ColumnVector) previous.cols[0];
        // The long is stored at the Hive (table) scale, NOT the file scale -- proves the gate.
        assertEquals((short) readScale, vector.scale);
        for (int i = 0; i < previous.size; i++) {
          if (c == DECIMAL64_ROWS) {
            break;
          }
          int idx = c % unscaled.length;
          if (idx == DECIMAL64_NULL_INDEX) {
            assertTrue("Expected null at pos " + c + " for " + label, vector.isNull[i]);
          } else {
            // Oracle: interpret the unscaled value at the file scale, enforce to the read type.
            oracle.set(HiveDecimal.create(BigInteger.valueOf(unscaled[idx]), fileScale));
            oracle.mutateEnforcePrecisionScale(readPrecision, readScale);
            if (!oracle.isSet()) {
              assertTrue("Expected NULL (out of range) at pos " + c + " for " + label, vector.isNull[i]);
            } else {
              assertFalse("Unexpected null at pos " + c + " for " + label, vector.isNull[i]);
              assertEquals("Scale-evolved Decimal64 must match HiveDecimal at pos " + c + " for " + label,
                  oracle.serialize64(readScale), vector.vector[i]);
            }
          }
          c++;
        }
      }
      assertEquals("Did not read all rows for " + label, DECIMAL64_ROWS, c);
    } finally {
      reader.close();
      if (fs.exists(DECIMAL64_FILE)) {
        fs.delete(DECIMAL64_FILE, true);
      }
    }
  }

  protected void decimal64ReadScaleEvolution() throws Exception {
    decimal64ReadScaleEvolution(PrimitiveTypeName.INT32, true);
    decimal64ReadScaleEvolution(PrimitiveTypeName.INT32, false);
    decimal64ReadScaleEvolution(PrimitiveTypeName.INT64, true);
    decimal64ReadScaleEvolution(PrimitiveTypeName.INT64, false);
  }

  /**
   * Bounds test for the Decimal64 identity fast path: file scale == table scale (so the fast path
   * engages) but the file precision is wider than the table precision. Values outside the table
   * precision -- and the pathological {@link Long#MIN_VALUE} -- must be NULLed by the fast path's
   * bounds check, exactly as the enforced path would. Writes DECIMAL(filePrecision,2), reads DECIMAL(5,2).
   */
  protected void decimal64ReadPrecisionNarrowing(PrimitiveTypeName physical, boolean dictionaryEncoding)
      throws Exception {
    final int scale = 2;
    final int filePrecision = (physical == PrimitiveTypeName.INT32) ? 9 : 18;
    final int readPrecision = 5;                 // max abs unscaled value = 99999
    final long absMax = 99999L;
    // Unscaled @ scale 2: 1001 -> 10.01 (fits), 99999 -> 999.99 (boundary, fits),
    // 100000 -> 1000.00 (precision 6 > 5 -> NULL), then an out-of-range negative -> NULL.
    long[] unscaled = (physical == PrimitiveTypeName.INT32)
        ? new long[] { 1001L, 99999L, 100000L, -100000L }
        : new long[] { 1001L, 99999L, 100000L, Long.MIN_VALUE };

    FileSystem fs = DECIMAL64_FILE.getFileSystem(conf);
    if (fs.exists(DECIMAL64_FILE)) {
      fs.delete(DECIMAL64_FILE, true);
    }
    MessageType writeSchema = Types.buildMessage()
        .optional(physical).as(LogicalTypeAnnotation.decimalType(scale, filePrecision)).named("value")
        .named("hive_schema");
    GroupWriteSupport.setSchema(writeSchema, conf);
    try (ParquetWriter<Group> writer = new ParquetWriter<>(
        DECIMAL64_FILE, new GroupWriteSupport(), GZIP, 1024 * 1024, 1024, 1024 * 1024,
        dictionaryEncoding, false, PARQUET_1_0, conf)) {
      SimpleGroupFactory f = new SimpleGroupFactory(writeSchema);
      for (int i = 0; i < DECIMAL64_ROWS; i++) {
        long v = unscaled[i % unscaled.length];
        Group group = f.newGroup();
        if (physical == PrimitiveTypeName.INT32) {
          group.append("value", (int) v);
        } else {
          group.append("value", v);
        }
        writer.write(group);
      }
    }

    Configuration readerConf = new Configuration();
    readerConf.set(IOConstants.COLUMNS, "value");
    readerConf.set(IOConstants.COLUMNS_TYPES, "decimal(" + readPrecision + "," + scale + ")");
    readerConf.setBoolean(ColumnProjectionUtils.READ_ALL_COLUMNS, false);
    readerConf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, "0");
    VectorizedParquetRecordReader reader = createTestParquetReader(
        writeSchema.toString(), readerConf,
        new DataTypePhysicalVariation[] { DataTypePhysicalVariation.DECIMAL_64 }, DECIMAL64_FILE);
    VectorizedRowBatch previous = reader.createValue();
    String label = physical + (dictionaryEncoding ? " (dict)" : " (plain)") + " precision-narrowing";
    try {
      int c = 0;
      while (reader.next(NullWritable.get(), previous)) {
        Decimal64ColumnVector vector = (Decimal64ColumnVector) previous.cols[0];
        assertEquals((short) scale, vector.scale);
        for (int i = 0; i < previous.size; i++) {
          if (c == DECIMAL64_ROWS) {
            break;
          }
          long v = unscaled[c % unscaled.length];
          if (v < -absMax || v > absMax) {
            assertTrue("Expected NULL (out of table precision) at pos " + c + " for " + label, vector.isNull[i]);
          } else {
            assertFalse("Unexpected null at pos " + c + " for " + label, vector.isNull[i]);
            assertEquals("In-range Decimal64 must be stored verbatim at pos " + c + " for " + label,
                v, vector.vector[i]);
          }
          c++;
        }
      }
      assertEquals("Did not read all rows for " + label, DECIMAL64_ROWS, c);
    } finally {
      reader.close();
      if (fs.exists(DECIMAL64_FILE)) {
        fs.delete(DECIMAL64_FILE, true);
      }
    }
  }

  protected void decimal64ReadPrecisionNarrowing() throws Exception {
    decimal64ReadPrecisionNarrowing(PrimitiveTypeName.INT32, true);
    decimal64ReadPrecisionNarrowing(PrimitiveTypeName.INT32, false);
    decimal64ReadPrecisionNarrowing(PrimitiveTypeName.INT64, true);
    decimal64ReadPrecisionNarrowing(PrimitiveTypeName.INT64, false);
  }

  /**
   * Coverage for the FIXED_LEN_BYTE_ARRAY-backed Decimal64 fast path: binaryToUnscaledLong decodes
   * the big-endian two's-complement bytes straight into the long. Uses a 16-byte fixed array (wider
   * than 8) so the multi-byte shift and leading sign-byte extension actually run, includes negative
   * values (encoded with leading 0xFF), and a value beyond the read precision that the bounds check
   * must NULL. Writes DECIMAL(18,2) so the fast path engages (file scale 2 == table scale, file
   * precision <= 18), reads as DECIMAL(10,2); every result is checked against the unscaled literal.
   */
  protected void decimal64ReadFixedLenByteArray(boolean dictionaryEncoding) throws Exception {
    final int scale = 2;
    final int filePrecision = 18;
    final int byteLen = 16;
    final int readPrecision = 10;                 // max abs unscaled value at scale 2 = 9_999_999_999
    final long absMax = 9_999_999_999L;
    // 10.01, -10.01, the +/- precision-10 boundary, then 11 digits -> exceeds DECIMAL(10,2) -> NULL.
    long[] unscaled = { 1001L, -1001L, absMax, -absMax, 10_000_000_000L };

    FileSystem fs = DECIMAL64_FILE.getFileSystem(conf);
    if (fs.exists(DECIMAL64_FILE)) {
      fs.delete(DECIMAL64_FILE, true);
    }
    MessageType writeSchema = Types.buildMessage()
        .optional(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY).length(byteLen)
        .as(LogicalTypeAnnotation.decimalType(scale, filePrecision)).named("value")
        .named("hive_schema");
    GroupWriteSupport.setSchema(writeSchema, conf);
    try (ParquetWriter<Group> writer = new ParquetWriter<>(
        DECIMAL64_FILE, new GroupWriteSupport(), GZIP, 1024 * 1024, 1024, 1024 * 1024,
        dictionaryEncoding, false, PARQUET_1_0, conf)) {
      SimpleGroupFactory f = new SimpleGroupFactory(writeSchema);
      for (int i = 0; i < DECIMAL64_ROWS; i++) {
        Group group = f.newGroup();
        group.append("value", Binary.fromConstantByteArray(toFixedLenBytes(unscaled[i % unscaled.length], byteLen)));
        writer.write(group);
      }
    }

    Configuration readerConf = new Configuration();
    readerConf.set(IOConstants.COLUMNS, "value");
    readerConf.set(IOConstants.COLUMNS_TYPES, "decimal(" + readPrecision + "," + scale + ")");
    readerConf.setBoolean(ColumnProjectionUtils.READ_ALL_COLUMNS, false);
    readerConf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, "0");
    VectorizedParquetRecordReader reader = createTestParquetReader(
        writeSchema.toString(), readerConf,
        new DataTypePhysicalVariation[] { DataTypePhysicalVariation.DECIMAL_64 }, DECIMAL64_FILE);
    VectorizedRowBatch previous = reader.createValue();
    String label = "FIXED_LEN_BYTE_ARRAY(" + byteLen + ")" + (dictionaryEncoding ? " (dict)" : " (plain)");
    try {
      int c = 0;
      while (reader.next(NullWritable.get(), previous)) {
        Decimal64ColumnVector vector = (Decimal64ColumnVector) previous.cols[0];
        assertEquals((short) scale, vector.scale);
        for (int i = 0; i < previous.size; i++) {
          if (c == DECIMAL64_ROWS) {
            break;
          }
          long v = unscaled[c % unscaled.length];
          if (v < -absMax || v > absMax) {
            assertTrue("Expected NULL (out of read precision) at pos " + c + " for " + label, vector.isNull[i]);
          } else {
            assertFalse("Unexpected null at pos " + c + " for " + label, vector.isNull[i]);
            assertEquals("Wide byte-array Decimal64 must decode the full unscaled value at pos " + c
                + " for " + label, v, vector.vector[i]);
          }
          c++;
        }
      }
      assertEquals("Did not read all rows for " + label, DECIMAL64_ROWS, c);
    } finally {
      reader.close();
      if (fs.exists(DECIMAL64_FILE)) {
        fs.delete(DECIMAL64_FILE, true);
      }
    }
  }

  protected void decimal64ReadFixedLenByteArray() throws Exception {
    decimal64ReadFixedLenByteArray(true);
    decimal64ReadFixedLenByteArray(false);
  }

  // Big-endian two's-complement of value, left-padded to exactly len bytes (FIXED_LEN_BYTE_ARRAY
  // storage). Negative values pad with 0xFF, exercising the leading sign-extension bytes that
  // binaryToUnscaledLong must drop losslessly.
  private static byte[] toFixedLenBytes(long value, int len) {
    byte[] bytes = new byte[len];
    if (value < 0) {
      Arrays.fill(bytes, (byte) 0xFF);
    }
    byte[] minimal = BigInteger.valueOf(value).toByteArray();
    System.arraycopy(minimal, 0, bytes, len - minimal.length, minimal.length);
    return bytes;
  }

  private static StructObjectInspector createStructObjectInspector(Configuration conf) {
    // Create row related objects
    String columnNames = conf.get(IOConstants.COLUMNS);
    List<String> columnNamesList = DataWritableReadSupport.getColumnNames(columnNames);
    String columnTypes = conf.get(IOConstants.COLUMNS_TYPES);
    List<TypeInfo> columnTypesList = DataWritableReadSupport.getColumnTypes(columnTypes);
    TypeInfo rowTypeInfo = TypeInfoFactory.getStructTypeInfo(columnNamesList, columnTypesList);
    return new ArrayWritableObjectInspector((StructTypeInfo) rowTypeInfo);
  }

  protected void timestampRead(boolean isDictionaryEncoding) throws InterruptedException,
      HiveException, IOException {
    conf.set(IOConstants.COLUMNS, "int96_field");
    conf.set(IOConstants.COLUMNS_TYPES, "timestamp");
    conf.setBoolean(ColumnProjectionUtils.READ_ALL_COLUMNS, false);
    conf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, "0");
    VectorizedParquetRecordReader reader = createTestParquetReader("message test { required " +
        "int96 int96_field;}", conf);
    VectorizedRowBatch previous = reader.createValue();
    try {
      int c = 0;
      while (reader.next(NullWritable.get(), previous)) {
        TimestampColumnVector vector = (TimestampColumnVector) previous.cols[0];
        assertTrue(vector.noNulls);
        for (int i = 0; i < vector.nanos.length; i++) {
          if (c == nElements) {
            break;
          }
          Timestamp expected = new Timestamp();
          if (isDictionaryEncoding) {
            expected.setTimeInMillis(c % UNIQUE_NUM);
          } else {
            expected.setTimeInMillis(c);
          }
          assertEquals("Not the same time at " + c, expected.toEpochMilli(), vector.getTime(i));
          assertEquals("Not the same nano at " + c, expected.getNanos(), vector.getNanos(i));
          assertFalse(vector.isNull[i]);
          c++;
        }
      }
      assertEquals(nElements, c);
    } finally {
      reader.close();
    }
  }

  protected void stringReadTimestamp(boolean isDictionaryEncoding) throws InterruptedException,
      HiveException, IOException {
    conf.set(IOConstants.COLUMNS, "int96_field");
    conf.set(IOConstants.COLUMNS_TYPES, "string");
    conf.setBoolean(ColumnProjectionUtils.READ_ALL_COLUMNS, false);
    conf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, "0");
    VectorizedParquetRecordReader reader = createTestParquetReader("message test { required " +
        "int96 int96_field;}", conf);
    VectorizedRowBatch previous = reader.createValue();
    try {
      int c = 0;
      while (reader.next(NullWritable.get(), previous)) {
        BytesColumnVector vector = (BytesColumnVector) previous.cols[0];
        assertTrue(vector.noNulls);
        for (int i = 0; i < vector.vector.length; i++) {
          if (c == nElements) {
            break;
          }

          Timestamp expected = new Timestamp();
          if (isDictionaryEncoding) {
            expected.setTimeInMillis(c % UNIQUE_NUM);
          } else {
            expected.setTimeInMillis(c);
          };
          String actual = new String(Arrays
              .copyOfRange(vector.vector[i], vector.start[i], vector.start[i] + vector.length[i]));
          assertEquals("Not the same time at " + c, expected.toString(), actual);

          assertFalse(vector.isNull[i]);
          c++;
        }
      }
      assertEquals(nElements, c);
    } finally {
      reader.close();
    }
  }

  protected void floatReadInt(boolean isDictionaryEncoding) throws InterruptedException,
      HiveException, IOException {
    conf.set(IOConstants.COLUMNS, "int32_field");
    conf.set(IOConstants.COLUMNS_TYPES, "float");
    conf.setBoolean(ColumnProjectionUtils.READ_ALL_COLUMNS, false);
    conf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, "0");
    VectorizedParquetRecordReader reader = createTestParquetReader("message test { required int32" +
        " int32_field;}", conf);
    VectorizedRowBatch previous = reader.createValue();
    try {
      int c = 0;
      while (reader.next(NullWritable.get(), previous)) {
        DoubleColumnVector vector = (DoubleColumnVector) previous.cols[0];
        assertTrue(vector.noNulls);
        for (int i = 0; i < vector.vector.length; i++) {
          if (c == nElements) {
            break;
          }
          assertEquals("Failed at " + c, getIntValue(isDictionaryEncoding, c), vector.vector[i], 0);
          assertFalse(vector.isNull[i]);
          c++;
        }
      }
      assertEquals(nElements, c);
    } finally {
      reader.close();
    }
  }

  protected void doubleReadInt(boolean isDictionaryEncoding) throws InterruptedException,
      HiveException, IOException {
    conf.set(IOConstants.COLUMNS, "int32_field");
    conf.set(IOConstants.COLUMNS_TYPES, "double");
    conf.setBoolean(ColumnProjectionUtils.READ_ALL_COLUMNS, false);
    conf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, "0");
    VectorizedParquetRecordReader reader =
        createTestParquetReader("message test { required int32 int32_field;}", conf);
    VectorizedRowBatch previous = reader.createValue();
    try {
      int c = 0;
      while (reader.next(NullWritable.get(), previous)) {
        DoubleColumnVector vector = (DoubleColumnVector) previous.cols[0];
        assertTrue(vector.noNulls);
        for (int i = 0; i < vector.vector.length; i++) {
          if (c == nElements) {
            break;
          }
          assertEquals("Failed at " + c, getIntValue(isDictionaryEncoding, c), vector.vector[i], 0);
          assertFalse(vector.isNull[i]);
          c++;
        }
      }
      assertEquals(nElements, c);
    } finally {
      reader.close();
    }
  }

  protected void longReadInt(boolean isDictionaryEncoding) throws InterruptedException,
      HiveException, IOException {
    Configuration c = new Configuration();
    c.set(IOConstants.COLUMNS, "int32_field");
    c.set(IOConstants.COLUMNS_TYPES, "bigint");
    c.setBoolean(ColumnProjectionUtils.READ_ALL_COLUMNS, false);
    c.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, "0");
    intRead(isDictionaryEncoding, c);
  }

  protected void intRead(boolean isDictionaryEncoding) throws InterruptedException,
      HiveException, IOException {
    Configuration c = new Configuration();
    c.set(IOConstants.COLUMNS, "int32_field");
    c.set(IOConstants.COLUMNS_TYPES, "int");
    c.setBoolean(ColumnProjectionUtils.READ_ALL_COLUMNS, false);
    c.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, "0");
    intRead(isDictionaryEncoding, c);
  }

  private void intRead(boolean isDictionaryEncoding, Configuration conf) throws
      InterruptedException, HiveException, IOException {
    VectorizedParquetRecordReader reader =
        createTestParquetReader("message test { required int32 int32_field;}", conf);
    VectorizedRowBatch previous = reader.createValue();
    try {
      int c = 0;
      while (reader.next(NullWritable.get(), previous)) {
        LongColumnVector vector = (LongColumnVector) previous.cols[0];
        assertTrue(vector.noNulls);
        for (int i = 0; i < vector.vector.length; i++) {
          if (c == nElements) {
            break;
          }
          assertEquals("Failed at " + c, getIntValue(isDictionaryEncoding, c), vector.vector[i]);
          assertFalse(vector.isNull[i]);
          c++;
        }
      }
      assertEquals(nElements, c);
    } finally {
      reader.close();
    }
  }

  protected void floatReadLong(boolean isDictionaryEncoding) throws Exception {
    Configuration c = new Configuration();
    c.set(IOConstants.COLUMNS, "int64_field");
    c.set(IOConstants.COLUMNS_TYPES, "float");
    c.setBoolean(ColumnProjectionUtils.READ_ALL_COLUMNS, false);
    c.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, "0");
    VectorizedParquetRecordReader reader =
        createTestParquetReader("message test { required int64 int64_field;}", c);
    VectorizedRowBatch previous = reader.createValue();
    try {
      int count = 0;
      while (reader.next(NullWritable.get(), previous)) {
        DoubleColumnVector vector = (DoubleColumnVector) previous.cols[0];
        assertTrue(vector.noNulls);
        for (int i = 0; i < vector.vector.length; i++) {
          if (count == nElements) {
            break;
          }
          assertEquals("Failed at " + count, getLongValue(isDictionaryEncoding, count), vector
              .vector[i], 0);
          assertFalse(vector.isNull[i]);
          count++;
        }
      }
      assertEquals(nElements, count);
    } finally {
      reader.close();
    }
  }

  protected void doubleReadLong(boolean isDictionaryEncoding) throws Exception {
    Configuration c = new Configuration();
    c.set(IOConstants.COLUMNS, "int64_field");
    c.set(IOConstants.COLUMNS_TYPES, "double");
    c.setBoolean(ColumnProjectionUtils.READ_ALL_COLUMNS, false);
    c.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, "0");
    VectorizedParquetRecordReader reader =
        createTestParquetReader("message test { required int64 int64_field;}", c);
    VectorizedRowBatch previous = reader.createValue();
    try {
      int count = 0;
      while (reader.next(NullWritable.get(), previous)) {
        DoubleColumnVector vector = (DoubleColumnVector) previous.cols[0];
        assertTrue(vector.noNulls);
        for (int i = 0; i < vector.vector.length; i++) {
          if (count == nElements) {
            break;
          }
          assertEquals("Failed at " + count, getLongValue(isDictionaryEncoding, count),
              vector.vector[i], 0);
          assertFalse(vector.isNull[i]);
          count++;
        }
      }
      assertEquals(nElements, count);
    } finally {
      reader.close();
    }
  }

  protected void longRead(boolean isDictionaryEncoding) throws Exception {
    Configuration c = new Configuration();
    c.set(IOConstants.COLUMNS, "int64_field");
    c.set(IOConstants.COLUMNS_TYPES, "bigint");
    c.setBoolean(ColumnProjectionUtils.READ_ALL_COLUMNS, false);
    c.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, "0");
    longRead(isDictionaryEncoding, c);
  }

  private void longRead(boolean isDictionaryEncoding, Configuration conf) throws Exception {
    VectorizedParquetRecordReader reader =
        createTestParquetReader("message test { required int64 int64_field;}", conf);
    VectorizedRowBatch previous = reader.createValue();
    try {
      int c = 0;
      while (reader.next(NullWritable.get(), previous)) {
        LongColumnVector vector = (LongColumnVector) previous.cols[0];
        assertTrue(vector.noNulls);
        for (int i = 0; i < vector.vector.length; i++) {
          if (c == nElements) {
            break;
          }
          assertEquals("Failed at " + c, getLongValue(isDictionaryEncoding, c), vector.vector[i]);
          assertFalse(vector.isNull[i]);
          c++;
        }
      }
      assertEquals(nElements, c);
    } finally {
      reader.close();
    }
  }

  protected void doubleRead(boolean isDictionaryEncoding) throws Exception {
    Configuration c = new Configuration();
    c.set(IOConstants.COLUMNS, "double_field");
    c.set(IOConstants.COLUMNS_TYPES, "double");
    c.setBoolean(ColumnProjectionUtils.READ_ALL_COLUMNS, false);
    c.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, "0");
    doubleRead(isDictionaryEncoding, c);
  }

  protected void stringReadDouble(boolean isDictionaryEncoding) throws Exception {
    Configuration readerConf = new Configuration();
    readerConf.set(IOConstants.COLUMNS, "double_field");
    readerConf.set(IOConstants.COLUMNS_TYPES, "string");
    readerConf.setBoolean(ColumnProjectionUtils.READ_ALL_COLUMNS, false);
    readerConf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, "0");
    VectorizedParquetRecordReader reader =
        createTestParquetReader("message test { required double double_field;}", readerConf);
    VectorizedRowBatch previous = reader.createValue();
    try {
      int c = 0;
      while (reader.next(NullWritable.get(), previous)) {
        BytesColumnVector vector = (BytesColumnVector) previous.cols[0];
        assertTrue(vector.noNulls);
        for (int i = 0; i < vector.vector.length; i++) {
          if (c == nElements) {
            break;
          }
          String actual = new String(Arrays.copyOfRange(vector.vector[i], vector.start[i], vector
              .start[i] + vector.length[i]));
          assertEquals("Failed at " + c, String.valueOf(getDoubleValue(isDictionaryEncoding, c)),
              actual);
          assertFalse(vector.isNull[i]);
          c++;
        }
      }
      assertEquals(nElements, c);
    } finally {
      reader.close();
    }
  }

  private void doubleRead(boolean isDictionaryEncoding, Configuration conf) throws Exception {
    VectorizedParquetRecordReader reader =
        createTestParquetReader("message test { required double double_field;}", conf);
    VectorizedRowBatch previous = reader.createValue();
    try {
      int c = 0;
      while (reader.next(NullWritable.get(), previous)) {
        DoubleColumnVector vector = (DoubleColumnVector) previous.cols[0];
        assertTrue(vector.noNulls);
        for (int i = 0; i < vector.vector.length; i++) {
          if (c == nElements) {
            break;
          }
          assertEquals("Failed at " + c, getDoubleValue(isDictionaryEncoding, c), vector.vector[i],
              0);
          assertFalse(vector.isNull[i]);
          c++;
        }
      }
      assertEquals(nElements, c);
    } finally {
      reader.close();
    }
  }

  protected void floatRead(boolean isDictionaryEncoding) throws Exception {
    Configuration c = new Configuration();
    c.set(IOConstants.COLUMNS, "float_field");
    c.set(IOConstants.COLUMNS_TYPES, "float");
    c.setBoolean(ColumnProjectionUtils.READ_ALL_COLUMNS, false);
    c.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, "0");
    floatRead(isDictionaryEncoding, c);
  }

  protected void doubleReadFloat(boolean isDictionaryEncoding) throws Exception {
    Configuration c = new Configuration();
    c.set(IOConstants.COLUMNS, "float_field");
    c.set(IOConstants.COLUMNS_TYPES, "double");
    c.setBoolean(ColumnProjectionUtils.READ_ALL_COLUMNS, false);
    c.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, "0");
    floatRead(isDictionaryEncoding, c);
  }

  private void floatRead(boolean isDictionaryEncoding, Configuration conf) throws Exception {
    VectorizedParquetRecordReader reader =
        createTestParquetReader("message test { required float float_field;}", conf);
    VectorizedRowBatch previous = reader.createValue();
    try {
      int c = 0;
      while (reader.next(NullWritable.get(), previous)) {
        DoubleColumnVector vector = (DoubleColumnVector) previous.cols[0];
        assertTrue(vector.noNulls);
        for (int i = 0; i < vector.vector.length; i++) {
          if (c == nElements) {
            break;
          }
          assertEquals("Failed at " + c, getFloatValue(isDictionaryEncoding, c), vector.vector[i],
              0);
          assertFalse(vector.isNull[i]);
          c++;
        }
      }
      assertEquals(nElements, c);
    } finally {
      reader.close();
    }
  }

  protected void booleanRead() throws Exception {
    Configuration conf = new Configuration();
    conf.set(IOConstants.COLUMNS, "boolean_field");
    conf.set(IOConstants.COLUMNS_TYPES, "boolean");
    conf.setBoolean(ColumnProjectionUtils.READ_ALL_COLUMNS, false);
    conf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, "0");
    VectorizedParquetRecordReader reader =
        createTestParquetReader("message test { required boolean boolean_field;}", conf);
    VectorizedRowBatch previous = reader.createValue();
    try {
      int c = 0;
      while (reader.next(NullWritable.get(), previous)) {
        LongColumnVector vector = (LongColumnVector) previous.cols[0];
        assertTrue(vector.noNulls);
        for (int i = 0; i < vector.vector.length; i++) {
          if (c == nElements) {
            break;
          }
          assertEquals("Failed at " + c, (getBooleanValue(c) ? 1 : 0), vector.vector[i]);
          assertFalse(vector.isNull[i]);
          c++;
        }
      }
      assertEquals(nElements, c);
    } finally {
      reader.close();
    }
  }

  protected void stringReadBoolean() throws Exception {
    Configuration conf = new Configuration();
    conf.set(IOConstants.COLUMNS, "boolean_field");
    conf.set(IOConstants.COLUMNS_TYPES, "string");
    conf.setBoolean(ColumnProjectionUtils.READ_ALL_COLUMNS, false);
    conf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, "0");
    VectorizedParquetRecordReader reader =
        createTestParquetReader("message test { required boolean boolean_field;}", conf);
    VectorizedRowBatch previous = reader.createValue();
    try {
      int c = 0;
      while (reader.next(NullWritable.get(), previous)) {
        BytesColumnVector vector = (BytesColumnVector) previous.cols[0];
        assertTrue(vector.noNulls);
        for (int i = 0; i < vector.vector.length; i++) {
          if (c == nElements) {
            break;
          }

          String actual = new String(Arrays.copyOfRange(vector.vector[i], vector.start[i], vector
              .start[i] + vector.length[i]));
          assertEquals("Failed at " + c, String.valueOf(getBooleanValue(c)), actual);
          assertFalse(vector.isNull[i]);
          c++;
        }
      }
      assertEquals(nElements, c);
    } finally {
      reader.close();
    }
  }

  protected void binaryRead(boolean isDictionaryEncoding) throws Exception {
    Configuration conf = new Configuration();
    conf.set(IOConstants.COLUMNS, "binary_field_some_null");
    conf.set(IOConstants.COLUMNS_TYPES, "string");
    conf.setBoolean(ColumnProjectionUtils.READ_ALL_COLUMNS, false);
    conf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, "0");
    VectorizedParquetRecordReader reader =
        createTestParquetReader("message test { required binary binary_field_some_null;}", conf);
    VectorizedRowBatch previous = reader.createValue();
    int c = 0;
    try {
      while (reader.next(NullWritable.get(), previous)) {
        BytesColumnVector vector = (BytesColumnVector) previous.cols[0];
        boolean noNull = true;
        for (int i = 0; i < vector.vector.length; i++) {
          if (c == nElements) {
            break;
          }
          String actual;
          assertEquals("Null assert failed at " + c, isNull(c), vector.isNull[i]);
          if (!vector.isNull[i]) {
            actual = new String(ArrayUtils
                .subarray(vector.vector[i], vector.start[i], vector.start[i] + vector.length[i]));
            assertEquals("failed at " + c, getStr(isDictionaryEncoding, c), actual);
          } else {
            noNull = false;
          }
          c++;
        }
        assertEquals("No Null check failed at " + c, noNull, vector.noNulls);
        assertFalse(vector.isRepeating);
      }
      assertEquals("It doesn't exit at expected position", nElements, c);
    } finally {
      reader.close();
    }
  }

  protected void structRead(boolean isDictionaryEncoding) throws Exception {
    Configuration conf = new Configuration();
    conf.set(IOConstants.COLUMNS, "struct_field");
    conf.set(IOConstants.COLUMNS_TYPES, "struct<a:int,b:double>");
    conf.setBoolean(ColumnProjectionUtils.READ_ALL_COLUMNS, false);
    conf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, "0");
    String schema = "message hive_schema {\n"
        + "group struct_field {\n"
        + "  optional int32 a;\n"
        + "  optional double b;\n"
        + "}\n"
        + "}\n";
    VectorizedParquetRecordReader reader = createTestParquetReader(schema, conf);
    VectorizedRowBatch previous = reader.createValue();
    int c = 0;
    try {
      while (reader.next(NullWritable.get(), previous)) {
        StructColumnVector vector = (StructColumnVector) previous.cols[0];
        LongColumnVector cv = (LongColumnVector) vector.fields[0];
        DoubleColumnVector dv = (DoubleColumnVector) vector.fields[1];

        for (int i = 0; i < cv.vector.length; i++) {
          if (c == nElements) {
            break;
          }
          assertEquals(getIntValue(isDictionaryEncoding, c), cv.vector[i]);
          assertEquals(getDoubleValue(isDictionaryEncoding, c), dv.vector[i], 0);
          assertFalse(vector.isNull[i]);
          assertFalse(vector.isRepeating);
          c++;
        }
      }
      assertEquals("It doesn't exit at expected position", nElements, c);
    } finally {
      reader.close();
    }
  }

  protected void nestedStructRead0(boolean isDictionaryEncoding) throws Exception {
    Configuration conf = new Configuration();
    conf.set(IOConstants.COLUMNS, "nested_struct_field");
    conf.set(IOConstants.COLUMNS_TYPES, "struct<nsf:struct<c:int,d:int>,e:double>");
    conf.setBoolean(ColumnProjectionUtils.READ_ALL_COLUMNS, false);
    conf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, "0");
    String schema = "message hive_schema {\n"
        + "group nested_struct_field {\n"
        + "  optional group nsf {\n"
        + "    optional int32 c;\n"
        + "    optional int32 d;\n"
        + "  }"
        + "optional double e;\n"
        + "}\n";
    VectorizedParquetRecordReader reader = createTestParquetReader(schema, conf);
    VectorizedRowBatch previous = reader.createValue();
    int c = 0;
    try {
      while (reader.next(NullWritable.get(), previous)) {
        StructColumnVector vector = (StructColumnVector) previous.cols[0];
        StructColumnVector sv = (StructColumnVector) vector.fields[0];
        LongColumnVector cv = (LongColumnVector) sv.fields[0];
        LongColumnVector dv = (LongColumnVector) sv.fields[1];
        DoubleColumnVector ev = (DoubleColumnVector) vector.fields[1];

        for (int i = 0; i < cv.vector.length; i++) {
          if (c == nElements) {
            break;
          }
          assertEquals(getIntValue(isDictionaryEncoding, c), cv.vector[i]);
          assertEquals(getIntValue(isDictionaryEncoding, c), dv.vector[i]);
          assertEquals(getDoubleValue(isDictionaryEncoding, c), ev.vector[i], 0);
          assertFalse(vector.isNull[i]);
          assertFalse(vector.isRepeating);
          c++;
        }
      }
      assertEquals("It doesn't exit at expected position", nElements, c);
    } finally {
      reader.close();
    }
  }

  protected void nestedStructRead1(boolean isDictionaryEncoding) throws Exception {
    Configuration conf = new Configuration();
    conf.set(IOConstants.COLUMNS, "nested_struct_field");
    conf.set(IOConstants.COLUMNS_TYPES, "struct<nsf:struct<c:int>>");
    conf.setBoolean(ColumnProjectionUtils.READ_ALL_COLUMNS, false);
    conf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, "0");
    String schema = "message hive_schema {\n"
        + "group nested_struct_field {\n"
        + "  optional group nsf {\n"
        + "    optional int32 c;\n"
        + "  }"
        + "}\n";
    VectorizedParquetRecordReader reader = createTestParquetReader(schema, conf);
    VectorizedRowBatch previous = reader.createValue();
    int c = 0;
    try {
      while (reader.next(NullWritable.get(), previous)) {
        StructColumnVector vector = (StructColumnVector) previous.cols[0];
        StructColumnVector sv = (StructColumnVector) vector.fields[0];
        LongColumnVector cv = (LongColumnVector) sv.fields[0];

        for (int i = 0; i < cv.vector.length; i++) {
          if (c == nElements) {
            break;
          }
          assertEquals(getIntValue(isDictionaryEncoding, c), cv.vector[i]);
          assertFalse(vector.isNull[i]);
          assertFalse(vector.isRepeating);
          c++;
        }
      }
      assertEquals("It doesn't exit at expected position", nElements, c);
    } finally {
      reader.close();
    }
  }

  protected void structReadSomeNull(boolean isDictionaryEncoding) throws Exception {
    Configuration conf = new Configuration();
    conf.set(IOConstants.COLUMNS, "struct_field_some_null");
    conf.set(IOConstants.COLUMNS_TYPES, "struct<f:int,g:double>");
    conf.setBoolean(ColumnProjectionUtils.READ_ALL_COLUMNS, false);
    conf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, "0");
    String schema = "message hive_schema {\n"
        + "group struct_field_some_null {\n"
        + "  optional int32 f;\n"
        + "  optional double g;\n"
        + "}\n";
    VectorizedParquetRecordReader reader = createTestParquetReader(schema, conf);
    VectorizedRowBatch previous = reader.createValue();
    int c = 0;
    try {
      while (reader.next(NullWritable.get(), previous)) {
        StructColumnVector sv = (StructColumnVector) previous.cols[0];
        LongColumnVector fv = (LongColumnVector) sv.fields[0];
        DoubleColumnVector gv = (DoubleColumnVector) sv.fields[1];

        for (int i = 0; i < fv.vector.length; i++) {
          if (c == nElements) {
            break;
          }
          assertEquals(c % 2 == 0, fv.isNull[i]);
          assertEquals(c % 3 == 0, gv.isNull[i]);
          assertEquals(c % /* 2*3 = */6 == 0, sv.isNull[i]);
          if (!sv.isNull[i]) {
            if (!fv.isNull[i]) {
              assertEquals(getIntValue(isDictionaryEncoding, c), fv.vector[i]);
            }
            if (!gv.isNull[i]) {
              assertEquals(getDoubleValue(isDictionaryEncoding, c), gv.vector[i], 0);
            }
          }
          assertFalse(fv.isRepeating);
          c++;
        }
      }
      assertEquals("It doesn't exit at expected position", nElements, c);
    } finally {
      reader.close();
    }
  }

  /**
   * Struct fields at definition levels 2 and 3, with NULLs at every level: the outer struct, the inner struct and
   * each field. The struct readers find their NULLs in the levels the first field records.
   */
  protected void structFieldNullsAtEveryLevel(boolean dictionaryEncoding) throws Exception {
    MessageType writeSchema = parseMessageType("message hive_schema { optional group outer { "
        + "optional group inner { optional int32 x; } optional int32 y; } }");
    Path path = new Path(System.getProperty("java.io.tmpdir"), "testStructNullsParquetFile");
    FileSystem fs = path.getFileSystem(conf);
    fs.delete(path, true);
    GroupWriteSupport.setSchema(writeSchema, conf);
    try (ParquetWriter<Group> writer = new ParquetWriter<>(path, new GroupWriteSupport(), GZIP, 1024 * 1024, 1024,
        1024 * 1024, dictionaryEncoding, false, PARQUET_1_0, conf)) {
      SimpleGroupFactory f = new SimpleGroupFactory(writeSchema);
      for (int i = 0; i < nElements; i++) {
        Group group = f.newGroup();
        if (i % 7 != 0) {
          Group outer = group.addGroup("outer");
          if (i % 5 != 0) {
            Group inner = outer.addGroup("inner");
            if (i % 3 != 0) {
              inner.append("x", getIntValue(dictionaryEncoding, i));
            }
          }
          if (i % 2 != 0) {
            outer.append("y", getIntValue(dictionaryEncoding, i));
          }
        }
        writer.write(group);
      }
    }
    Configuration readerConf = new Configuration();
    readerConf.set(IOConstants.COLUMNS, "outer");
    readerConf.set(IOConstants.COLUMNS_TYPES, "struct<inner:struct<x:int>,y:int>");
    readerConf.setBoolean(ColumnProjectionUtils.READ_ALL_COLUMNS, false);
    readerConf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, "0");
    VectorizedParquetRecordReader reader = createTestParquetReader(writeSchema.toString(), readerConf, null, path);
    VectorizedRowBatch previous = reader.createValue();
    try {
      int c = 0;
      while (reader.next(NullWritable.get(), previous)) {
        StructColumnVector outer = (StructColumnVector) previous.cols[0];
        StructColumnVector inner = (StructColumnVector) outer.fields[0];
        LongColumnVector x = (LongColumnVector) inner.fields[0];
        LongColumnVector y = (LongColumnVector) outer.fields[1];
        for (int i = 0; i < previous.size; i++, c++) {
          boolean outerNull = c % 7 == 0;
          boolean innerNull = outerNull || c % 5 == 0;
          boolean xNull = innerNull || c % 3 == 0;
          boolean yNull = outerNull || c % 2 == 0;
          assertEquals("outer at " + c, outerNull, outer.isNull[i]);
          assertEquals("inner at " + c, innerNull, inner.isNull[i]);
          assertEquals("x at " + c, xNull, x.isNull[i]);
          assertEquals("y at " + c, yNull, y.isNull[i]);
          if (!xNull) {
            assertEquals("x at " + c, getIntValue(dictionaryEncoding, c), x.vector[i]);
          }
          if (!yNull) {
            assertEquals("y at " + c, getIntValue(dictionaryEncoding, c), y.vector[i]);
          }
        }
      }
      assertEquals(nElements, c);
    } finally {
      reader.close();
      fs.delete(path, true);
    }
  }

  protected void stringReadDecimal(boolean isDictionaryEncoding) throws Exception {
    Configuration conf = new Configuration();
    conf.set(IOConstants.COLUMNS, "value");
    conf.set(IOConstants.COLUMNS_TYPES, "string");
    conf.setBoolean(ColumnProjectionUtils.READ_ALL_COLUMNS, false);
    conf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, "0");
    VectorizedParquetRecordReader reader =
        createTestParquetReader("message hive_schema { required value (DECIMAL(5,2));}", conf);
    VectorizedRowBatch previous = reader.createValue();
    try {
      int c = 0;
      while (reader.next(NullWritable.get(), previous)) {
        BytesColumnVector vector = (BytesColumnVector) previous.cols[0];
        assertTrue(vector.noNulls);
        for (int i = 0; i < vector.vector.length; i++) {
          if (c == nElements) {
            break;
          }

          String actual = new String(Arrays.copyOfRange(vector.vector[i], vector.start[i], vector
              .start[i] + vector.length[i]));
          assertEquals("Check failed at pos " + c, getDecimal(isDictionaryEncoding, c).toString(),
              actual);

          assertFalse(vector.isNull[i]);
          c++;
        }
      }
      assertEquals(nElements, c);
    } finally {
      reader.close();
    }
  }

  protected void decimalRead(boolean isDictionaryEncoding) throws Exception {
    Configuration readerConf = new Configuration();
    readerConf.set(IOConstants.COLUMNS, "value");
    readerConf.set(IOConstants.COLUMNS_TYPES, "decimal(5,2)");
    readerConf.setBoolean(ColumnProjectionUtils.READ_ALL_COLUMNS, false);
    readerConf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, "0");
    VectorizedParquetRecordReader reader =
        createTestParquetReader("message hive_schema { required value (DECIMAL(5,2));}", readerConf);
    VectorizedRowBatch previous = reader.createValue();
    try {
      int c = 0;
      while (reader.next(NullWritable.get(), previous)) {
        DecimalColumnVector vector = (DecimalColumnVector) previous.cols[0];
        assertTrue(vector.noNulls);
        for (int i = 0; i < vector.vector.length; i++) {
          if (c == nElements) {
            break;
          }
          assertEquals("Check failed at pos " + c, getDecimal(isDictionaryEncoding, c),
              vector.vector[i].getHiveDecimal());

          assertFalse(vector.isNull[i]);
          c++;
        }
      }
      assertEquals(nElements, c);
    } finally {
      reader.close();
    }
  }

  protected void verifyBatchOffsets() throws Exception {
    Configuration c = new Configuration();
    c.set(IOConstants.COLUMNS, "int64_field");
    c.set(IOConstants.COLUMNS_TYPES, "bigint");
    c.setBoolean(ColumnProjectionUtils.READ_ALL_COLUMNS, false);
    c.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, "0");
    VectorizedParquetRecordReader reader =
        createTestParquetReader("message test { required int64 int64_field;}", c);
    VectorizedRowBatch previous = reader.createValue();
    try {
      int batchCount = 0;
      while (reader.next(NullWritable.get(), previous)) {
        assertEquals(VectorizedRowBatch.DEFAULT_SIZE * batchCount++, reader.getRowNumber());
      }
      assertEquals(reader.getRowNumber(), nElements);
    } finally {
      reader.close();
    }
  }

  // Flat columns in 4 KB pages, read a column at a time through VectorizedPrimitiveColumnReader in batches of any
  // size: batches span pages and NULLs fall on batch edges.

  protected static final int FLAT_ROWS = 20_000;
  protected static final int[] FLAT_BATCH_SIZES = { 1000, 1024, 333 };
  protected static final MessageType FLAT_SCHEMA = Types.buildMessage()
      .optional(PrimitiveTypeName.INT32).named("i32")
      .required(PrimitiveTypeName.INT32).named("i32r")
      .optional(PrimitiveTypeName.INT64).named("i64")
      .optional(PrimitiveTypeName.FLOAT).named("f")
      .optional(PrimitiveTypeName.DOUBLE).named("d")
      .optional(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY).length(4)
          .as(LogicalTypeAnnotation.decimalType(2, 7)).named("dec72")
      .optional(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY).length(8)
          .as(LogicalTypeAnnotation.decimalType(4, 18)).named("dec18")
      .optional(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY).length(5)
          .as(LogicalTypeAnnotation.decimalType(3, 11)).named("dec11")
      .optional(PrimitiveTypeName.INT32).named("allnull")
      .required(PrimitiveTypeName.INT32).named("constant")
      .required(PrimitiveTypeName.INT32).named("constant_plain")
      .optional(PrimitiveTypeName.INT32).as(LogicalTypeAnnotation.decimalType(2, 7)).named("dec32")
      .optional(PrimitiveTypeName.INT64).as(LogicalTypeAnnotation.decimalType(3, 15)).named("dec64")
      .optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("s")
      .required(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("sr")
      .optional(PrimitiveTypeName.BINARY).named("bin")
      .optional(PrimitiveTypeName.INT32).as(LogicalTypeAnnotation.dateType()).named("dt")
      .required(PrimitiveTypeName.INT32).as(LogicalTypeAnnotation.dateType()).named("dtr")
      .optional(PrimitiveTypeName.BOOLEAN).named("b")
      .required(PrimitiveTypeName.BOOLEAN).named("br")
      .named("m");

  protected static final String[] FLAT_COLUMNS = { "i32", "i32r", "i64", "f", "d", "dec72", "dec18", "dec11",
      "allnull", "constant", "constant_plain", "dec32", "dec64" };

  /** Written values per column (unscaled longs for decimals, raw bits for float/double). */
  private static final long[][] FLAT_VALUES = new long[FLAT_COLUMNS.length][FLAT_ROWS];
  private static final boolean[][] FLAT_NULLS = new boolean[FLAT_COLUMNS.length][FLAT_ROWS];

  static {
    Random rnd = new Random(42);
    for (int r = 0; r < FLAT_ROWS; r++) {
      for (int c = 0; c < FLAT_COLUMNS.length; c++) {
        boolean optional = FLAT_SCHEMA.getType(c).isRepetition(Type.Repetition.OPTIONAL);
        FLAT_NULLS[c][r] = c == 8 || (optional && (r % 1000 == 0 || r % 1000 == 999 || rnd.nextInt(20) == 0));
      }
      FLAT_VALUES[0][r] = rnd.nextInt(50);
      FLAT_VALUES[1][r] = r < 5000 ? r % 100 : (r * 7919L) % 1_000_003L;
      FLAT_VALUES[2][r] = rnd.nextInt(100) * 1_000_000_007L;
      FLAT_VALUES[3][r] = Float.floatToRawIntBits(rnd.nextFloat() * 1000f);
      FLAT_VALUES[4][r] = Double.doubleToRawLongBits(rnd.nextInt(100) * 1.25);
      FLAT_VALUES[5][r] = (rnd.nextInt(100) - 50) * 12_345L;
      FLAT_VALUES[6][r] = rnd.nextLong() % 1_000_000_000_000_000_000L;
      FLAT_VALUES[7][r] = (rnd.nextInt(100) - 50) * 999_999_999L;
      FLAT_VALUES[9][r] = 42;
      FLAT_VALUES[10][r] = 42;
      FLAT_VALUES[11][r] = (r * 7919L) % 9_999_999L - 4_999_999L;
      FLAT_VALUES[12][r] = rnd.nextLong() % 1_000_000_000_000_000L;
    }
  }

  protected static Path flatFile(WriterVersion version, boolean dictionaryEncoding) {
    return new Path(System.getProperty("java.io.tmpdir"),
        "testFlatColumns-" + version + (dictionaryEncoding ? "-dictionary" : "-plain") + ".parquet");
  }

  /**
   * The flat columns. With dictionary encoding, a 2 KB dictionary page makes i32r and s, low cardinality at first,
   * fall back to PLAIN within the chunk; constant_plain is never dictionary encoded.
   */
  protected static void writeFlatFile(WriterVersion version, boolean dictionaryEncoding) throws IOException {
    Path path = flatFile(version, dictionaryEncoding);
    path.getFileSystem(conf).delete(path, false);
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(path).withConf(conf).withType(FLAT_SCHEMA)
        .withWriterVersion(version).withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
        .withDictionaryEncoding(dictionaryEncoding).withDictionaryEncoding("constant_plain", false)
        .withDictionaryPageSize(2048).withPageSize(4096).withRowGroupSize(64L << 20).build()) {
      SimpleGroupFactory f = new SimpleGroupFactory(FLAT_SCHEMA);
      for (int r = 0; r < FLAT_ROWS; r++) {
        Group g = f.newGroup();
        for (int c = 0; c < FLAT_COLUMNS.length; c++) {
          if (FLAT_NULLS[c][r]) {
            continue;
          }
          long v = FLAT_VALUES[c][r];
          switch (c) {
          case 0, 1, 9, 10, 11 -> g.append(FLAT_COLUMNS[c], (int) v);
          case 2, 12 -> g.append(FLAT_COLUMNS[c], v);
          case 3 -> g.append(FLAT_COLUMNS[c], Float.intBitsToFloat((int) v));
          case 4 -> g.append(FLAT_COLUMNS[c], Double.longBitsToDouble(v));
          default -> g.append(FLAT_COLUMNS[c], Binary.fromConstantByteArray(
              toFixedLenBytes(v, FLAT_SCHEMA.getType(c).asPrimitiveType().getTypeLength())));
          }
        }
        if (flatString(r) != null) {
          g.append("s", flatString(r));
        }
        g.append("sr", flatRequiredString(r));
        if (flatBinary(r) != null) {
          g.append("bin", Binary.fromConstantByteArray(flatBinary(r)));
        }
        if (r % 19 != 0) {
          g.append("dt", flatDate(r));
        }
        g.append("dtr", flatDate(r));
        if (r % 23 != 0) {
          g.append("b", flatBoolean(r));
        }
        g.append("br", flatBoolean(r));
        writer.write(g);
      }
    }
  }

  protected static void deleteFlatFiles(boolean dictionaryEncoding) throws IOException {
    for (WriterVersion version : WriterVersion.values()) {
      flatFile(version, dictionaryEncoding).getFileSystem(conf).delete(flatFile(version, dictionaryEncoding), false);
    }
  }

  /**
   * Strings: low cardinality at first, NULL every 13th row, the empty string every 7th, a value whose UTF-8 runs two,
   * three and four bytes per character every 11th, and lengths a CHAR(5) has to truncate.
   */
  private static String flatString(int r) {
    if (r % 13 == 0) {
      return null;
    }
    if (r % 7 == 0) {
      return "";
    }
    String tail = Integer.toString(r < 5000 ? r % 100 : r);
    return r % 11 == 0 ? "é中😀" + tail : "value" + tail;
  }

  private static String flatRequiredString(int r) {
    String value = flatString(r);
    return value == null ? "" : value;
  }

  /** An unannotated BINARY whose bytes are not valid UTF-8. */
  private static byte[] flatBinary(int r) {
    if (r % 17 == 0) {
      return null;
    }
    int v = r < 5000 ? r % 50 : r;
    return new byte[] { (byte) 0xFF, (byte) v, (byte) (v >>> 8), (byte) 0x80 };
  }

  /** Day numbers either side of the 1582 switchover, so the hybrid to proleptic shift is exercised. */
  private static int flatDate(int r) {
    return r < 5000 ? (r % 40) * -6000 : r * 40 - 300_000;
  }

  /** Long runs and alternating stretches, so an RLE boolean page carries runs and bit-packed groups. */
  private static boolean flatBoolean(int r) {
    return (r / 64) % 3 == 0 || r % 2 == 0;
  }

  protected static TypeInfo flatHiveType(String column) {
    return switch (column) {
    case "i32r", "i64" -> TypeInfoFactory.longTypeInfo;
    case "f" -> TypeInfoFactory.floatTypeInfo;
    case "d" -> TypeInfoFactory.doubleTypeInfo;
    case "dec72", "dec32" -> TypeInfoFactory.getDecimalTypeInfo(7, 2);
    case "dec18" -> TypeInfoFactory.getDecimalTypeInfo(18, 4);
    case "dec11" -> TypeInfoFactory.getDecimalTypeInfo(11, 3);
    case "dec64" -> TypeInfoFactory.getDecimalTypeInfo(15, 3);
    default -> TypeInfoFactory.intTypeInfo;
    };
  }

  /** One batch as read: flags, NULL map and values. */
  protected record Batch(boolean noNulls, boolean isRepeating, boolean[] isNull, long[] longs, double[] doubles,
      HiveDecimal[] decimals, byte[][] bytes, java.sql.Timestamp[] timestamps) {
  }

  protected static ColumnVector newVector(TypeInfo hiveType, boolean decimal64, int size) {
    if (hiveType instanceof DecimalTypeInfo d) {
      return decimal64 ? new Decimal64ColumnVector(size, d.getPrecision(), d.getScale())
          : new DecimalColumnVector(size, d.getPrecision(), d.getScale());
    }
    return switch (((PrimitiveTypeInfo) hiveType).getPrimitiveCategory()) {
    case FLOAT, DOUBLE -> new DoubleColumnVector(size);
    case STRING, CHAR, VARCHAR, BINARY -> {
      BytesColumnVector bytes = new BytesColumnVector(size);
      bytes.init();
      yield bytes;
    }
    case DATE -> new DateColumnVector(size);
    case TIMESTAMP -> new TimestampColumnVector(size);
    default -> new LongColumnVector(size);
    };
  }

  protected static List<Batch> readColumn(Path path, String column, TypeInfo hiveType, boolean decimal64,
      int batchSize, boolean skipProleptic) throws IOException {
    return readColumn(path, column, hiveType, decimal64, batchSize, skipProleptic, null, true);
  }

  /** Every row group of a column, in batches of {@code batchSize}, through the reader's public constructor. */
  protected static List<Batch> readColumn(Path path, String column, TypeInfo hiveType, boolean decimal64,
      int batchSize, boolean skipProleptic, ZoneId writerZone, boolean legacyConversion) throws IOException {
    List<Batch> batches = new ArrayList<>();
    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, conf))) {
      MessageType fileSchema = reader.getFooter().getFileMetaData().getSchema();
      ColumnDescriptor descriptor = fileSchema.getColumnDescription(new String[] { column });
      PageReadStore pages;
      while ((pages = reader.readNextRowGroup()) != null) {
        VectorizedPrimitiveColumnReader columnReader = new VectorizedPrimitiveColumnReader(descriptor,
            pages.getPageReader(descriptor), false, writerZone, skipProleptic, legacyConversion,
            fileSchema.getType(column), hiveType);
        for (long left = pages.getRowCount(); left > 0; ) {
          int n = (int) Math.min(batchSize, left);
          ColumnVector cv = newVector(hiveType, decimal64, batchSize);
          cv.reset();
          cv.isRepeating = true;
          columnReader.readBatch(n, cv, hiveType);
          batches.add(snapshot(cv, n));
          left -= n;
        }
      }
    }
    return batches;
  }

  protected static Batch snapshot(ColumnVector cv, int n) {
    boolean[] isNull = Arrays.copyOf(cv.isNull, n);
    long[] longs = null;
    double[] doubles = null;
    HiveDecimal[] decimals = null;
    byte[][] bytes = null;
    java.sql.Timestamp[] timestamps = null;
    if (cv instanceof LongColumnVector c) {
      longs = Arrays.copyOf(c.vector, n);
    } else if (cv instanceof DoubleColumnVector c) {
      doubles = Arrays.copyOf(c.vector, n);
    } else if (cv instanceof BytesColumnVector c) {
      bytes = new byte[n][];
      for (int i = 0; i < n; i++) {
        bytes[i] = isNull[i] ? null : Arrays.copyOfRange(c.vector[i], c.start[i], c.start[i] + c.length[i]);
      }
    } else if (cv instanceof TimestampColumnVector c) {
      timestamps = new java.sql.Timestamp[n];
      for (int i = 0; i < n; i++) {
        if (!isNull[i]) {
          timestamps[i] = new java.sql.Timestamp(c.time[i]);
          timestamps[i].setNanos(c.nanos[i]);
        }
      }
    } else {
      DecimalColumnVector c = (DecimalColumnVector) cv;
      decimals = new HiveDecimal[n];
      for (int i = 0; i < n; i++) {
        decimals[i] = isNull[i] ? null : c.vector[i].getHiveDecimal();
      }
    }
    return new Batch(cv.noNulls, cv.isRepeating, isNull, longs, doubles, decimals, bytes, timestamps);
  }

  /**
   * Batches against the written values: {@code absMax} NULLs the decimal64 values a narrowed Hive precision cannot
   * hold; constant and constant_plain are the columns whose batches repeat.
   */
  protected static void assertWrittenValues(String label, int col, List<Batch> batches, long absMax) {
    int row = 0;
    for (Batch b : batches) {
      boolean sawNull = false;
      for (int i = 0; i < b.isNull.length; i++, row++) {
        long v = FLAT_VALUES[col][row];
        boolean isNull = FLAT_NULLS[col][row] || (absMax > 0 && (v < -absMax || v > absMax));
        sawNull |= isNull;
        assertEquals(label + " isNull at " + row, isNull, b.isNull[i]);
        if (isNull) {
          continue;
        }
        if (b.longs != null) {
          assertEquals(label + " at " + row, v, b.longs[i]);
        } else if (col == 3) {
          assertEquals(label + " at " + row, Float.intBitsToFloat((int) v), b.doubles[i], 0.0);
        } else {
          assertEquals(label + " at " + row, Double.longBitsToDouble(v), b.doubles[i], 0.0);
        }
      }
      assertEquals(label + " noNulls", !sawNull, b.noNulls);
      assertEquals(label + " isRepeating", col == 9 || col == 10, b.isRepeating);
    }
    assertEquals(FLAT_ROWS, row);
  }

  /** HiveDecimal batches against the written unscaled values, at the file scale; they never repeat. */
  private static void assertWrittenDecimals(String label, int col, List<Batch> batches, DecimalTypeInfo hiveType) {
    int fileScale = ((LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) FLAT_SCHEMA.getType(col)
        .getLogicalTypeAnnotation()).getScale();
    HiveDecimalWritable expected = new HiveDecimalWritable();
    int row = 0;
    for (Batch b : batches) {
      for (int i = 0; i < b.isNull.length; i++, row++) {
        expected.set(HiveDecimal.create(BigInteger.valueOf(FLAT_VALUES[col][row]), fileScale));
        expected.mutateEnforcePrecisionScale(hiveType.getPrecision(), hiveType.getScale());
        boolean isNull = FLAT_NULLS[col][row] || !expected.isSet();
        assertEquals(label + " isNull at " + row, isNull, b.isNull[i]);
        if (!isNull) {
          assertEquals(label + " at " + row, expected.getHiveDecimal(), b.decimals[i]);
        }
      }
      assertFalse(label + " isRepeating", b.isRepeating);
    }
    assertEquals(FLAT_ROWS, row);
  }

  /** Every flat column in batches of each size, decimals into decimal64 and HiveDecimal vectors. */
  protected void flatColumnsRead(WriterVersion version, boolean dictionaryEncoding) throws IOException {
    Path file = flatFile(version, dictionaryEncoding);
    for (int batchSize : FLAT_BATCH_SIZES) {
      for (int c = 0; c < FLAT_COLUMNS.length; c++) {
        String column = FLAT_COLUMNS[c];
        String label = file.getName() + " " + column + " batch " + batchSize;
        TypeInfo hiveType = flatHiveType(column);
        boolean decimal = hiveType instanceof DecimalTypeInfo;
        assertWrittenValues(label, c, readColumn(file, column, hiveType, decimal, batchSize, false), 0);
        if (decimal) {
          assertWrittenDecimals(label + " HiveDecimal", c, readColumn(file, column, hiveType, false, batchSize, false),
              (DecimalTypeInfo) hiveType);
        }
      }
    }
  }

  /**
   * Precision narrowing, DECIMAL(7,2) read as DECIMAL(5,2): decimal64 NULLs the values out of range, HiveDecimal
   * enforces the precision per value.
   */
  protected void flatDecimalNarrowingRead(WriterVersion version, boolean dictionaryEncoding) throws IOException {
    Path file = flatFile(version, dictionaryEncoding);
    DecimalTypeInfo narrow = TypeInfoFactory.getDecimalTypeInfo(5, 2);
    for (int batchSize : FLAT_BATCH_SIZES) {
      String label = file.getName() + " dec72 as " + narrow + " batch " + batchSize;
      assertWrittenValues(label, 5, readColumn(file, "dec72", narrow, true, batchSize, false), 99_999L);
      assertWrittenDecimals(label, 5, readColumn(file, "dec72", narrow, false, batchSize, false), narrow);
    }
  }

  /**
   * A dictionary that falls back to PLAIN within the chunk: both encodings are in the chunk, a batch spans the switch,
   * and the values read across it.
   */
  protected void dictionaryFallbackToPlainRead(WriterVersion version) throws IOException {
    Path file = flatFile(version, true);
    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(file, conf))) {
      for (String column : List.of("i32r", "s")) {
        Set<Encoding> encodings = reader.getFooter().getBlocks().get(0).getColumns().stream()
            .filter(c -> c.getPath().toDotString().equals(column)).findFirst().orElseThrow().getEncodings();
        assertTrue(column + " " + encodings, encodings.contains(Encoding.PLAIN)
            || encodings.contains(Encoding.DELTA_BINARY_PACKED) || encodings.contains(Encoding.DELTA_BYTE_ARRAY));
        assertTrue(column + " " + encodings,
            encodings.contains(Encoding.PLAIN_DICTIONARY) || encodings.contains(Encoding.RLE_DICTIONARY));
      }
    }
    for (int batchSize : FLAT_BATCH_SIZES) {
      assertWrittenValues(file.getName() + " i32r batch " + batchSize, 1,
          readColumn(file, "i32r", TypeInfoFactory.longTypeInfo, false, batchSize, false), 0);
    }
  }

  /** DATE from INT32 into a DateColumnVector, shifted out of the hybrid calendar unless the file is proleptic. */
  protected void flatDateRead(WriterVersion version, boolean dictionaryEncoding) throws IOException {
    Path file = flatFile(version, dictionaryEncoding);
    for (int batchSize : FLAT_BATCH_SIZES) {
      for (String column : List.of("dt", "dtr")) {
        for (boolean skipProleptic : List.of(false, true)) {
          String label = file.getName() + " " + column + " batch " + batchSize + " skipProleptic " + skipProleptic;
          int row = 0;
          for (Batch b : readColumn(file, column, TypeInfoFactory.dateTypeInfo, false, batchSize, skipProleptic)) {
            for (int i = 0; i < b.isNull.length; i++, row++) {
              boolean isNull = column.equals("dt") && row % 19 == 0;
              assertEquals(label + " isNull at " + row, isNull, b.isNull[i]);
              if (!isNull) {
                int written = flatDate(row);
                assertEquals(label + " at " + row,
                    skipProleptic ? written : CalendarUtils.convertDateToProleptic(written), b.longs[i]);
              }
            }
          }
          assertEquals(FLAT_ROWS, row);
        }
      }
    }
  }

  /** BOOLEAN: bit-packed on a PLAIN page, a bit-width-1 hybrid stream on a page v2 one. */
  protected void flatBooleanRead(WriterVersion version, boolean dictionaryEncoding) throws IOException {
    Path file = flatFile(version, dictionaryEncoding);
    for (int batchSize : FLAT_BATCH_SIZES) {
      for (String column : List.of("b", "br")) {
        String label = file.getName() + " " + column + " batch " + batchSize;
        int row = 0;
        for (Batch b : readColumn(file, column, TypeInfoFactory.booleanTypeInfo, false, batchSize, false)) {
          for (int i = 0; i < b.isNull.length; i++, row++) {
            boolean isNull = column.equals("b") && row % 23 == 0;
            assertEquals(label + " isNull at " + row, isNull, b.isNull[i]);
            if (!isNull) {
              assertEquals(label + " at " + row, flatBoolean(row) ? 1L : 0L, b.longs[i]);
            }
          }
        }
        assertEquals(FLAT_ROWS, row);
      }
    }
  }

  /** A zero-row batch decodes nothing and leaves the flags the caller set, whatever the type. */
  protected void emptyBatchRead(WriterVersion version, boolean dictionaryEncoding) throws IOException {
    Path file = flatFile(version, dictionaryEncoding);
    Map<String, TypeInfo> columns = Map.of("i32", TypeInfoFactory.intTypeInfo,
        "dec72", TypeInfoFactory.getDecimalTypeInfo(7, 2), "s", TypeInfoFactory.stringTypeInfo,
        "bin", TypeInfoFactory.binaryTypeInfo, "b", TypeInfoFactory.booleanTypeInfo, "dt", TypeInfoFactory.dateTypeInfo);
    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(file, conf))) {
      MessageType fileSchema = reader.getFooter().getFileMetaData().getSchema();
      PageReadStore pages = reader.readNextRowGroup();
      for (Map.Entry<String, TypeInfo> c : columns.entrySet()) {
        ColumnDescriptor descriptor = fileSchema.getColumnDescription(new String[] { c.getKey() });
        VectorizedPrimitiveColumnReader columnReader = new VectorizedPrimitiveColumnReader(descriptor,
            pages.getPageReader(descriptor), false, null, false, true, fileSchema.getType(c.getKey()), c.getValue());
        ColumnVector cv = newVector(c.getValue(), false, 16);
        cv.reset();
        cv.isRepeating = true;
        columnReader.readBatch(0, cv, c.getValue());
        assertTrue(file.getName() + " empty " + c.getKey() + " noNulls", cv.noNulls);
        assertTrue(file.getName() + " empty " + c.getKey() + " isRepeating", cv.isRepeating);
      }
    }
  }

  /**
   * UTF8 BINARY into STRING and BINARY whole, into CHAR and VARCHAR truncated to a character count; an unannotated
   * BINARY never truncates, not even into a CHAR.
   */
  protected void flatStringRead(WriterVersion version, boolean dictionaryEncoding) throws IOException {
    Path file = flatFile(version, dictionaryEncoding);
    List<TypeInfo> asText = List.of(TypeInfoFactory.stringTypeInfo, TypeInfoFactory.getCharTypeInfo(5),
        TypeInfoFactory.getVarcharTypeInfo(5), TypeInfoFactory.binaryTypeInfo);
    for (int batchSize : FLAT_BATCH_SIZES) {
      for (String column : List.of("s", "sr")) {
        for (TypeInfo hiveType : asText) {
          int max = hiveType instanceof BaseCharTypeInfo c ? c.getLength() : -1;
          String label = file.getName() + " " + column + " as " + hiveType.getTypeName() + " batch " + batchSize;
          int row = 0;
          for (Batch b : readColumn(file, column, hiveType, false, batchSize, false)) {
            for (int i = 0; i < b.isNull.length; i++, row++) {
              String written = column.equals("s") ? flatString(row) : flatRequiredString(row);
              assertEquals(label + " isNull at " + row, written == null, b.isNull[i]);
              if (written != null) {
                int[] points = written.codePoints().toArray();
                String kept = max > 0 && points.length > max ? new String(points, 0, max) : written;
                assertArrayEquals(label + " at " + row, kept.getBytes(StandardCharsets.UTF_8), b.bytes[i]);
              }
            }
            assertFalse(label + " isRepeating", b.isRepeating);
          }
          assertEquals(FLAT_ROWS, row);
        }
      }
      for (TypeInfo hiveType : List.of(TypeInfoFactory.binaryTypeInfo, TypeInfoFactory.getCharTypeInfo(2))) {
        String label = file.getName() + " bin as " + hiveType.getTypeName() + " batch " + batchSize;
        int row = 0;
        for (Batch b : readColumn(file, "bin", hiveType, false, batchSize, false)) {
          for (int i = 0; i < b.isNull.length; i++, row++) {
            byte[] written = flatBinary(row);
            assertEquals(label + " isNull at " + row, written == null, b.isNull[i]);
            if (written != null) {
              assertArrayEquals(label + " at " + row, written, b.bytes[i]);
            }
          }
        }
        assertEquals(FLAT_ROWS, row);
      }
    }
  }

  private static Path writeSmallFile(String name, MessageType writeSchema, boolean dictionaryEncoding, int rows,
      java.util.function.BiConsumer<Integer, Group> row) throws IOException {
    Path path = new Path(System.getProperty("java.io.tmpdir"), name + (dictionaryEncoding ? "-dictionary" : "-plain"));
    path.getFileSystem(conf).delete(path, false);
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(path).withConf(conf).withType(writeSchema)
        .withDictionaryEncoding(dictionaryEncoding).build()) {
      SimpleGroupFactory f = new SimpleGroupFactory(writeSchema);
      for (int r = 0; r < rows; r++) {
        Group g = f.newGroup();
        row.accept(r, g);
        writer.write(g);
      }
    }
    return path;
  }

  /**
   * Values the Hive type cannot hold read as NULL with value 0: BIGINT read as INT, DOUBLE as FLOAT, UINT_32 as INT and
   * INT32 as TINYINT.
   */
  protected void rangeNullsRead(boolean dictionaryEncoding) throws IOException {
    long[] longs = { 1, Integer.MAX_VALUE + 1L, -5, Long.MIN_VALUE };
    double[] doubles = { 1.5, Double.MAX_VALUE, -2.25, -1e300 };
    int[] unsigned = { 7, -1, 100, Integer.MIN_VALUE };
    int[] ints = { 1, 200, -128, -129 };
    MessageType writeSchema = parseMessageType("message m { required int64 l; required double d; "
        + "required int32 u (UINT_32); required int32 t; }");
    Path path = writeSmallFile("testRangeNulls", writeSchema, dictionaryEncoding, 200, (r, g) -> g
        .append("l", longs[r % 4]).append("d", doubles[r % 4]).append("u", unsigned[r % 4]).append("t", ints[r % 4]));
    try {
      List<Batch> l = readColumn(path, "l", TypeInfoFactory.intTypeInfo, false, 64, false);
      List<Batch> d = readColumn(path, "d", TypeInfoFactory.floatTypeInfo, false, 64, false);
      List<Batch> u = readColumn(path, "u", TypeInfoFactory.intTypeInfo, false, 64, false);
      List<Batch> t = readColumn(path, "t", TypeInfoFactory.byteTypeInfo, false, 64, false);
      for (int b = 0, row = 0; b < l.size(); b++) {
        for (int i = 0; i < l.get(b).isNull.length; i++, row++) {
          boolean isNull = row % 2 == 1;
          String at = (dictionaryEncoding ? "dictionary" : "plain") + " row " + row;
          for (Batch batch : List.of(l.get(b), d.get(b), u.get(b), t.get(b))) {
            assertEquals(at, isNull, batch.isNull[i]);
            assertFalse(at, batch.noNulls);
          }
          assertEquals(at, isNull ? 0 : longs[row % 4], l.get(b).longs[i]);
          assertEquals(at, isNull ? 0 : (float) doubles[row % 4], d.get(b).doubles[i], 0);
          assertEquals(at, isNull ? 0 : unsigned[row % 4], u.get(b).longs[i]);
          assertEquals(at, isNull ? 0 : ints[row % 4], t.get(b).longs[i]);
        }
      }
    } finally {
      path.getFileSystem(conf).delete(path, false);
    }
  }

  /** Instants around the daylight saving transitions of Los Angeles and Amsterdam in 2021, and older ones. */
  private static final long[][] INT96_INSTANTS = {
      instant("2021-03-14T09:59:59", 999_000_000), instant("2021-03-14T10:00:00", 0),
      instant("2021-03-14T10:30:00", 123_000), instant("2021-11-07T08:30:00", 0),
      instant("2021-11-07T09:00:00", 1_000_000), instant("2021-11-07T09:30:00", 999),
      instant("2021-03-28T00:59:59", 5), instant("2021-03-28T01:00:00", 0), instant("2021-10-31T00:30:00", 0),
      instant("2021-10-31T01:00:00", 0), instant("2021-10-31T01:30:00", 7), instant("1900-01-01T00:00:00", 0),
      instant("1883-11-18T12:00:00", 500_000_000), instant("1582-10-15T00:00:00", 0),
      instant("1000-06-15T12:34:56", 123_456_789), instant("1969-12-31T23:59:59", 999_999_999),
  };

  private static long[] instant(String utc, int nanos) {
    return new long[] { LocalDateTime.parse(utc).toEpochSecond(ZoneOffset.UTC), nanos };
  }

  private static NanoTime int96NanoTime(long[] instant) {
    return new NanoTime((int) (Math.floorDiv(instant[0], 86_400L) + 2_440_588),
        Math.floorMod(instant[0], 86_400L) * 1_000_000_000L + instant[1]);
  }

  /**
   * INT96 into a writer zone with daylight saving, legacy conversion on and off, against the conversion of each row on
   * its own.
   */
  protected void int96WriterZoneRead(boolean dictionaryEncoding) throws IOException {
    MessageType writeSchema = parseMessageType("message m { optional int96 ts; }");
    Path path = writeSmallFile("testInt96WriterZone", writeSchema, dictionaryEncoding, 2000, (r, g) -> {
      if (r % 11 != 0) {
        g.append("ts", int96NanoTime(INT96_INSTANTS[r % INT96_INSTANTS.length]).toBinary());
      }
    });
    try {
      for (ZoneId zone : List.of(ZoneId.of("America/Los_Angeles"), ZoneId.of("Europe/Amsterdam"))) {
        for (boolean legacy : List.of(false, true)) {
          for (int batchSize : new int[] { 1024, 333 }) {
            String label = zone + " legacy " + legacy + " batch " + batchSize;
            int row = 0;
            for (Batch b : readColumn(path, "ts", TypeInfoFactory.timestampTypeInfo, false, batchSize, false, zone,
                legacy)) {
              for (int i = 0; i < b.isNull.length; i++, row++) {
                assertEquals(label + " isNull at " + row, row % 11 == 0, b.isNull[i]);
                if (row % 11 != 0) {
                  NanoTime written = int96NanoTime(INT96_INSTANTS[row % INT96_INSTANTS.length]);
                  assertEquals(label + " at " + row,
                      NanoTimeUtils.getTimestamp(written, zone, legacy).toSqlTimestamp(), b.timestamps[i]);
                }
              }
            }
            assertEquals(2000, row);
          }
        }
      }
    } finally {
      path.getFileSystem(conf).delete(path, false);
    }
  }

  /**
   * An INT96 value the conversion rejects, a Julian 1500-02-29 that java.time has no day for, fails only the batch
   * that reads it, as the per-value conversion does.
   */
  protected void rejectedTimestampRead(boolean dictionaryEncoding) throws IOException {
    MessageType writeSchema = parseMessageType("message m { required int96 ts; }");
    NanoTime bad = new NanoTime(2_268_992, 0);
    Path path = writeSmallFile("testRejectedTimestamp", writeSchema, dictionaryEncoding, 2000, (r, g) ->
        g.append("ts", r == 1500 ? bad.toBinary() : new NanoTime(2_459_000 + r % 10, 0).toBinary()));
    String expected = null;
    try {
      NanoTimeUtils.getTimestamp(bad, ZoneOffset.UTC, false);
    } catch (RuntimeException e) {
      expected = e.toString();
    }
    assertTrue("the conversion rejects " + bad, expected != null);
    TypeInfo timestamp = TypeInfoFactory.timestampTypeInfo;
    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, conf))) {
      ColumnDescriptor descriptor = writeSchema.getColumnDescription(new String[] { "ts" });
      PageReadStore pages = reader.readNextRowGroup();
      VectorizedPrimitiveColumnReader columnReader = new VectorizedPrimitiveColumnReader(descriptor,
          pages.getPageReader(descriptor), false, ZoneOffset.UTC, false, false, writeSchema.getType("ts"), timestamp);
      TimestampColumnVector cv = new TimestampColumnVector(1000);
      cv.reset();
      columnReader.readBatch(1000, cv, timestamp);
      assertEquals(NanoTimeUtils.getTimestamp(new NanoTime(2_459_009, 0), ZoneOffset.UTC, false).toEpochMilli(),
          cv.getTime(999));
      cv.reset();
      String actual = null;
      try {
        columnReader.readBatch(1000, cv, timestamp);
      } catch (RuntimeException e) {
        actual = e.toString();
      }
      assertEquals(expected, actual);
    } finally {
      path.getFileSystem(conf).delete(path, false);
    }
  }

  /**
   * A constant struct field is not flagged repeating, which only the caller allows; a constant top-level column is.
   */
  protected void structFieldNotRepeating(boolean dictionaryEncoding) throws Exception {
    MessageType writeSchema = parseMessageType("message hive_schema { optional group s { required int32 c; } "
        + "required int32 k; }");
    Path path = writeSmallFile("testStructFieldNotRepeating", writeSchema, dictionaryEncoding, nElements,
        (r, g) -> {
          g.addGroup("s").append("c", 42);
          g.append("k", 42);
        });
    Configuration readerConf = new Configuration();
    readerConf.set(IOConstants.COLUMNS, "s,k");
    readerConf.set(IOConstants.COLUMNS_TYPES, "struct<c:int>,int");
    readerConf.setBoolean(ColumnProjectionUtils.READ_ALL_COLUMNS, false);
    readerConf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, "0,1");
    VectorizedParquetRecordReader reader = createTestParquetReader(writeSchema.toString(), readerConf, null, path);
    VectorizedRowBatch batch = reader.createValue();
    try {
      int batches = 0;
      while (reader.next(NullWritable.get(), batch)) {
        LongColumnVector c = (LongColumnVector) ((StructColumnVector) batch.cols[0]).fields[0];
        assertEquals(42, c.vector[batch.size - 1]);
        assertFalse("struct field batch " + batches, c.isRepeating);
        assertTrue("top-level batch " + batches, batch.cols[1].isRepeating);
        assertEquals(42, ((LongColumnVector) batch.cols[1]).vector[0]);
        batches++;
      }
      assertTrue(batches > 1);
    } finally {
      reader.close();
      path.getFileSystem(conf).delete(path, false);
    }
  }
}
