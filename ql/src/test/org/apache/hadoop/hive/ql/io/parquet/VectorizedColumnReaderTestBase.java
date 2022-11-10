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

package org.apache.hadoop.hive.ql.io.parquet;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
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
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;

import static junit.framework.Assert.assertTrue;
import static junit.framework.TestCase.assertFalse;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0;
import static org.apache.parquet.hadoop.api.ReadSupport.PARQUET_READ_SCHEMA;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.GZIP;
import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;
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
    conf.set(PARQUET_READ_SCHEMA, schemaString);
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, true);
    HiveConf.setVar(conf, HiveConf.ConfVars.PLAN, "//tmp");
    Job vectorJob = new Job(conf, "read vector");
    ParquetInputFormat.setInputPaths(vectorJob, file);
    initialVectorizedRowBatchCtx(conf);
    return new VectorizedParquetRecordReader(getFileSplit(vectorJob), new JobConf(conf));
  }

  protected static FileSplit getFileSplit(Job vectorJob) throws IOException, InterruptedException {
    ParquetInputFormat parquetInputFormat = new ParquetInputFormat(GroupReadSupport.class);
    InputSplit split = (InputSplit) parquetInputFormat.getSplits(vectorJob).get(0);
    FileSplit fsplit = new FileSplit(file, 0L, split.getLength(), split.getLocations());
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

      Group some_null_g = group.addGroup("struct_field_some_null");
      if (i % 2 != 0) {
        some_null_g.append("f", intVal);
      }
      if (i % 3 != 0) {
        some_null_g.append("g", doubleVal);
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
    MapWork mapWork = new MapWork();
    VectorizedRowBatchCtx rbCtx = new VectorizedRowBatchCtx();
    rbCtx.init(createStructObjectInspector(conf), new String[0]);
    mapWork.setVectorMode(true);
    mapWork.setVectorizedRowBatchCtx(rbCtx);
    Utilities.setMapWork(conf, mapWork);
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
}
