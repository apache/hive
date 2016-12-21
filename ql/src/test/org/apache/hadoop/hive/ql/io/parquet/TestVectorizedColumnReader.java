/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.io.parquet;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.parquet.read.DataWritableReadSupport;
import org.apache.hadoop.hive.ql.io.parquet.serde.ArrayWritableObjectInspector;
import org.apache.hadoop.hive.ql.io.parquet.vector.VectorizedParquetRecordReader;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Random;

import static junit.framework.Assert.assertTrue;
import static junit.framework.TestCase.assertFalse;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0;
import static org.apache.parquet.hadoop.api.ReadSupport.PARQUET_READ_SCHEMA;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.GZIP;
import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;
import static org.junit.Assert.assertEquals;

public class TestVectorizedColumnReader {

  private static final int nElements = 2500;
  protected static final Configuration conf = new Configuration();
  protected static final Path file =
    new Path("target/test/TestParquetVectorReader/testParquetFile");
  private static String[] uniqueStrs = new String[nElements];
  private static boolean[] isNulls = new boolean[nElements];
  private static Random random = new Random();
  protected static final MessageType schema = parseMessageType(
    "message test { "
      + "required int32 int32_field; "
      + "required int64 int64_field; "
      + "required int96 int96_field; "
      + "required double double_field; "
      + "required float float_field; "
      + "required boolean boolean_field; "
      + "required fixed_len_byte_array(3) flba_field; "
      + "optional fixed_len_byte_array(1) some_null_field; "
      + "optional fixed_len_byte_array(1) all_null_field; "
      + "optional binary binary_field; "
      + "optional binary binary_field_non_repeating; "
      + "} ");

  @AfterClass
  public static void cleanup() throws IOException {
    FileSystem fs = file.getFileSystem(conf);
    if (fs.exists(file)) {
      fs.delete(file, true);
    }
  }

  @BeforeClass
  public static void prepareFile() throws IOException {
    cleanup();

    boolean dictionaryEnabled = true;
    boolean validating = false;
    GroupWriteSupport.setSchema(schema, conf);
    SimpleGroupFactory f = new SimpleGroupFactory(schema);
    ParquetWriter<Group> writer = new ParquetWriter<Group>(
      file,
      new GroupWriteSupport(),
      GZIP, 1024*1024, 1024, 1024*1024,
      dictionaryEnabled, validating, PARQUET_1_0, conf);
    writeData(f, writer);
  }

  protected static void writeData(SimpleGroupFactory f, ParquetWriter<Group> writer) throws IOException {
    initialStrings(uniqueStrs);
    for (int i = 0; i < nElements; i++) {
      Group group = f.newGroup()
        .append("int32_field", i)
        .append("int64_field", (long) 2 * i)
        .append("int96_field", Binary.fromReusedByteArray("999999999999".getBytes()))
        .append("double_field", i * 1.0)
        .append("float_field", ((float) (i * 2.0)))
        .append("boolean_field", i % 5 == 0)
        .append("flba_field", "abc");

      if (i % 2 == 1) {
        group.append("some_null_field", "x");
      }

      if (i % 13 != 1) {
        int binaryLen = i % 10;
        group.append("binary_field",
          Binary.fromString(new String(new char[binaryLen]).replace("\0", "x")));
      }

      if (uniqueStrs[i] != null) {
        group.append("binary_field_non_repeating", Binary.fromString(uniqueStrs[i]));
      }
      writer.write(group);
    }
    writer.close();
  }

  private static String getRandomStr() {
    int len = random.nextInt(10);
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < len; i++) {
      sb.append((char) ('a' + random.nextInt(25)));
    }
    return sb.toString();
  }

  public static void initialStrings(String[] uniqueStrs) {
    for (int i = 0; i < uniqueStrs.length; i++) {
      String str = getRandomStr();
      if (!str.isEmpty()) {
        uniqueStrs[i] = str;
        isNulls[i] = false;
      }else{
        isNulls[i] = true;
      }
    }
  }

  private VectorizedParquetRecordReader createParquetReader(String schemaString, Configuration conf)
    throws IOException, InterruptedException, HiveException {
    conf.set(PARQUET_READ_SCHEMA, schemaString);
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, true);
    HiveConf.setVar(conf, HiveConf.ConfVars.PLAN, "//tmp");

    Job vectorJob = new Job(conf, "read vector");
    ParquetInputFormat.setInputPaths(vectorJob, file);
    ParquetInputFormat parquetInputFormat = new ParquetInputFormat(GroupReadSupport.class);
    InputSplit split = (InputSplit) parquetInputFormat.getSplits(vectorJob).get(0);
    initialVectorizedRowBatchCtx(conf);
    return new VectorizedParquetRecordReader(split, new JobConf(conf));
  }

  private void initialVectorizedRowBatchCtx(Configuration conf) throws HiveException {
    MapWork mapWork = new MapWork();
    VectorizedRowBatchCtx rbCtx = new VectorizedRowBatchCtx();
    rbCtx.init(createStructObjectInspector(conf), new String[0]);
    mapWork.setVectorMode(true);
    mapWork.setVectorizedRowBatchCtx(rbCtx);
    Utilities.setMapWork(conf, mapWork);
  }

  private StructObjectInspector createStructObjectInspector(Configuration conf) {
    // Create row related objects
    String columnNames = conf.get(IOConstants.COLUMNS);
    List<String> columnNamesList = DataWritableReadSupport.getColumnNames(columnNames);
    String columnTypes = conf.get(IOConstants.COLUMNS_TYPES);
    List<TypeInfo> columnTypesList = DataWritableReadSupport.getColumnTypes(columnTypes);
    TypeInfo rowTypeInfo = TypeInfoFactory.getStructTypeInfo(columnNamesList, columnTypesList);
    return new ArrayWritableObjectInspector((StructTypeInfo) rowTypeInfo);
  }

  @Test
  public void testIntRead() throws Exception {
    Configuration conf = new Configuration();
    conf.set(IOConstants.COLUMNS,"int32_field");
    conf.set(IOConstants.COLUMNS_TYPES,"int");
    conf.setBoolean(ColumnProjectionUtils.READ_ALL_COLUMNS, false);
    conf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, "0");
    VectorizedParquetRecordReader reader =
      createParquetReader("message test { required int32 int32_field;}", conf);
    VectorizedRowBatch previous = reader.createValue();
    try {
      long c = 0;
      while (reader.next(NullWritable.get(), previous)) {
        LongColumnVector vector = (LongColumnVector) previous.cols[0];
        assertTrue(vector.noNulls);
        for (int i = 0; i < vector.vector.length; i++) {
          if(c == nElements){
            break;
          }
          assertEquals(c, vector.vector[i]);
          assertFalse(vector.isNull[i]);
          c++;
        }
      }
      assertEquals(nElements, c);
    } finally {
      reader.close();
    }
  }

  @Test
  public void testLongRead() throws Exception {
    Configuration conf = new Configuration();
    conf.set(IOConstants.COLUMNS,"int64_field");
    conf.set(IOConstants.COLUMNS_TYPES, "bigint");
    conf.setBoolean(ColumnProjectionUtils.READ_ALL_COLUMNS, false);
    conf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, "0");
    VectorizedParquetRecordReader reader =
      createParquetReader("message test { required int64 int64_field;}", conf);
    VectorizedRowBatch previous = reader.createValue();
    try {
      long c = 0;
      while (reader.next(NullWritable.get(), previous)) {
        LongColumnVector vector = (LongColumnVector) previous.cols[0];
        assertTrue(vector.noNulls);
        for (int i = 0; i < vector.vector.length; i++) {
          if(c == nElements){
            break;
          }
          assertEquals(2 * c, vector.vector[i]);
          assertFalse(vector.isNull[i]);
          c++;
        }
      }
      assertEquals(nElements, c);
    } finally {
      reader.close();
    }
  }

  @Test
  public void testDoubleRead() throws Exception {
    Configuration conf = new Configuration();
    conf.set(IOConstants.COLUMNS,"double_field");
    conf.set(IOConstants.COLUMNS_TYPES, "double");
    conf.setBoolean(ColumnProjectionUtils.READ_ALL_COLUMNS, false);
    conf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, "0");
    VectorizedParquetRecordReader reader =
      createParquetReader("message test { required double double_field;}", conf);
    VectorizedRowBatch previous = reader.createValue();
    try {
      long c = 0;
      while (reader.next(NullWritable.get(), previous)) {
        DoubleColumnVector vector = (DoubleColumnVector) previous.cols[0];
        assertTrue(vector.noNulls);
        for (int i = 0; i < vector.vector.length; i++) {
          if(c == nElements){
            break;
          }
          assertEquals(1.0 * c, vector.vector[i], 0);
          assertFalse(vector.isNull[i]);
          c++;
        }
      }
      assertEquals(nElements, c);
    } finally {
      reader.close();
    }
  }

  @Test
  public void testFloatRead() throws Exception {
    Configuration conf = new Configuration();
    conf.set(IOConstants.COLUMNS,"float_field");
    conf.set(IOConstants.COLUMNS_TYPES, "float");
    conf.setBoolean(ColumnProjectionUtils.READ_ALL_COLUMNS, false);
    conf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, "0");
    VectorizedParquetRecordReader reader =
      createParquetReader("message test { required float float_field;}", conf);
    VectorizedRowBatch previous = reader.createValue();
    try {
      long c = 0;
      while (reader.next(NullWritable.get(), previous)) {
        DoubleColumnVector vector = (DoubleColumnVector) previous.cols[0];
        assertTrue(vector.noNulls);
        for (int i = 0; i < vector.vector.length; i++) {
          if(c == nElements){
            break;
          }
          assertEquals((float)2.0 * c, vector.vector[i], 0);
          assertFalse(vector.isNull[i]);
          c++;
        }
      }
      assertEquals(nElements, c);
    } finally {
      reader.close();
    }
  }

  @Test
  public void testBooleanRead() throws Exception {
    Configuration conf = new Configuration();
    conf.set(IOConstants.COLUMNS,"boolean_field");
    conf.set(IOConstants.COLUMNS_TYPES, "boolean");
    conf.setBoolean(ColumnProjectionUtils.READ_ALL_COLUMNS, false);
    conf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, "0");
    VectorizedParquetRecordReader reader =
      createParquetReader("message test { required boolean boolean_field;}", conf);
    VectorizedRowBatch previous = reader.createValue();
    try {
      long c = 0;
      while (reader.next(NullWritable.get(), previous)) {
        LongColumnVector vector = (LongColumnVector) previous.cols[0];
        assertTrue(vector.noNulls);
        for (int i = 0; i < vector.vector.length; i++) {
          if(c == nElements){
            break;
          }
          int e = (c % 5 == 0) ? 1 : 0;
          assertEquals(e, vector.vector[i]);
          assertFalse(vector.isNull[i]);
          c++;
        }
      }
      assertEquals(nElements, c);
    } finally {
      reader.close();
    }
  }

  @Test
  public void testBinaryReadDictionaryEncoding() throws Exception {
    Configuration conf = new Configuration();
    conf.set(IOConstants.COLUMNS,"binary_field");
    conf.set(IOConstants.COLUMNS_TYPES, "string");
    conf.setBoolean(ColumnProjectionUtils.READ_ALL_COLUMNS, false);
    conf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, "0");
    VectorizedParquetRecordReader reader =
      createParquetReader("message test { required binary binary_field;}", conf);
    VectorizedRowBatch previous = reader.createValue();
    int c = 0;
    try {
      while (reader.next(NullWritable.get(), previous)) {
        BytesColumnVector vector = (BytesColumnVector) previous.cols[0];
        boolean noNull = true;
        for (int i = 0; i < vector.vector.length; i++) {
          if(c == nElements){
            break;
          }
          if (c % 13 == 1) {
            assertTrue(vector.isNull[i]);
          } else {
            assertFalse(vector.isNull[i]);
            int binaryLen = c % 10;
            String expected = new String(new char[binaryLen]).replace("\0", "x");
            String actual = new String(ArrayUtils
              .subarray(vector.vector[i], vector.start[i], vector.start[i] + vector.length[i]));
            assertEquals("Failed at " + c, expected, actual);
            noNull = false;
          }
          c++;
        }
        assertEquals("No Null check failed at " + c, noNull, vector.noNulls);
        assertFalse(vector.isRepeating);
      }
      assertEquals(nElements, c);
    } finally {
      reader.close();
    }
  }

  @Test
  public void testBinaryRead() throws Exception {
    Configuration conf = new Configuration();
    conf.set(IOConstants.COLUMNS,"binary_field_non_repeating");
    conf.set(IOConstants.COLUMNS_TYPES, "string");
    conf.setBoolean(ColumnProjectionUtils.READ_ALL_COLUMNS, false);
    conf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, "0");
    VectorizedParquetRecordReader reader =
      createParquetReader("message test { required binary binary_field_non_repeating;}", conf);
    VectorizedRowBatch previous = reader.createValue();
    int c = 0;
    try {
      while (reader.next(NullWritable.get(), previous)) {
        BytesColumnVector vector = (BytesColumnVector) previous.cols[0];
        boolean noNull = true;
        for (int i = 0; i < vector.vector.length; i++) {
          if(c == nElements){
            break;
          }
          String actual;
          assertEquals("Null assert failed at " + c, isNulls[c], vector.isNull[i]);
          if (!vector.isNull[i]) {
            actual = new String(ArrayUtils
              .subarray(vector.vector[i], vector.start[i], vector.start[i] + vector.length[i]));
            assertEquals("failed at " + c, uniqueStrs[c], actual);
          }else{
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
}
