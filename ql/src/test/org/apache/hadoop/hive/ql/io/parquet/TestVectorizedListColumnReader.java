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
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.parquet.vector.VectorizedParquetRecordReader;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.api.Binary;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestVectorizedListColumnReader extends VectorizedColumnReaderTestBase {

  protected static void writeListData(ParquetWriter<Group> writer, boolean isDictionaryEncoding,
    int elementNum) throws IOException {
    SimpleGroupFactory f = new SimpleGroupFactory(schema);
    int listMaxSize = 4;
    int listElementIndex = 0;
    for (int i = 0; i < elementNum; i++) {
      boolean isNull = isNull(i);
      Group group = f.newGroup();

      int listSize = i % listMaxSize + 1;
      if (!isNull) {
        for (int j = 0; j < listSize; j++) {
          group.append("list_int32_field", getIntValue(isDictionaryEncoding, listElementIndex));
          group.append("list_int64_field", getLongValue(isDictionaryEncoding, listElementIndex));
          group.append("list_double_field", getDoubleValue(isDictionaryEncoding, listElementIndex));
          group.append("list_float_field", getFloatValue(isDictionaryEncoding, listElementIndex));
          group.append("list_boolean_field", getBooleanValue(listElementIndex));
          group.append("list_binary_field", getBinaryValue(isDictionaryEncoding, listElementIndex));

          HiveDecimal hd = getDecimal(isDictionaryEncoding, listElementIndex).setScale(2);
          HiveDecimalWritable hdw = new HiveDecimalWritable(hd);
          group.append("list_decimal_field", Binary.fromConstantByteArray(hdw.getInternalStorage()));
          listElementIndex++;
        }
      }
      for (int j = 0; j < listMaxSize; j++) {
        group.append("list_binary_field_for_repeat_test", getBinaryValue(isDictionaryEncoding, i));
      }

      writer.write(group);
    }
    writer.close();
  }

  protected static void writeRepeateListData(ParquetWriter<Group> writer,
    int elementNum, boolean isNull) throws IOException {
    SimpleGroupFactory f = new SimpleGroupFactory(schema);
    int listMaxSize = 4;
    for (int i = 0; i < elementNum; i++) {
      Group group = f.newGroup();
      if (!isNull) {
        for (int j = 0; j < listMaxSize; j++) {
          group.append("list_int32_field_for_repeat_test", j);
        }
      }
      writer.write(group);
    }
    writer.close();
  }

  @Test
  public void testListReadLessOneBatch() throws Exception {
    boolean isDictionaryEncoding = false;
    removeFile();
    writeListData(initWriterFromFile(), isDictionaryEncoding, 1023);
    testListReadAllType(isDictionaryEncoding, 1023);
    removeFile();
    isDictionaryEncoding = true;
    writeListData(initWriterFromFile(), isDictionaryEncoding, 1023);
    testListReadAllType(isDictionaryEncoding, 1023);
    removeFile();
  }

  @Test
  public void testListReadEqualOneBatch() throws Exception {
    boolean isDictionaryEncoding = false;
    removeFile();
    writeListData(initWriterFromFile(), isDictionaryEncoding, 1024);
    testListReadAllType(isDictionaryEncoding, 1024);
    removeFile();
    isDictionaryEncoding = true;
    writeListData(initWriterFromFile(), isDictionaryEncoding, 1024);
    testListReadAllType(isDictionaryEncoding, 1024);
    removeFile();
  }

  @Test
  public void testListReadMoreOneBatch() throws Exception {
    boolean isDictionaryEncoding = false;
    removeFile();
    writeListData(initWriterFromFile(), isDictionaryEncoding, 1025);
    testListReadAllType(isDictionaryEncoding, 1025);
    removeFile();
    isDictionaryEncoding = true;
    writeListData(initWriterFromFile(), isDictionaryEncoding, 1025);
    testListReadAllType(isDictionaryEncoding, 1025);
    removeFile();
  }

  @Test
  public void testRepeateListRead() throws Exception {
    removeFile();
    writeRepeateListData(initWriterFromFile(), 1023, false);
    testRepeateListRead(1023, false);
    removeFile();
    writeRepeateListData(initWriterFromFile(), 1023, true);
    testRepeateListRead(1023, true);
    removeFile();
    writeRepeateListData(initWriterFromFile(), 1024, false);
    testRepeateListRead(1024, false);
    removeFile();
    writeRepeateListData(initWriterFromFile(), 1024, true);
    testRepeateListRead(1024, true);
    removeFile();
    writeRepeateListData(initWriterFromFile(), 1025, false);
    testRepeateListRead(1025, false);
    removeFile();
    writeRepeateListData(initWriterFromFile(), 1025, true);
    testRepeateListRead(1025, true);
    removeFile();
  }

  @Test
  public void testUnrepeatedStringWithoutNullListRead() throws Exception {
    removeFile();
    writeListData(initWriterFromFile(), false, 1025);
    testUnRepeateStringWithoutNullListRead();
    removeFile();
  }

  @Test
  public void testVectorizedRowBatchSizeChange() throws Exception {
    removeFile();
    writeListData(initWriterFromFile(), false, 1200);
    testVectorizedRowBatchSizeChangeListRead();
    removeFile();
  }

  private void testListReadAllType(boolean isDictionaryEncoding, int elementNum) throws Exception {
    testListRead(isDictionaryEncoding, "int", elementNum);
    testListRead(isDictionaryEncoding, "long", elementNum);
    testListRead(isDictionaryEncoding, "double", elementNum);
    testListRead(isDictionaryEncoding, "float", elementNum);
    testListRead(isDictionaryEncoding, "boolean", elementNum);
    testListRead(isDictionaryEncoding, "binary", elementNum);
    testListRead(isDictionaryEncoding, "decimal", elementNum);
  }

  private void setTypeConfiguration(String type, Configuration conf) {
    if ("int".equals(type)) {
      conf.set(IOConstants.COLUMNS, "list_int32_field");
      conf.set(IOConstants.COLUMNS_TYPES, "array<int>");
    } else if ("long".equals(type)) {
      conf.set(IOConstants.COLUMNS, "list_int64_field");
      conf.set(IOConstants.COLUMNS_TYPES, "array<bigint>");
    } else if ("double".equals(type)) {
      conf.set(IOConstants.COLUMNS, "list_double_field");
      conf.set(IOConstants.COLUMNS_TYPES, "array<double>");
    } else if ("float".equals(type)) {
      conf.set(IOConstants.COLUMNS, "list_float_field");
      conf.set(IOConstants.COLUMNS_TYPES, "array<float>");
    } else if ("boolean".equals(type)) {
      conf.set(IOConstants.COLUMNS, "list_boolean_field");
      conf.set(IOConstants.COLUMNS_TYPES, "array<boolean>");
    } else if ("binary".equals(type)) {
      conf.set(IOConstants.COLUMNS, "list_binary_field");
      conf.set(IOConstants.COLUMNS_TYPES, "array<string>");
    } else if ("decimal".equals(type)) {
      conf.set(IOConstants.COLUMNS, "list_decimal_field");
      conf.set(IOConstants.COLUMNS_TYPES, "array<decimal(5,2)>");
    }
  }

  private String getSchema(String type) {
    if ("int".equals(type)) {
      return "message hive_schema {repeated int32 list_int32_field;}";
    } else if ("long".equals(type)) {
      return "message hive_schema {repeated int64 list_int64_field;}";
    } else if ("double".equals(type)) {
      return "message hive_schema {repeated double list_double_field;}";
    } else if ("float".equals(type)) {
      return "message hive_schema {repeated float list_float_field;}";
    } else if ("boolean".equals(type)) {
      return "message hive_schema {repeated boolean list_boolean_field;}";
    } else if ("binary".equals(type)) {
      return "message hive_schema {repeated binary list_binary_field;}";
    } else if ("decimal".equals(type)) {
      return "message hive_schema {repeated binary list_decimal_field (DECIMAL(5,2));}";
    } else {
      throw new RuntimeException("Unsupported type for TestVectorizedListColumnReader!");
    }
  }

  private void assertValue(String type, ColumnVector childVector,
    boolean isDictionaryEncoding, int valueIndex, int position) {
    if ("int".equals(type)) {
      assertEquals(getIntValue(isDictionaryEncoding, valueIndex), ((LongColumnVector)childVector).vector[position]);
    } else if ("long".equals(type)) {
      assertEquals(getLongValue(isDictionaryEncoding, valueIndex), ((LongColumnVector)childVector).vector[position]);
    } else if ("double".equals(type)) {
      assertEquals(getDoubleValue(isDictionaryEncoding, valueIndex), ((DoubleColumnVector)childVector).vector[position], 0);
    } else if ("float".equals(type)) {
      assertEquals(getFloatValue(isDictionaryEncoding, valueIndex), ((DoubleColumnVector)childVector).vector[position], 0);
    } else if ("boolean".equals(type)) {
      assertEquals((getBooleanValue(valueIndex) ? 1 : 0), ((LongColumnVector)childVector).vector[position]);
    } else if ("binary".equals(type)) {
      String actual = new String(ArrayUtils
          .subarray(((BytesColumnVector)childVector).vector[position], ((BytesColumnVector)childVector).start[position],
              ((BytesColumnVector)childVector).start[position] + ((BytesColumnVector)childVector).length[position]));
      assertEquals(getStr(isDictionaryEncoding, valueIndex), actual);
    } else if ("decimal".equals(type)) {
      assertEquals(getDecimal(isDictionaryEncoding, valueIndex),
          ((DecimalColumnVector)childVector).vector[position].getHiveDecimal());
    } else {
      throw new RuntimeException("Unsupported type for TestVectorizedListColumnReader!");
    }

  }

  private void testListRead(boolean isDictionaryEncoding, String type, int elementNum) throws Exception {
    Configuration conf = new Configuration();
    setTypeConfiguration(type, conf);
    conf.setBoolean(ColumnProjectionUtils.READ_ALL_COLUMNS, false);
    conf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, "0");
    VectorizedParquetRecordReader reader = createTestParquetReader(getSchema(type), conf);
    VectorizedRowBatch previous = reader.createValue();
    int row = 0;
    int index = 0;
    try {
      while (reader.next(NullWritable.get(), previous)) {
        ListColumnVector vector = (ListColumnVector) previous.cols[0];

        //since Repeating only happens when offset length is 1.
        assertEquals((vector.offsets.length == 1),vector.isRepeating);

        for (int i = 0; i < vector.offsets.length; i++) {
          if (row == elementNum) {
            assertEquals(i, vector.offsets.length - 1);
            break;
          }
          long start = vector.offsets[i];
          long length = vector.lengths[i];
          boolean isNull = isNull(row);
          if (isNull) {
            assertEquals(vector.isNull[i], true);
          } else {
            for (long j = 0; j < length; j++) {
              assertValue(type, vector.child, isDictionaryEncoding, index, (int) (start + j));
              index++;
            }
          }
          row++;
        }
      }
      assertEquals("It doesn't exit at expected position", elementNum, row);
    } finally {
      reader.close();
    }
  }

  private void testRepeateListRead(int elementNum, boolean isNull) throws Exception {
    Configuration conf = new Configuration();
    conf.set(IOConstants.COLUMNS, "list_int32_field_for_repeat_test");
    conf.set(IOConstants.COLUMNS_TYPES, "array<int>");
    conf.setBoolean(ColumnProjectionUtils.READ_ALL_COLUMNS, false);
    conf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, "0");
    VectorizedParquetRecordReader reader = createTestParquetReader(
        "message hive_schema {repeated int32 list_int32_field_for_repeat_test;}", conf);
    VectorizedRowBatch previous = reader.createValue();
    int row = 0;
    try {
      while (reader.next(NullWritable.get(), previous)) {
        ListColumnVector vector = (ListColumnVector) previous.cols[0];

        assertTrue(vector.isRepeating);
        assertEquals(isNull, vector.isNull[0]);

        for (int i = 0; i < vector.offsets.length; i++) {
          if (row == elementNum) {
            assertEquals(i, vector.offsets.length - 1);
            break;
          }
          row++;
        }
      }
      assertEquals("It doesn't exit at expected position", elementNum, row);
    } finally {
      reader.close();
    }
  }

  private void testUnRepeateStringWithoutNullListRead() throws Exception {
    Configuration conf = new Configuration();
    conf.set(IOConstants.COLUMNS, "list_binary_field_for_repeat_test");
    conf.set(IOConstants.COLUMNS_TYPES, "array<string>");
    conf.setBoolean(ColumnProjectionUtils.READ_ALL_COLUMNS, false);
    conf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, "0");
    VectorizedParquetRecordReader reader = createTestParquetReader(
        "message hive_schema {repeated binary list_binary_field_for_repeat_test;}", conf);
    VectorizedRowBatch previous = reader.createValue();
    try {
      while (reader.next(NullWritable.get(), previous)) {
        ListColumnVector vector = (ListColumnVector) previous.cols[0];
        assertEquals((vector.offsets.length == 1),vector.isRepeating);
      }
    } finally {
      reader.close();
    }
  }

  private void testVectorizedRowBatchSizeChangeListRead() throws Exception {
    Configuration conf = new Configuration();
    conf.set(IOConstants.COLUMNS, "list_binary_field_for_repeat_test");
    conf.set(IOConstants.COLUMNS_TYPES, "array<string>");
    conf.setBoolean(ColumnProjectionUtils.READ_ALL_COLUMNS, false);
    conf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, "0");
    VectorizedParquetRecordReader reader = createTestParquetReader(
        "message hive_schema {repeated binary list_binary_field_for_repeat_test;}", conf);
    VectorizedRowBatch previous = reader.createValue();
    try {
      while (reader.next(NullWritable.get(), previous)) {
        ListColumnVector vector = (ListColumnVector) previous.cols[0];
        // When deal with big data, the VectorizedRowBatch will be used for the different file split
        // to cache the data. Here is the situation: the first split only have 100 rows,
        // and VectorizedRowBatch cache them, meanwhile, the size of VectorizedRowBatch will be
        // updated to 100. The following code is to simulate the size change, but there will be no
        // ArrayIndexOutOfBoundsException when process the next split which has more than 100 rows.
        vector.lengths = new long[100];
        vector.offsets = new long[100];
      }
    } finally {
      reader.close();
    }
  }
}
