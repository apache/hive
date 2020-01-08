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

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
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

public class TestVectorizedMapColumnReader extends VectorizedColumnReaderTestBase {

  protected static void writeMapData(ParquetWriter<Group> writer, boolean isDictionaryEncoding,
    int elementNum) throws IOException {
    SimpleGroupFactory f = new SimpleGroupFactory(schema);
    int mapMaxSize = 4;
    int mapElementIndex = 0;
    for (int i = 0; i < elementNum; i++) {
      boolean isNull = isNull(i);
      Group group = f.newGroup();

      int mapSize = i % mapMaxSize + 1;
      if (!isNull) {
        // the map_field is to test multiple level map definition
        Group multipleLevelGroup = group.addGroup("map_field");
        for (int j = 0; j < mapSize; j++) {
          int intValForMap = getIntValue(isDictionaryEncoding, mapElementIndex);
          long longValForMap = getLongValue(isDictionaryEncoding, mapElementIndex);
          double doubleValForMap = getDoubleValue(isDictionaryEncoding, mapElementIndex);
          float floatValForMap = getFloatValue(isDictionaryEncoding, mapElementIndex);
          Binary binaryValForMap = getBinaryValue(isDictionaryEncoding, mapElementIndex);
          HiveDecimal hd = getDecimal(isDictionaryEncoding, mapElementIndex).setScale(2);
          HiveDecimalWritable hdw = new HiveDecimalWritable(hd);
          Binary decimalValForMap = Binary.fromConstantByteArray(hdw.getInternalStorage());
          group.addGroup("map_int32").append("key", intValForMap).append("value", intValForMap);
          group.addGroup("map_int64").append("key", longValForMap).append("value", longValForMap);
          group.addGroup("map_double").append("key", doubleValForMap)
              .append("value", doubleValForMap);
          group.addGroup("map_float").append("key", floatValForMap).append("value", floatValForMap);
          group.addGroup("map_binary").append("key", binaryValForMap)
              .append("value", binaryValForMap);
          group.addGroup("map_decimal").append("key", decimalValForMap)
              .append("value", decimalValForMap);
          multipleLevelGroup.addGroup("map").append("key", binaryValForMap)
              .append("value", binaryValForMap);
          mapElementIndex++;
        }
      }
      writer.write(group);
    }
    writer.close();
  }

  protected static void writeRepeateMapData(
    ParquetWriter<Group> writer, int elementNum, boolean isNull) throws IOException {
    SimpleGroupFactory f = new SimpleGroupFactory(schema);
    int mapMaxSize = 4;
    for (int i = 0; i < elementNum; i++) {
      Group group = f.newGroup();
      if (!isNull) {
        for (int j = 0; j < mapMaxSize; j++) {
          group.addGroup("map_int32_for_repeat_test").append("key", j).append("value", j);
        }
      }
      writer.write(group);
    }
    writer.close();
  }

  @Test
  public void testMapReadLessOneBatch() throws Exception {
    boolean isDictionaryEncoding = false;
    removeFile();
    writeMapData(initWriterFromFile(), isDictionaryEncoding, 1023);
    testMapReadAllType(isDictionaryEncoding, 1023);
    removeFile();
    isDictionaryEncoding = true;
    writeMapData(initWriterFromFile(), isDictionaryEncoding, 1023);
    testMapReadAllType(isDictionaryEncoding, 1023);
    removeFile();
  }

  @Test
  public void testMapReadEqualOneBatch() throws Exception {
    boolean isDictionaryEncoding = false;
    removeFile();
    writeMapData(initWriterFromFile(), isDictionaryEncoding, 1024);
    testMapReadAllType(isDictionaryEncoding, 1024);
    removeFile();
    isDictionaryEncoding = true;
    writeMapData(initWriterFromFile(), isDictionaryEncoding, 1024);
    testMapReadAllType(isDictionaryEncoding, 1024);
    removeFile();
  }

  @Test
  public void testMapReadMoreOneBatch() throws Exception {
    boolean isDictionaryEncoding = false;
    removeFile();
    writeMapData(initWriterFromFile(), isDictionaryEncoding, 1025);
    testMapReadAllType(isDictionaryEncoding, 1025);
    removeFile();
    isDictionaryEncoding = true;
    writeMapData(initWriterFromFile(), isDictionaryEncoding, 1025);
    testMapReadAllType(isDictionaryEncoding, 1025);
    removeFile();
  }

  @Test
  public void testRepeateMapRead() throws Exception {
    removeFile();
    writeRepeateMapData(initWriterFromFile(), 1023, false);
    testRepeateMapRead(1023, false);
    removeFile();
    writeRepeateMapData(initWriterFromFile(), 1023, true);
    testRepeateMapRead(1023, true);
    removeFile();
    writeRepeateMapData(initWriterFromFile(), 1024, false);
    testRepeateMapRead(1024, false);
    removeFile();
    writeRepeateMapData(initWriterFromFile(), 1024, true);
    testRepeateMapRead(1024, true);
    removeFile();
    writeRepeateMapData(initWriterFromFile(), 1025, false);
    testRepeateMapRead(1025, false);
    removeFile();
    writeRepeateMapData(initWriterFromFile(), 1025, true);
    testRepeateMapRead(1025, true);
    removeFile();
  }

  @Test
  public void testMultipleDefinitionMapRead() throws Exception {
    removeFile();
    writeMapData(initWriterFromFile(), false, 1023);
    testMapRead(false, "multipleLevel", 1023);
    removeFile();
  }

  private void testMapReadAllType(boolean isDictionaryEncoding, int elementNum) throws Exception {
    testMapRead(isDictionaryEncoding, "int", elementNum);
    testMapRead(isDictionaryEncoding, "long", elementNum);
    testMapRead(isDictionaryEncoding, "double", elementNum);
    testMapRead(isDictionaryEncoding, "float", elementNum);
    testMapRead(isDictionaryEncoding, "binary", elementNum);
    testMapRead(isDictionaryEncoding, "decimal", elementNum);
  }

  private void testMapRead(boolean isDictionaryEncoding, String type,
      int elementNum) throws Exception {
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
        MapColumnVector mapVector = (MapColumnVector) previous.cols[0];

        //since Repeating only happens when offset length is 1.
        assertEquals((mapVector.offsets.length == 1),mapVector.isRepeating);

        for (int i = 0; i < mapVector.offsets.length; i++) {
          if (row == elementNum) {
            assertEquals(i, mapVector.offsets.length - 1);
            break;
          }
          long start = mapVector.offsets[i];
          long length = mapVector.lengths[i];
          boolean isNull = isNull(row);
          if (isNull) {
            assertEquals(mapVector.isNull[i], true);
          } else {
            for (long j = 0; j < length; j++) {
              assertValue(type, mapVector.keys, isDictionaryEncoding, index, (int) (start + j));
              assertValue(type, mapVector.values, isDictionaryEncoding, index, (int) (start + j));
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

  private void testRepeateMapRead(int elementNum, boolean isNull) throws Exception {
    Configuration conf = new Configuration();
    conf.set(IOConstants.COLUMNS, "map_int32_for_repeat_test");
    conf.set(IOConstants.COLUMNS_TYPES, "map<int,int>");
    conf.setBoolean(ColumnProjectionUtils.READ_ALL_COLUMNS, false);
    conf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, "0");
    String schema = "message hive_schema {\n"
        + "  repeated group map_int32_for_repeat_test (MAP_KEY_VALUE) {\n"
        + "    required int32 key;\n"
        + "    optional int32 value;\n"
        + "  }\n"
        + "}\n";
    VectorizedParquetRecordReader reader = createTestParquetReader(schema, conf);
    VectorizedRowBatch previous = reader.createValue();
    int row = 0;
    try {
      while (reader.next(NullWritable.get(), previous)) {
        MapColumnVector mapVector = (MapColumnVector) previous.cols[0];

        assertTrue(mapVector.isRepeating);
        assertEquals(isNull, mapVector.isNull[0]);

        for (int i = 0; i < mapVector.offsets.length; i++) {
          if (row == elementNum) {
            assertEquals(i, mapVector.offsets.length - 1);
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

  private void setTypeConfiguration(String type, Configuration conf) {
    if ("int".equals(type)) {
      conf.set(IOConstants.COLUMNS, "map_int32");
      conf.set(IOConstants.COLUMNS_TYPES, "map<int,int>");
    } else if ("long".equals(type)) {
      conf.set(IOConstants.COLUMNS, "map_int64");
      conf.set(IOConstants.COLUMNS_TYPES, "map<bigint,bigint>");
    } else if ("double".equals(type)) {
      conf.set(IOConstants.COLUMNS, "map_double");
      conf.set(IOConstants.COLUMNS_TYPES, "map<double,double>");
    } else if ("float".equals(type)) {
      conf.set(IOConstants.COLUMNS, "map_float");
      conf.set(IOConstants.COLUMNS_TYPES, "map<float,float>");
    } else if ("binary".equals(type)) {
      conf.set(IOConstants.COLUMNS, "map_binary");
      conf.set(IOConstants.COLUMNS_TYPES, "map<string,string>");
    } else if ("decimal".equals(type)) {
      conf.set(IOConstants.COLUMNS, "map_decimal");
      conf.set(IOConstants.COLUMNS_TYPES, "map<decimal(5,2),decimal(5,2)>");
    } else if ("multipleLevel".equals(type)) {
      conf.set(IOConstants.COLUMNS, "map_field");
      conf.set(IOConstants.COLUMNS_TYPES, "map<string,string>");
    }
  }

  private String getSchema(String type) {
    String schemaFormat = "message hive_schema {\n"
        + "  repeated group map_%s (MAP_KEY_VALUE) {\n"
        + "    required %s key %s;\n"
        + "    optional %s value %s;\n"
        + "  }\n"
        + "}\n";
    switch (type){
      case "int":
        return String.format(schemaFormat, "int32", "int32", "", "int32", "");
      case "long":
        return String.format(schemaFormat, "int64", "int64", "", "int64", "");
      case "double":
        return String.format(schemaFormat, "double", "double", "", "double", "");
      case "float":
        return String.format(schemaFormat, "float", "float", "", "float", "");
      case "binary":
        return String.format(schemaFormat, "binary", "binary", "", "binary", "");
      case "decimal":
        return String.format(schemaFormat, "decimal", "binary", "(DECIMAL(5,2))",
            "binary", "(DECIMAL(5,2))");
      case "multipleLevel":
        return "message hive_schema {\n"
            + "optional group map_field (MAP) {\n"
            + "  repeated group map (MAP_KEY_VALUE) {\n"
            + "    required binary key;\n"
            + "    optional binary value;\n"
            + "  }\n"
            + "}\n"
            + "}\n";
      default:
        throw new RuntimeException("Unsupported type for TestVectorizedMapColumnReader!");
    }
  }

  private void assertValue(String type, ColumnVector childVector, boolean isDictionaryEncoding,
    int valueIndex, int position) {
    if ("int".equals(type)) {
      assertEquals(getIntValue(isDictionaryEncoding, valueIndex),
          ((LongColumnVector)childVector).vector[position]);
    } else if ("long".equals(type)) {
      assertEquals(getLongValue(isDictionaryEncoding, valueIndex),
          ((LongColumnVector)childVector).vector[position]);
    } else if ("double".equals(type)) {
      assertEquals(getDoubleValue(isDictionaryEncoding, valueIndex),
          ((DoubleColumnVector)childVector).vector[position], 0);
    } else if ("float".equals(type)) {
      assertEquals(getFloatValue(isDictionaryEncoding, valueIndex),
          ((DoubleColumnVector)childVector).vector[position], 0);
    } else if ("binary".equals(type) || "multipleLevel".equals(type)) {
      String actual = new String(ArrayUtils
          .subarray(((BytesColumnVector)childVector).vector[position],
              ((BytesColumnVector)childVector).start[position],
              ((BytesColumnVector)childVector).start[position]
                  + ((BytesColumnVector)childVector).length[position]));
      assertEquals(getStr(isDictionaryEncoding, valueIndex), actual);
    } else if ("decimal".equals(type)) {
      assertEquals(getDecimal(isDictionaryEncoding, valueIndex),
          ((DecimalColumnVector)childVector).vector[position].getHiveDecimal());
    } else {
      throw new RuntimeException("Unsupported type for TestVectorizedMapColumnReader!");
    }
  }
}
