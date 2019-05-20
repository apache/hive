/*
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.parquet.serde.ArrayWritableObjectInspector;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hadoop.hive.ql.io.parquet.write.DataWritableWriter;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.ParquetHiveRecord;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class TestDataWritableWriter {
  @Mock private RecordConsumer mockRecordConsumer;
  private InOrder inOrder;

  @Before
  public void initMocks() {
    MockitoAnnotations.initMocks(this);
    inOrder = inOrder(mockRecordConsumer);
  }

  private void startMessage() {
    inOrder.verify(mockRecordConsumer).startMessage();
  }

  private void endMessage() {
    inOrder.verify(mockRecordConsumer).endMessage();
    verifyNoMoreInteractions(mockRecordConsumer);
  }

  private void startField(String name, int index) {
    inOrder.verify(mockRecordConsumer).startField(name, index);
  }

  private void endField(String name, int index) {
    inOrder.verify(mockRecordConsumer).endField(name, index);
  }

  private void addInteger(int value) {
    inOrder.verify(mockRecordConsumer).addInteger(value);
  }

  private void addLong(int value) {
    inOrder.verify(mockRecordConsumer).addLong(value);
  }

  private void addFloat(float value) {
    inOrder.verify(mockRecordConsumer).addFloat(value);
  }

  private void addDouble(double value) {
    inOrder.verify(mockRecordConsumer).addDouble(value);
  }

  private void addBoolean(boolean value) {
    inOrder.verify(mockRecordConsumer).addBoolean(value);
  }

  private void addString(String value) {
    inOrder.verify(mockRecordConsumer).addBinary(Binary.fromString(value));
  }

  private void startGroup() {
    inOrder.verify(mockRecordConsumer).startGroup();
  }

  private void endGroup() {
    inOrder.verify(mockRecordConsumer).endGroup();
  }

  private Writable createNull() { return null; }

  private ByteWritable createTinyInt(byte value) { return new ByteWritable(value); }

  private ShortWritable createSmallInt(short value) { return new ShortWritable(value); }

  private LongWritable createBigInt(long value) { return new LongWritable(value); }

  private IntWritable createInt(int value) {
    return new IntWritable(value);
  }

  private FloatWritable createFloat(float value) {
    return new FloatWritable(value);
  }

  private DoubleWritable createDouble(double value) {
    return new DoubleWritable(value);
  }

  private BooleanWritable createBoolean(boolean value) {
    return new BooleanWritable(value);
  }

  private BytesWritable createString(String value) {
    return new BytesWritable(value.getBytes(StandardCharsets.UTF_8));
  }

  private ArrayWritable createGroup(Writable...values) {
    return new ArrayWritable(Writable.class, values);
  }

  private ArrayWritable createArray(Writable...values) {
    return new ArrayWritable(Writable.class, createGroup(values).get());
  }

  private List<String> createHiveColumnsFrom(final String columnNamesStr) {
    List<String> columnNames;
    if (columnNamesStr.length() == 0) {
      columnNames = new ArrayList<String>();
    } else {
      columnNames = Arrays.asList(columnNamesStr.split(","));
    }

    return columnNames;
  }

  private List<TypeInfo> createHiveTypeInfoFrom(final String columnsTypeStr) {
    List<TypeInfo> columnTypes;

    if (columnsTypeStr.length() == 0) {
      columnTypes = new ArrayList<TypeInfo>();
    } else {
      columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnsTypeStr);
    }

    return columnTypes;
  }

  private ArrayWritableObjectInspector getObjectInspector(final String columnNames, final String columnTypes) {
    List<TypeInfo> columnTypeList = createHiveTypeInfoFrom(columnTypes);
    List<String> columnNameList = createHiveColumnsFrom(columnNames);
    StructTypeInfo rowTypeInfo = (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(columnNameList, columnTypeList);

    return new ArrayWritableObjectInspector(rowTypeInfo);
  }

  private ParquetHiveRecord getParquetWritable(String columnNames, String columnTypes, ArrayWritable record) throws SerDeException {
    Properties recordProperties = new Properties();
    recordProperties.setProperty("columns", columnNames);
    recordProperties.setProperty("columns.types", columnTypes);

    ParquetHiveSerDe serDe = new ParquetHiveSerDe();
    SerDeUtils.initializeSerDe(serDe, new Configuration(), recordProperties, null);

    return new ParquetHiveRecord(serDe.deserialize(record), getObjectInspector(columnNames, columnTypes));
  }

  private void writeParquetRecord(String schema, ParquetHiveRecord record) throws SerDeException {
    MessageType fileSchema = MessageTypeParser.parseMessageType(schema);
    DataWritableWriter hiveParquetWriter = new DataWritableWriter(mockRecordConsumer, fileSchema);
    hiveParquetWriter.write(record);
  }

  @Test
  public void testSimpleType() throws Exception {
    String columnNames = "int,double,boolean,float,string,tinyint,smallint,bigint";
    String columnTypes = "int,double,boolean,float,string,tinyint,smallint,bigint";

    String fileSchema = "message hive_schema {\n"
        + "  optional int32 int;\n"
        + "  optional double double;\n"
        + "  optional boolean boolean;\n"
        + "  optional float float;\n"
        + "  optional binary string (UTF8);\n"
        + "  optional int32 tinyint;\n"
        + "  optional int32 smallint;\n"
        + "  optional int64 bigint;\n"
        + "}\n";

    ArrayWritable hiveRecord = createGroup(
        createInt(1),
        createDouble(1.0),
        createBoolean(true),
        createFloat(1.0f),
        createString("one"),
        createTinyInt((byte)1),
        createSmallInt((short)1),
        createBigInt((long)1)
    );

    // Write record to Parquet format
    writeParquetRecord(fileSchema, getParquetWritable(columnNames, columnTypes, hiveRecord));

    // Verify record was written correctly to Parquet
    startMessage();
      startField("int", 0);
        addInteger(1);
      endField("int", 0);
      startField("double", 1);
        addDouble(1.0);
      endField("double", 1);
      startField("boolean", 2);
        addBoolean(true);
      endField("boolean", 2);
      startField("float", 3);
        addFloat(1.0f);
      endField("float", 3);
      startField("string", 4);
        addString("one");
      endField("string", 4);
      startField("tinyint", 5);
        addInteger(1);
      endField("tinyint", 5);
      startField("smallint", 6);
        addInteger(1);
      endField("smallint", 6);
      startField("bigint", 7);
        addLong(1);
      endField("bigint", 7);
    endMessage();
  }

  @Test
  public void testStructType() throws Exception {
    String columnNames = "structCol";
    String columnTypes = "struct<a:int,b:double,c:boolean>";

    String fileSchema = "message hive_schema {\n"
        + "  optional group structCol {\n"
        + "    optional int32 a;\n"
        + "    optional double b;\n"
        + "    optional boolean c;\n"
        + "  }\n"
        + "}\n";

    ArrayWritable hiveRecord = createGroup(
        createGroup(
            createInt(1),
            createDouble(1.0),
            createBoolean(true)
        )
    );

    // Write record to Parquet format
    writeParquetRecord(fileSchema, getParquetWritable(columnNames, columnTypes, hiveRecord));

    // Verify record was written correctly to Parquet
    startMessage();
      startField("structCol", 0);
        startGroup();
          startField("a", 0);
            addInteger(1);
          endField("a", 0);
          startField("b", 1);
            addDouble(1.0);
          endField("b", 1);
          startField("c", 2);
            addBoolean(true);
          endField("c", 2);
        endGroup();
      endField("structCol", 0);
    endMessage();
  }

  @Test
  public void testArrayType() throws Exception {
    String columnNames = "arrayCol";
    String columnTypes = "array<int>";

    String fileSchema = "message hive_schema {\n"
        + "  optional group arrayCol (LIST) {\n"
        + "    repeated group array {\n"
        + "      optional int32 array_element;\n"
        + "    }\n"
        + "  }\n"
        + "}\n";

    ArrayWritable hiveRecord = createGroup(
      createArray(
          createInt(1),
          createNull(),
          createInt(2)
      )
    );

    // Write record to Parquet format
    writeParquetRecord(fileSchema, getParquetWritable(columnNames, columnTypes, hiveRecord));

    // Verify record was written correctly to Parquet
    startMessage();
      startField("arrayCol", 0);
        startGroup();
          startField("array", 0);
            startGroup();
              startField("array_element", 0);
                addInteger(1);
              endField("array_element", 0);
            endGroup();
            startGroup();
            endGroup();
            startGroup();
            startField("array_element", 0);
              addInteger(2);
            endField("array_element", 0);
            endGroup();
          endField("array", 0);
        endGroup();
      endField("arrayCol", 0);
    endMessage();
  }

  @Test
  public void testMapType() throws Exception {
    String columnNames = "mapCol";
    String columnTypes = "map<string,int>";

    String fileSchema = "message hive_schema {\n"
        + "  optional group mapCol (MAP) {\n"
        + "    repeated group map (MAP_KEY_VALUE) {\n"
        + "      required binary key;\n"
        + "      optional int32 value;\n"
        + "    }\n"
        + "  }\n"
        + "}\n";

    ArrayWritable hiveRecord = createGroup(
        createGroup(
            createArray(
                createString("key1"),
                createInt(1)
            ),
            createArray(
                createString("key2"),
                createInt(2)
            ),
            createArray(
                createString("key3"),
                createNull()
            )
        )
    );

    // Write record to Parquet format
    writeParquetRecord(fileSchema, getParquetWritable(columnNames, columnTypes, hiveRecord));

    // Verify record was written correctly to Parquet
    startMessage();
      startField("mapCol", 0);
        startGroup();
          startField("map", 0);
            startGroup();
              startField("key", 0);
                addString("key1");
              endField("key", 0);
              startField("value", 1);
                addInteger(1);
              endField("value", 1);
            endGroup();
            startGroup();
              startField("key", 0);
                addString("key2");
              endField("key", 0);
              startField("value", 1);
                addInteger(2);
              endField("value", 1);
            endGroup();
            startGroup();
              startField("key", 0);
                addString("key3");
              endField("key", 0);
            endGroup();
          endField("map", 0);
        endGroup();
      endField("mapCol", 0);
    endMessage();
  }

  @Test
  public void testEmptyArrays() throws Exception {
    String columnNames = "arrayCol";
    String columnTypes = "array<int>";

    String fileSchema = "message hive_schema {\n"
        + "  optional group arrayCol (LIST) {\n"
        + "    repeated group array {\n"
        + "      optional int32 array_element;\n"
        + "    }\n"
        + "  }\n"
        + "}\n";

    ArrayWritable hiveRecord = createGroup(
       new ArrayWritable(Writable.class) // Empty array
    );

   // Write record to Parquet format
    writeParquetRecord(fileSchema, getParquetWritable(columnNames, columnTypes, hiveRecord));

    // Verify record was written correctly to Parquet
    startMessage();
      startField("arrayCol", 0);
        startGroup();
        endGroup();
      endField("arrayCol", 0);
    endMessage();
  }

  @Test
  public void testArrayOfArrays() throws Exception {
    String columnNames = "array_of_arrays";
    String columnTypes = "array<array<int>>";

    String fileSchema = "message hive_schema {\n"
        + "  optional group array_of_arrays (LIST) {\n"
        + "    repeated group array {\n"
        + "      optional group array_element (LIST) {\n"
        + "        repeated group array {\n"
        + "          optional int32 array_element;\n"
        + "        }\n"
        + "      }\n"
        + "    }\n"
        + "  }\n"
        + "}\n";

    ArrayWritable hiveRecord = createGroup(
        createArray(
            createArray(
                createInt(1),
                createInt(2)
            )
        )
    );

    // Write record to Parquet format
    writeParquetRecord(fileSchema, getParquetWritable(columnNames, columnTypes, hiveRecord));

    // Verify record was written correctly to Parquet
    startMessage();
      startField("array_of_arrays", 0);
        startGroup();
          startField("array", 0);
            startGroup();
              startField("array_element", 0);
                startGroup();
                  startField("array", 0);
                    startGroup();
                      startField("array_element", 0);
                        addInteger(1);
                      endField("array_element", 0);
                    endGroup();
                    startGroup();
                      startField("array_element", 0);
                        addInteger(2);
                      endField("array_element", 0);
                    endGroup();
                  endField("array", 0);
                endGroup();
              endField("array_element", 0);
            endGroup();
          endField("array", 0);
        endGroup();
      endField("array_of_arrays", 0);
    endMessage();
  }

  @Test
  public void testExpectedStructTypeOnRecord() throws Exception {
    String columnNames = "structCol";
    String columnTypes = "int";

    ArrayWritable hiveRecord = createGroup(
        createInt(1)
    );

    String fileSchema = "message hive_schema {\n"
        + "  optional group structCol {\n"
      + "      optional int32 int;\n"
      + "    }\n"
        + "}\n";

    try {
      writeParquetRecord(fileSchema, getParquetWritable(columnNames, columnTypes, hiveRecord));
      fail();
    } catch (RuntimeException e) {
      assertEquals("Parquet record is malformed: Invalid data type: expected STRUCT type, but found: PRIMITIVE", e.getMessage());
    }
  }

  @Test
  public void testExpectedArrayTypeOnRecord() throws Exception {
    String columnNames = "arrayCol";
    String columnTypes = "int";

    ArrayWritable hiveRecord = createGroup(
        createInt(1)
    );

    String fileSchema = "message hive_schema {\n"
        + "  optional group arrayCol (LIST) {\n"
        + "    repeated group bag {\n"
        + "      optional int32 array_element;\n"
        + "    }\n"
        + "  }\n"
        + "}\n";

    try {
      writeParquetRecord(fileSchema, getParquetWritable(columnNames, columnTypes, hiveRecord));
      fail();
    } catch (RuntimeException e) {
      assertEquals("Parquet record is malformed: Invalid data type: expected LIST type, but found: PRIMITIVE", e.getMessage());
    }
  }

  @Test
  public void testExpectedMapTypeOnRecord() throws Exception {
    String columnNames = "mapCol";
    String columnTypes = "int";

    ArrayWritable hiveRecord = createGroup(
        createInt(1)
    );

    String fileSchema = "message hive_schema {\n"
        + "  optional group mapCol (MAP) {\n"
        + "    repeated group map (MAP_KEY_VALUE) {\n"
        + "      required binary key;\n"
        + "      optional int32 value;\n"
        + "    }\n"
        + "  }\n"
        + "}\n";

    try {
      writeParquetRecord(fileSchema, getParquetWritable(columnNames, columnTypes, hiveRecord));
      fail();
    } catch (RuntimeException e) {
      assertEquals("Parquet record is malformed: Invalid data type: expected MAP type, but found: PRIMITIVE", e.getMessage());
    }
  }
}
