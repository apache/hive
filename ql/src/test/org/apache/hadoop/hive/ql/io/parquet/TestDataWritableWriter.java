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

import org.apache.hadoop.hive.ql.io.parquet.write.DataWritableWriter;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.*;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import parquet.io.api.Binary;
import parquet.io.api.RecordConsumer;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;

import java.io.UnsupportedEncodingException;

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

  private BytesWritable createString(String value) throws UnsupportedEncodingException {
    return new BytesWritable(value.getBytes("UTF-8"));
  }

  private ArrayWritable createGroup(Writable...values) {
    return new ArrayWritable(Writable.class, values);
  }

  private ArrayWritable createArray(Writable...values) {
    return new ArrayWritable(Writable.class, createGroup(values).get());
  }

  private void writeParquetRecord(String schemaStr, ArrayWritable record) {
    MessageType schema = MessageTypeParser.parseMessageType(schemaStr);
    DataWritableWriter hiveParquetWriter = new DataWritableWriter(mockRecordConsumer, schema);
    hiveParquetWriter.write(record);
  }

  @Test
  public void testSimpleType() throws Exception {
    String schemaStr = "message hive_schema {\n"
        + "  optional int32 int;\n"
        + "  optional double double;\n"
        + "  optional boolean boolean;\n"
        + "  optional float float;\n"
        + "  optional binary string;\n"
        + "}\n";

    ArrayWritable hiveRecord = createGroup(
        createInt(1),
        createDouble(1.0),
        createBoolean(true),
        createFloat(1.0f),
        createString("one")
    );

    // Write record to Parquet format
    writeParquetRecord(schemaStr, hiveRecord);

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
    endMessage();
  }

  @Test
  public void testStructType() throws Exception {
    String schemaStr = "message hive_schema {\n"
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
    writeParquetRecord(schemaStr, hiveRecord);

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
    String schemaStr = "message hive_schema {\n"
        + "  optional group arrayCol (LIST) {\n"
        + "    repeated group bag {\n"
        + "      optional int32 array_element;\n"
        + "    }\n"
        + "  }\n"
        + "}\n";

    ArrayWritable hiveRecord = createGroup(
        createGroup(
            createArray(
                createInt(1),
                createNull(),
                createInt(2)
            )
        )
    );

    // Write record to Parquet format
    writeParquetRecord(schemaStr, hiveRecord);

    // Verify record was written correctly to Parquet
    startMessage();
      startField("arrayCol", 0);
        startGroup();
          startField("bag", 0);
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
          endField("bag", 0);
        endGroup();
      endField("arrayCol", 0);
    endMessage();
  }

  @Test
  public void testMapType() throws Exception {
    String schemaStr = "message hive_schema {\n"
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
        )
    );

    // Write record to Parquet format
    writeParquetRecord(schemaStr, hiveRecord);

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
  public void testArrayOfArrays() throws Exception {
    String schemaStr = "message hive_schema {\n"
        + "  optional group array_of_arrays (LIST) {\n"
        + "    repeated group array {\n"
        + "      required group element (LIST) {\n"
        + "        repeated group array {\n"
        + "          required int32 element;\n"
        + "        }\n"
        + "      }\n"
        + "    }\n"
        + "  }\n"
        + "}\n";

    ArrayWritable hiveRecord = createGroup(
        createGroup(
            createArray(
                createGroup(
                    createArray(
                        createInt(1),
                        createInt(2)
                    )
                )
            )
        )
    );

    // Write record to Parquet format
    writeParquetRecord(schemaStr, hiveRecord);

    // Verify record was written correctly to Parquet
    startMessage();
      startField("array_of_arrays", 0);
        startGroup();
          startField("array", 0);
            startGroup();
              startField("element", 0);
                startGroup();
                  startField("array", 0);
                    startGroup();
                      startField("element", 0);
                        addInteger(1);
                      endField("element", 0);
                    endGroup();
                    startGroup();
                      startField("element", 0);
                        addInteger(2);
                      endField("element", 0);
                    endGroup();
                  endField("array", 0);
                endGroup();
              endField("element", 0);
            endGroup();
          endField("array", 0);
        endGroup();
      endField("array_of_arrays", 0);
    endMessage();
  }

  @Test
  public void testGroupFieldIsNotArrayWritable() throws Exception {
    String schemaStr = "message hive_schema {\n"
        + "  optional group a {\n"
        + "    optional int32 b;\n"
        + "  }\n"
        + "}\n";

    ArrayWritable hiveRecord = createGroup(
          createInt(1)
    );

    try {
      // Write record to Parquet format
      writeParquetRecord(schemaStr, hiveRecord);
      fail();
    } catch (RuntimeException e) {
      assertEquals("Parquet record is malformed: Field value is not an ArrayWritable object: " +
          "optional group a {\n  optional int32 b;\n}", e.getMessage());
    }
  }

  @Test
  public void testArrayGroupElementIsNotArrayWritable() throws Exception {
    String schemaStr = "message hive_schema {\n"
        + "  optional group array_of_arrays (LIST) {\n"
        + "    repeated group array {\n"
        + "      required group element (LIST) {\n"
        + "        required int32 element;\n"
        + "      }\n"
        + "    }\n"
        + "  }\n"
        + "}\n";

    ArrayWritable hiveRecord = createGroup(
        createGroup(
            createArray(
                createInt(1)
            )
        )
    );

    try {
      // Write record to Parquet format
      writeParquetRecord(schemaStr, hiveRecord);
      fail();
    } catch (RuntimeException e) {
      assertEquals("Parquet record is malformed: Field value is not an ArrayWritable object: " +
          "required group element (LIST) {\n  required int32 element;\n}", e.getMessage());
    }
  }

  @Test
  public void testMapElementIsNotArrayWritable() throws Exception {
    String schemaStr = "message hive_schema {\n"
        + "  optional group mapCol (MAP) {\n"
        + "    repeated group map (MAP_KEY_VALUE) {\n"
        + "      required binary key;\n"
        + "      optional group value {\n"
        + "        required int32 value;"
        + "      }\n"
        + "    }\n"
        + "  }\n"
        + "}\n";

    ArrayWritable hiveRecord = createGroup(
        createGroup(
            createArray(
                createGroup(
                    createString("key1"),
                    createInt(1)
                )
            )
        )
    );

    try {
      // Write record to Parquet format
      writeParquetRecord(schemaStr, hiveRecord);
      fail();
    } catch (RuntimeException e) {
      assertEquals(
          "Parquet record is malformed: Field value is not an ArrayWritable object: " +
              "optional group value {\n  required int32 value;\n}", e.getMessage());
    }
  }

  @Test
  public void testMapKeyValueIsNotArrayWritable() throws Exception {
    String schemaStr = "message hive_schema {\n"
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
            )
        )
    );

    try {
      // Write record to Parquet format
      writeParquetRecord(schemaStr, hiveRecord);
      fail();
    } catch (RuntimeException e) {
      assertEquals("Parquet record is malformed: Map key-value pair is not an ArrayWritable object on record 0", e.getMessage());
    }
  }

  @Test
  public void testMapKeyValueIsNull() throws Exception {
    String schemaStr = "message hive_schema {\n"
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
                createNull()
            )
        )
    );

    try {
      // Write record to Parquet format
      writeParquetRecord(schemaStr, hiveRecord);
      fail();
    } catch (RuntimeException e) {
      assertEquals("Parquet record is malformed: Map key-value pair is null on record 0", e.getMessage());
    }
  }
}
