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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.AbstractTestVariantFileFormat;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.parquet.read.DataWritableReadSupport;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.io.ParquetHiveRecord;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.VariantObjectInspector;
import org.apache.hadoop.hive.serde2.variant.Variant;
import org.apache.hadoop.hive.serde2.variant.VariantBuilder;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/** Writes and reads real Parquet files: a variant is the group of its two binaries. */
public class TestParquetVariant extends AbstractTestVariantFileFormat {

  @Override
  protected String extension() {
    return "parquet";
  }

  @Override
  protected void writeRows(Path path, String columnTypes, StructObjectInspector oi, List<List<Object>> rows)
      throws Exception {
    Properties tableProperties = new Properties();
    tableProperties.setProperty(IOConstants.COLUMNS, COLUMNS);
    tableProperties.setProperty(IOConstants.COLUMNS_TYPES, columnTypes);
    FileSinkOperator.RecordWriter writer = new MapredParquetOutputFormat().getHiveRecordWriter(
        new JobConf(), path, ParquetHiveRecord.class, false, tableProperties, null);
    for (List<Object> row : rows) {
      writer.write(new ParquetHiveRecord(row, oi));
    }
    writer.close(false);
  }

  @Override
  protected void forEachRow(Path path, String columnNames, String columnTypes, RowConsumer consumer)
      throws Exception {
    forEachRow(path, jobConf(columnNames, columnTypes), consumer);
  }

  /** A variant is a group of its two binaries; no annotation on it or on the list elements. */
  @Override
  protected void assertSameFileSchema(Path variantFile, Path structFile) throws Exception {
    MessageType variantSchema = schemaOf(variantFile);
    MessageType structSchema = schemaOf(structFile);
    assertEquals(structSchema, variantSchema);
    assertEquals(structSchema.toString(), variantSchema.toString());
    assertEquals(2, variantSchema.getType("v").asGroupType().getFieldCount());
    assertNull(variantSchema.getType("v").getLogicalTypeAnnotation());
  }

  /** A file from another writer: the element group holds value before metadata. */
  @Test
  public void testListElementsAreReadByNameNotByPosition() throws Exception {
    Variant first = VariantBuilder.parseJson("{\"a\":[1,2]}", false);
    Variant second = VariantBuilder.parseJson("\"text\"", false);
    MessageType fileSchema = MessageTypeParser.parseMessageType("message hive_schema {\n"
        + "  optional group vs (LIST) {\n"
        + "    repeated group list {\n"
        + "      optional group element {\n"
        + "        optional binary value;\n"
        + "        optional binary metadata;\n"
        + "      }\n"
        + "    }\n"
        + "  }\n"
        + "}\n");
    Path path = writeDirect("reordered", fileSchema, consumer -> {
      consumer.startMessage();
      consumer.startField("vs", 0);
      consumer.startGroup();
      consumer.startField("list", 0);
      for (Variant variant : Arrays.asList(first, second)) {
        consumer.startGroup();
        consumer.startField("element", 0);
        consumer.startGroup();
        addBinary(consumer, "value", 0, variant.getValue());
        addBinary(consumer, "metadata", 1, variant.getMetadata());
        consumer.endGroup();
        consumer.endField("element", 0);
        consumer.endGroup();
      }
      consumer.endField("list", 0);
      consumer.endGroup();
      consumer.endField("vs", 0);
      consumer.endMessage();
    });

    List<Variant> expected = Arrays.asList(first, second);
    assertEquals(Arrays.asList(expected), readVariants(path, "vs", "array<" + ENCODING + ">"));
    assertEquals(Arrays.asList(expected), readVariants(path, "vs", "array<variant>"));
  }

  /** variant_get(v, '$.a') and variant_get(s.v, '$.a') push down paths v.a and s.v.a; the variants are read whole. */
  @Test
  public void testNestedPathBelowAVariantReadsTheWholeVariant() throws Exception {
    Variant top = VariantBuilder.parseJson("{\"a\":15,\"b\":\"x\"}", false);
    Variant nested = VariantBuilder.parseJson("{\"a\":25}", false);
    MessageType schema = MessageTypeParser.parseMessageType("message hive_schema {\n"
        + "  optional group v { optional binary metadata; optional binary value; }\n"
        + "  optional group s {\n"
        + "    optional group v { optional binary metadata; optional binary value; }\n"
        + "    optional int32 n;\n"
        + "  }\n"
        + "}\n");
    Group row = new SimpleGroupFactory(schema).newGroup();
    addVariant(row, "v", top);
    Group s = row.addGroup("s");
    addVariant(s, "v", nested);
    s.append("n", 1);
    Path path = writeGroups("nested-path", schema, row);

    JobConf jobConf = jobConf("v,s", "variant,struct<v:variant,n:int>");
    jobConf.setBoolean(ColumnProjectionUtils.READ_ALL_COLUMNS, false);
    jobConf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, "0,1");
    jobConf.set(ColumnProjectionUtils.READ_NESTED_COLUMN_PATH_CONF_STR, "v.a,s.v.a");

    List<Object> read = readRows(path, jobConf).getFirst();
    assertEquals(top, read.get(0));
    assertEquals(nested, ((List<?>) read.get(1)).getFirst());
  }

  /** A short string of 4 bytes, read with metadata and value swapped, passes as metadata and decodes as "". */
  @Test
  public void testMapValuesAreReadByName() throws Exception {
    Variant object = VariantBuilder.parseJson("{\"a\":1}", false);
    Variant text = VariantBuilder.parseJson("\"abcd\"", false);
    MessageType schema = MessageTypeParser.parseMessageType("message hive_schema {\n"
        + "  optional group m (MAP) {\n"
        + "    repeated group key_value {\n"
        + "      required binary key (STRING);\n"
        + "      optional group value { optional binary value; optional binary metadata; }\n"
        + "    }\n"
        + "  }\n"
        + "}\n");
    Group row = new SimpleGroupFactory(schema).newGroup();
    Group map = row.addGroup("m");
    for (Map.Entry<String, Variant> entry : Map.of("x", object, "y", text).entrySet()) {
      Group keyValue = map.addGroup("key_value");
      keyValue.append("key", entry.getKey());
      addVariant(keyValue, "value", entry.getValue());
    }
    Path path = writeGroups("map-values", schema, row);

    assertEquals(Map.of("x", object, "y", text),
        readRows(path, jobConf("m", "map<string,variant>")).getFirst().getFirst());
  }

  @Test
  public void testNestedListElementsAreReadByName() throws Exception {
    Variant first = VariantBuilder.parseJson("{\"a\":1}", false);
    Variant second = VariantBuilder.parseJson("\"abcd\"", false);
    MessageType schema = MessageTypeParser.parseMessageType("message hive_schema {\n"
        + "  optional group l (LIST) {\n"
        + "    repeated group list {\n"
        + "      optional group element (LIST) {\n"
        + "        repeated group list {\n"
        + "          optional group element { optional binary value; optional binary metadata; }\n"
        + "        }\n"
        + "      }\n"
        + "    }\n"
        + "  }\n"
        + "}\n");
    Group row = new SimpleGroupFactory(schema).newGroup();
    Group inner = row.addGroup("l").addGroup("list").addGroup("element");
    for (Variant variant : List.of(first, second)) {
      addVariant(inner.addGroup("list"), "element", variant);
    }
    Path path = writeGroups("nested-lists", schema, row);

    assertEquals(List.of(List.of(first, second)),
        readRows(path, jobConf("l", "array<array<variant>>")).getFirst().getFirst());
  }

  /** Index access maps the column by position; its fields are still found by name. */
  @Test
  public void testIndexAccessReadsVariantFieldsByName() throws Exception {
    Variant variant = VariantBuilder.parseJson("\"abcd\"", false);
    MessageType schema = MessageTypeParser.parseMessageType(
        "message hive_schema { optional group file_name { optional binary value; optional binary metadata; } }");
    Group row = new SimpleGroupFactory(schema).newGroup();
    addVariant(row, "file_name", variant);
    Path path = writeGroups("index-access", schema, row);

    JobConf jobConf = jobConf("v", "variant");
    jobConf.setBoolean(DataWritableReadSupport.PARQUET_COLUMN_INDEX_ACCESS, true);
    assertEquals(variant, readRows(path, jobConf).getFirst().getFirst());
  }

  /** A shredded variant fails once it is read, wherever it sits; a query that does not read it is unaffected. */
  @Test
  public void testShreddedVariantFailsNamingTheField() throws Exception {
    String shredded = "{ optional binary value; optional binary metadata; optional int64 typed_value; }";
    MessageType schema = MessageTypeParser.parseMessageType("message hive_schema {\n"
        + "  optional int32 id;\n"
        + "  optional group v " + shredded + "\n"
        + "  optional group s { optional group sv " + shredded + " }\n"
        + "  optional group l (LIST) { repeated group list { optional group element " + shredded + " } }\n"
        + "  optional group m (MAP) {\n"
        + "    repeated group key_value { required binary key (STRING); optional group value " + shredded + " }\n"
        + "  }\n"
        + "}\n");
    Variant variant = VariantBuilder.parseJson("{\"b\":\"x\"}", false);
    Group row = new SimpleGroupFactory(schema).newGroup();
    row.append("id", 7);
    addVariant(row, "v", variant);
    addVariant(row.addGroup("s"), "sv", variant);
    addVariant(row.addGroup("l").addGroup("list"), "element", variant);
    Group keyValue = row.addGroup("m").addGroup("key_value");
    keyValue.append("key", "k");
    addVariant(keyValue, "value", variant);
    Path path = writeGroups("shredded", schema, row);

    String columns = "id,v,s,l,m";
    String types = "int,variant,struct<sv:variant>,array<variant>,map<string,variant>";
    Map<Integer, String> fieldOfColumn = Map.of(1, "v", 2, "sv", 3, "element", 4, "value");
    for (boolean indexAccess : new boolean[] {false, true}) {
      for (Map.Entry<Integer, String> column : fieldOfColumn.entrySet()) {
        JobConf jobConf = jobConf(columns, types);
        jobConf.setBoolean(DataWritableReadSupport.PARQUET_COLUMN_INDEX_ACCESS, indexAccess);
        ColumnProjectionUtils.setReadColumns(jobConf, List.of(column.getKey()));
        Exception e = assertThrows(Exception.class, () -> readRows(path, jobConf));
        assertShreddedVariantError(e, column.getValue());
      }
      JobConf idOnly = jobConf(columns, types);
      idOnly.setBoolean(DataWritableReadSupport.PARQUET_COLUMN_INDEX_ACCESS, indexAccess);
      ColumnProjectionUtils.setReadColumns(idOnly, List.of(0));
      assertEquals(7, readRows(path, idOnly).getFirst().getFirst());
    }
  }

  static void assertShreddedVariantError(Throwable e, String field) {
    Throwable cause = e;
    while (cause.getCause() != null && !(cause instanceof UnsupportedOperationException)) {
      cause = cause.getCause();
    }
    assertTrue(String.valueOf(cause), cause instanceof UnsupportedOperationException);
    assertTrue(cause.getMessage(), cause.getMessage().contains("'" + field + "'"));
    assertTrue(cause.getMessage(), cause.getMessage().contains("typed_value"));
  }

  /** Adds {@code variant} to {@code parent} as the group {@code field}, whatever the order of its two binaries. */
  private static void addVariant(Group parent, String field, Variant variant) {
    parent.addGroup(field)
        .append("metadata", Binary.fromConstantByteArray(variant.getMetadata()))
        .append("value", Binary.fromConstantByteArray(variant.getValue()));
  }

  private Path writeGroups(String name, MessageType schema, Group... rows) throws Exception {
    Path path = path(name);
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(path).withType(schema).build()) {
      for (Group row : rows) {
        writer.write(row);
      }
    }
    return path;
  }

  private static JobConf jobConf(String columns, String columnTypes) {
    JobConf jobConf = new JobConf();
    jobConf.set(IOConstants.COLUMNS, columns);
    jobConf.set(IOConstants.COLUMNS_TYPES, columnTypes);
    return jobConf;
  }

  /** Reads every row of {@code path} through the Parquet serde and the row reader. */
  private static void forEachRow(Path path, JobConf jobConf, RowConsumer consumer) throws Exception {
    Properties tableProperties = new Properties();
    tableProperties.setProperty(IOConstants.COLUMNS, jobConf.get(IOConstants.COLUMNS));
    tableProperties.setProperty(IOConstants.COLUMNS_TYPES, jobConf.get(IOConstants.COLUMNS_TYPES));
    ParquetHiveSerDe serDe = new ParquetHiveSerDe();
    serDe.initialize(jobConf, tableProperties, null);
    StructObjectInspector oi = (StructObjectInspector) serDe.getObjectInspector();

    long length = path.getFileSystem(jobConf).getFileStatus(path).getLen();
    RecordReader<NullWritable, ArrayWritable> reader = new MapredParquetInputFormat().getRecordReader(
        new FileSplit(path, 0, length, (String[]) null), jobConf, null);
    NullWritable key = reader.createKey();
    ArrayWritable row = reader.createValue();
    while (reader.next(key, row)) {
      consumer.accept(oi, row);
    }
    reader.close();
  }

  /** Reads every row of {@code path} through the Parquet serde and the row reader, as plain Java values. */
  private static List<List<Object>> readRows(Path path, JobConf jobConf) throws Exception {
    List<List<Object>> rows = new ArrayList<>();
    forEachRow(path, jobConf, (oi, row) -> {
      List<Object> values = new ArrayList<>();
      for (StructField field : oi.getAllStructFieldRefs()) {
        values.add(toJava(field.getFieldObjectInspector(), oi.getStructFieldData(row, field)));
      }
      rows.add(values);
    });
    return rows;
  }

  /** {@code data} as a variant, a list (struct or array), a map or a primitive Java value. */
  private static Object toJava(ObjectInspector oi, Object data) {
    if (data == null) {
      return null;
    }
    return switch (oi.getCategory()) {
      case VARIANT -> ((VariantObjectInspector) oi).getVariant(data);
      case STRUCT -> {
        StructObjectInspector struct = (StructObjectInspector) oi;
        List<Object> fields = new ArrayList<>();
        for (StructField field : struct.getAllStructFieldRefs()) {
          fields.add(toJava(field.getFieldObjectInspector(), struct.getStructFieldData(data, field)));
        }
        yield fields;
      }
      case LIST -> {
        ListObjectInspector list = (ListObjectInspector) oi;
        List<Object> elements = new ArrayList<>();
        for (int i = 0; i < list.getListLength(data); i++) {
          elements.add(toJava(list.getListElementObjectInspector(), list.getListElement(data, i)));
        }
        yield elements;
      }
      case MAP -> {
        MapObjectInspector map = (MapObjectInspector) oi;
        Map<Object, Object> entries = new LinkedHashMap<>();
        map.getMap(data).forEach((k, v) -> entries.put(toJava(map.getMapKeyObjectInspector(), k),
            toJava(map.getMapValueObjectInspector(), v)));
        yield entries;
      }
      case PRIMITIVE -> ((PrimitiveObjectInspector) oi).getPrimitiveJavaObject(data);
      default -> throw new IllegalArgumentException("Unexpected " + oi.getTypeName());
    };
  }

  /** Writes one record, produced by {@code writer} on the record consumer, with exactly {@code schema}. */
  private Path writeDirect(String name, MessageType schema, Consumer<RecordConsumer> writer) throws Exception {
    Path path = path(name);
    WriteSupport<Void> support = new WriteSupport<Void>() {
      private RecordConsumer consumer;

      @Override
      public WriteContext init(Configuration configuration) {
        return new WriteContext(schema, new HashMap<>());
      }

      @Override
      public void prepareForWrite(RecordConsumer recordConsumer) {
        consumer = recordConsumer;
      }

      @Override
      public void write(Void record) {
        writer.accept(consumer);
      }
    };
    try (ParquetWriter<Void> parquetWriter = new ParquetWriter<>(path, support)) {
      parquetWriter.write(null);
    }
    return path;
  }

  private static void addBinary(RecordConsumer consumer, String name, int index, byte[] bytes) {
    consumer.startField(name, index);
    consumer.addBinary(Binary.fromConstantByteArray(bytes));
    consumer.endField(name, index);
  }

  private static MessageType schemaOf(Path path) throws Exception {
    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, new Configuration()))) {
      return reader.getFooter().getFileMetaData().getSchema();
    }
  }
}
