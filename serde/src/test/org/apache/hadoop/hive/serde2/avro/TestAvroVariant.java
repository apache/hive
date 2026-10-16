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
package org.apache.hadoop.hive.serde2.avro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.variant.Variant;
import org.apache.hadoop.hive.serde2.variant.VariantBuilder;
import org.apache.hadoop.hive.serde2.variant.VariantEncoding;
import org.junit.Test;

import static org.apache.hadoop.hive.serde2.variant.VariantTestUtils.ENCODING;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestAvroVariant {
  private static final String LOGICAL_TYPE = ",\"logicalType\":\"variant\"";

  private static Schema columnSchema(String type) {
    Schema schema = new TypeInfoToSchema().convert(List.of("v"),
        List.of(TypeInfoUtils.getTypeInfoFromTypeString(type)), List.of(), null, null, null);
    return AvroSerdeUtils.getOtherTypeFromNullableType(schema.getField("v").schema());
  }

  @Test
  public void testSchemaIsIcebergsVariantRecord() {
    Schema asVariant = columnSchema("variant");
    assertEquals(Schema.Type.RECORD, asVariant.getType());
    // two non-nullable binaries without defaults, as Iceberg writes them
    assertEquals(List.of(new Schema.Field(VariantEncoding.METADATA, Schema.create(Schema.Type.BYTES)),
        new Schema.Field(VariantEncoding.VALUE, Schema.create(Schema.Type.BYTES))), asVariant.getFields());
    assertEquals(AvroSerDe.VARIANT_TYPE_NAME, asVariant.getProp(AvroSerDe.AVRO_PROP_LOGICAL_TYPE));
    assertNull(columnSchema(ENCODING).getProp(AvroSerDe.AVRO_PROP_LOGICAL_TYPE));
  }

  @Test
  public void testTaggedRecordReadsBackAsVariant() throws Exception {
    assertEquals(TypeInfoFactory.getVariantTypeInfo(),
        SchemaToTypeInfo.generateTypeInfo(columnSchema("variant"), null));
    assertEquals(TypeInfoUtils.getTypeInfoFromTypeString(ENCODING),
        SchemaToTypeInfo.generateTypeInfo(columnSchema(ENCODING), null));
  }

  private static AvroSerDe serde(String types) throws Exception {
    Properties props = new Properties();
    props.setProperty(serdeConstants.LIST_COLUMNS, "v,a,m");
    props.setProperty(serdeConstants.LIST_COLUMN_TYPES, types);
    AvroSerDe serde = new AvroSerDe();
    serde.initialize(new Configuration(), props, null);
    return serde;
  }

  private static StructObjectInspector row(ObjectInspector element) {
    return ObjectInspectorFactory.getStandardStructObjectInspector(List.of("v", "a", "m"),
        List.of(element, ObjectInspectorFactory.getStandardListObjectInspector(element),
            ObjectInspectorFactory.getStandardMapObjectInspector(
                PrimitiveObjectInspectorFactory.javaStringObjectInspector, element)));
  }

  /** The datum as Avro puts it in a data file, without the file header that carries the schema. */
  private static byte[] datumBytes(AvroGenericRecordWritable written) throws IOException {
    GenericRecord record = written.getRecord();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    new GenericDatumWriter<GenericRecord>(record.getSchema()).write(record, encoder);
    encoder.flush();
    return out.toByteArray();
  }

  @Test
  public void testDatumHoldsEachVariantAsTwoPlainBinaries() throws Exception {
    Variant a = VariantBuilder.parseJson("{\"k\":[1,\"x\"]}", false);
    Variant b = VariantBuilder.parseJson("2", false);
    AvroGenericRecordWritable written = (AvroGenericRecordWritable) serde(
        "variant,array<variant>,map<string,variant>").serialize(
        Arrays.asList(a, Arrays.asList(b, null), null),
        row(ObjectInspectorFactory.getStandardVariantObjectInspector()));

    // a nullable column or element writes its union branch; the binaries inside a variant are not nullable
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder expected = EncoderFactory.get().binaryEncoder(out, null);
    expected.writeIndex(1);
    expected.writeBytes(a.getMetadata());
    expected.writeBytes(a.getValue());
    expected.writeIndex(1);
    expected.writeArrayStart();
    expected.setItemCount(2);
    expected.startItem();
    expected.writeIndex(1);
    expected.writeBytes(b.getMetadata());
    expected.writeBytes(b.getValue());
    expected.startItem();
    expected.writeIndex(0);
    expected.writeArrayEnd();
    expected.writeIndex(0);
    expected.flush();
    assertArrayEquals(out.toByteArray(), datumBytes(written));

    // the schema of the file header tags each of the three variant records
    assertEquals(3, written.getRecord().getSchema().toString().split(LOGICAL_TYPE, -1).length - 1);
  }

  @Test
  public void testSerdeInitializedFromTheStoredSchemaLiteralStillReadsVariants() throws Exception {
    Properties props = new Properties();
    props.setProperty(serdeConstants.LIST_COLUMNS, "v");
    props.setProperty(serdeConstants.LIST_COLUMN_TYPES, "variant");
    new AvroSerDe().initialize(new Configuration(), props, null);

    // the first initialize stores the schema as avro.schema.literal; a second serde (task side) starts from it
    props.remove(serdeConstants.LIST_COLUMNS);
    props.remove(serdeConstants.LIST_COLUMN_TYPES);
    AvroSerDe serde = new AvroSerDe();
    serde.initialize(new Configuration(), props, null);

    assertEquals("variant", props.getProperty(serdeConstants.LIST_COLUMN_TYPES));
    assertEquals(ObjectInspector.Category.VARIANT, ((StructObjectInspector) serde.getObjectInspector())
        .getAllStructFieldRefs().get(0).getFieldObjectInspector().getCategory());
  }

  private static final String PLAIN_RECORD = "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
      + "{\"name\":\"" + VariantEncoding.METADATA + "\",\"type\":\"bytes\"},"
      + "{\"name\":\"" + VariantEncoding.VALUE + "\",\"type\":\"bytes\"}]}";

  private static AvroSerDe serdeOfLiteral(String columnSchema, Properties props) throws Exception {
    props.setProperty(AvroSerdeUtils.AvroTableProperties.SCHEMA_LITERAL.getPropName(),
        "{\"type\":\"record\",\"name\":\"t\",\"fields\":[{\"name\":\"v\",\"type\":[\"null\"," + columnSchema + "]}]}");
    AvroSerDe serde = new AvroSerDe();
    serde.initialize(new Configuration(), props, null);
    return serde;
  }

  @Test
  public void testUserSchemaRecordWithoutTheLogicalTypeStaysAStruct() throws Exception {
    Properties props = new Properties();
    AvroSerDe serde = serdeOfLiteral(PLAIN_RECORD, props);

    assertEquals(ENCODING, props.getProperty(serdeConstants.LIST_COLUMN_TYPES));
    StructObjectInspector rowOI = (StructObjectInspector) serde.getObjectInspector();
    StructField ref = rowOI.getAllStructFieldRefs().get(0);
    assertEquals(ObjectInspector.Category.STRUCT, ref.getFieldObjectInspector().getCategory());

    Schema schema = AvroSerdeUtils.getSchemaFor(
        props.getProperty(AvroSerdeUtils.AvroTableProperties.SCHEMA_LITERAL.getPropName()));
    GenericData.Record encoded =
        new GenericData.Record(AvroSerdeUtils.getOtherTypeFromNullableType(schema.getField("v").schema()));
    encoded.put(VariantEncoding.METADATA, ByteBuffer.wrap(new byte[] {1, 0, 0}));
    encoded.put(VariantEncoding.VALUE, ByteBuffer.wrap(new byte[] {12, 7}));
    GenericData.Record record = new GenericData.Record(schema);
    record.put("v", encoded);
    List<?> fields =
        (List<?>) rowOI.getStructFieldData(serde.deserialize(Utils.serializeAndDeserializeRecord(record)), ref);
    assertArrayEquals(new byte[] {1, 0, 0}, (byte[]) fields.get(0));
    assertArrayEquals(new byte[] {12, 7}, (byte[]) fields.get(1));
  }

  @Test
  public void testUserSchemaRecordWithTheLogicalTypeIsAVariant() throws Exception {
    Properties props = new Properties();
    AvroSerDe serde = serdeOfLiteral(
        PLAIN_RECORD.replace("\"name\":\"r\",", "\"name\":\"r\",\"logicalType\":\"variant\","), props);

    assertEquals("variant", props.getProperty(serdeConstants.LIST_COLUMN_TYPES));
    StructObjectInspector rowOI = (StructObjectInspector) serde.getObjectInspector();
    assertEquals(ObjectInspector.Category.VARIANT,
        rowOI.getAllStructFieldRefs().get(0).getFieldObjectInspector().getCategory());
  }

  @Test
  public void testLogicalTypeOnANonRecordIsNotAVariant() throws Exception {
    Properties props = new Properties();
    AvroSerDe serde = serdeOfLiteral("{\"type\":\"bytes\",\"logicalType\":\"variant\"}", props);

    assertEquals("binary", props.getProperty(serdeConstants.LIST_COLUMN_TYPES));
    assertEquals(ObjectInspector.Category.PRIMITIVE,
        ((StructObjectInspector) serde.getObjectInspector()).getAllStructFieldRefs().get(0)
            .getFieldObjectInspector().getCategory());
  }
}
