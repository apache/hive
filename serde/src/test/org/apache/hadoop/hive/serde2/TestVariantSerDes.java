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
package org.apache.hadoop.hive.serde2;

import java.io.IOException;
import java.rmi.server.UID;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;
import org.apache.hadoop.hive.serde2.avro.AvroSerDe;
import org.apache.hadoop.hive.serde2.binarysortable.BinarySortableSerDe;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe2;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.VariantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.variant.Variant;
import org.apache.hadoop.hive.serde2.variant.VariantEncoding;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import static org.apache.hadoop.hive.serde2.variant.VariantTestUtils.ENCODING;
import static org.apache.hadoop.hive.serde2.variant.VariantTestUtils.columns;
import static org.apache.hadoop.hive.serde2.variant.VariantTestUtils.init;
import static org.apache.hadoop.hive.serde2.variant.VariantTestUtils.row;
import static org.apache.hadoop.hive.serde2.variant.VariantTestUtils.variant;
import static org.apache.hadoop.hive.serde2.variant.VariantTestUtils.variantOI;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

/**
 * Each serde stores a variant as the struct of its two binaries: a variant column writes the bytes of that struct and
 * reads back as a variant, at the top level and nested in an array, a map and a struct.
 */
@RunWith(Parameterized.class)
public class TestVariantSerDes {

  private static final Configuration CONF = new Configuration();

  /** A serde under test, created for columns (v, l, m, s) of the element type given. */
  record Format(String name, boolean writesTheEncodingStruct, SerDeFactory factory) {
    AbstractSerDe serde(String element) throws Exception {
      return factory.create(columns("v,l,m,s",
          element + ",array<" + element + ">,map<string," + element + ">,struct<a:" + element + ">"));
    }

    @Override
    public String toString() {
      return name;
    }
  }

  @FunctionalInterface
  interface SerDeFactory {
    AbstractSerDe create(Properties props) throws Exception;
  }

  @Parameters(name = "{0}")
  public static List<Object[]> formats() {
    return List.of(
        format("LazySimpleSerDe", true, props -> init(new LazySimpleSerDe(), props)),
        format("LazyBinarySerDe", true, props -> init(new LazyBinarySerDe(), props)),
        format("LazyBinarySerDe2", true, props -> init(new LazyBinarySerDe2(), props)),
        format("ColumnarSerDe", true, props -> init(new ColumnarSerDe(), props)),
        format("LazyBinaryColumnarSerDe", true, props -> init(new LazyBinaryColumnarSerDe(), props)),
        format("BinarySortableSerDe ascending", true, props -> binarySortable(props, "++++")),
        format("BinarySortableSerDe descending", true, props -> binarySortable(props, "----")),
        format("JsonSerDe", true, props -> init(new JsonSerDe(), props)),
        // Avro writes Iceberg's variant record of two plain binaries: TestAvroVariant pins its datum
        format("AvroSerDe", false, props -> init(new AvroSerDe(), props)));
  }

  private static Object[] format(String name, boolean writesTheEncodingStruct, SerDeFactory factory) {
    return new Object[] {new Format(name, writesTheEncodingStruct, factory)};
  }

  private static AbstractSerDe binarySortable(Properties props, String order) throws SerDeException {
    props.setProperty(serdeConstants.SERIALIZATION_SORT_ORDER, order);
    props.setProperty(serdeConstants.SERIALIZATION_NULL_SORT_ORDER, "aaaa");
    return init(new BinarySortableSerDe(), props);
  }

  private final Format format;

  public TestVariantSerDes(Format format) {
    this.format = format;
  }

  @Test
  public void testVariantColumnsWriteTheEncodingStructBytes() throws Exception {
    assumeTrue(format.writesTheEncodingStruct());
    List<Object> values = Arrays.asList(variant("{\"k\":[1,\"x\"]}"),
        Arrays.asList(variant("1"), null, variant("{\"b\":\"c\"}")),
        map("k", variant("[2,3]"), "n", null), Arrays.asList(variant("\"s\"")));
    for (List<Object> row : List.of(values, Arrays.asList(null, null, null, null))) {
      Writable asVariant = keep(format.serde("variant").serialize(row, rowOI(variantOI())));
      Writable asStruct = keep(format.serde(ENCODING).serialize(encode(row),
          rowOI(VariantEncoding.JAVA_OBJECT_INSPECTOR)));
      assertEquals(asStruct, asVariant);
    }
  }

  /**
   * The lazy serdes read each row into the same objects and reuse a binary's buffer when the next binary fits. Each
   * metadata and value binary of the first row is longer than its counterpart in the second, so a variant that shared
   * such a buffer would change when the second row is read.
   */
  @Test
  public void testVariantsReadBackAndOutliveTheirRow() throws Exception {
    List<Object> longer = Arrays.asList(variant("{\"a_long_key\":\"" + "x".repeat(1 << 20) + "\"}"),
        Arrays.asList(variant("{\"alpha\":[1,2,3],\"beta\":\"a long string value\"}"), null, variant("7")),
        map("k", variant("{\"k\":[1,\"x\"]}"), "n", null), Arrays.asList(variant("{\"name\":[true,null,\"zz\"]}")));
    List<Object> shorter = Arrays.asList(variant("{\"a\":1}"), Arrays.asList(variant("2")),
        map("k", variant("3")), Arrays.asList(variant("4")));
    AbstractSerDe serde = format.serde("variant");
    Writable first = keep(serde.serialize(longer, rowOI(variantOI())));
    Writable second = keep(serde.serialize(shorter, rowOI(variantOI())));

    ObjectInspector columnOI =
        ((StructObjectInspector) serde.getObjectInspector()).getAllStructFieldRefs().get(0).getFieldObjectInspector();
    assertEquals(ObjectInspector.Category.VARIANT, columnOI.getCategory());
    assertEquals("variant", columnOI.getTypeName());

    Object firstRead = readBack(serde, first);
    Object secondRead = readBack(serde, second);
    assertEquals(longer, firstRead);
    assertEquals(shorter, secondRead);
  }

  @Test
  public void testJsonNullAndSqlNullStayDistinct() throws Exception {
    Variant jsonNull = variant("null");
    List<Object> jsonNulls = Arrays.asList(jsonNull, Arrays.asList(jsonNull, null), map("j", jsonNull, "n", null),
        Arrays.asList(jsonNull));
    List<Object> sqlNulls = Arrays.asList(null, null, null, null);
    AbstractSerDe serde = format.serde("variant");

    List<?> read = (List<?>) readBack(serde, keep(serde.serialize(jsonNulls, rowOI(variantOI()))));
    assertEquals(jsonNull, read.get(0));
    assertEquals(jsonNulls, read);
    assertEquals(sqlNulls, readBack(serde, keep(serde.serialize(sqlNulls, rowOI(variantOI())))));
  }

  /** The row inspector of (v, l, m, s) over the given variant or encoding struct inspector. */
  private static StructObjectInspector rowOI(ObjectInspector element) {
    return row(List.of("v", "l", "m", "s"), List.of(element,
        ObjectInspectorFactory.getStandardListObjectInspector(element),
        ObjectInspectorFactory.getStandardMapObjectInspector(
            PrimitiveObjectInspectorFactory.javaStringObjectInspector, element),
        row(List.of("a"), List.of(element))));
  }

  private static Map<String, Object> map(String key1, Object value1, String key2, Object value2) {
    Map<String, Object> map = map(key1, value1);
    map.put(key2, value2);
    return map;
  }

  private static Map<String, Object> map(String key, Object value) {
    Map<String, Object> map = new LinkedHashMap<>();
    map.put(key, value);
    return map;
  }

  /** The value with each variant in it replaced by its encoding struct. */
  private static Object encode(Object value) {
    if (value instanceof Variant variant) {
      return VariantEncoding.fields(variant);
    }
    if (value instanceof List<?> list) {
      List<Object> encoded = new ArrayList<>();
      list.forEach(element -> encoded.add(encode(element)));
      return encoded;
    }
    if (value instanceof Map<?, ?> map) {
      Map<Object, Object> encoded = new LinkedHashMap<>();
      map.forEach((k, v) -> encoded.put(k, encode(v)));
      return encoded;
    }
    return value;
  }

  /** A copy of a written row, as a reader gets it from a file; a serde reuses its output object. */
  private static Writable keep(Writable written) throws IOException {
    if (written instanceof AvroGenericRecordWritable avro) {
      AvroGenericRecordWritable record = new AvroGenericRecordWritable(avro.getRecord());
      record.setRecordReaderID(new UID());
      record.setFileSchema(avro.getRecord().getSchema());
      return WritableUtils.clone(record, CONF);
    }
    return WritableUtils.clone(written, CONF);
  }

  private static Object readBack(AbstractSerDe serde, Writable written) throws SerDeException {
    return toJava(serde.getObjectInspector(), serde.deserialize(written));
  }

  /** {@code data} as a variant, a list (of a struct's fields or an array's elements), a map or a Java value. */
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
}
