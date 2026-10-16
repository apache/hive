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
package org.apache.hadoop.hive.serde2.variant;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.VerifyFast;
import org.apache.hadoop.hive.serde2.fast.DeserializeRead;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.VariantObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/** Helpers for the serde tests of VARIANT columns. */
public final class VariantTestUtils {

  /** The type name of the struct of the two binaries that serdes store a variant as. */
  public static final String ENCODING = VariantEncoding.TYPE_INFO.getTypeName();

  private VariantTestUtils() {
  }

  public static Variant variant(String json) {
    try {
      return VariantBuilder.parseJson(json, false);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static Properties columns(String names, String types) {
    Properties props = new Properties();
    props.setProperty(serdeConstants.LIST_COLUMNS, names);
    props.setProperty(serdeConstants.LIST_COLUMN_TYPES, types);
    return props;
  }

  public static <T extends AbstractSerDe> T init(T serde, Properties props) throws SerDeException {
    serde.initialize(new Configuration(), props, null);
    return serde;
  }

  public static StructObjectInspector row(List<String> names, List<ObjectInspector> fieldOIs) {
    return ObjectInspectorFactory.getStandardStructObjectInspector(names, fieldOIs);
  }

  public static ObjectInspector variantOI() {
    return ObjectInspectorFactory.getStandardVariantObjectInspector();
  }

  /** The variant in field {@code field} of a row that a serde read. */
  public static Variant read(StructObjectInspector rowOI, Object row, int field) {
    StructField ref = rowOI.getAllStructFieldRefs().get(field);
    return ((VariantObjectInspector) ref.getFieldObjectInspector()).getVariant(rowOI.getStructFieldData(row, ref));
  }

  /** Asserts that {@code fields}, an encoding struct read by a fast reader, holds the binaries of {@code expected}. */
  public static void assertEncodingFields(Variant expected, Object fields) {
    List<?> binaries = (List<?>) fields;
    assertArrayEquals(expected.getMetadata(),
        ((BytesWritable) binaries.get(VariantEncoding.METADATA_INDEX)).copyBytes());
    assertArrayEquals(expected.getValue(), ((BytesWritable) binaries.get(VariantEncoding.VALUE_INDEX)).copyBytes());
  }

  /**
   * Reads a row of (variant, int, variant) that holds (v, 42, NULL): the first variant as its encoding struct, then
   * the int after it, then the NULL.
   */
  public static void assertReadsAroundVariantFields(DeserializeRead reader, Variant v) throws IOException {
    assertEncodingFields(v, VerifyFast.deserializeReadComplexType(reader, VariantEncoding.TYPE_INFO));
    assertTrue(reader.readNextField());
    assertEquals(42, reader.currentInt);
    assertFalse(reader.readNextField());
  }

  /** Reads a row of {@code (array<variant>, map<string,variant>)} that holds ([a, b], {k: b}) as encoding structs. */
  public static void assertReadsNestedVariantsAsEncodingStructs(DeserializeRead reader, Variant a, Variant b)
      throws IOException {
    List<?> list = (List<?>) VerifyFast.deserializeReadComplexType(reader,
        TypeInfoFactory.getListTypeInfo(VariantEncoding.TYPE_INFO));
    assertEquals(2, list.size());
    assertEncodingFields(a, list.get(0));
    assertEncodingFields(b, list.get(1));
    Map<?, ?> map = (Map<?, ?>) VerifyFast.deserializeReadComplexType(reader,
        TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.stringTypeInfo, VariantEncoding.TYPE_INFO));
    assertEquals(1, map.size());
    assertEncodingFields(b, map.get(new Text("k")));
  }

  /**
   * Asserts that two serdes over the same column type return the same row inspector, so the static inspector caches
   * of the lazy serdes do not grow with each initialize.
   */
  public static void assertRowInspectorIsShared(Supplier<AbstractSerDe> serde) throws SerDeException {
    for (String type : new String[] {"variant", "array<variant>", "map<string,variant>", "struct<a:variant>"}) {
      ObjectInspector first = init(serde.get(), columns("c", type)).getObjectInspector();
      ObjectInspector second = init(serde.get(), columns("c", type)).getObjectInspector();
      assertSame(type, first, second);
    }
  }
}
