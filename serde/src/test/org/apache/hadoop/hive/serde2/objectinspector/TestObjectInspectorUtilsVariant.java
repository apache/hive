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
package org.apache.hadoop.hive.serde2.objectinspector;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.NullValueOption;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.variant.Variant;
import org.apache.hadoop.hive.serde2.variant.VariantBuilder;
import org.apache.hadoop.hive.serde2.variant.VariantEncoding;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.hadoop.hive.serde2.variant.VariantTestUtils.variant;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class TestObjectInspectorUtilsVariant {

  private static final StandardVariantObjectInspector STANDARD =
      ObjectInspectorFactory.getStandardVariantObjectInspector();
  private static final StructEncodedVariantObjectInspector ENCODED =
      new StructEncodedVariantObjectInspector(VariantEncoding.JAVA_OBJECT_INSPECTOR);
  private static final ObjectInspector BINARY = PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
  private static final StructEncodedVariantObjectInspector WRITABLE_ENCODED = new StructEncodedVariantObjectInspector(
      ObjectInspectorFactory.getStandardStructObjectInspector(List.of(VariantEncoding.METADATA, VariantEncoding.VALUE),
          List.of(PrimitiveObjectInspectorFactory.writableBinaryObjectInspector,
              PrimitiveObjectInspectorFactory.writableBinaryObjectInspector)));

  private record Inspected(Object data, ObjectInspector oi) {
    @Override
    public String toString() {
      return oi.getClass().getSimpleName() + (data == null ? " NULL" : "");
    }
  }

  @Test
  public void testStandardObjectInspectorOfAnyVariantIsTheStandardOne() {
    for (ObjectInspectorCopyOption option : ObjectInspectorCopyOption.values()) {
      assertSame(STANDARD, ObjectInspectorUtils.getStandardObjectInspector(ENCODED, option));
      assertSame(STANDARD, ObjectInspectorUtils.getStandardObjectInspector(STANDARD, option));
    }
  }

  @Test
  public void testCopyToStandardObjectReadsTheVariant() {
    Variant v = variant("{\"k\":[1,2]}");
    assertEquals(v, ObjectInspectorUtils.copyToStandardObject(VariantEncoding.fields(v), ENCODED));
    assertEquals(v, ObjectInspectorUtils.copyToStandardObject(v, STANDARD));
    assertNull(ObjectInspectorUtils.copyToStandardObject(null, ENCODED));
  }

  @Test
  public void testConverterBetweenVariantInspectors() {
    Variant v = variant("\"s\"");
    Converter converter = ObjectInspectorConverters.getConverter(ENCODED, STANDARD);
    assertEquals(v, converter.convert(VariantEncoding.fields(v)));
    assertNull(converter.convert(null));
    assertTrue(ObjectInspectorConverters.getConverter(STANDARD, STANDARD)
        instanceof ObjectInspectorConverters.IdentityConverter);
  }

  @Test
  public void testStructDoesNotConvertToVariant() {
    RuntimeException e = Assert.assertThrows(RuntimeException.class,
        () -> ObjectInspectorConverters.getConverter(VariantEncoding.JAVA_OBJECT_INSPECTOR, STANDARD));
    assertTrue(e.getMessage(), e.getMessage().contains("to variant"));
  }

  @Test
  public void testVariantConvertsToString() {
    Converter converter =
        ObjectInspectorConverters.getConverter(ENCODED, PrimitiveObjectInspectorFactory.writableStringObjectInspector);
    // objects and arrays give their JSON, a string its characters, other scalars their plain text
    for (String[] jsonAndText : new String[][] {
        {"{\"a\":1,\"b\":\"x\"}", "{\"a\":1,\"b\":\"x\"}"}, {"[1,\"a\",null]", "[1,\"a\",null]"},
        {"\"text\"", "text"}, {"\"say \\\"hi\\\"\"", "say \"hi\""}, {"true", "true"}, {"42", "42"},
        {"1.50", "1.5"}, {"1e10", "1.0E10"}}) {
      assertEquals(jsonAndText[0], new Text(jsonAndText[1]),
          converter.convert(VariantEncoding.fields(variant(jsonAndText[0]))));
    }
    VariantBuilder binary = new VariantBuilder(false);
    binary.appendBinary(new byte[] {(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF});
    assertEquals(new Text("3q2+7w=="), converter.convert(VariantEncoding.fields(binary.result())));
    VariantBuilder date = new VariantBuilder(false);
    date.appendDate(20662);
    assertEquals(new Text("2026-07-28"), converter.convert(VariantEncoding.fields(date.result())));

    // a JSON null and a SQL NULL both give NULL
    assertNull(converter.convert(VariantEncoding.fields(variant("null"))));
    assertNull(converter.convert(null));
    assertNull(ObjectInspectorConverters.getConverter(STANDARD,
        PrimitiveObjectInspectorFactory.javaStringObjectInspector).convert(variant("null")));
  }

  @Test
  public void testConvertedObjectInspector() {
    assertSame(STANDARD, ObjectInspectorConverters.getConvertedOI(STANDARD, ENCODED));
    assertSame(STANDARD, ObjectInspectorConverters.getConvertedOI(ENCODED, STANDARD));
    assertTrue(ObjectInspectorUtils.hasAllFieldsSettable(STANDARD));
    assertFalse(ObjectInspectorUtils.hasAllFieldsSettable(ENCODED));
  }

  @Test
  public void testConstantObjectInspector() {
    Variant v = variant("[true]");
    ConstantObjectInspector fromStandard = ObjectInspectorUtils.getConstantObjectInspector(STANDARD, v);
    assertTrue(fromStandard instanceof StandardConstantVariantObjectInspector);
    assertEquals(v, fromStandard.getWritableConstantValue());

    ConstantObjectInspector fromEncoded =
        ObjectInspectorUtils.getConstantObjectInspector(ENCODED, VariantEncoding.fields(v));
    assertEquals(v, fromEncoded.getWritableConstantValue());

    assertNull(ObjectInspectorUtils.getConstantObjectInspector(STANDARD, null).getWritableConstantValue());
    assertFalse(ObjectInspectorUtils.supportsConstantObjectInspector(STANDARD));
  }

  @Test
  public void testTypeChecks() {
    assertTrue(ObjectInspectorUtils.compareTypes(STANDARD, ENCODED));
    assertFalse(ObjectInspectorUtils.compareTypes(STANDARD, VariantEncoding.JAVA_OBJECT_INSPECTOR));
    assertTrue(ObjectInspectorUtils.compareSupported(STANDARD));
    assertEquals("StandardVariantObjectInspector", ObjectInspectorUtils.getObjectInspectorName(STANDARD));
  }

  @Test
  public void testHashCodeMurmurMatchesTheStructPath() {
    // the long string has value header 0xA1 and "é" has metadata bytes 0xC3 0xA9: bytes at or above 0x80
    for (String json : List.of("1", "\"abc\"", "{\"a\":1,\"b\":[true,null]}", "null",
        "\"" + "x".repeat(40) + "\"", "{\"é\":1}")) {
      Variant v = variant(json);
      assertEquals(json, ObjectInspectorUtils.hashCodeMurmur(VariantEncoding.fields(v),
          VariantEncoding.JAVA_OBJECT_INSPECTOR, ByteBuffer.allocate(8)),
          ObjectInspectorUtils.hashCodeMurmur(v, STANDARD, ByteBuffer.allocate(8)));
    }
  }

  /** A writable whose backing array holds high-bit bytes past its length, as a reused serde buffer does. */
  private static BytesWritable padded(byte[] bytes) {
    byte[] buffer = Arrays.copyOf(bytes, bytes.length + 8);
    Arrays.fill(buffer, bytes.length, buffer.length, (byte) 0xFF);
    return new BytesWritable(buffer, bytes.length);
  }

  /** The variant, or SQL NULL, as each inspector flavour holds it. */
  private static List<Inspected> flavours(Variant v) {
    return List.of(new Inspected(v, STANDARD),
        new Inspected(v, ObjectInspectorFactory.getStandardConstantVariantObjectInspector(v)),
        new Inspected(v == null ? null : VariantEncoding.fields(v), ENCODED),
        new Inspected(v == null ? null : List.of(padded(v.getMetadata()), padded(v.getValue())), WRITABLE_ENCODED));
  }

  private static Variant variantOf(Inspected inspected) {
    return ((VariantObjectInspector) inspected.oi()).getVariant(inspected.data());
  }

  /** The metadata binaries compared, then the value binaries. */
  private static int binaryCompare(Inspected a, Inspected b, NullValueOption nulls) {
    if (a.data() == null) {
      return b.data() == null ? 0 : nulls.getCmpReturnValue();
    }
    if (b.data() == null) {
      return -nulls.getCmpReturnValue();
    }
    Variant v1 = variantOf(a);
    Variant v2 = variantOf(b);
    int r = ObjectInspectorUtils.compare(v1.getMetadata(), BINARY, v2.getMetadata(), BINARY, null, nulls);
    return r != 0 ? r : ObjectInspectorUtils.compare(v1.getValue(), BINARY, v2.getValue(), BINARY, null, nulls);
  }

  /** 31 times the metadata binary's hash plus the value binary's hash. */
  private static int binaryHashCode(Inspected a) {
    if (a.data() == null) {
      return 0;
    }
    Variant v = variantOf(a);
    return 31 * ObjectInspectorUtils.hashCode(v.getMetadata(), BINARY)
        + ObjectInspectorUtils.hashCode(v.getValue(), BINARY);
  }

  @Test
  public void testCompareAndHashCodeAreTheBinaryResultsForEveryInspector() {
    Variant one = variant("1");
    byte[] longerValue = Arrays.copyOf(one.getValue(), one.getValue().length + 1);
    longerValue[longerValue.length - 1] = (byte) 0x80;
    byte[] longerMetadata = Arrays.copyOf(one.getMetadata(), one.getMetadata().length + 1);
    List<Variant> values = Arrays.asList(null, one, variant("2"), variant("\"a\""),
        // the same value bytes under different metadata
        variant("{\"a\":1}"), variant("{\"b\":1}"),
        // a value, then metadata, that extends another's
        new Variant(longerValue, one.getMetadata()), new Variant(one.getValue(), longerMetadata),
        // empty
        new Variant(new byte[0], one.getMetadata()), variant("\"\""), variant("{}"), variant("[]"),
        // value header 0xA1 and metadata bytes 0xC3 0xA9: bytes at or above 0x80
        variant("\"" + "x".repeat(40) + "\""), variant("{\"é\":1}"), variant("{\"ab\":1}"),
        // a value that starts inside its buffer
        variant("[1,\"xy\"]").getElementAtIndex(1));

    boolean sawMagnitude = false;
    for (int i = 0; i < values.size(); i++) {
      for (Inspected a : flavours(values.get(i))) {
        assertEquals(i + " " + a, binaryHashCode(a), ObjectInspectorUtils.hashCode(a.data(), a.oi()));
        for (int j = 0; j < values.size(); j++) {
          for (Inspected b : flavours(values.get(j))) {
            for (NullValueOption nulls : NullValueOption.values()) {
              int expected = binaryCompare(a, b, nulls);
              sawMagnitude |= Math.abs(expected) > 1;
              assertEquals(i + " " + a + " vs " + j + " " + b + " " + nulls, expected,
                  ObjectInspectorUtils.compare(a.data(), a.oi(), b.data(), b.oi(), null, nulls));
            }
          }
        }
      }
    }
    // the exact numbers are pinned, not only their signs
    assertTrue(sawMagnitude);
  }
}
