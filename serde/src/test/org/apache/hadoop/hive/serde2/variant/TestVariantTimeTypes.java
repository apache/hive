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

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.DateTimeException;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;

import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.VariantObjectInspector;
import org.junit.Test;

import static org.apache.hadoop.hive.serde2.variant.VariantUtil.TIME;
import static org.apache.hadoop.hive.serde2.variant.VariantUtil.TIMESTAMP_NANOS;
import static org.apache.hadoop.hive.serde2.variant.VariantUtil.TIMESTAMP_NTZ_NANOS;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

/**
 * Variant primitive types TIME, TIMESTAMP_NANOS and TIMESTAMP_NTZ_NANOS, built byte by byte as the Parquet variant
 * spec encodes them.
 */
public class TestVariantTimeTypes {

  // version 1, empty dictionary
  private static final byte[] EMPTY_METADATA = {0x01, 0x00, 0x00};
  // version 1, dictionary [a, b, c]
  private static final byte[] ABC_METADATA = {0x01, 0x03, 0x00, 0x01, 0x02, 0x03, 'a', 'b', 'c'};

  // 13:45:30.123456
  private static final long TIME_MICROS = 49_530_123_456L;
  // 2024-01-15 13:45:30.123456789 UTC
  private static final long NANOS = 1_705_326_330_123_456_789L;
  // 1969-12-31 23:59:59.876543211 UTC
  private static final long PRE_EPOCH_NANOS = -123_456_789L;

  private static final ZoneId LOS_ANGELES = ZoneId.of("America/Los_Angeles");

  // Header (type id << 2 | basic type PRIMITIVE) followed by an 8-byte little-endian payload.
  private static byte[] primitive(int typeId, long payload) {
    return ByteBuffer.allocate(9).order(ByteOrder.LITTLE_ENDIAN)
        .put((byte) (typeId << 2)).putLong(payload).array();
  }

  // An array with a 1-byte size and 1-byte offsets.
  private static byte[] array(byte[]... elements) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.write(0x03);
    out.write(elements.length);
    writeOffsets(out, elements);
    return out.toByteArray();
  }

  // An object with a 1-byte size, ids and offsets; field i has dictionary id i.
  private static byte[] object(byte[]... fields) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.write(0x02);
    out.write(fields.length);
    for (int i = 0; i < fields.length; i++) {
      out.write(i);
    }
    writeOffsets(out, fields);
    return out.toByteArray();
  }

  private static void writeOffsets(ByteArrayOutputStream out, byte[]... values) {
    int offset = 0;
    out.write(offset);
    for (byte[] value : values) {
      offset += value.length;
      out.write(offset);
    }
    for (byte[] value : values) {
      out.writeBytes(value);
    }
  }

  private static Variant variant(byte[] value) {
    return new Variant(value, EMPTY_METADATA);
  }

  @Test
  public void testTime() {
    Variant v = variant(primitive(TIME, TIME_MICROS));
    assertEquals(VariantUtil.Type.TIME, v.getType());
    assertEquals(TIME_MICROS, v.getLong());
    assertEquals(9, VariantUtil.valueSize(v.getValue(), 0));
    assertEquals("\"13:45:30.123456\"", v.toJson(ZoneOffset.UTC));
    assertEquals("\"13:45:30.123456\"", v.toJson(LOS_ANGELES));
    assertEquals("\"00:00:00\"", variant(primitive(TIME, 0)).toJson(ZoneOffset.UTC));
    assertEquals("\"23:59:59.999999\"",
        variant(primitive(TIME, 86_399_999_999L)).toJson(ZoneOffset.UTC));
  }

  @Test
  public void testTimestampNanos() {
    Variant v = variant(primitive(TIMESTAMP_NANOS, NANOS));
    assertEquals(VariantUtil.Type.TIMESTAMP_NANOS, v.getType());
    assertEquals(NANOS, v.getLong());
    assertEquals(9, VariantUtil.valueSize(v.getValue(), 0));
    assertEquals("\"2024-01-15 13:45:30.123456789+00:00\"", v.toJson(ZoneOffset.UTC));
    assertEquals("\"2024-01-15 05:45:30.123456789-08:00\"", v.toJson(LOS_ANGELES));
    assertEquals("\"1969-12-31 23:59:59.876543211+00:00\"",
        variant(primitive(TIMESTAMP_NANOS, PRE_EPOCH_NANOS)).toJson(ZoneOffset.UTC));
  }

  @Test
  public void testTimestampNtzNanos() {
    Variant v = variant(primitive(TIMESTAMP_NTZ_NANOS, NANOS));
    assertEquals(VariantUtil.Type.TIMESTAMP_NTZ_NANOS, v.getType());
    assertEquals(NANOS, v.getLong());
    assertEquals(9, VariantUtil.valueSize(v.getValue(), 0));
    assertEquals("\"2024-01-15 13:45:30.123456789\"", v.toJson(ZoneOffset.UTC));
    assertEquals("\"2024-01-15 13:45:30.123456789\"", v.toJson(LOS_ANGELES));
    assertEquals("\"1969-12-31 23:59:59.876543211\"",
        variant(primitive(TIMESTAMP_NTZ_NANOS, PRE_EPOCH_NANOS)).toJson(ZoneOffset.UTC));
  }

  @Test
  public void testNanosRenderLikeMicros() {
    long micros = NANOS / 1_000;
    for (ZoneId zone : new ZoneId[] {ZoneOffset.UTC, LOS_ANGELES}) {
      assertEquals(variant(primitive(VariantUtil.TIMESTAMP, micros)).toJson(zone),
          variant(primitive(TIMESTAMP_NANOS, micros * 1_000)).toJson(zone));
      assertEquals(variant(primitive(VariantUtil.TIMESTAMP_NTZ, micros)).toJson(zone),
          variant(primitive(TIMESTAMP_NTZ_NANOS, micros * 1_000)).toJson(zone));
    }
  }

  @Test
  public void testInArray() {
    byte[] time = primitive(TIME, TIME_MICROS);
    byte[] nanos = primitive(TIMESTAMP_NANOS, NANOS);
    byte[] ntzNanos = primitive(TIMESTAMP_NTZ_NANOS, PRE_EPOCH_NANOS);
    Variant v = variant(array(time, nanos, ntzNanos));
    assertEquals("[\"13:45:30.123456\",\"2024-01-15 13:45:30.123456789+00:00\"," +
        "\"1969-12-31 23:59:59.876543211\"]", v.toJson(ZoneOffset.UTC));
    assertArrayEquals(time, v.getElementAtIndex(0).getValue());
    assertArrayEquals(nanos, v.getElementAtIndex(1).getValue());
    assertArrayEquals(ntzNanos, v.getElementAtIndex(2).getValue());
  }

  @Test
  public void testInObject() {
    byte[] time = primitive(TIME, TIME_MICROS);
    byte[] nanos = primitive(TIMESTAMP_NANOS, PRE_EPOCH_NANOS);
    byte[] ntzNanos = primitive(TIMESTAMP_NTZ_NANOS, NANOS);
    Variant v = new Variant(object(time, nanos, ntzNanos), ABC_METADATA);
    assertEquals("{\"a\":\"13:45:30.123456\",\"b\":\"1969-12-31 23:59:59.876543211+00:00\"," +
        "\"c\":\"2024-01-15 13:45:30.123456789\"}", v.toJson(ZoneOffset.UTC));
    assertArrayEquals(time, v.getFieldByKey("a").getValue());
    assertArrayEquals(nanos, v.getFieldByKey("b").getValue());
    assertArrayEquals(ntzNanos, v.getFieldByKey("c").getValue());
    assertEquals(PRE_EPOCH_NANOS, v.getFieldByKey("b").getLong());
  }

  @Test
  public void testSerDeUtilsDecodesTheVariant() {
    byte[] value = object(primitive(TIME, TIME_MICROS), primitive(TIMESTAMP_NANOS, NANOS),
        array(primitive(TIMESTAMP_NTZ_NANOS, PRE_EPOCH_NANOS)));
    List<Object> row = List.of(ABC_METADATA, value);
    ObjectInspector oi = VariantObjectInspector.get();
    String expected = "{\"a\":\"13:45:30.123456\",\"b\":\"2024-01-15 13:45:30.123456789+00:00\"," +
        "\"c\":[\"1969-12-31 23:59:59.876543211\"]}";
    assertEquals(expected, SerDeUtils.getJSONString(row, oi, "null", true));
    assertEquals(expected, SerDeUtils.toThriftPayload(row, oi, 10));
  }

  @Test
  public void testMalformedValuesThrow() {
    byte[] truncated = new byte[] {TIME << 2, 1, 2, 3, 4};
    RuntimeException e = assertThrows(RuntimeException.class,
        () -> variant(truncated).toJson(ZoneOffset.UTC));
    assertEquals("MALFORMED_VARIANT", e.getMessage());
    e = assertThrows(RuntimeException.class, () -> VariantUtil.valueSize(new byte[] {21 << 2}, 0));
    assertEquals("UNKNOWN_PRIMITIVE_TYPE_IN_VARIANT{id=21}", e.getMessage());
    assertThrows(DateTimeException.class, () -> variant(primitive(TIME, -1)).toJson(ZoneOffset.UTC));
    assertThrows(DateTimeException.class,
        () -> variant(primitive(TIME, 86_400_000_000L)).toJson(ZoneOffset.UTC));
  }
}
