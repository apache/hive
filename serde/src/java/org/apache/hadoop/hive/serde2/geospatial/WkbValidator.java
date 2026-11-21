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
package org.apache.hadoop.hive.serde2.geospatial;

/**
 * Structural validator for ISO well-known binary (WKB), the encoding of geospatial values in the
 * Iceberg and Parquet specifications.
 *
 * <p>It walks the bytes without materializing a geometry and rejects:
 * <ul>
 *   <li>empty, truncated or over-long input and invalid byte-order markers,</li>
 *   <li>unknown geometry types and extended WKB (EWKB) flags, such as an embedded SRID,</li>
 *   <li>collection members with an unexpected type or dimension,</li>
 *   <li>infinite coordinates, and NaN coordinates outside points (NaN encodes an empty point),</li>
 *   <li>line strings with a single point, and polygon rings that are not closed or have fewer than four
 *   points.</li>
 * </ul>
 */
public final class WkbValidator {

  private static final int MAX_NESTING_DEPTH = 64;

  private static final int POINT = 1;
  private static final int LINESTRING = 2;
  private static final int POLYGON = 3;
  private static final int MULTIPOINT = 4;
  private static final int MULTILINESTRING = 5;
  private static final int MULTIPOLYGON = 6;
  private static final int GEOMETRYCOLLECTION = 7;

  private static final long EWKB_FLAGS = 0xE0000000L;

  /** Byte order marker, type code and element count or one coordinate: the smallest possible member. */
  private static final int MIN_GEOMETRY_SIZE = 1 + 4 + 4;

  private final byte[] bytes;
  private final int offset;
  private final int end;
  private int pos;
  private boolean littleEndian;

  private WkbValidator(byte[] bytes, int offset, int length) {
    this.bytes = bytes;
    this.offset = offset;
    this.pos = offset;
    this.end = offset + length;
  }

  public static void validate(byte[] bytes) {
    validate(bytes, 0, bytes.length);
  }

  /**
   * Validates {@code length} bytes of {@code bytes} starting at {@code offset}.
   *
   * @throws IllegalArgumentException if the bytes are not valid ISO WKB
   */
  public static void validate(byte[] bytes, int offset, int length) {
    if (length == 0) {
      throw new IllegalArgumentException("Invalid WKB: empty input");
    }
    WkbValidator validator = new WkbValidator(bytes, offset, length);
    validator.readGeometry(0, 0, -1);
    if (validator.pos != validator.end) {
      throw validator.error("unexpected trailing bytes", validator.pos);
    }
  }

  /**
   * Reads one geometry. {@code expectedType} and {@code expectedDimensions} constrain collection members;
   * zero and -1 mean unconstrained.
   */
  private void readGeometry(int depth, int expectedType, int expectedDimensions) {
    if (depth > MAX_NESTING_DEPTH) {
      throw error("geometry collections are nested deeper than " + MAX_NESTING_DEPTH, pos);
    }
    int start = pos;
    readByteOrder();
    long typeCode = readUnsignedInt();
    if ((typeCode & EWKB_FLAGS) != 0) {
      throw error("extended WKB (EWKB) is not supported, geometry values must be ISO WKB", start + 1);
    }
    int type = (int) (typeCode % 1000);
    int dimensionCode = (int) (typeCode / 1000);
    if (type < POINT || type > GEOMETRYCOLLECTION || dimensionCode > 3) {
      throw error("invalid or unsupported geometry type " + typeCode, start + 1);
    }
    if (expectedType != 0 && type != expectedType) {
      throw error("geometry type " + type + " is not allowed here, expected " + expectedType, start + 1);
    }
    int dimensions = dimensionCode == 0 ? 2 : dimensionCode == 3 ? 4 : 3;
    if (expectedDimensions != -1 && dimensions != expectedDimensions) {
      throw error("collection members must have the same dimension as the collection", start + 1);
    }

    switch (type) {
    case POINT:
      readCoordinate(dimensions, true);
      break;
    case LINESTRING:
      readLineString(dimensions);
      break;
    case POLYGON:
      readPolygon(dimensions);
      break;
    case MULTIPOINT:
      readMembers(depth, POINT, dimensions);
      break;
    case MULTILINESTRING:
      readMembers(depth, LINESTRING, dimensions);
      break;
    case MULTIPOLYGON:
      readMembers(depth, POLYGON, dimensions);
      break;
    default:
      readMembers(depth, 0, dimensions);
      break;
    }
  }

  private void readLineString(int dimensions) {
    int countPos = pos;
    long points = readCount(dimensions * Double.BYTES);
    if (points == 1) {
      throw error("a line string must have zero or at least two points", countPos);
    }
    for (long i = 0; i < points; i++) {
      readCoordinate(dimensions, false);
    }
  }

  private void readPolygon(int dimensions) {
    long rings = readCount(4);
    for (long ring = 0; ring < rings; ring++) {
      int ringStart = pos;
      long points = readCount(dimensions * Double.BYTES);
      if (points < 4) {
        throw error("a polygon ring must have at least four points", ringStart);
      }
      int first = pos;
      for (long i = 0; i < points; i++) {
        readCoordinate(dimensions, false);
      }
      int last = pos - dimensions * Double.BYTES;
      if (readDouble(first) != readDouble(last)
          || readDouble(first + Double.BYTES) != readDouble(last + Double.BYTES)) {
        throw error("a polygon ring must be closed", ringStart);
      }
    }
  }

  private void readMembers(int depth, int memberType, int dimensions) {
    long members = readCount(MIN_GEOMETRY_SIZE);
    for (long i = 0; i < members; i++) {
      readGeometry(depth + 1, memberType, dimensions);
    }
  }

  private void readCoordinate(int dimensions, boolean allowNaN) {
    ensureAvailable(dimensions * Double.BYTES);
    for (int i = 0; i < dimensions; i++) {
      int valuePos = pos;
      double value = readDouble(pos);
      pos += Double.BYTES;
      if (Double.isInfinite(value) || (!allowNaN && Double.isNaN(value))) {
        throw error("coordinates must be finite", valuePos);
      }
    }
  }

  private void readByteOrder() {
    ensureAvailable(1);
    byte order = bytes[pos];
    if (order != 0 && order != 1) {
      throw error("invalid byte order " + order, pos);
    }
    littleEndian = order == 1;
    pos++;
  }

  /** Reads an element count and checks that the elements, at least {@code minElementSize} bytes each, fit. */
  private long readCount(int minElementSize) {
    int countPos = pos;
    long count = readUnsignedInt();
    if (count * minElementSize > end - pos) {
      throw error("element count " + count + " exceeds the remaining bytes", countPos);
    }
    return count;
  }

  private long readUnsignedInt() {
    ensureAvailable(4);
    long value = littleEndian
        ? (bytes[pos] & 0xFFL) | (bytes[pos + 1] & 0xFFL) << 8 | (bytes[pos + 2] & 0xFFL) << 16
            | (bytes[pos + 3] & 0xFFL) << 24
        : (bytes[pos] & 0xFFL) << 24 | (bytes[pos + 1] & 0xFFL) << 16 | (bytes[pos + 2] & 0xFFL) << 8
            | (bytes[pos + 3] & 0xFFL);
    pos += 4;
    return value;
  }

  private double readDouble(int at) {
    long bits = 0;
    for (int i = 0; i < Double.BYTES; i++) {
      int shift = littleEndian ? 8 * i : 8 * (Double.BYTES - 1 - i);
      bits |= (bytes[at + i] & 0xFFL) << shift;
    }
    return Double.longBitsToDouble(bits);
  }

  private void ensureAvailable(int count) {
    if (end - pos < count) {
      throw error("unexpected end of input", pos);
    }
  }

  private IllegalArgumentException error(String reason, int at) {
    return new IllegalArgumentException("Invalid WKB: " + reason + " at position " + (at - offset + 1));
  }
}
