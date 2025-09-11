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
package org.apache.hadoop.hive.serde2.typeinfo;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * Utility class for parsing variant metadata and extracting dictionary strings.
 */
public class VariantUtil {

  /**
   * Parses variant metadata to extract a dictionary of strings.
   * 
   * @param metadata the variant metadata bytes
   * @return list of dictionary strings
   * @throws IOException if the metadata is invalid or corrupted
   */
  public static List<String> parseMetadata(byte[] metadata) throws IOException {
    if (metadata == null || metadata.length == 0) {
      return new ArrayList<>();
    }
    ByteBuffer buf = ByteBuffer.wrap(metadata).order(ByteOrder.LITTLE_ENDIAN);

    byte header = buf.get();
    int version = header & 0x0F;
    if (version != 1) {
      throw new IOException("Unsupported variant metadata version: " + version);
    }
    int offsetSizeMinusOne = (header >> 6) & 0x03;
    int offsetSize = offsetSizeMinusOne + 1;

    int dictionarySize = readUnsignedLE(buf, offsetSize);

    int[] offsets = new int[dictionarySize + 1];
    for (int i = 0; i <= dictionarySize; i++) {
      offsets[i] = readUnsignedLE(buf, offsetSize);
    }

    List<String> dictionary = new ArrayList<>(dictionarySize);
    int bytesStart = buf.position();

    for (int i = 0; i < dictionarySize; i++) {
      int start = offsets[i];
      int end = offsets[i + 1];
      int length = end - start;

      if (length < 0 || bytesStart + start + length > metadata.length) {
        throw new IOException("Invalid string offset in dictionary");
      }

      byte[] stringBytes = new byte[length];
      buf.position(bytesStart + start);
      buf.get(stringBytes);
      dictionary.add(new String(stringBytes, StandardCharsets.UTF_8));
    }

    return dictionary;
  }

  /**
   * Reads an unsigned little-endian integer from the buffer.
   * 
   * @param buf the byte buffer
   * @param bytes number of bytes to read
   * @return the unsigned integer value
   */
  public static int readUnsignedLE(ByteBuffer buf, int bytes) {
    int result = 0;
    for (int i = 0; i < bytes; i++) {
      result |= (buf.get() & 0xFF) << (8 * i);
    }
    return result;
  }

  /**
   * Generic array decoding logic that handles the common parsing logic.
   * 
   * @param arrayHeader the array header
   * @param buf the byte buffer
   * @param dictionary the string dictionary
   * @param sb the StringBuilder to append to
   * @param decoder callback to decode individual array elements
   * @throws IOException if the array is invalid
   */
  public static void decodeArray(int arrayHeader, ByteBuffer buf, List<String> dictionary,
        StringBuilder sb, ArrayElementDecoder decoder) throws IOException {
    int isLarge = (arrayHeader >> 2) & 0x01;
    int fieldOffsetSizeMinusOne = arrayHeader & 0x03;
    int fieldOffsetSize = fieldOffsetSizeMinusOne + 1;

    // Read number of elements
    int numElements;
    if (isLarge == 1) {
      if (buf.remaining() < 4)
        return;
      numElements = buf.getInt();
    } else {
      if (buf.remaining() < 1)
        return;
      numElements = buf.get() & 0xFF;
    }

    if (numElements < 0)
      return;

    // Read element offsets
    int[] fieldOffsets = new int[numElements + 1];
    for (int i = 0; i <= numElements; i++) {
      if (buf.remaining() < fieldOffsetSize)
        return;
      fieldOffsets[i] = readUnsignedLE(buf, fieldOffsetSize);
    }

    // Remember current position for relative offsets
    int valuesStart = buf.position();

    sb.append("[");
    for (int i = 0; i < numElements; i++) {
      if (i > 0) {
        sb.append(",");
}
      // Decode the array element
      buf.position(valuesStart + fieldOffsets[i]);
      decoder.apply(buf, dictionary, sb);
    }
    sb.append("]");

    // Restore position to end of array
    buf.position(valuesStart + fieldOffsets[numElements]);
  }

  /**
   * Callback interface for decoding array elements in JSON format.
   */
  @FunctionalInterface
  public interface ArrayElementDecoder {
    void apply(ByteBuffer buf, List<String> dictionary, StringBuilder sb) throws IOException;
  }

  /**
   * Generic object decoding logic that handles the common parsing logic.
   * 
   * @param objectHeader the object header
   * @param buf the byte buffer
   * @param dictionary the string dictionary
   * @param sb the StringBuilder to append to
   * @param decoder callback to decode individual field values
   * @throws IOException if the object is invalid
   */
  public static void decodeObject(int objectHeader, ByteBuffer buf, List<String> dictionary,
        StringBuilder sb, ObjectFieldDecoder decoder) throws IOException {
    int isLarge = (objectHeader >> 4) & 0x01;
    int fieldIdSizeMinusOne = (objectHeader >> 2) & 0x03;
    int fieldOffsetSizeMinusOne = objectHeader & 0x03;

    int fieldIdSize = fieldIdSizeMinusOne + 1;
    int fieldOffsetSize = fieldOffsetSizeMinusOne + 1;

    // Read number of elements
    int numElements;
    if (isLarge == 1) {
      if (buf.remaining() < 4)
        return;
      numElements = buf.getInt();
    } else {
      if (buf.remaining() < 1)
        return;
      numElements = buf.get() & 0xFF;
    }

    if (numElements < 0)
      return;

    // Read field IDs
    int[] fieldIds = new int[numElements];
    for (int i = 0; i < numElements; i++) {
      if (buf.remaining() < fieldIdSize)
        return;
      fieldIds[i] = readUnsignedLE(buf, fieldIdSize);
    }

    // Read field offsets
    int[] fieldOffsets = new int[numElements + 1];
    for (int i = 0; i <= numElements; i++) {
      if (buf.remaining() < fieldOffsetSize)
        return;
      fieldOffsets[i] = readUnsignedLE(buf, fieldOffsetSize);
    }

    // Remember current position for relative offsets
    int valuesStart = buf.position();

    sb.append("{");
    for (int i = 0; i < numElements; i++) {
      if (i > 0) {
        sb.append(",");
      }

      // Get field name from dictionary
      if (fieldIds[i] < 0 || fieldIds[i] >= dictionary.size()) {
        throw new IOException("Invalid field ID: " + fieldIds[i] + ", dictionary size: " + dictionary.size());
      }
      String fieldName = dictionary.get(fieldIds[i]);
      sb.append("\"").append(escapeJson(fieldName)).append("\":");

      // Decode the field value
      buf.position(valuesStart + fieldOffsets[i]);
      decoder.apply(buf, dictionary, sb);
    }
    sb.append("}");

    // Restore position to end of object
    buf.position(valuesStart + fieldOffsets[numElements]);
  }

  /**
   * Callback interface for decoding object field values in JSON format.
   */
  @FunctionalInterface
  public interface ObjectFieldDecoder {
    void apply(ByteBuffer buf, List<String> dictionary, StringBuilder sb) throws IOException;
  }

  public static String decodeString(ByteBuffer buf) {
    int length = buf.getInt();
    byte[] bytes = new byte[length];
    buf.get(bytes);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  public static String decodeShortString(int length, ByteBuffer buf) {
    byte[] bytes = new byte[length];
    buf.get(bytes);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  public static String decodeDate(ByteBuffer buf) {
    int days = buf.getInt();
    LocalDate date = LocalDate.ofEpochDay(days);
    return date.toString();
  }

  /**
   * Generic timestamp decoding logic that handles the common parsing logic.
   * 
   * @param timestampType the timestamp type (12 = with timezone, 13 = without timezone)
   * @param buf the byte buffer
   * @return the decoded timestamp as a string
   */
  public static String decodeTimestamp(int timestampType, ByteBuffer buf) {
    long micros = buf.getLong();
    Instant instant = Instant.ofEpochSecond(micros / 1_000_000, (micros % 1_000_000) * 1000);

    switch (timestampType) {
      case 12: // timestamp with timezone (MICROS)
        return instant.toString(); // Instant includes 'Z' for UTC
      case 13: // timestamp without timezone (MICROS)
        // For timestamp without timezone, we format without timezone offset
        return instant.toString().replace("Z", ""); // Remove 'Z' for no timezone
      default:
        return instant.toString(); // Fallback for unknown types
    }
  }

  /**
   * Common logic for decoding decimal unscaled values.
   * 
   * @param primitiveHeader the primitive header
   * @param buf the byte buffer
   * @return the unscaled value as BigInteger
   * @throws IOException if the decimal is invalid
   */
  public static BigInteger decodeDecimalUnscaled(int primitiveHeader, ByteBuffer buf) throws IOException {
    switch (primitiveHeader) {
      case 8: // decimal4 - 4 bytes little-endian
        int intValue = buf.getInt();
        return BigInteger.valueOf(intValue);
      case 9: // decimal8 - 8 bytes little-endian
        long longValue = buf.getLong();
        return BigInteger.valueOf(longValue);
      case 10: // decimal16 - 16 bytes little-endian
        byte[] bytes = new byte[16];
        buf.get(bytes);

        // Convert from little-endian to big-endian for BigInteger
        // Reverse the byte order
        for (int i = 0; i < 8; i++) {
          byte temp = bytes[i];
          bytes[i] = bytes[15 - i];
          bytes[15 - i] = temp;
        }
        return new BigInteger(bytes);
      default:
        throw new IOException("Invalid decimal primitive type: " + primitiveHeader);
    }
  }

  public static String decodeBinary(ByteBuffer buf) {
    int length = buf.getInt();
    byte[] bytes = new byte[length];
    buf.get(bytes);
    return Base64.getEncoder().encodeToString(bytes);
  }

  /**
   * Escapes a string for JSON output.
   * This implementation preserves Unicode characters and only escapes the essential JSON control characters.
   * 
   * @param str the string to escape
   * @return the escaped string
   */
  public static String escapeJson(String str) {
    return str.replace("\\", "\\\\")
        .replace("\"", "\\\"")
        .replace("\b", "\\b")
        .replace("\f", "\\f")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t");
  }
}