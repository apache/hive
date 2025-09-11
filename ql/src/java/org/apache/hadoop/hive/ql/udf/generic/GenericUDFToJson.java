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
package org.apache.hadoop.hive.ql.udf.generic;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.VariantVal;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class GenericUDFToJson extends GenericUDF {
  private StructObjectInspector soi;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 1) {
      throw new UDFArgumentException("to_json takes exactly 1 argument");
    }
    if (!(arguments[0] instanceof StructObjectInspector)) {
      throw new UDFArgumentTypeException(0, "Argument must be VARIANT (struct<metadata:binary,value:binary>)");
    }
    soi = (StructObjectInspector) arguments[0];
    return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    Object variantObj = arguments[0].get();
    if (variantObj == null) {
      return null;
    }
    VariantVal field = VariantVal.from(soi.getStructFieldsDataAsList(variantObj));

    if (field.getMetadata() == null || field.getValue() == null) {
      return null;
    }
    try {
      return VariantJsonDecoder.toJson(field.getMetadata(), field.getValue());
    } catch (Exception e) {
      throw new HiveException("Error decoding variant to JSON", e);
    }
  }

  @Override
  public String getDisplayString(String[] children) {
    return "to_json(" + children[0] + ")";
  }

  static class VariantJsonDecoder {

    public static String toJson(ByteBuffer metadataBuf, ByteBuffer valueBuf) throws IOException {
      // Parse metadata first
      List<String> dictionary = parseMetadata(metadataBuf);

      StringBuilder sb = new StringBuilder();
      decodeValue(valueBuf, dictionary, sb);
      return sb.toString();
    }

    private static List<String> parseMetadata(ByteBuffer buf) throws IOException {
      byte header = buf.get();
      int version = header & 0x0F;
      boolean sortedStrings = ((header >> 4) & 0x01) == 1;
      int offsetSizeMinusOne = (header >> 6) & 0x03;
      int offsetSize = offsetSizeMinusOne + 1;

      if (version != 1) {
        throw new IOException("Unsupported variant metadata version: " + version);
      }

      // Read dictionary size
      int dictionarySize = readUnsignedLE(buf, offsetSize);

      // Read offsets
      int[] offsets = new int[dictionarySize + 1];
      for (int i = 0; i <= dictionarySize; i++) {
        offsets[i] = readUnsignedLE(buf, offsetSize);
      }

      // Read dictionary strings
      List<String> dictionary = new ArrayList<>(dictionarySize);
      int bytesStart = buf.position();
      for (int i = 0; i < dictionarySize; i++) {
        int start = offsets[i];
        int end = offsets[i + 1];
        int length = end - start;

        byte[] stringBytes = new byte[length];
        buf.position(bytesStart + start);
        buf.get(stringBytes);
        dictionary.add(new String(stringBytes, StandardCharsets.UTF_8));
      }

      if (sortedStrings) {
        // Verify the dictionary is actually sorted (optional but good for debugging)
        for (int i = 1; i < dictionary.size(); i++) {
          if (dictionary.get(i).compareTo(dictionary.get(i - 1)) < 0) {
            throw new IOException("Metadata claims sorted strings but dictionary is not sorted");
          }
        }
      }

      return dictionary;
    }

    private static void decodeValue(ByteBuffer valueBuf, List<String> dictionary, StringBuilder sb) throws IOException {
      if (!valueBuf.hasRemaining()) {
        throw new IOException("Unexpected end of value buffer");
      }

      byte valueMetadata = valueBuf.get();
      int basicType = valueMetadata & 0x03;
      int valueHeader = (valueMetadata >> 2) & 0x3F;

      switch (basicType) {
        case 0: // Primitive type
          decodePrimitive(valueHeader, valueBuf, sb);
          break;
        case 1: // Short string
          decodeShortString(valueHeader, valueBuf, sb);
          break;
        case 2: // Object
          decodeObject(valueHeader, valueBuf, dictionary, sb);
          break;
        case 3: // Array
          decodeArray(valueHeader, valueBuf, dictionary, sb);
          break;
        default:
          throw new IOException("Unknown basic type: " + basicType);
      }
    }

    private static void decodePrimitive(int primitiveHeader, ByteBuffer buf, StringBuilder sb) throws IOException {
      switch (primitiveHeader) {
        case 0: // null
          sb.append("null");
          break;
        case 1: // boolean true
          sb.append("true");
          break;
        case 2: // boolean false
          sb.append("false");
          break;
        case 3: // int8
          sb.append(buf.get());
          break;
        case 4: // int16
          sb.append(buf.getShort());
          break;
        case 5: // int32
          sb.append(buf.getInt());
          break;
        case 6: // int64
          sb.append(buf.getLong());
          break;
        case 7: // double
          sb.append(buf.getDouble());
          break;
        case 8: // decimal4
        case 9: // decimal8
        case 10: // decimal16
          decodeDecimal(primitiveHeader, buf, sb);
          break;
        case 11: // date
          decodeDate(buf, sb);
          break;
        case 12: // timestamp with timezone (MICROS)
        case 13: // timestamp without timezone (MICROS)
          decodeTimestamp(primitiveHeader, buf, sb);
          break;
        case 14: // float
          sb.append(buf.getFloat());
          break;
        case 15: // binary
          decodeBinary(buf, sb);
          break;
        case 16: // string
          decodeString(buf, sb);
          break;
        default:
          throw new IOException("Unknown primitive type: " + primitiveHeader);
      }
    }

    private static void decodeShortString(int length, ByteBuffer buf, StringBuilder sb) {
      byte[] bytes = new byte[length];
      buf.get(bytes);
      String str = new String(bytes, StandardCharsets.UTF_8);
      sb.append("\"").append(escapeJson(str)).append("\"");
    }

    private static void decodeObject(int objectHeader, ByteBuffer buf, List<String> dictionary, StringBuilder sb)
        throws IOException {
      int isLarge = (objectHeader >> 4) & 0x01;
      int fieldIdSizeMinusOne = (objectHeader >> 2) & 0x03;
      int fieldOffsetSizeMinusOne = objectHeader & 0x03;

      int fieldIdSize = fieldIdSizeMinusOne + 1;
      int fieldOffsetSize = fieldOffsetSizeMinusOne + 1;

      // Read number of elements
      int numElements = isLarge == 1 ? buf.getInt() : (buf.get() & 0xFF);

      // Read field IDs
      int[] fieldIds = new int[numElements];
      for (int i = 0; i < numElements; i++) {
        fieldIds[i] = readUnsignedLE(buf, fieldIdSize);
      }

      // Read field offsets
      int[] fieldOffsets = new int[numElements + 1];
      for (int i = 0; i <= numElements; i++) {
        fieldOffsets[i] = readUnsignedLE(buf, fieldOffsetSize);
      }

      // Remember current position for relative offsets
      int valuesStart = buf.position();

      sb.append("{");
      for (int i = 0; i < numElements; i++) {
        if (i > 0)
          sb.append(",");

        // Get field name from dictionary
        if (fieldIds[i] < 0 || fieldIds[i] >= dictionary.size()) {
          throw new IOException("Invalid field ID: " + fieldIds[i] + ", dictionary size: " + dictionary.size());
        }
        String fieldName = dictionary.get(fieldIds[i]);
        sb.append("\"").append(escapeJson(fieldName)).append("\":");

        // Decode the field value
        buf.position(valuesStart + fieldOffsets[i]);
        decodeValue(buf, dictionary, sb);
      }
      sb.append("}");

      // Restore position to end of object
      buf.position(valuesStart + fieldOffsets[numElements]);
    }

    private static void decodeArray(int arrayHeader, ByteBuffer buf, List<String> dictionary, StringBuilder sb)
        throws IOException {
      int isLarge = (arrayHeader >> 2) & 0x01;
      int fieldOffsetSizeMinusOne = arrayHeader & 0x03;
      int fieldOffsetSize = fieldOffsetSizeMinusOne + 1;

      // Read number of elements
      int numElements = isLarge == 1 ? buf.getInt() : (buf.get() & 0xFF);

      // Read field offsets
      int[] fieldOffsets = new int[numElements + 1];
      for (int i = 0; i <= numElements; i++) {
        fieldOffsets[i] = readUnsignedLE(buf, fieldOffsetSize);
      }

      // Remember current position for relative offsets
      int valuesStart = buf.position();

      sb.append("[");
      for (int i = 0; i < numElements; i++) {
        if (i > 0)
          sb.append(",");

        // Decode the array element
        buf.position(valuesStart + fieldOffsets[i]);
        decodeValue(buf, dictionary, sb);
      }
      sb.append("]");

      // Restore position to end of array
      buf.position(valuesStart + fieldOffsets[numElements]);
    }

    // Helper methods for specific types
    private static void decodeDecimal(int primitiveHeader, ByteBuffer buf, StringBuilder sb) throws IOException {
      int scale = buf.get() & 0xFF; // 1-byte scale (unsigned)
      BigInteger unscaled;

      switch (primitiveHeader) {
        case 8: // decimal4 - 4 bytes little-endian
          int intValue = buf.getInt();
          unscaled = BigInteger.valueOf(intValue);
          break;
        case 9: // decimal8 - 8 bytes little-endian
          long longValue = buf.getLong();
          unscaled = BigInteger.valueOf(longValue);
          break;
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
          unscaled = new BigInteger(bytes);
          break;
        default:
          throw new IOException("Invalid decimal primitive type: " + primitiveHeader);
      }

      BigDecimal decimal = new BigDecimal(unscaled, scale);
      sb.append(decimal);
    }


    private static void decodeDate(ByteBuffer buf, StringBuilder sb) {
      int days = buf.getInt();
      LocalDate date = LocalDate.ofEpochDay(days);
      sb.append("\"").append(date).append("\"");
    }

    private static void decodeTimestamp(int timestampType, ByteBuffer buf, StringBuilder sb) throws IOException {
      long micros = buf.getLong();
      switch (timestampType) {
        case 12: // timestamp with timezone (MICROS)
          Instant instantWithTz = Instant.ofEpochSecond(micros / 1_000_000, (micros % 1_000_000) * 1000);
          sb.append("\"").append(instantWithTz.toString()).append("\""); // Instant includes 'Z' for UTC
          break;
        case 13: // timestamp without timezone (MICROS)
          // For timestamp without timezone, we format without timezone offset
          Instant instantNoTz = Instant.ofEpochSecond(micros / 1_000_000, (micros % 1_000_000) * 1000);
          String formatted = instantNoTz.toString().replace("Z", ""); // Remove 'Z' for no timezone
          sb.append("\"").append(formatted).append("\"");
          break;
        default:
          throw new IOException("Invalid timestamp type: " + timestampType);
      }
    }

    private static void decodeBinary(ByteBuffer buf, StringBuilder sb) {
      int length = buf.getInt();
      byte[] bytes = new byte[length];
      buf.get(bytes);
      sb.append("\"").append(Base64.getEncoder().encodeToString(bytes)).append("\"");
    }

    private static void decodeString(ByteBuffer buf, StringBuilder sb) {
      int length = buf.getInt();
      byte[] bytes = new byte[length];
      buf.get(bytes);
      String str = new String(bytes, StandardCharsets.UTF_8);
      sb.append("\"").append(escapeJson(str)).append("\"");
    }

    // Utility methods
    private static int readUnsignedLE(ByteBuffer buf, int bytes) {
      int result = 0;
      for (int i = 0; i < bytes; i++) {
        result |= (buf.get() & 0xFF) << (8 * i);
      }
      return result;
    }

    private static String escapeJson(String str) {
      return str.replace("\\", "\\\\").replace("\"", "\\\"").replace("\b", "\\b").replace("\f", "\\f")
          .replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t");
    }
  }
}