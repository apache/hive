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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;

@Description(name = "parse_json", value = "_FUNC_(json_string) - Parses a JSON string into a VARIANT type", extended = """
    Example:
      > SELECT _FUNC_('{"a":5}');
      {"a":5}""")
public class GenericUDFParseJson extends GenericUDF {
  private PrimitiveObjectInspector inputOI;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 1) {
      throw new UDFArgumentLengthException("parse_json requires one argument");
    }
    if (arguments[0].getCategory() != ObjectInspector.Category.PRIMITIVE
        || ((PrimitiveObjectInspector) arguments[0]).getPrimitiveCategory()
        != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
      throw new UDFArgumentTypeException(0, "Only string input is accepted");
    }
    inputOI = (PrimitiveObjectInspector) arguments[0];

    // Return a Variant OI
    return ObjectInspectorFactory.getVariantObjectInspector();
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (arguments[0] == null || arguments[0].get() == null) {
      return null;
    }
    String json = inputOI.getPrimitiveJavaObject(arguments[0].get()).toString();

    try {
      return VariantJsonEncoder.fromJson(json);
    } catch (IOException e) {
      throw new HiveException("Failed to parse JSON: " + json, e);
    }
  }

  @Override
  public String getDisplayString(String[] children) {
    return "parse_json(" + children[0] + ")";
  }

  static final class VariantJsonEncoder {
    private static final ObjectMapper mapper = new ObjectMapper();

    private static final int MAX_FIELD_ID_SIZE = 4;
    private static final int MAX_FIELD_OFFSET_SIZE = 4;

    public static List<Object> fromJson(String json) throws IOException {
      JsonNode root = mapper.readTree(json);

      // 1) build sorted dictionary of keys
      List<String> dictionary = collectDictionary(root);

      // 2) build dict index map
      Map<String, Integer> dictIndex = HashMap.newHashMap(dictionary.size());
      for (int i = 0; i < dictionary.size(); i++) {
        dictIndex.put(dictionary.get(i), i);
      }

      // 3) encode metadata and value
      byte[] metadataBytes = encodeMetadata(dictionary);
      byte[] valueBytes = encodeValue(root, dictIndex);

      return Arrays.asList(metadataBytes, valueBytes);
    }

    private static List<String> collectDictionary(JsonNode node) {
      Set<String> keys = new TreeSet<>(Comparator.naturalOrder());
      collectKeys(node, keys);
      return new ArrayList<>(keys);
    }

    private static void collectKeys(JsonNode node, Set<String> keys) {
      if (node == null)
        return;
      if (node.isObject()) {
        node.fieldNames().forEachRemaining(keys::add);
        node.elements().forEachRemaining(child -> collectKeys(child, keys));
      } else if (node.isArray()) {
        node.elements().forEachRemaining(child -> collectKeys(child, keys));
      }
    }

    // metadata: header + dictionary_size (offset_size bytes) + offsets (dictionary_size+1) + bytes
    private static byte[] encodeMetadata(List<String> dictionary) throws IOException {
      // Build the bytes and offsets for dictionary strings
      ByteArrayOutputStream dictBytesOut = new ByteArrayOutputStream();
      int[] offsets = new int[dictionary.size() + 1];
      offsets[0] = 0;
      int cur = 0;
      for (int i = 0; i < dictionary.size(); i++) {
        byte[] bs = dictionary.get(i).getBytes(StandardCharsets.UTF_8);
        dictBytesOut.write(bs);
        cur += bs.length;
        offsets[i + 1] = cur;
      }
      byte[] dictBytes = dictBytesOut.toByteArray();

      // choose minimal offset_size that can hold largest offset and dictionary_size
      int maxOffset = Math.max(dictionary.size(), dictBytes.length);
      int offsetSize = minimalBytesForUnsigned(maxOffset);
      if (offsetSize < 1)
        offsetSize = 1;
      if (offsetSize > 4)
        offsetSize = 4;

      // header: version (4 bits)=1, sorted_strings(1)=1, offset_size_minus_one (2 bits)
      int offsetSizeMinusOne = offsetSize - 1;
      byte header = (byte) ((1 & 0xF) | (1 << 4) | ((offsetSizeMinusOne & 0x3) << 6));

      ByteArrayOutputStream out = new ByteArrayOutputStream();
      // header
      out.write(header);

      // dictionary_size as little-endian offsetSize bytes
      writeUnsignedLE(out, dictionary.size(), offsetSize);

      // offsets: dictionary.size()+1 little-endian entries
      for (int off : offsets) {
        writeUnsignedLE(out, off, offsetSize);
      }

      // dictionary bytes
      out.write(dictBytes);

      return out.toByteArray();
    }

    private static int minimalBytesForUnsigned(int value) {
      if (value <= 0xFF)
        return 1;
      if (value <= 0xFFFF)
        return 2;
      if (value <= 0xFFFFFF)
        return 3;
      return 4;
    }

    private static byte[] encodeValue(JsonNode node, java.util.Map<String, Integer> dictIndex)
        throws IOException {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      encodeNode(node, dictIndex, out);
      return out.toByteArray();
    }

    // Main encoding: writes value_metadata + optional value_data
    private static void encodeNode(JsonNode node, java.util.Map<String, Integer> dictIndex,
        OutputStream out) throws IOException {
      // null
      if (node == null || node.isNull()) {
        // primitive null ID = 0 ; basic_type = 0
        out.write(0);
        return;
      }

      // boolean
      if (node.isBoolean()) {
        int id = node.booleanValue() ? 1 : 2;
        out.write((id << 2));
        return;
      }

      // decimal or floating numbers should be checked BEFORE integer types
      if (node.isBigDecimal() || node.isFloatingPointNumber() || (node.isNumber() && !node.isIntegralNumber())) {
        // Try BigDecimal path
        java.math.BigDecimal bd = node.decimalValue();

        if (bd.scale() > 0) {
          // use decimal4/8/16 depending on precision
          int precision = bd.precision();
          int scale = bd.scale();
          java.math.BigInteger unscaled = bd.unscaledValue();

          byte[] unscaledLE = bigIntegerToLittleEndian(unscaled, requiredBytesForPrecision(precision));
          if (precision <= 9) {
            // decimal4 -> primitive id 8 (decimal4)
            out.write((8 << 2));
            out.write(scale & 0xFF); // 1 byte scale
            out.write(unscaledLE);
            return;
          } else if (precision <= 18) {
            out.write((9 << 2)); // decimal8
            out.write(scale & 0xFF);
            out.write(unscaledLE);
            return;
          } else if (precision <= 38) {
            out.write((10 << 2)); // decimal16
            out.write(scale & 0xFF);
            out.write(unscaledLE);
            return;
          } else {
            throw new IOException("Decimal precision > 38 not supported: " + precision);
          }
        } else {
          // no fractional part and fits in double? use integer mapping if fits else double
          if (node.isFloat()) {
            out.write((14 << 2)); // float
            writeFloatLE(out, (float) node.doubleValue());
            return;
          } else {
            // if it's exact integer would have hit integral path earlier. fallback to double
            out.write((7 << 2)); // double
            writeDoubleLE(out, node.doubleValue());
            return;
          }
        }
      }

      // integer types: choose smallest width that fits
      if (node.isInt() || node.isLong() || node.canConvertToLong()) {
        long v = node.longValue();
        if (v >= Byte.MIN_VALUE && v <= Byte.MAX_VALUE) {
          out.write((3 << 2)); // int8
          out.write((int) (v & 0xFF));
          return;
        } else if (v >= Short.MIN_VALUE && v <= Short.MAX_VALUE) {
          out.write((4 << 2)); // int16
          writeShortLE(out, (int) v);
          return;
        } else if (v >= Integer.MIN_VALUE && v <= Integer.MAX_VALUE) {
          out.write((5 << 2)); // int32
          writeIntLE(out, (int) v);
          return;
        } else {
          out.write((6 << 2)); // int64
          writeLongLE(out, v);
          return;
        }
      }

      // textual: attempt special types (date/time/timestamp/uuid) else string
      if (node.isTextual()) {
        String s = node.textValue();

        // try uuid
        if (looksLikeUUID(s)) {
          // Fallback to string until Iceberg supports UUID (20)
          byte[] utf8 = s.getBytes(StandardCharsets.UTF_8);
          out.write((16 << 2)); // STRING primitive id 16
          writeIntLE(out, utf8.length);
          out.write(utf8);
          return;
        }

        // try date
        java.time.LocalDate ldate = tryParseDate(s);
        if (ldate != null) {
          out.write((11 << 2)); // date id 11
          int days = (int) ldate.toEpochDay();
          writeIntLE(out, days);
          return;
        }

        // try time (no timezone)
        java.time.LocalTime ltime = tryParseTime(s);
        if (ltime != null) {
          // Fallback to string - Iceberg doesn't support TIME_NTZ (case 17)
          byte[] utf8 = s.getBytes(StandardCharsets.UTF_8);
          if (utf8.length < 64) {
            int basicType = 1;
            int header = utf8.length & 0x3F;
            out.write((header << 2) | basicType);
            out.write(utf8);
          } else {
            out.write((16 << 2)); // STRING primitive id 16
            writeIntLE(out, utf8.length);
            out.write(utf8);
          }
          return;
        }

        // try timestamp (ISO with timezone or Z)
        java.time.Instant inst = tryParseInstant(s);
        if (inst != null) {
          // Check if the original string had timezone information
          boolean hasTimezone = s.contains("Z") || s.contains("+") || s.contains("-");

          if (hasTimezone) {
            out.write((12 << 2)); // timestamp with timezone (MICROS)
          } else {
            out.write((13 << 2)); // timestamp without timezone (MICROS)
          }
          long micros = inst.getEpochSecond() * 1_000_000L + (inst.getNano() / 1000);
          writeLongLE(out, micros);
          return;
        }

        // otherwise string
        byte[] utf8 = s.getBytes(StandardCharsets.UTF_8);
        if (utf8.length < 64) {
          // short string basic_type = 1, header = length
          int basicType = 1;
          int header = utf8.length & 0x3F;
          out.write((header << 2) | basicType);
          out.write(utf8);
        } else {
          out.write((16 << 2)); // STRING primitive id 16
          writeIntLE(out, utf8.length);
          out.write(utf8);
        }
        return;
      }

      // binary node:
      if (node.isBinary()) {
        byte[] bytes = node.binaryValue();
        out.write((15 << 2)); // binary id 15
        writeIntLE(out, bytes.length);
        out.write(bytes);
        return;
      }

      // array
      if (node.isArray()) {
        int n = node.size();
        // choose field_offset_size minimal to fit offsets
        // We'll encode children and collect their lengths
        List<byte[]> encodedChildren = new ArrayList<>(n);
        int[] offsets = new int[n + 1];
        offsets[0] = 0;
        for (int i = 0; i < n; i++) {
          ByteArrayOutputStream tmp = new ByteArrayOutputStream();
          encodeNode(node.get(i), dictIndex, tmp);
          byte[] eb = tmp.toByteArray();
          encodedChildren.add(eb);
          offsets[i + 1] = offsets[i] + eb.length;
        }

        int maxOffset = offsets[n];
        int fieldOffsetSize = minimalBytesForUnsigned(maxOffset);
        if (fieldOffsetSize < 1)
          fieldOffsetSize = 1;
        if (fieldOffsetSize > MAX_FIELD_OFFSET_SIZE)
          fieldOffsetSize = MAX_FIELD_OFFSET_SIZE;

        // is_large if n > 255
        boolean isLarge = n > 0xFF;
        int isLargeBit = isLarge ? 1 : 0;
        int fieldOffsetSizeMinusOne = fieldOffsetSize - 1;

        // array valueHeader: (is_large << 2) | field_offset_size_minus_one  (fits in 6 bits's high)
        int valueHeader = (isLargeBit << 2) | (fieldOffsetSizeMinusOne & 0x3);
        int basicType = 3; // array
        out.write((valueHeader << 2) | basicType);

        // write num_elements (1 or 4 bytes LE)
        if (!isLarge) {
          out.write(n & 0xFF);
        } else {
          writeUnsignedLE(out, n, 4);
        }

        // write field_offset list (n+1 entries) each fieldOffsetSize bytes LE
        for (int off : offsets) {
          writeUnsignedLE(out, off, fieldOffsetSize);
        }

        // write concatenated child bytes
        for (byte[] eb : encodedChildren)
          out.write(eb);

        return;
      }

      // object
      if (node.isObject()) {
        // Collect keys in lexicographic order required by spec — keys must be looked up in dictionary
        List<String> fnames = new ArrayList<>();
        node.fieldNames().forEachRemaining(fnames::add);
        java.util.Collections.sort(fnames); // lexicographic using String.compareTo

        int m = fnames.size();

        // Encode each field value
        List<byte[]> encodedVals = new ArrayList<>(m);
        int[] offsets = new int[m + 1];
        offsets[0] = 0;
        for (int i = 0; i < m; i++) {
          JsonNode child = node.get(fnames.get(i));
          ByteArrayOutputStream tmp = new ByteArrayOutputStream();
          encodeNode(child, dictIndex, tmp);
          byte[] vb = tmp.toByteArray();
          encodedVals.add(vb);
          offsets[i + 1] = offsets[i] + vb.length;
        }

        // choose sizes for field_id and field_offset
        int maxFieldId = 0;
        for (String f : fnames) {
          Integer id = dictIndex.get(f);
          if (id == null)
            throw new IOException("Field " + f + " not in dictionary");
          if (id > maxFieldId)
            maxFieldId = id;
        }
        int fieldIdSize = minimalBytesForUnsigned(maxFieldId);
        if (fieldIdSize < 1)
          fieldIdSize = 1;
        if (fieldIdSize > MAX_FIELD_ID_SIZE)
          fieldIdSize = MAX_FIELD_ID_SIZE;

        int maxOffset = offsets[m];
        int fieldOffsetSize = minimalBytesForUnsigned(maxOffset);
        if (fieldOffsetSize < 1)
          fieldOffsetSize = 1;
        if (fieldOffsetSize > MAX_FIELD_OFFSET_SIZE)
          fieldOffsetSize = MAX_FIELD_OFFSET_SIZE;

        boolean isLarge = m > 0xFF;
        int isLargeBit = isLarge ? 1 : 0;
        int fieldIdSizeMinusOne = fieldIdSize - 1;
        int fieldOffsetSizeMinusOne = fieldOffsetSize - 1;

        int valueHeader = (isLargeBit << 4) | ((fieldIdSizeMinusOne & 0x3) << 2) | (fieldOffsetSizeMinusOne & 0x3);
        int basicType = 2; // object
        out.write((valueHeader << 2) | basicType);

        // write num_elements
        if (!isLarge) {
          out.write(m & 0xFF);
        } else {
          writeUnsignedLE(out, m, 4);
        }

        // field_id list: m entries, each fieldIdSize bytes LE (dictionary index)
        for (String f : fnames) {
          int id = dictIndex.get(f);
          writeUnsignedLE(out, id, fieldIdSize);
        }

        // field_offset list: m+1 entries
        for (int off : offsets) {
          writeUnsignedLE(out, off, fieldOffsetSize);
        }

        // concatenated values
        for (byte[] vb : encodedVals)
          out.write(vb);

        return;
      }

      // fallback: convert to string
      String s = node.asText();
      byte[] utf8 = s.getBytes(StandardCharsets.UTF_8);
      if (utf8.length < 64) {
        int basicType = 1;
        int header = utf8.length & 0x3F;
        out.write((header << 2) | basicType);
        out.write(utf8);
      } else {
        out.write((16 << 2));
        writeIntLE(out, utf8.length);
        out.write(utf8);
      }
    }

    // Utilities

    private static void writeUnsignedLE(OutputStream out, int value, int bytes) throws IOException {
      for (int i = 0; i < bytes; i++) {
        out.write((value >> (8 * i)) & 0xFF);
      }
    }

    private static void writeShortLE(OutputStream out, int v) throws IOException {
      out.write(v & 0xFF);
      out.write((v >> 8) & 0xFF);
    }

    private static void writeIntLE(OutputStream out, int v) throws IOException {
      out.write(v & 0xFF);
      out.write((v >> 8) & 0xFF);
      out.write((v >> 16) & 0xFF);
      out.write((v >> 24) & 0xFF);
    }

    private static void writeLongLE(OutputStream out, long v) throws IOException {
      for (int i = 0; i < 8; i++) {
        out.write((int) ((v >> (8 * i)) & 0xFF));
      }
    }

    private static void writeFloatLE(OutputStream out, float v) throws IOException {
      int bits = Float.floatToRawIntBits(v);
      writeIntLE(out, bits);
    }

    private static void writeDoubleLE(OutputStream out, double v) throws IOException {
      long bits = Double.doubleToRawLongBits(v);
      writeLongLE(out, bits);
    }

    // decimal helpers: number of bytes required for given precision
    private static int requiredBytesForPrecision(int precision) {
      // decimal4 uses 4 bytes (precision <= 9), decimal8 uses 8 bytes (<=18), decimal16 uses 16 bytes (<=38)
      if (precision <= 9)
        return 4;
      if (precision <= 18)
        return 8;
      return 16;
    }

    private static byte[] bigIntegerToLittleEndian(java.math.BigInteger bi, int length) {
      byte[] be = bi.toByteArray();
      byte[] le = new byte[length];

      // Handle sign extension
      byte fillByte = (bi.signum() < 0) ? (byte) 0xFF : (byte) 0x00;
      Arrays.fill(le, fillByte);

      // Copy bytes in proper little-endian order
      int bytesToCopy = Math.min(be.length, length);
      for (int i = 0; i < bytesToCopy; i++) {
        le[i] = be[be.length - 1 - i];
      }

      return le;
    }

    private static boolean looksLikeUUID(String s) {
      return s != null && s.length() == 36 && s.charAt(8) == '-' && s.charAt(13) == '-' && s.charAt(18) == '-'
          && s.charAt(23) == '-';
    }

    private static java.time.LocalDate tryParseDate(String s) {
      try {
        return java.time.LocalDate.parse(s);
      } catch (Exception e) {
        return null;
      }
    }

    private static java.time.LocalTime tryParseTime(String s) {
      try {
        return java.time.LocalTime.parse(s);
      } catch (Exception e) {
        return null;
      }
    }

    private static java.time.Instant tryParseInstant(String s) {
      try {
        // Accept ISO_INSTANT and offset datetime
        try {
          return java.time.Instant.parse(s);
        } catch (Exception e) {
          // Try OffsetDateTime
          java.time.OffsetDateTime odt = java.time.OffsetDateTime.parse(s);
          return odt.toInstant();
        }
      } catch (Exception e) {
        return null;
      }
    }
  }

}