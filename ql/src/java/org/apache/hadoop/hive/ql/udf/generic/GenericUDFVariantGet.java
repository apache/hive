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
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.VariantVal;
import org.apache.hadoop.hive.serde2.typeinfo.VariantUtil;

@Description(name = "variant_get", value = "_FUNC_(variant, path[, type]) - Extracts a sub-variant from variant according to path, and casts it to type", extended = """
    Example:
    > SELECT _FUNC_(parse_json('{"a": 1}'), '$.a', 'int');
    1
    > SELECT _FUNC_(parse_json('{"a": 1}'), '$.b', 'int');
    NULL
    > SELECT _FUNC_(parse_json('[1, "2"]'), '$[1]', 'string');
    2
    > SELECT _FUNC_(parse_json('[1, "hello"]'), '$[1]');
    "hello\"""")
public class GenericUDFVariantGet extends GenericUDF {
  private StructObjectInspector variantOI;
  private PrimitiveObjectInspector pathOI;
  private PrimitiveObjectInspector typeOI;
  private boolean hasTypeArgument;

  // Inner classes
  enum VariantType {NULL, PRIMITIVE, STRING, OBJECT, ARRAY}

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length < 2 || arguments.length > 3) {
      throw new UDFArgumentException("variant_get requires 2 or 3 arguments");
    }

    if (!(arguments[0] instanceof StructObjectInspector)) {
      throw new UDFArgumentException("First argument must be VARIANT");
    }
    variantOI = (StructObjectInspector) arguments[0];

    if (!(arguments[1] instanceof PrimitiveObjectInspector)) {
      throw new UDFArgumentException("Second argument must be string path");
    }
    pathOI = (PrimitiveObjectInspector) arguments[1];

    hasTypeArgument = arguments.length == 3;
    if (hasTypeArgument) {
      if (!(arguments[2] instanceof PrimitiveObjectInspector)) {
        throw new UDFArgumentException("Third argument must be string type name");
      }
      typeOI = (PrimitiveObjectInspector) arguments[2];
    }

    return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    try {
      Object variantObj = arguments[0].get();
      if (variantObj == null) {
        return null;
      }
      VariantVal variantVal = VariantVal.from(variantOI.getStructFieldsDataAsList(variantObj));

      if (variantVal.getMetadata() == null || variantVal.getValue() == null) {
        return null;
      }

      Object pathObj = arguments[1].get();
      if (pathObj == null)
        return null;
      String path = pathOI.getPrimitiveJavaObject(pathObj).toString();

      String targetType = "variant";
      if (hasTypeArgument) {
        Object typeObj = arguments[2].get();
        if (typeObj != null) {
          targetType = typeOI.getPrimitiveJavaObject(typeObj).toString();
        }
      }

      VariantPath parsedPath = VariantPathParser.parse(path);
      if (parsedPath == null)
        return null;

      // Parse metadata to get dictionary
      List<String> dictionary = VariantUtil.parseMetadata(variantVal.getMetadata());

      // Extract value using path
      Object result = VariantDecoder.extractValue(variantVal.getValue(), 0, dictionary, parsedPath, 0);

      // Cast to target type
      return castValue(result, targetType);

    } catch (Exception e) {
      throw new HiveException("Failed to extract variant: " + e.getMessage(), e);
    }
  }

  static class VariantDecoder {

    private static Object extractValue(byte[] value, int pos, List<String> dictionary, VariantPath path, int segmentIndex)
          throws IOException {
      if (pos >= value.length)
        return null;

      if (segmentIndex >= path.getSegments().size()) {
        return decodeValue(value, pos, dictionary);
      }

      PathSegment segment = path.getSegments().get(segmentIndex);
      VariantType type = getValueType(value, pos);

      if (segment instanceof FieldSegment && type == VariantType.OBJECT) {
        String fieldName = ((FieldSegment) segment).fieldName();
        int fieldPos = findFieldPosition(value, pos, dictionary, fieldName);
        if (fieldPos == -1)
          return null;
        return extractValue(value, fieldPos, dictionary, path, segmentIndex + 1);
      } else if (segment instanceof IndexSegment && type == VariantType.ARRAY) {
        int index = ((IndexSegment) segment).index();
        int elementPos = getArrayElementPosition(value, pos, index);
        if (elementPos == -1)
          return null;
        return extractValue(value, elementPos, dictionary, path, segmentIndex + 1);
      }

      return null;
    }

    private static Object decodeValue(byte[] value, int pos, List<String> dictionary) throws IOException {
      if (pos >= value.length)
        return null;

      byte valueMetadata = value[pos];
      int basicType = valueMetadata & 0x03;
      int valueHeader = (valueMetadata >> 2) & 0x3F;

      ByteBuffer buf = ByteBuffer.wrap(value, pos + 1, value.length - pos - 1)
          .order(ByteOrder.LITTLE_ENDIAN);

      switch (basicType) {
        case 0: // Primitive type
          return decodePrimitive(valueHeader, buf);
        case 1: // Short string
          return VariantUtil.decodeShortString(valueHeader, buf);
        case 2: // Object
          return decodeObject(value, pos, dictionary);
        case 3: // Array
          return decodeArray(value, pos, dictionary);
        default:
          return null;
      }
    }

    private static Object decodePrimitive(int primitiveHeader, ByteBuffer buf) throws IOException {
      switch (primitiveHeader) {
        case 0: // null
          return null;
        case 1: // boolean true
          return true;
        case 2: // boolean false
          return false;
        case 3: // int8
          return (int) buf.get();
        case 4: // int16
          return (int) buf.getShort();
        case 5: // int32
          return buf.getInt();
        case 6: // int64
          return buf.getLong();
        case 7: // double
          return buf.getDouble();
        case 8: // decimal4
        case 9: // decimal8
        case 10: // decimal16
          return decodeDecimal(primitiveHeader, buf);
        case 11: // date
          return VariantUtil.decodeDate(buf);
        case 12: // timestamp (MICROS)
        case 13: // timestamp without time zone (MICROS)
          return VariantUtil.decodeTimestamp(primitiveHeader, buf);
        case 14: // float
          return buf.getFloat();
        case 15: // binary
          return VariantUtil.decodeBinary(buf);
        case 16: // string
          return VariantUtil.decodeString(buf);
        default:
          return null;
      }
    }

    private static Object decodeDecimal(int primitiveHeader, ByteBuffer buf) throws IOException {
      byte scale = buf.get();
      BigInteger unscaled = VariantUtil.decodeDecimalUnscaled(primitiveHeader, buf);
      return new BigDecimal(unscaled, scale).toString();
    }

    private static Object decodeObject(byte[] value, int pos, List<String> dictionary) throws IOException {
      if (pos >= value.length)
        return null;

      ByteBuffer buf = ByteBuffer.wrap(value, pos + 1, value.length - pos - 1)
          .order(ByteOrder.LITTLE_ENDIAN);
      int header = (value[pos] >> 2) & 0x3F;

      StringBuilder json = new StringBuilder();
      VariantUtil.decodeObject(header, buf, dictionary, json, (b, d, s) -> {
        // Convert ByteBuffer position back to byte array position
        int fieldPos = pos + 1 + b.position();
        Object fieldValue = decodeValue(value, fieldPos, dictionary);
        s.append(fieldValue);
      });

      return json.toString();
    }

    private static Object decodeArray(byte[] value, int pos, List<String> dictionary) throws IOException {
      if (pos >= value.length)
        return null;

      ByteBuffer buf = ByteBuffer.wrap(value, pos + 1, value.length - pos - 1)
          .order(ByteOrder.LITTLE_ENDIAN);
      int header = (value[pos] >> 2) & 0x3F;

      StringBuilder json = new StringBuilder();
      VariantUtil.decodeArray(header, buf, dictionary, json, (b, d, s) -> {
        // Decode array element
        int elementPos = pos + 1 + b.position();
        Object elementValue = decodeValue(value, elementPos, dictionary);
        s.append(elementValue);
      });

      return json.toString();
    }

    private static int findFieldPosition(byte[] value, int pos, List<String> dictionary, String fieldName) {
      try {
        if (pos >= value.length)
          return -1;

        ByteBuffer buf = ByteBuffer.wrap(value, pos + 1, value.length - pos - 1)
            .order(ByteOrder.LITTLE_ENDIAN);

        int header = (value[pos] >> 2) & 0x3F;
        int isLarge = (header >> 4) & 0x01;
        int fieldIdSizeMinusOne = (header >> 2) & 0x03;
        int fieldOffsetSizeMinusOne = header & 0x03;

        int fieldIdSize = fieldIdSizeMinusOne + 1;
        int fieldOffsetSize = fieldOffsetSizeMinusOne + 1;

        // Read number of elements
        int numElements;
        if (isLarge == 1) {
          if (buf.remaining() < 4)
            return -1;
          numElements = buf.getInt();
        } else {
          if (buf.remaining() < 1)
            return -1;
          numElements = buf.get() & 0xFF;
        }

        if (numElements < 0)
          return -1;

        // Read field IDs and find matching field
        int[] fieldIds = new int[numElements];
        for (int i = 0; i < numElements; i++) {
          if (buf.remaining() < fieldIdSize)
            return -1;
          fieldIds[i] = VariantUtil.readUnsignedLE(buf, fieldIdSize);
        }

        // Read field offsets
        int[] fieldOffsets = new int[numElements + 1];
        for (int i = 0; i <= numElements; i++) {
          if (buf.remaining() < fieldOffsetSize)
            return -1;
          fieldOffsets[i] = VariantUtil.readUnsignedLE(buf, fieldOffsetSize);
        }

        int valuesStart = buf.position();

        // Find the field with matching name
        for (int i = 0; i < numElements; i++) {
          if (fieldIds[i] >= 0 && fieldIds[i] < dictionary.size()) {
            String currentFieldName = dictionary.get(fieldIds[i]);
            if (fieldName.equals(currentFieldName)) {
              return valuesStart + fieldOffsets[i];
            }
          }
        }

        return -1;

      } catch (Exception e) {
        return -1;
      }
    }

    private static int getArrayElementPosition(byte[] value, int pos, int index) {
      try {
        if (pos >= value.length)
          return -1;

        ByteBuffer buf = ByteBuffer.wrap(value, pos + 1, value.length - pos - 1)
            .order(ByteOrder.LITTLE_ENDIAN);

        int header = (value[pos] >> 2) & 0x3F;
        int isLarge = (header >> 2) & 0x01;
        int fieldOffsetSizeMinusOne = header & 0x03;
        int fieldOffsetSize = fieldOffsetSizeMinusOne + 1;

        // Read number of elements
        int numElements;
        if (isLarge == 1) {
          if (buf.remaining() < 4)
            return -1;
          numElements = buf.getInt();
        } else {
          if (buf.remaining() < 1)
            return -1;
          numElements = buf.get() & 0xFF;
        }

        if (index < 0 || index >= numElements) {
          return -1; // Index out of bounds
        }

        // Read element offsets
        int[] elementOffsets = new int[numElements + 1];
        for (int i = 0; i <= numElements; i++) {
          if (buf.remaining() < fieldOffsetSize)
            return -1;
          elementOffsets[i] = VariantUtil.readUnsignedLE(buf, fieldOffsetSize);
        }

        int valuesStart = buf.position();
        return valuesStart + elementOffsets[index];

      } catch (Exception e) {
        return -1;
      }
    }
  }

  private static Object castValue(Object value, String targetType) {
    if (value == null)
      return null;

    try {
      switch (targetType.toLowerCase()) {
        case "int":
        case "integer":
          if (value instanceof Number) {
            return ((Number) value).intValue();
          } else if (value instanceof String) {
            return Integer.parseInt((String) value); // No quote stripping needed
          }
          break;
        case "string":
          return value.toString();
        case "boolean":
        case "bool":
          if (value instanceof Boolean) {
            return value;
          } else if (value instanceof String) {
            return Boolean.parseBoolean((String) value);
          }
          break;
        case "double":
          if (value instanceof Number) {
            return ((Number) value).doubleValue();
          } else if (value instanceof String) {
            return Double.parseDouble((String) value);
          }
          break;
        case "long":
          if (value instanceof Number) {
            return ((Number) value).longValue();
          } else if (value instanceof String) {
            return Long.parseLong((String) value);
          }
          break;
        default:
          return value;
      }
    } catch (NumberFormatException e) {
      return null;
    }
    return null;
  }

  // Helper methods
  private static VariantType getValueType(byte[] value, int pos) {
    if (pos >= value.length)
      return VariantType.NULL;
    byte firstByte = value[pos];
    int basicType = firstByte & 0x03;
    switch (basicType) {
      case 0:
        return VariantType.PRIMITIVE;
      case 1:
        return VariantType.STRING;
      case 2:
        return VariantType.OBJECT;
      case 3:
        return VariantType.ARRAY;
      default:
        return VariantType.NULL;
    }
  }

  @Override
  public String getDisplayString(String[] children) {
    return "variant_get(" + String.join(", ", children) + ")";
  }

  private static class VariantPath {
    private final List<PathSegment> segments;

    private VariantPath(List<PathSegment> segments) {
      this.segments = segments;
    }

    private List<PathSegment> getSegments() {
      return segments;
    }
  }

  private interface PathSegment {
  }

  private static class FieldSegment implements PathSegment {
    private final String fieldName;

    private FieldSegment(String fieldName) {
      this.fieldName = fieldName;
    }

    private String fieldName() {
      return fieldName;
    }
  }

  private static class IndexSegment implements PathSegment {
    private final int index;

    private IndexSegment(int index) {
      this.index = index;
    }

    private int index() {
      return index;
    }
  }

  private static class VariantPathParser {

    private static VariantPath parse(String path) {
      if (path == null || !path.startsWith("$"))
        return null;
      List<PathSegment> segments = new ArrayList<>();
      String remaining = path.substring(1);

      while (!remaining.isEmpty()) {
        if (remaining.startsWith(".")) {
          int end = findEnd(remaining.substring(1));
          if (end == -1)
            return null;
          String fieldName = remaining.substring(1, end + 1);
          segments.add(new FieldSegment(fieldName));
          remaining = remaining.substring(end + 1);
        } else if (remaining.startsWith("[")) {
          int close = remaining.indexOf(']');
          if (close == -1)
            return null;
          String content = remaining.substring(1, close).trim();

          if (content.matches("\\d+")) {
            segments.add(new IndexSegment(Integer.parseInt(content)));
          } else if ((content.startsWith("'") && content.endsWith("'")) || (content.startsWith("\"")
              && content.endsWith("\""))) {
            String fieldName = content.substring(1, content.length() - 1);
            segments.add(new FieldSegment(fieldName));
          } else {
            return null;
          }
          remaining = remaining.substring(close + 1);
        } else {
          return null;
        }
      }
      return new VariantPath(segments);
    }

    private static int findEnd(String str) {
      for (int i = 0; i < str.length(); i++) {
        char c = str.charAt(i);
        if (c == '.' || c == '[')
          return i;
      }
      return str.length();
    }
  }
}