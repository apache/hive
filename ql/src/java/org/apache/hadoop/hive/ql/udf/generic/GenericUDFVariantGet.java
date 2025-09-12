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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.variant.Variant;
import org.apache.hadoop.hive.serde2.variant.VariantUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;

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
  private static final Logger LOG = LoggerFactory.getLogger(GenericUDFVariantGet.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private StructObjectInspector variantOI;
  private PrimitiveObjectInspector pathOI;

  private PrimitiveObjectInspector typeOI;
  private boolean hasTypeArgument;

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
      Variant variant = Variant.from(variantOI.getStructFieldsDataAsList(variantObj));

      Object pathObj = arguments[1].get();
      if (pathObj == null) {
        return null;
      }
      String path = pathOI.getPrimitiveJavaObject(pathObj).toString();

      String targetType = null;
      if (hasTypeArgument) {
        Object typeObj = arguments[2].get();
        if (typeObj != null) {
          targetType = typeOI.getPrimitiveJavaObject(typeObj).toString();
        }
      }

      Variant result = extractValueByPath(variant, path);
      // cast to target type
      return castValue(result, targetType);

    } catch (Exception e) {
      throw new HiveException("Failed to extract variant: " + e.getMessage(), e);
    }
  }

  @Override
  public String getDisplayString(String[] children) {
    return "variant_get(" + String.join(", ", children) + ")";
  }

  /**
   * Extract a variant value by following a JSONPath-like path.
   * Supports complex nested patterns like: $.field, $[0], $.field[1], $.mixed[4].key, etc.
   */
  private static Variant extractValueByPath(Variant variant, String path) {
    if (variant == null || path == null) {
      return null;
    }
    try {
      List<VariantToken> tokens = VariantPathParser.parse(path);
      Variant current = variant;

      for (VariantToken token : tokens) {
        if (current == null) {
          // The path goes deeper than the object structure.
          return null;
        }
        current = token.get(current);
      }
      return current;

    } catch (IllegalArgumentException e) {
      LOG.warn("Invalid path syntax provided: {}", e.getMessage());
      return null;
    }
  }

  private static Object castValue(Variant value, String targetType) {
    if (value == null || value.getType() == VariantUtil.Type.NULL) {
      return null;
    }
    if (targetType == null) {
      return unescapeJson(value.toJson(ZoneOffset.UTC));
    }
    try {
      return switch (targetType.toLowerCase()) {
        case "boolean", "bool" -> toBoolean(value);
        case "int", "integer" -> toInteger(value);
        case "long" -> toLong(value);
        case "double" -> toDouble(value);
        case "string" -> toString(value);
        default -> throw new IllegalArgumentException("Unsupported target type: " + targetType);
      };
    } catch (NumberFormatException e) {
      LOG.warn("Invalid target type syntax provided: {}", e.getMessage());
      return null;
    }
  }

  private static Integer toInteger(Variant value) {
    return switch (value.getType()) {
      case LONG -> (int) value.getLong();
      case DOUBLE -> (int) value.getDouble();
      case FLOAT -> (int) value.getFloat();
      case DECIMAL -> value.getDecimal().intValue();
      case STRING -> Integer.parseInt(value.getString());
      case BOOLEAN -> value.getBoolean() ? 1 : 0;
      default -> null;
    };
  }

  private static Long toLong(Variant value) {
    return switch (value.getType()) {
      case LONG -> value.getLong();
      case DOUBLE -> (long) value.getDouble();
      case FLOAT -> (long) value.getFloat();
      case DECIMAL -> value.getDecimal().longValue();
      case STRING -> Long.parseLong(value.getString());
      case BOOLEAN -> value.getBoolean() ? 1L : 0L;
      case DATE -> value.getLong(); // Return days since epoch
      case TIMESTAMP, TIMESTAMP_NTZ -> value.getLong(); // Return microseconds since epoch
      default -> null;
    };
  }

  private static Double toDouble(Variant value) {
    return switch (value.getType()) {
      case LONG -> (double) value.getLong();
      case DOUBLE -> value.getDouble();
      case FLOAT -> (double) value.getFloat();
      case DECIMAL -> value.getDecimal().doubleValue();
      case STRING -> Double.parseDouble(value.getString());
      case BOOLEAN -> value.getBoolean() ? 1.0 : 0.0;
      default -> null;
    };
  }

  private static Boolean toBoolean(Variant value) {
    return switch (value.getType()) {
      case BOOLEAN -> value.getBoolean();
      case LONG -> value.getLong() != 0;
      case DOUBLE -> value.getDouble() != 0.0;
      case FLOAT -> value.getFloat() != 0.0f;
      case STRING -> Boolean.parseBoolean(value.getString());
      default -> null;
    };
  }

  private static String toString(Variant value) {
    return switch (value.getType()) {
      case BOOLEAN -> String.valueOf(value.getBoolean());
      case LONG -> String.valueOf(value.getLong());
      case DOUBLE -> String.valueOf(value.getDouble());
      case FLOAT -> String.valueOf(value.getFloat());
      case DECIMAL -> value.getDecimal().toPlainString();
      case STRING -> value.getString();
      case BINARY -> Base64.getEncoder().encodeToString(value.getBinary());
      case DATE -> LocalDate.ofEpochDay(value.getLong()).toString();
      case TIMESTAMP, TIMESTAMP_NTZ -> {
        Instant instant = Instant.EPOCH.plus(value.getLong(), ChronoUnit.MICROS);
        yield instant.toString();
      }
      case UUID -> value.getUuid().toString();
      case OBJECT, ARRAY -> value.toJson(ZoneOffset.UTC);
      default -> null;
    };
  }

  /**
   * Represents a single segment in a parsed path.
   */
  private interface VariantToken {
    Variant get(Variant target);
  }

  /**
   * A {@link VariantToken} representing an object field access (e.g., ".name").
   */
  private record FieldToken(String key) implements VariantToken {
    @Override
    public Variant get(Variant target) {
      if (target != null && target.getType() == VariantUtil.Type.OBJECT) {
        return target.getFieldByKey(key);
      }
      return null;
    }
  }

  /**
   * A {@link VariantToken} representing an array element access (e.g., "[123]").
   */
  private record IndexToken(int index) implements VariantToken {
    @Override
    public Variant get(Variant target) {
      if (target != null && target.getType() == VariantUtil.Type.ARRAY) {
        return target.getElementAtIndex(index);
      }
      return null;
    }
  }

  /**
   * A simple parser for a simplified JSONPath-like syntax.
   */
  private static final class VariantPathParser {
    /**
     * Parses a path string into a sequence of {@link VariantToken} tokens.
     */
    public static List<VariantToken> parse(String path) {
      if (path == null || !path.startsWith("$")) {
        throw new IllegalArgumentException("Invalid path: must start with '$'.");
      }
      if (path.length() == 1) {
        return Collections.emptyList(); // root path itself
      }
      List<VariantToken> tokens = new ArrayList<>();
      int i = 1; // Current position, start after the '$'

      while (i < path.length()) {
        char c = path.charAt(i);
        if (c == '.') {
          i++; // Move past the dot
          int start = i;
          // Find the end of the field name (next dot or bracket)
          while (i < path.length() && path.charAt(i) != '.' && path.charAt(i) != '[') {
            i++;
          }
          String key = path.substring(start, i);
          if (key.isEmpty()) {
            throw new IllegalArgumentException(
                "Invalid path: empty field name at position " + start);
          }
          tokens.add(new FieldToken(key));

        } else if (c == '[') {
          i++; // Move past the opening bracket
          int start = i;
          int end = path.indexOf(']', start);
          if (end == -1) {
            throw new IllegalArgumentException(
                "Invalid path: unclosed array index at position " + start);
          }
          String indexStr = path.substring(start, end).trim();
          try {
            int index = Integer.parseInt(indexStr);
            tokens.add(new IndexToken(index));

          } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                "Invalid path: non-integer array index '" + indexStr + "'");
          }
          i = end + 1; // Move past the closing bracket
        } else {
          throw new IllegalArgumentException(
              "Invalid path: unexpected character '" + c + "' at position " + i);
        }
      }
      return tokens;
    }
  }

  private static String unescapeJson(String str) {
    if (str == null) {
      return null;
    }
    // For arrays and objects, return as-is (e.g., "[]", "{}")
    if (str.startsWith("[") || str.startsWith("{")) {
      return str;
    }
    try {
      return MAPPER.readValue(str, String.class);
    } catch (JsonProcessingException e) {
      return null;
    }
  }
}