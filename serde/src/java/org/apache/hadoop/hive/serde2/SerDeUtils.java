/**
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

package org.apache.hadoop.hive.serde2;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveIntervalDayTimeObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveIntervalYearMonthObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SerDeUtils.
 *
 */
public final class SerDeUtils {

  public static final char QUOTE = '"';
  public static final char COLON = ':';
  public static final char COMMA = ',';
  // we should use '\0' for COLUMN_NAME_DELIMITER if column name contains COMMA
  // but we should also take care of the backward compatibility
  public static final char COLUMN_COMMENTS_DELIMITER = '\0';
  public static final String LBRACKET = "[";
  public static final String RBRACKET = "]";
  public static final String LBRACE = "{";
  public static final String RBRACE = "}";

  // lower case null is used within json objects
  private static final String JSON_NULL = "null";
  public static final String LIST_SINK_OUTPUT_FORMATTER = "list.sink.output.formatter";
  public static final String LIST_SINK_OUTPUT_PROTOCOL = "list.sink.output.protocol";
  public static final Logger LOG = LoggerFactory.getLogger(SerDeUtils.class.getName());

  /**
   * Escape a String in JSON format.
   */
  public static String escapeString(String str) {
    int length = str.length();
    StringBuilder escape = new StringBuilder(length + 16);

    for (int i = 0; i < length; ++i) {
      char c = str.charAt(i);
      switch (c) {
      case '"':
      case '\\':
        escape.append('\\');
        escape.append(c);
        break;
      case '\b':
        escape.append('\\');
        escape.append('b');
        break;
      case '\f':
        escape.append('\\');
        escape.append('f');
        break;
      case '\n':
        escape.append('\\');
        escape.append('n');
        break;
      case '\r':
        escape.append('\\');
        escape.append('r');
        break;
      case '\t':
        escape.append('\\');
        escape.append('t');
        break;
      default:
        // Control characeters! According to JSON RFC u0020
        if (c < ' ') {
          String hex = Integer.toHexString(c);
          escape.append('\\');
          escape.append('u');
          for (int j = 4; j > hex.length(); --j) {
            escape.append('0');
          }
          escape.append(hex);
        } else {
          escape.append(c);
        }
        break;
      }
    }
    return (escape.toString());
  }

  public static String lightEscapeString(String str) {
    int length = str.length();
    StringBuilder escape = new StringBuilder(length + 16);

    for (int i = 0; i < length; ++i) {
      char c = str.charAt(i);
      switch (c) {
      case '\n':
        escape.append('\\');
        escape.append('n');
        break;
      case '\r':
        escape.append('\\');
        escape.append('r');
        break;
      case '\t':
        escape.append('\\');
        escape.append('t');
        break;
      default:
        escape.append(c);
        break;
      }
    }
    return (escape.toString());
  }

  /**
   * Convert a Object to a standard Java object in compliance with JDBC 3.0 (see JDBC 3.0
   * Specification, Table B-3: Mapping from JDBC Types to Java Object Types).
   *
   * This method is kept consistent with {@link HiveResultSetMetaData#hiveTypeToSqlType}.
   */
  public static Object toThriftPayload(Object val, ObjectInspector valOI, int version) {
    if (valOI.getCategory() == ObjectInspector.Category.PRIMITIVE) {
      if (val == null) {
        return null;
      }
      Object obj = ObjectInspectorUtils.copyToStandardObject(val, valOI,
          ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA);
      // uses string type for binary before HIVE_CLI_SERVICE_PROTOCOL_V6
      if (version < 5 && ((PrimitiveObjectInspector)valOI).getPrimitiveCategory() ==
          PrimitiveObjectInspector.PrimitiveCategory.BINARY) {
        // todo HIVE-5269
        return new String((byte[])obj);
      }
      return obj;
    }
    // for now, expose non-primitive as a string
    // TODO: expose non-primitive as a structured object while maintaining JDBC compliance
    return SerDeUtils.getJSONString(val, valOI);
  }

  public static String getJSONString(Object o, ObjectInspector oi) {
    return getJSONString(o, oi, JSON_NULL);
  }

  /**
   * Use this if you need to have custom representation of top level null .
   * (ie something other than 'null')
   * eg, for hive output, we want to to print NULL for a null map object.
   * @param o Object
   * @param oi ObjectInspector
   * @param nullStr The custom string used to represent null value
   * @return
   */
  public static String getJSONString(Object o, ObjectInspector oi, String nullStr) {
    StringBuilder sb = new StringBuilder();
    buildJSONString(sb, o, oi, nullStr);
    return sb.toString();
  }


  static void buildJSONString(StringBuilder sb, Object o, ObjectInspector oi, String nullStr) {
    switch (oi.getCategory()) {
    case PRIMITIVE: {
      PrimitiveObjectInspector poi = (PrimitiveObjectInspector) oi;
      if (o == null) {
        sb.append(nullStr);
      } else {
        switch (poi.getPrimitiveCategory()) {
        case BOOLEAN: {
          boolean b = ((BooleanObjectInspector) poi).get(o);
          sb.append(b ? "true" : "false");
          break;
        }
        case BYTE: {
          sb.append(((ByteObjectInspector) poi).get(o));
          break;
        }
        case SHORT: {
          sb.append(((ShortObjectInspector) poi).get(o));
          break;
        }
        case INT: {
          sb.append(((IntObjectInspector) poi).get(o));
          break;
        }
        case LONG: {
          sb.append(((LongObjectInspector) poi).get(o));
          break;
        }
        case FLOAT: {
          sb.append(((FloatObjectInspector) poi).get(o));
          break;
        }
        case DOUBLE: {
          sb.append(((DoubleObjectInspector) poi).get(o));
          break;
        }
        case STRING: {
          sb.append('"');
          sb.append(escapeString(((StringObjectInspector) poi)
              .getPrimitiveJavaObject(o)));
          sb.append('"');
          break;
        }
        case CHAR: {
          sb.append('"');
          sb.append(escapeString(((HiveCharObjectInspector) poi)
              .getPrimitiveJavaObject(o).toString()));
          sb.append('"');
          break;
        }
        case VARCHAR: {
          sb.append('"');
          sb.append(escapeString(((HiveVarcharObjectInspector) poi)
              .getPrimitiveJavaObject(o).toString()));
          sb.append('"');
          break;
        }
        case DATE: {
          sb.append('"');
          sb.append(((DateObjectInspector) poi)
              .getPrimitiveWritableObject(o));
          sb.append('"');
          break;
        }
        case TIMESTAMP: {
          sb.append('"');
          sb.append(((TimestampObjectInspector) poi)
              .getPrimitiveWritableObject(o));
          sb.append('"');
          break;
        }
        case BINARY: {
          BytesWritable bw = ((BinaryObjectInspector) oi).getPrimitiveWritableObject(o);
          Text txt = new Text();
          txt.set(bw.getBytes(), 0, bw.getLength());
          sb.append(txt.toString());
          break;
        }
        case DECIMAL: {
          sb.append(((HiveDecimalObjectInspector) oi).getPrimitiveJavaObject(o));
          break;
        }
        case INTERVAL_YEAR_MONTH: {
          sb.append(((HiveIntervalYearMonthObjectInspector) oi).getPrimitiveJavaObject(o));
          break;
        }
        case INTERVAL_DAY_TIME: {
          sb.append(((HiveIntervalDayTimeObjectInspector) oi).getPrimitiveJavaObject(o));
          break;
        }

        default:
          throw new RuntimeException("Unknown primitive type: "
              + poi.getPrimitiveCategory());
        }
      }
      break;
    }
    case LIST: {
      ListObjectInspector loi = (ListObjectInspector) oi;
      ObjectInspector listElementObjectInspector = loi
          .getListElementObjectInspector();
      List<?> olist = loi.getList(o);
      if (olist == null) {
        sb.append(nullStr);
      } else {
        sb.append(LBRACKET);
        for (int i = 0; i < olist.size(); i++) {
          if (i > 0) {
            sb.append(COMMA);
          }
          buildJSONString(sb, olist.get(i), listElementObjectInspector, JSON_NULL);
        }
        sb.append(RBRACKET);
      }
      break;
    }
    case MAP: {
      MapObjectInspector moi = (MapObjectInspector) oi;
      ObjectInspector mapKeyObjectInspector = moi.getMapKeyObjectInspector();
      ObjectInspector mapValueObjectInspector = moi
          .getMapValueObjectInspector();
      Map<?, ?> omap = moi.getMap(o);
      if (omap == null) {
        sb.append(nullStr);
      } else {
        sb.append(LBRACE);
        boolean first = true;
        for (Object entry : omap.entrySet()) {
          if (first) {
            first = false;
          } else {
            sb.append(COMMA);
          }
          Map.Entry<?, ?> e = (Map.Entry<?, ?>) entry;
          buildJSONString(sb, e.getKey(), mapKeyObjectInspector, JSON_NULL);
          sb.append(COLON);
          buildJSONString(sb, e.getValue(), mapValueObjectInspector, JSON_NULL);
        }
        sb.append(RBRACE);
      }
      break;
    }
    case STRUCT: {
      StructObjectInspector soi = (StructObjectInspector) oi;
      List<? extends StructField> structFields = soi.getAllStructFieldRefs();
      if (o == null) {
        sb.append(nullStr);
      } else {
        sb.append(LBRACE);
        for (int i = 0; i < structFields.size(); i++) {
          if (i > 0) {
            sb.append(COMMA);
          }
          sb.append(QUOTE);
          sb.append(structFields.get(i).getFieldName());
          sb.append(QUOTE);
          sb.append(COLON);
          buildJSONString(sb, soi.getStructFieldData(o, structFields.get(i)),
              structFields.get(i).getFieldObjectInspector(), JSON_NULL);
        }
        sb.append(RBRACE);
      }
      break;
    }
    case UNION: {
      UnionObjectInspector uoi = (UnionObjectInspector) oi;
      if (o == null) {
        sb.append(nullStr);
      } else {
        sb.append(LBRACE);
        sb.append(uoi.getTag(o));
        sb.append(COLON);
        buildJSONString(sb, uoi.getField(o),
              uoi.getObjectInspectors().get(uoi.getTag(o)), JSON_NULL);
        sb.append(RBRACE);
      }
      break;
    }
    default:
      throw new RuntimeException("Unknown type in ObjectInspector!");
    }
  }

  /**
   * return false though element is null if nullsafe flag is true for that
   */
  public static boolean hasAnyNullObject(List o, StructObjectInspector loi,
      boolean[] nullSafes) {
    List<? extends StructField> fields = loi.getAllStructFieldRefs();
    for (int i = 0; i < o.size();i++) {
      if ((nullSafes == null || !nullSafes[i])
          && hasAnyNullObject(o.get(i), fields.get(i).getFieldObjectInspector())) {
        return true;
      }
    }
    return false;
  }
  /**
   * True if Object passed is representing null object.
   *
   * @param o The object
   * @param oi The ObjectInspector
   *
   * @return true if the object passed is representing NULL object
   *         false otherwise
   */
  public static boolean hasAnyNullObject(Object o, ObjectInspector oi) {
    switch (oi.getCategory()) {
    case PRIMITIVE: {
      if (o == null) {
        return true;
      }
      return false;
    }
    case LIST: {
      ListObjectInspector loi = (ListObjectInspector) oi;
      ObjectInspector listElementObjectInspector = loi
          .getListElementObjectInspector();
      List<?> olist = loi.getList(o);
      if (olist == null) {
        return true;
      } else {
        // there are no elements in the list
        if (olist.size() == 0) {
          return false;
        }
        // if all the elements are representing null, then return true
        for (int i = 0; i < olist.size(); i++) {
          if (hasAnyNullObject(olist.get(i), listElementObjectInspector)) {
            return true;
          }
        }
        return false;
      }
    }
    case MAP: {
      MapObjectInspector moi = (MapObjectInspector) oi;
      ObjectInspector mapKeyObjectInspector = moi.getMapKeyObjectInspector();
      ObjectInspector mapValueObjectInspector = moi
          .getMapValueObjectInspector();
      Map<?, ?> omap = moi.getMap(o);
      if (omap == null) {
        return true;
      } else {
        // there are no elements in the map
        if (omap.entrySet().size() == 0) {
          return false;
        }
        // if all the entries of map are representing null, then return true
        for (Map.Entry<?, ?> entry : omap.entrySet()) {
          if (hasAnyNullObject(entry.getKey(), mapKeyObjectInspector)
              || hasAnyNullObject(entry.getValue(), mapValueObjectInspector)) {
            return true;
          }
        }
        return false;
      }
    }
    case STRUCT: {
      StructObjectInspector soi = (StructObjectInspector) oi;
      List<? extends StructField> structFields = soi.getAllStructFieldRefs();
      if (o == null) {
        return true;
      } else {
        // there are no fields in the struct
        if (structFields.size() == 0) {
          return false;
        }
        // if any the fields of struct are representing null, then return true
        for (int i = 0; i < structFields.size(); i++) {
          if (hasAnyNullObject(soi.getStructFieldData(o, structFields.get(i)),
              structFields.get(i).getFieldObjectInspector())) {
            return true;
          }
        }
        return false;
      }
    }
    case UNION: {
      UnionObjectInspector uoi = (UnionObjectInspector) oi;
      if (o == null) {
        return true;
      } else {
        // there are no elements in the union
        if (uoi.getObjectInspectors().size() == 0) {
          return false;
        }
        return hasAnyNullObject(uoi.getField(o),
            uoi.getObjectInspectors().get(uoi.getTag(o)));
      }
    }
    default:
      throw new RuntimeException("Unknown type in ObjectInspector!");
    }
  }

  /**
   * Returns the union of table and partition properties,
   * with partition properties taking precedence.
   * @param tblProps
   * @param partProps
   * @return the overlayed properties
   */
  public static Properties createOverlayedProperties(Properties tblProps, Properties partProps) {
    Properties props = new Properties();
    props.putAll(tblProps);
    if (partProps != null) {
      props.putAll(partProps);
    }
    return props;
  }

  /**
   * Initializes a SerDe.
   * @param deserializer
   * @param conf
   * @param tblProps
   * @param partProps
   * @throws SerDeException
   */
  public static void initializeSerDe(Deserializer deserializer, Configuration conf,
                                            Properties tblProps, Properties partProps)
                                                throws SerDeException {
    if (deserializer instanceof AbstractSerDe) {
      ((AbstractSerDe) deserializer).initialize(conf, tblProps, partProps);
      String msg = ((AbstractSerDe) deserializer).getConfigurationErrors();
      if (msg != null && !msg.isEmpty()) {
        throw new SerDeException(msg);
      }
    } else {
      deserializer.initialize(conf, createOverlayedProperties(tblProps, partProps));
    }
  }

  /**
   * Initializes a SerDe.
   * @param deserializer
   * @param conf
   * @param tblProps
   * @param partProps
   * @throws SerDeException
   */
  public static void initializeSerDeWithoutErrorCheck(Deserializer deserializer,
                                                      Configuration conf, Properties tblProps,
                                                      Properties partProps) throws SerDeException {
    if (deserializer instanceof AbstractSerDe) {
      ((AbstractSerDe) deserializer).initialize(conf, tblProps, partProps);
    } else {
      deserializer.initialize(conf, createOverlayedProperties(tblProps, partProps));
    }
  }

  private SerDeUtils() {
    // prevent instantiation
  }

  public static Text transformTextToUTF8(Text text, Charset previousCharset) {
    return new Text(new String(text.getBytes(), 0, text.getLength(), previousCharset));
  }

  public static Text transformTextFromUTF8(Text text, Charset targetCharset) {
    return new Text(new String(text.getBytes(), 0, text.getLength()).getBytes(targetCharset));
  }

  public static void writeLong(byte[] writeBuffer, int offset, long value) {
    writeBuffer[offset] = (byte) ((value >> 0)  & 0xff);
    writeBuffer[offset + 1] = (byte) ((value >> 8)  & 0xff);
    writeBuffer[offset + 2] = (byte) ((value >> 16) & 0xff);
    writeBuffer[offset + 3] = (byte) ((value >> 24) & 0xff);
    writeBuffer[offset + 4] = (byte) ((value >> 32) & 0xff);
    writeBuffer[offset + 5] = (byte) ((value >> 40) & 0xff);
    writeBuffer[offset + 6] = (byte) ((value >> 48) & 0xff);
    writeBuffer[offset + 7] = (byte) ((value >> 56) & 0xff);
  }
}
