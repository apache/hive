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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

/**
 * SerDeUtils.
 *
 */
public final class SerDeUtils {

  public static final char QUOTE = '"';
  public static final char COLON = ':';
  public static final char COMMA = ',';
  public static final String LBRACKET = "[";
  public static final String RBRACKET = "]";
  public static final String LBRACE = "{";
  public static final String RBRACE = "}";

  private static ConcurrentHashMap<String, Class<?>> serdes =
    new ConcurrentHashMap<String, Class<?>>();

  public static void registerSerDe(String name, Class<?> serde) {
    if (serdes.containsKey(name)) {
      throw new RuntimeException("double registering serde " + name);
    }
    serdes.put(name, serde);
  }

  public static Deserializer lookupDeserializer(String name) throws SerDeException {
    Class<?> c;
    if (serdes.containsKey(name)) {
      c = serdes.get(name);
    } else {
      try {
        c = Class.forName(name, true, JavaUtils.getClassLoader());
      } catch (ClassNotFoundException e) {
        throw new SerDeException("SerDe " + name + " does not exist");
      }
    }
    try {
      return (Deserializer) c.newInstance();
    } catch (Exception e) {
      throw new SerDeException(e);
    }
  }

  private static List<String> nativeSerDeNames = new ArrayList<String>();
  static {
    nativeSerDeNames
        .add(org.apache.hadoop.hive.serde2.dynamic_type.DynamicSerDe.class
        .getName());
    nativeSerDeNames
        .add(org.apache.hadoop.hive.serde2.MetadataTypedColumnsetSerDe.class
        .getName());
    // For backward compatibility
    nativeSerDeNames.add("org.apache.hadoop.hive.serde.thrift.columnsetSerDe");
    nativeSerDeNames
        .add(org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.class.getName());
    nativeSerDeNames.add(org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe.class.getName());
  }

  public static boolean shouldGetColsFromSerDe(String serde) {
    return (serde != null) && !nativeSerDeNames.contains(serde);
  }

  private static boolean initCoreSerDes = registerCoreSerDes();

  protected static boolean registerCoreSerDes() {
    // Eagerly load SerDes so they will register their symbolic names even on
    // Lazy Loading JVMs
    try {
      // loading these classes will automatically register the short names
      Class
          .forName(org.apache.hadoop.hive.serde2.MetadataTypedColumnsetSerDe.class
          .getName());
      Class.forName(org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.class
          .getName());
      Class
          .forName(org.apache.hadoop.hive.serde2.thrift.ThriftDeserializer.class
          .getName());
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(
          "IMPOSSIBLE Exception: Unable to initialize core serdes", e);
    }
    return true;
  }

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

  public static String getJSONString(Object o, ObjectInspector oi) {
    StringBuilder sb = new StringBuilder();
    buildJSONString(sb, o, oi);
    return sb.toString();
  }

  static void buildJSONString(StringBuilder sb, Object o, ObjectInspector oi) {

    switch (oi.getCategory()) {
    case PRIMITIVE: {
      PrimitiveObjectInspector poi = (PrimitiveObjectInspector) oi;
      if (o == null) {
        sb.append("null");
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
        sb.append("null");
      } else {
        sb.append(LBRACKET);
        for (int i = 0; i < olist.size(); i++) {
          if (i > 0) {
            sb.append(COMMA);
          }
          buildJSONString(sb, olist.get(i), listElementObjectInspector);
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
        sb.append("null");
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
          buildJSONString(sb, e.getKey(), mapKeyObjectInspector);
          sb.append(COLON);
          buildJSONString(sb, e.getValue(), mapValueObjectInspector);
        }
        sb.append(RBRACE);
      }
      break;
    }
    case STRUCT: {
      StructObjectInspector soi = (StructObjectInspector) oi;
      List<? extends StructField> structFields = soi.getAllStructFieldRefs();
      if (o == null) {
        sb.append("null");
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
              structFields.get(i).getFieldObjectInspector());
        }
        sb.append(RBRACE);
      }
      break;
    }
    case UNION: {
      UnionObjectInspector uoi = (UnionObjectInspector) oi;
      if (o == null) {
        sb.append("null");
      } else {
        sb.append(LBRACE);
        sb.append(uoi.getTag(o));
        sb.append(COLON);
        buildJSONString(sb, uoi.getField(o),
              uoi.getObjectInspectors().get(uoi.getTag(o)));
        sb.append(RBRACE);
      }
      break;
    }
    default:
      throw new RuntimeException("Unknown type in ObjectInspector!");
    }
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

  private SerDeUtils() {
    // prevent instantiation
  }
}
