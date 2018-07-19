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

package org.apache.hadoop.hive.serde2.json;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.BaseCharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.hive.common.util.TimestampParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

public class HiveJsonStructReader {

  private static final Logger LOG = LoggerFactory.getLogger(HiveJsonStructReader.class);

  private ObjectInspector oi;
  private JsonFactory factory;


  Set<String> reportedUnknownFieldNames = new HashSet<>();

  private static boolean ignoreUnknownFields;
  private static boolean hiveColIndexParsing;
  private boolean writeablePrimitives;

  private TimestampParser tsParser;

  public HiveJsonStructReader(TypeInfo t) {
    this(t, new TimestampParser());
  }

  public HiveJsonStructReader(TypeInfo t, TimestampParser tsParser) {
    this.tsParser = tsParser;
    oi = TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(t);
    factory = new JsonFactory();
  }

  public Object parseStruct(String text) throws JsonParseException, IOException, SerDeException {
    JsonParser parser = factory.createParser(text);
    return parseInternal(parser);
  }

  public Object parseStruct(InputStream is) throws JsonParseException, IOException, SerDeException {
    JsonParser parser = factory.createParser(is);
    return parseInternal(parser);
  }

  private Object parseInternal(JsonParser parser) throws SerDeException {
    try {
      parser.nextToken();
      Object res = parseDispatcher(parser, oi);
      return res;
    } catch (Exception e) {
      String locationStr = parser.getCurrentLocation().getLineNr() + "," + parser.getCurrentLocation().getColumnNr();
      throw new SerDeException("at[" + locationStr + "]: " + e.getMessage(), e);
    }
  }

  private Object parseDispatcher(JsonParser parser, ObjectInspector oi)
      throws JsonParseException, IOException, SerDeException {

    switch (oi.getCategory()) {
    case PRIMITIVE:
      return parsePrimitive(parser, (PrimitiveObjectInspector) oi);
    case LIST:
      return parseList(parser, (ListObjectInspector) oi);
    case STRUCT:
      return parseStruct(parser, (StructObjectInspector) oi);
    case MAP:
      return parseMap(parser, (MapObjectInspector) oi);
    default:
      throw new SerDeException("parsing of: " + oi.getCategory() + " is not handled");
    }
  }

  private Object parseMap(JsonParser parser, MapObjectInspector oi) throws IOException, SerDeException {

    if (parser.getCurrentToken() == JsonToken.VALUE_NULL) {
      parser.nextToken();
      return null;
    }

    Map<Object, Object> ret = new LinkedHashMap<>();

    if (parser.getCurrentToken() != JsonToken.START_OBJECT) {
      throw new SerDeException("struct expected");
    }

    if (!(oi.getMapKeyObjectInspector() instanceof PrimitiveObjectInspector)) {
      throw new SerDeException("map key must be a primitive");
    }
    PrimitiveObjectInspector keyOI = (PrimitiveObjectInspector) oi.getMapKeyObjectInspector();
    ObjectInspector valOI = oi.getMapValueObjectInspector();

    JsonToken currentToken = parser.nextToken();
    while (currentToken != null && currentToken != JsonToken.END_OBJECT) {

      if (currentToken != JsonToken.FIELD_NAME) {
        throw new SerDeException("unexpected token: " + currentToken);
      }

      Object key = parseMapKey(parser, keyOI);
      Object val = parseDispatcher(parser, valOI);
      ret.put(key, val);

      currentToken = parser.getCurrentToken();
    }
    if (currentToken != null) {
      parser.nextToken();
    }
    return ret;

  }

  private Object parseStruct(JsonParser parser, StructObjectInspector oi)
      throws JsonParseException, IOException, SerDeException {

    Object[] ret = new Object[oi.getAllStructFieldRefs().size()];

    if (parser.getCurrentToken() == JsonToken.VALUE_NULL) {
      parser.nextToken();
      return null;
    }
    if (parser.getCurrentToken() != JsonToken.START_OBJECT) {
      throw new SerDeException("struct expected");
    }
    JsonToken currentToken = parser.nextToken();
    while (currentToken != null && currentToken != JsonToken.END_OBJECT) {

      switch (currentToken) {
      case FIELD_NAME:
        String name = parser.getCurrentName();
        try {
          StructField field = null;
          try {
            field = getStructField(oi, name);
          } catch (RuntimeException e) {
            if (ignoreUnknownFields) {
              if (!reportedUnknownFieldNames.contains(name)) {
                LOG.warn("ignoring field:" + name);
                reportedUnknownFieldNames.add(name);
              }
              parser.nextToken();
              skipValue(parser);
              break;
            }
          }
          if (field == null) {
            throw new SerDeException("undeclared field");
          }
          parser.nextToken();
          ret[field.getFieldID()] = parseDispatcher(parser, field.getFieldObjectInspector());
        } catch (Exception e) {
          throw new SerDeException("struct field " + name + ": " + e.getMessage(), e);
        }
        break;
      default:
        throw new SerDeException("unexpected token: " + currentToken);
      }
      currentToken = parser.getCurrentToken();
    }
    if (currentToken != null) {
      parser.nextToken();
    }
    return ret;
  }

  private StructField getStructField(StructObjectInspector oi, String name) {
    if (hiveColIndexParsing) {
      int colIndex = getColIndex(name);
      if (colIndex >= 0) {
        return oi.getAllStructFieldRefs().get(colIndex);
      }
    }
    // FIXME: linear scan inside the below method...get a map here or something..
    return oi.getStructFieldRef(name);
  }

  Pattern internalPattern = Pattern.compile("^_col([0-9]+)$");

  private int getColIndex(String internalName) {
    // The above line should have been all the implementation that
    // we need, but due to a bug in that impl which recognizes
    // only single-digit columns, we need another impl here.
    Matcher m = internalPattern.matcher(internalName);
    if (!m.matches()) {
      return -1;
    } else {
      return Integer.parseInt(m.group(1));
    }
  }

  private static void skipValue(JsonParser parser) throws JsonParseException, IOException {

    int array = 0;
    int object = 0;
    do {
      JsonToken currentToken = parser.getCurrentToken();
      if(currentToken == JsonToken.START_ARRAY) {
        array++;
      }
      if (currentToken == JsonToken.END_ARRAY) {
        array--;
      }
      if (currentToken == JsonToken.START_OBJECT) {
        object++;
      }
      if (currentToken == JsonToken.END_OBJECT) {
        object--;
      }

      parser.nextToken();

    } while (array > 0 || object > 0);

  }

  private Object parseList(JsonParser parser, ListObjectInspector oi)
      throws JsonParseException, IOException, SerDeException {
    List<Object> ret = new ArrayList<>();

    if (parser.getCurrentToken() == JsonToken.VALUE_NULL) {
      parser.nextToken();
      return null;
    }

    if (parser.getCurrentToken() != JsonToken.START_ARRAY) {
      throw new SerDeException("array expected");
    }
    ObjectInspector eOI = oi.getListElementObjectInspector();
    JsonToken currentToken = parser.nextToken();
    try {
      while (currentToken != null && currentToken != JsonToken.END_ARRAY) {
        ret.add(parseDispatcher(parser, eOI));
        currentToken = parser.getCurrentToken();
      }
    } catch (Exception e) {
      throw new SerDeException("array: " + e.getMessage(), e);
    }

    currentToken = parser.nextToken();

    return ret;
  }

  private Object parsePrimitive(JsonParser parser, PrimitiveObjectInspector oi)
      throws SerDeException, IOException {
    JsonToken currentToken = parser.getCurrentToken();
    if (currentToken == null) {
      return null;
    }
    try {
      switch (parser.getCurrentToken()) {
      case VALUE_FALSE:
      case VALUE_TRUE:
      case VALUE_NUMBER_INT:
      case VALUE_NUMBER_FLOAT:
      case VALUE_STRING:
        return getObjectOfCorrespondingPrimitiveType(parser.getValueAsString(), oi);
      case VALUE_NULL:
        return null;
      default:
        throw new SerDeException("unexpected token type: " + currentToken);
      }
    } finally {
      parser.nextToken();

    }
  }

  private Object getObjectOfCorrespondingPrimitiveType(String s, PrimitiveObjectInspector oi)
      throws IOException {
    PrimitiveTypeInfo typeInfo = oi.getTypeInfo();
    if (writeablePrimitives) {
      Converter c = ObjectInspectorConverters.getConverter(PrimitiveObjectInspectorFactory.javaStringObjectInspector, oi);
      return c.convert(s);
    }

    switch (typeInfo.getPrimitiveCategory()) {
    case INT:
      return Integer.valueOf(s);
    case BYTE:
      return Byte.valueOf(s);
    case SHORT:
      return Short.valueOf(s);
    case LONG:
      return Long.valueOf(s);
    case BOOLEAN:
      return (s.equalsIgnoreCase("true"));
    case FLOAT:
      return Float.valueOf(s);
    case DOUBLE:
      return Double.valueOf(s);
    case STRING:
      return s;
    case BINARY:
      try {
        String t = Text.decode(s.getBytes(), 0, s.getBytes().length);
        return t.getBytes();
      } catch (CharacterCodingException e) {
        LOG.warn("Error generating json binary type from object.", e);
        return null;
      }
    case DATE:
      return Date.valueOf(s);
    case TIMESTAMP:
      return tsParser.parseTimestamp(s);
    case DECIMAL:
      return HiveDecimal.create(s);
    case VARCHAR:
      return new HiveVarchar(s, ((BaseCharTypeInfo) typeInfo).getLength());
    case CHAR:
      return new HiveChar(s, ((BaseCharTypeInfo) typeInfo).getLength());
    }
    throw new IOException("Could not convert from string to map type " + typeInfo.getTypeName());
  }

  private Object parseMapKey(JsonParser parser, PrimitiveObjectInspector oi) throws SerDeException, IOException {
    JsonToken currentToken = parser.getCurrentToken();
    if (currentToken == null) {
      return null;
    }
    try {
      switch (parser.getCurrentToken()) {
      case FIELD_NAME:
        return getObjectOfCorrespondingPrimitiveType(parser.getValueAsString(), oi);
      case VALUE_NULL:
        return null;
      default:
        throw new SerDeException("unexpected token type: " + currentToken);
      }
    } finally {
      parser.nextToken();

    }
  }

  public void setIgnoreUnknownFields(boolean b) {
    ignoreUnknownFields = b;
  }

  public void enableHiveColIndexParsing(boolean b) {
    hiveColIndexParsing = b;
  }

  public void setWritablesUsage(boolean b) {
    writeablePrimitives = b;
  }

  public ObjectInspector getObjectInspector() {
    return oi;
  }
}
