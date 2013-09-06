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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.data;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema.Type;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchemaUtils;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonSerDe implements SerDe {

  private static final Logger LOG = LoggerFactory.getLogger(JsonSerDe.class);
  private List<String> columnNames;
  private List<TypeInfo> columnTypes;

  private StructTypeInfo rowTypeInfo;
  private HCatSchema schema;

  private JsonFactory jsonFactory = null;

  private HCatRecordObjectInspector cachedObjectInspector;

  @Override
  public void initialize(Configuration conf, Properties tbl)
    throws SerDeException {


    LOG.debug("Initializing JsonSerDe");
    LOG.debug("props to serde: {}", tbl.entrySet());


    // Get column names and types
    String columnNameProperty = tbl.getProperty(serdeConstants.LIST_COLUMNS);
    String columnTypeProperty = tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES);

    // all table column names
    if (columnNameProperty.length() == 0) {
      columnNames = new ArrayList<String>();
    } else {
      columnNames = Arrays.asList(columnNameProperty.split(","));
    }

    // all column types
    if (columnTypeProperty.length() == 0) {
      columnTypes = new ArrayList<TypeInfo>();
    } else {
      columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
    }

    LOG.debug("columns: {}, {}", columnNameProperty, columnNames);
    LOG.debug("types: {}, {} ", columnTypeProperty, columnTypes);

    assert (columnNames.size() == columnTypes.size());

    rowTypeInfo = (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(columnNames, columnTypes);

    cachedObjectInspector = HCatRecordObjectInspectorFactory.getHCatRecordObjectInspector(rowTypeInfo);
    try {
      schema = HCatSchemaUtils.getHCatSchema(rowTypeInfo).get(0).getStructSubSchema();
      LOG.debug("schema : {}", schema);
      LOG.debug("fields : {}", schema.getFieldNames());
    } catch (HCatException e) {
      throw new SerDeException(e);
    }

    jsonFactory = new JsonFactory();
  }

  /**
   * Takes JSON string in Text form, and has to return an object representation above
   * it that's readable by the corresponding object inspector.
   *
   * For this implementation, since we're using the jackson parser, we can construct
   * our own object implementation, and we use HCatRecord for it
   */
  @Override
  public Object deserialize(Writable blob) throws SerDeException {

    Text t = (Text) blob;
    JsonParser p;
    List<Object> r = new ArrayList<Object>(Collections.nCopies(columnNames.size(), null));
    try {
      p = jsonFactory.createJsonParser(new ByteArrayInputStream((t.getBytes())));
      if (p.nextToken() != JsonToken.START_OBJECT) {
        throw new IOException("Start token not found where expected");
      }
      JsonToken token;
      while (((token = p.nextToken()) != JsonToken.END_OBJECT) && (token != null)) {
        // iterate through each token, and create appropriate object here.
        populateRecord(r, token, p, schema);
      }
    } catch (JsonParseException e) {
      LOG.warn("Error [{}] parsing json text [{}].", e, t);
      LOG.debug(null, e);
      throw new SerDeException(e);
    } catch (IOException e) {
      LOG.warn("Error [{}] parsing json text [{}].", e, t);
      LOG.debug(null, e);
      throw new SerDeException(e);
    }

    return new DefaultHCatRecord(r);
  }

  private void populateRecord(List<Object> r, JsonToken token, JsonParser p, HCatSchema s) throws IOException {
    if (token != JsonToken.FIELD_NAME) {
      throw new IOException("Field name expected");
    }
    String fieldName = p.getText();
    int fpos;
    try {
      fpos = s.getPosition(fieldName);
    } catch (NullPointerException npe) {
      fpos = getPositionFromHiveInternalColumnName(fieldName);
      LOG.debug("NPE finding position for field [{}] in schema [{}]", fieldName, s);
      if (!fieldName.equalsIgnoreCase(getHiveInternalColumnName(fpos))) {
        LOG.error("Hive internal column name {} and position "
          + "encoding {} for the column name are at odds", fieldName, fpos);
        throw npe;
      }
      if (fpos == -1) {
        return; // unknown field, we return.
      }
    }
    HCatFieldSchema hcatFieldSchema = s.getFields().get(fpos);
    Object currField = extractCurrentField(p, null, hcatFieldSchema, false);
    r.set(fpos, currField);
  }

  public String getHiveInternalColumnName(int fpos) {
    return HiveConf.getColumnInternalName(fpos);
  }

  public int getPositionFromHiveInternalColumnName(String internalName) {
//    return HiveConf.getPositionFromInternalName(fieldName);
    // The above line should have been all the implementation that
    // we need, but due to a bug in that impl which recognizes
    // only single-digit columns, we need another impl here.
    Pattern internalPattern = Pattern.compile("_col([0-9]+)");
    Matcher m = internalPattern.matcher(internalName);
    if (!m.matches()) {
      return -1;
    } else {
      return Integer.parseInt(m.group(1));
    }
  }

  /**
   * Utility method to extract current expected field from given JsonParser
   *
   * To get the field, we need either a type or a hcatFieldSchema(necessary for complex types)
   * It is possible that one of them can be null, and so, if so, the other is instantiated
   * from the other
   *
   * isTokenCurrent is a boolean variable also passed in, which determines
   * if the JsonParser is already at the token we expect to read next, or
   * needs advancing to the next before we read.
   */
  private Object extractCurrentField(JsonParser p, Type t,
                     HCatFieldSchema hcatFieldSchema, boolean isTokenCurrent) throws IOException, JsonParseException,
    HCatException {
    Object val = null;
    JsonToken valueToken;
    if (isTokenCurrent) {
      valueToken = p.getCurrentToken();
    } else {
      valueToken = p.nextToken();
    }

    if (hcatFieldSchema != null) {
      t = hcatFieldSchema.getType();
    }
    switch (t) {
    case INT:
      val = (valueToken == JsonToken.VALUE_NULL) ? null : p.getIntValue();
      break;
    case TINYINT:
      val = (valueToken == JsonToken.VALUE_NULL) ? null : p.getByteValue();
      break;
    case SMALLINT:
      val = (valueToken == JsonToken.VALUE_NULL) ? null : p.getShortValue();
      break;
    case BIGINT:
      val = (valueToken == JsonToken.VALUE_NULL) ? null : p.getLongValue();
      break;
    case BOOLEAN:
      String bval = (valueToken == JsonToken.VALUE_NULL) ? null : p.getText();
      if (bval != null) {
        val = Boolean.valueOf(bval);
      } else {
        val = null;
      }
      break;
    case FLOAT:
      val = (valueToken == JsonToken.VALUE_NULL) ? null : p.getFloatValue();
      break;
    case DOUBLE:
      val = (valueToken == JsonToken.VALUE_NULL) ? null : p.getDoubleValue();
      break;
    case STRING:
      val = (valueToken == JsonToken.VALUE_NULL) ? null : p.getText();
      break;
    case BINARY:
      throw new IOException("JsonSerDe does not support BINARY type");
    case ARRAY:
      if (valueToken == JsonToken.VALUE_NULL) {
        val = null;
        break;
      }
      if (valueToken != JsonToken.START_ARRAY) {
        throw new IOException("Start of Array expected");
      }
      List<Object> arr = new ArrayList<Object>();
      while ((valueToken = p.nextToken()) != JsonToken.END_ARRAY) {
        arr.add(extractCurrentField(p, null, hcatFieldSchema.getArrayElementSchema().get(0), true));
      }
      val = arr;
      break;
    case MAP:
      if (valueToken == JsonToken.VALUE_NULL) {
        val = null;
        break;
      }
      if (valueToken != JsonToken.START_OBJECT) {
        throw new IOException("Start of Object expected");
      }
      Map<Object, Object> map = new LinkedHashMap<Object, Object>();
      Type keyType = hcatFieldSchema.getMapKeyType();
      HCatFieldSchema valueSchema = hcatFieldSchema.getMapValueSchema().get(0);
      while ((valueToken = p.nextToken()) != JsonToken.END_OBJECT) {
        Object k = getObjectOfCorrespondingPrimitiveType(p.getCurrentName(), keyType);
        Object v;
        if (valueSchema.getType() == HCatFieldSchema.Type.STRUCT) {
          v = extractCurrentField(p, null, valueSchema, false);
        } else {
          v = extractCurrentField(p, null, valueSchema, true);
        }

        map.put(k, v);
      }
      val = map;
      break;
    case STRUCT:
      if (valueToken == JsonToken.VALUE_NULL) {
        val = null;
        break;
      }
      if (valueToken != JsonToken.START_OBJECT) {
        throw new IOException("Start of Object expected");
      }
      HCatSchema subSchema = hcatFieldSchema.getStructSubSchema();
      int sz = subSchema.getFieldNames().size();

      List<Object> struct = new ArrayList<Object>(Collections.nCopies(sz, null));
      while ((valueToken = p.nextToken()) != JsonToken.END_OBJECT) {
        populateRecord(struct, valueToken, p, subSchema);
      }
      val = struct;
      break;
    }
    return val;
  }

  private Object getObjectOfCorrespondingPrimitiveType(String s, Type t) throws IOException {
    switch (t) {
    case INT:
      return Integer.valueOf(s);
    case TINYINT:
      return Byte.valueOf(s);
    case SMALLINT:
      return Short.valueOf(s);
    case BIGINT:
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
      throw new IOException("JsonSerDe does not support BINARY type");
    }
    throw new IOException("Could not convert from string to map type " + t);
  }

  /**
   * Given an object and object inspector pair, traverse the object
   * and generate a Text representation of the object.
   */
  @Override
  public Writable serialize(Object obj, ObjectInspector objInspector)
    throws SerDeException {
    StringBuilder sb = new StringBuilder();
    try {

      StructObjectInspector soi = (StructObjectInspector) objInspector;
      List<? extends StructField> structFields = soi.getAllStructFieldRefs();
      assert (columnNames.size() == structFields.size());
      if (obj == null) {
        sb.append("null");
      } else {
        sb.append(SerDeUtils.LBRACE);
        for (int i = 0; i < structFields.size(); i++) {
          if (i > 0) {
            sb.append(SerDeUtils.COMMA);
          }
          sb.append(SerDeUtils.QUOTE);
          sb.append(columnNames.get(i));
          sb.append(SerDeUtils.QUOTE);
          sb.append(SerDeUtils.COLON);
          buildJSONString(sb, soi.getStructFieldData(obj, structFields.get(i)),
            structFields.get(i).getFieldObjectInspector());
        }
        sb.append(SerDeUtils.RBRACE);
      }

    } catch (IOException e) {
      LOG.warn("Error generating json text from object.", e);
      throw new SerDeException(e);
    }
    return new Text(sb.toString());
  }

  // TODO : code section copied over from SerDeUtils because of non-standard json production there
  // should use quotes for all field names. We should fix this there, and then remove this copy.
  // See http://jackson.codehaus.org/1.7.3/javadoc/org/codehaus/jackson/JsonParser.Feature.html#ALLOW_UNQUOTED_FIELD_NAMES
  // for details - trying to enable Jackson to ignore that doesn't seem to work(compilation failure
  // when attempting to use that feature, so having to change the production itself.
  // Also, throws IOException when Binary is detected.
  private static void buildJSONString(StringBuilder sb, Object o, ObjectInspector oi) throws IOException {

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
          sb.append(SerDeUtils.escapeString(((StringObjectInspector) poi)
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
          throw new IOException("JsonSerDe does not support BINARY type");
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
        sb.append(SerDeUtils.LBRACKET);
        for (int i = 0; i < olist.size(); i++) {
          if (i > 0) {
            sb.append(SerDeUtils.COMMA);
          }
          buildJSONString(sb, olist.get(i), listElementObjectInspector);
        }
        sb.append(SerDeUtils.RBRACKET);
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
        sb.append(SerDeUtils.LBRACE);
        boolean first = true;
        for (Object entry : omap.entrySet()) {
          if (first) {
            first = false;
          } else {
            sb.append(SerDeUtils.COMMA);
          }
          Map.Entry<?, ?> e = (Map.Entry<?, ?>) entry;
          StringBuilder keyBuilder = new StringBuilder();
          buildJSONString(keyBuilder, e.getKey(), mapKeyObjectInspector);
          String keyString = keyBuilder.toString().trim();
          boolean doQuoting = (!keyString.isEmpty()) && (keyString.charAt(0) != SerDeUtils.QUOTE);
          if (doQuoting) {
            sb.append(SerDeUtils.QUOTE);
          }
          sb.append(keyString);
          if (doQuoting) {
            sb.append(SerDeUtils.QUOTE);
          }
          sb.append(SerDeUtils.COLON);
          buildJSONString(sb, e.getValue(), mapValueObjectInspector);
        }
        sb.append(SerDeUtils.RBRACE);
      }
      break;
    }
    case STRUCT: {
      StructObjectInspector soi = (StructObjectInspector) oi;
      List<? extends StructField> structFields = soi.getAllStructFieldRefs();
      if (o == null) {
        sb.append("null");
      } else {
        sb.append(SerDeUtils.LBRACE);
        for (int i = 0; i < structFields.size(); i++) {
          if (i > 0) {
            sb.append(SerDeUtils.COMMA);
          }
          sb.append(SerDeUtils.QUOTE);
          sb.append(structFields.get(i).getFieldName());
          sb.append(SerDeUtils.QUOTE);
          sb.append(SerDeUtils.COLON);
          buildJSONString(sb, soi.getStructFieldData(o, structFields.get(i)),
            structFields.get(i).getFieldObjectInspector());
        }
        sb.append(SerDeUtils.RBRACE);
      }
      break;
    }
    case UNION: {
      UnionObjectInspector uoi = (UnionObjectInspector) oi;
      if (o == null) {
        sb.append("null");
      } else {
        sb.append(SerDeUtils.LBRACE);
        sb.append(uoi.getTag(o));
        sb.append(SerDeUtils.COLON);
        buildJSONString(sb, uoi.getField(o),
          uoi.getObjectInspectors().get(uoi.getTag(o)));
        sb.append(SerDeUtils.RBRACE);
      }
      break;
    }
    default:
      throw new RuntimeException("Unknown type in ObjectInspector!");
    }
  }


  /**
   *  Returns an object inspector for the specified schema that
   *  is capable of reading in the object representation of the JSON string
   */
  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return cachedObjectInspector;
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return Text.class;
  }

  @Override
  public SerDeStats getSerDeStats() {
    // no support for statistics yet
    return null;
  }

}
