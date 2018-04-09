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
package org.apache.hive.hcatalog.data;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.CharacterCodingException;
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
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeSpec;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.SerDeUtils;
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
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.BaseCharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hive.common.util.HiveStringUtils;
import org.apache.hive.common.util.TimestampParser;
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

@SerDeSpec(schemaProps = {serdeConstants.LIST_COLUMNS,
                          serdeConstants.LIST_COLUMN_TYPES,
                          serdeConstants.TIMESTAMP_FORMATS})

public class JsonSerDe extends AbstractSerDe {

  private static final Logger LOG = LoggerFactory.getLogger(JsonSerDe.class);
  private List<String> columnNames;
  private HCatSchema schema;

  private JsonFactory jsonFactory = null;

  private HCatRecordObjectInspector cachedObjectInspector;
  private TimestampParser tsParser;

  @Override
  public void initialize(Configuration conf, Properties tbl)
    throws SerDeException {
    List<TypeInfo> columnTypes;
    StructTypeInfo rowTypeInfo;

    LOG.debug("Initializing JsonSerDe: {}", tbl.entrySet());

    // Get column names and types
    String columnNameProperty = tbl.getProperty(serdeConstants.LIST_COLUMNS);
    String columnTypeProperty = tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES);
    final String columnNameDelimiter = tbl.containsKey(serdeConstants.COLUMN_NAME_DELIMITER) ? tbl
        .getProperty(serdeConstants.COLUMN_NAME_DELIMITER) : String.valueOf(SerDeUtils.COMMA);
    // all table column names
    if (columnNameProperty.isEmpty()) {
      columnNames = Collections.emptyList();
    } else {
      columnNames = Arrays.asList(columnNameProperty.split(columnNameDelimiter));
    }

    // all column types
    if (columnTypeProperty.isEmpty()) {
      columnTypes = Collections.emptyList();
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
    tsParser = new TimestampParser(
        HiveStringUtils.splitAndUnEscape(tbl.getProperty(serdeConstants.TIMESTAMP_FORMATS)));
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
      throw new SerDeException(e);
    } catch (IOException e) {
      LOG.warn("Error [{}] parsing json text [{}].", e, t);
      throw new SerDeException(e);
    }

    return new DefaultHCatRecord(r);
  }

  private void populateRecord(List<Object> r, JsonToken token, JsonParser p, HCatSchema s) throws IOException {
    if (token != JsonToken.FIELD_NAME) {
      throw new IOException("Field name expected");
    }
    String fieldName = p.getText();
    Integer fpos = s.getPosition(fieldName);
    if (fpos == null) {
      fpos = getPositionFromHiveInternalColumnName(fieldName);
      LOG.debug("NPE finding position for field [{}] in schema [{}],"
        +" attempting to check if it is an internal column name like _col0", fieldName, s);
      if (fpos == -1) {
        skipValue(p);
        return; // unknown field, we return. We'll continue from the next field onwards.
      }
      // If we get past this, then the column name did match the hive pattern for an internal
      // column name, such as _col0, etc, so it *MUST* match the schema for the appropriate column.
      // This means people can't use arbitrary column names such as _col0, and expect us to ignore it
      // if we find it.
      if (!fieldName.equalsIgnoreCase(getHiveInternalColumnName(fpos))) {
        LOG.error("Hive internal column name {} and position "
          + "encoding {} for the column name are at odds", fieldName, fpos);
        throw new IOException("Hive internal column name ("+ fieldName
            + ") and position encoding (" + fpos
            + ") for the column name are at odds");
      }
      // If we reached here, then we were successful at finding an alternate internal
      // column mapping, and we're about to proceed.
    }
    HCatFieldSchema hcatFieldSchema = s.getFields().get(fpos);
    Object currField = extractCurrentField(p, hcatFieldSchema, false);
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
   * Utility method to extract (and forget) the next value token from the JsonParser,
   * as a whole. The reason this function gets called is to yank out the next value altogether,
   * because it corresponds to a field name that we do not recognize, and thus, do not have
   * a schema/type for. Thus, this field is to be ignored.
   * @throws IOException
   * @throws JsonParseException
   */
  private void skipValue(JsonParser p) throws JsonParseException, IOException {
    JsonToken valueToken = p.nextToken();

    if ((valueToken == JsonToken.START_ARRAY) || (valueToken == JsonToken.START_OBJECT)){
      // if the currently read token is a beginning of an array or object, move stream forward
      // skipping any child tokens till we're at the corresponding END_ARRAY or END_OBJECT token
      p.skipChildren();
    }
    // At the end of this function, the stream should be pointing to the last token that
    // corresponds to the value being skipped. This way, the next call to nextToken
    // will advance it to the next field name.
  }

  /**
   * Utility method to extract current expected field from given JsonParser
   *
   * isTokenCurrent is a boolean variable also passed in, which determines
   * if the JsonParser is already at the token we expect to read next, or
   * needs advancing to the next before we read.
   */
  private Object extractCurrentField(JsonParser p, HCatFieldSchema hcatFieldSchema,
                                     boolean isTokenCurrent) throws IOException {
    Object val = null;
    JsonToken valueToken;
    if (isTokenCurrent) {
      valueToken = p.getCurrentToken();
    } else {
      valueToken = p.nextToken();
    }
    switch (hcatFieldSchema.getType()) {
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
      String b = (valueToken == JsonToken.VALUE_NULL) ? null : p.getText();
      if (b != null) {
        try {
          String t = Text.decode(b.getBytes(), 0, b.getBytes().length);
          return t.getBytes();
        } catch (CharacterCodingException e) {
          LOG.warn("Error generating json binary type from object.", e);
          return null;
        }
      } else {
        val = null;
      }
      break;
      case DATE:
      val = (valueToken == JsonToken.VALUE_NULL) ? null : Date.valueOf(p.getText());
      break;
    case TIMESTAMP:
      val = (valueToken == JsonToken.VALUE_NULL) ? null : tsParser.parseTimestamp(p.getText());
      break;
    case DECIMAL:
      val = (valueToken == JsonToken.VALUE_NULL) ? null : HiveDecimal.create(p.getText());
      break;
    case VARCHAR:
      int vLen = ((BaseCharTypeInfo)hcatFieldSchema.getTypeInfo()).getLength();
      val = (valueToken == JsonToken.VALUE_NULL) ? null : new HiveVarchar(p.getText(), vLen);
      break;
    case CHAR:
      int cLen = ((BaseCharTypeInfo)hcatFieldSchema.getTypeInfo()).getLength();
      val = (valueToken == JsonToken.VALUE_NULL) ? null : new HiveChar(p.getText(), cLen);
      break;
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
        arr.add(extractCurrentField(p, hcatFieldSchema.getArrayElementSchema().get(0), true));
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
      HCatFieldSchema valueSchema = hcatFieldSchema.getMapValueSchema().get(0);
      while ((valueToken = p.nextToken()) != JsonToken.END_OBJECT) {
        Object k = getObjectOfCorrespondingPrimitiveType(p.getCurrentName(), hcatFieldSchema.getMapKeyTypeInfo());
        Object v = extractCurrentField(p, valueSchema, false);
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
    default:
      LOG.error("Unknown type found: " + hcatFieldSchema.getType());
      return null;
    }
    return val;
  }

  private Object getObjectOfCorrespondingPrimitiveType(String s, PrimitiveTypeInfo mapKeyType)
    throws IOException {
    switch (Type.getPrimitiveHType(mapKeyType)) {
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
      return Timestamp.valueOf(s);
    case DECIMAL:
      return HiveDecimal.create(s);
    case VARCHAR:
      return new HiveVarchar(s, ((BaseCharTypeInfo)mapKeyType).getLength());
    case CHAR:
      return new HiveChar(s, ((BaseCharTypeInfo)mapKeyType).getLength());
    }
    throw new IOException("Could not convert from string to map type " + mapKeyType.getTypeName());
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
          appendWithQuotes(sb, columnNames.get(i));
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
  private static StringBuilder appendWithQuotes(StringBuilder sb, String value) {
    return sb == null ? null : sb.append(SerDeUtils.QUOTE).append(value).append(SerDeUtils.QUOTE);
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
          String s =
                  SerDeUtils.escapeString(((StringObjectInspector) poi).getPrimitiveJavaObject(o));
          appendWithQuotes(sb, s);
          break;
        }
        case BINARY:
          byte[] b = ((BinaryObjectInspector) oi).getPrimitiveJavaObject(o);
          Text txt = new Text();
          txt.set(b, 0, b.length);
          appendWithQuotes(sb, SerDeUtils.escapeString(txt.toString()));
          break;
        case DATE:
          Date d = ((DateObjectInspector)poi).getPrimitiveJavaObject(o);
          appendWithQuotes(sb, d.toString());
          break;
        case TIMESTAMP: {
          Timestamp t = ((TimestampObjectInspector) poi).getPrimitiveJavaObject(o);
          appendWithQuotes(sb, t.toString());
          break;
        }
        case DECIMAL:
          sb.append(((HiveDecimalObjectInspector)poi).getPrimitiveJavaObject(o));
          break;
        case VARCHAR: {
          String s = SerDeUtils.escapeString(
              ((HiveVarcharObjectInspector) poi).getPrimitiveJavaObject(o).toString());
          appendWithQuotes(sb, s);
          break;
        }
        case CHAR: {
          //this should use HiveChar.getPaddedValue() but it's protected; currently (v0.13)
          // HiveChar.toString() returns getPaddedValue()
          String s = SerDeUtils.escapeString(
              ((HiveCharObjectInspector) poi).getPrimitiveJavaObject(o).toString());
          appendWithQuotes(sb, s);
          break;
        }
        default:
          throw new RuntimeException("Unknown primitive type: " + poi.getPrimitiveCategory());
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
          if((!keyString.isEmpty()) && (keyString.charAt(0) != SerDeUtils.QUOTE)) {
            appendWithQuotes(sb, keyString);
          }
          else {
            sb.append(keyString);
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
          appendWithQuotes(sb, structFields.get(i).getFieldName());
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
