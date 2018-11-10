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
package org.apache.hadoop.hive.serde2;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.json.HiveJsonStructReader;
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
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hive.common.util.HiveStringUtils;
import org.apache.hive.common.util.TimestampParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SerDeSpec(schemaProps = {serdeConstants.LIST_COLUMNS,
    serdeConstants.LIST_COLUMN_TYPES,
    serdeConstants.TIMESTAMP_FORMATS })

public class JsonSerDe extends AbstractSerDe {

  private static final Logger LOG = LoggerFactory.getLogger(JsonSerDe.class);
  private List<String> columnNames;

  private HiveJsonStructReader structReader;
  private StructTypeInfo rowTypeInfo;

  @Override
  public void initialize(Configuration conf, Properties tbl)
    throws SerDeException {
    List<TypeInfo> columnTypes;
    LOG.debug("Initializing JsonSerDe: {}", tbl.entrySet());

    // Get column names
    String columnNameProperty = tbl.getProperty(serdeConstants.LIST_COLUMNS);
    final String columnNameDelimiter = tbl.containsKey(serdeConstants.COLUMN_NAME_DELIMITER) ? tbl
        .getProperty(serdeConstants.COLUMN_NAME_DELIMITER)
      : String.valueOf(SerDeUtils.COMMA);
    // all table column names
    if (columnNameProperty.isEmpty()) {
      columnNames = Collections.emptyList();
    } else {
      columnNames = Arrays.asList(columnNameProperty.split(columnNameDelimiter));
    }

    // all column types
    String columnTypeProperty = tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES);
    if (columnTypeProperty.isEmpty()) {
      columnTypes = Collections.emptyList();
    } else {
      columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
    }

    LOG.debug("columns: {}, {}", columnNameProperty, columnNames);
    LOG.debug("types: {}, {} ", columnTypeProperty, columnTypes);

    assert (columnNames.size() == columnTypes.size());

    rowTypeInfo = (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(columnNames, columnTypes);

    TimestampParser tsParser = new TimestampParser(
        HiveStringUtils.splitAndUnEscape(tbl.getProperty(serdeConstants.TIMESTAMP_FORMATS)));
    structReader = new HiveJsonStructReader(rowTypeInfo, tsParser);
    structReader.setIgnoreUnknownFields(true);
    structReader.enableHiveColIndexParsing(true);
    structReader.setWritablesUsage(true);
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

    Object row;
    Text t = (Text) blob;
    try {
      row = structReader.parseStruct(new ByteArrayInputStream((t.getBytes()), 0, t.getLength()));
      return row;
    } catch (Exception e) {
      LOG.warn("Error [{}] parsing json text [{}].", e, t);
      throw new SerDeException(e);
    }
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
          Date d = ((DateObjectInspector) poi).getPrimitiveJavaObject(o);
          appendWithQuotes(sb, d.toString());
          break;
        case TIMESTAMP: {
          Timestamp t = ((TimestampObjectInspector) poi).getPrimitiveJavaObject(o);
          appendWithQuotes(sb, t.toString());
          break;
        }
        case DECIMAL:
          sb.append(((HiveDecimalObjectInspector) poi).getPrimitiveJavaObject(o));
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
          if ((!keyString.isEmpty()) && (keyString.charAt(0) != SerDeUtils.QUOTE)) {
            appendWithQuotes(sb, keyString);
          } else {
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
    return structReader.getObjectInspector();
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

  public StructTypeInfo getTypeInfo() {
    return rowTypeInfo;
  }

  public void setWriteablesUsage(boolean b) {
    structReader.setWritablesUsage(b);
  }

}
