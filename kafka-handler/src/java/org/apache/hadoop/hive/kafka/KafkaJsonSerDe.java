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

package org.apache.hadoop.hive.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.common.type.TimestampTZ;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampLocalTZWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TimestampLocalTZTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Basic JsonSerDe to make use of such storage handler smooth and easy and testing basic primitive Json.
 * For production please use Hive native JsonSerde.
 */
public class KafkaJsonSerDe extends AbstractSerDe {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaJsonSerDe.class);
  private static final DateTimeFormatter TS_PARSER = createAutoParser();
  static Function<TypeInfo, ObjectInspector>
      typeInfoToObjectInspector = typeInfo ->
      PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
          TypeInfoFactory.getPrimitiveTypeInfo(typeInfo.getTypeName()));
  private List<String> columnNames;
  private List<TypeInfo> columnTypes;
  private ObjectInspector inspector;
  private final ObjectMapper mapper = new ObjectMapper();
  private long rowCount = 0L;
  private long rawDataSize = 0L;

  @Override public void initialize(@Nullable Configuration conf, Properties tbl) {
    final List<ObjectInspector> inspectors;
    // Get column names and types
    String columnNameProperty = tbl.getProperty(serdeConstants.LIST_COLUMNS);
    String columnTypeProperty = tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES);
    final String
        columnNameDelimiter =
        tbl.containsKey(serdeConstants.COLUMN_NAME_DELIMITER) ?
            tbl.getProperty(serdeConstants.COLUMN_NAME_DELIMITER) :
            String.valueOf(SerDeUtils.COMMA);
    // all table column names
    if (!columnNameProperty.isEmpty()) {
      columnNames = Arrays.asList(columnNameProperty.split(columnNameDelimiter));
    }
    // all column types
    if (!columnTypeProperty.isEmpty()) {
      columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("columns: {}, {}", columnNameProperty, columnNames);
      LOG.debug("types: {}, {} ", columnTypeProperty, columnTypes);
    }

    inspectors = columnTypes.stream().map(typeInfoToObjectInspector).collect(Collectors.toList());
    inspector = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, inspectors);
  }

  @Override public Class<? extends Writable> getSerializedClass() {
    return BytesRefWritable.class;
  }

  @Override public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
    throw new SerDeException("unimplemented");
  }

  @Override public SerDeStats getSerDeStats() {
    SerDeStats serDeStats = new SerDeStats();
    serDeStats.setRawDataSize(rawDataSize);
    serDeStats.setRowCount(rowCount);
    return serDeStats;
  }

  @Override public Object deserialize(Writable blob) throws SerDeException {
    BytesWritable record = (BytesWritable) blob;
    Map<String, JsonNode> payload;
    try {
      payload = parseAsJson(record.getBytes());
      rowCount += 1;
      rawDataSize += record.getLength();
    } catch (IOException e) {
      throw new SerDeException(e);
    }

    final List<Object> output = new ArrayList<>(columnNames.size());

    for (int i = 0; i < columnNames.size(); i++) {
      final String name = columnNames.get(i);
      final TypeInfo typeInfo = columnTypes.get(i);
      final JsonNode value = payload.get(name);
      if (value == null) {
        output.add(null);
      } else {
        switch (columnTypes.get(i).getCategory()) {
        case PRIMITIVE:
          output.add(parseAsPrimitive(value, typeInfo));
          break;
        case MAP:
        case LIST:
        case UNION:
        case STRUCT:
        default:
          throw new SerDeException("not supported yet");
        }
      }

    }
    return output;
  }

  private Object parseAsPrimitive(JsonNode value, TypeInfo typeInfo) throws SerDeException {
    switch (TypeInfoFactory.getPrimitiveTypeInfo(typeInfo.getTypeName()).getPrimitiveCategory()) {
    case TIMESTAMP:
      TimestampWritable timestampWritable = new TimestampWritable();
      timestampWritable.setTime(TS_PARSER.parseMillis(value.textValue()));
      return timestampWritable;

    case TIMESTAMPLOCALTZ:
      final long numberOfMillis = TS_PARSER.parseMillis(value.textValue());
      return new TimestampLocalTZWritable(new TimestampTZ(ZonedDateTime.ofInstant(Instant.ofEpochMilli(numberOfMillis),
          ((TimestampLocalTZTypeInfo) typeInfo).timeZone())));

    case BYTE:
      return new ByteWritable((byte) value.intValue());
    case SHORT:
      return (new ShortWritable(value.shortValue()));
    case INT:
      return new IntWritable(value.intValue());
    case LONG:
      return (new LongWritable((value.longValue())));
    case FLOAT:
      return (new FloatWritable(value.floatValue()));
    case DOUBLE:
      return (new DoubleWritable(value.doubleValue()));
    case DECIMAL:
      return (new HiveDecimalWritable(HiveDecimal.create(value.decimalValue())));
    case CHAR:
      return (new HiveCharWritable(new HiveChar(value.textValue(), ((CharTypeInfo) typeInfo).getLength())));
    case VARCHAR:
      return (new HiveVarcharWritable(new HiveVarchar(value.textValue(), ((CharTypeInfo) typeInfo).getLength())));
    case STRING:
      return (new Text(value.textValue()));
    case BOOLEAN:
      return (new BooleanWritable(value.isBoolean() ? value.booleanValue() : Boolean.valueOf(value.textValue())));
    default:
      throw new SerDeException("Unknown type: " + typeInfo.getTypeName());
    }
  }

  private Map<String, JsonNode> parseAsJson(byte[] value) throws IOException {
    JsonNode document = mapper.readValue(value, JsonNode.class);
    //Hive Column names are case insensitive.
    Map<String, JsonNode> documentMap = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
    document.fields().forEachRemaining(field -> documentMap.put(field.getKey().toLowerCase(), field.getValue()));
    return documentMap;
  }

  @Override public ObjectInspector getObjectInspector() throws SerDeException {
    if (inspector == null) {
      throw new SerDeException("null inspector ??");
    }
    return inspector;
  }

  private static DateTimeFormatter createAutoParser() {
    final DateTimeFormatter
        offsetElement =
        new DateTimeFormatterBuilder().appendTimeZoneOffset("Z", true, 2, 4).toFormatter();

    DateTimeParser
        timeOrOffset =
        new DateTimeFormatterBuilder().append(null,
            new DateTimeParser[] {
                new DateTimeFormatterBuilder().appendLiteral('T').toParser(),
                new DateTimeFormatterBuilder().appendLiteral(' ').toParser() })
            .appendOptional(ISODateTimeFormat.timeElementParser().getParser())
            .appendOptional(offsetElement.getParser())
            .toParser();

    return new DateTimeFormatterBuilder().append(ISODateTimeFormat.dateElementParser())
        .appendOptional(timeOrOffset)
        .toFormatter();
  }
}
