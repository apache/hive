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
package org.apache.hadoop.hive.druid.serde;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.druid.query.Druids;
import org.apache.druid.query.Druids.SegmentMetadataQueryBuilder;
import org.apache.druid.query.metadata.metadata.ColumnAnalysis;
import org.apache.druid.query.metadata.metadata.SegmentAnalysis;
import org.apache.druid.query.metadata.metadata.SegmentMetadataQuery;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.common.type.TimestampTZ;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.druid.DruidStorageHandler;
import org.apache.hadoop.hive.druid.DruidStorageHandlerUtils;
import org.apache.hadoop.hive.druid.conf.DruidConstants;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeSpec;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampLocalTZObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TimestampLocalTZTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.joda.time.format.ISODateTimeFormat.dateOptionalTimeParser;

/**
 * DruidSerDe that is used to  deserialize objects from a Druid data source.
 */
@SerDeSpec(schemaProps = { Constants.DRUID_DATA_SOURCE })
public class DruidSerDe extends AbstractSerDe {

  private String[] columns;
  private PrimitiveTypeInfo[] types;
  private ObjectInspector inspector;
  private TimestampLocalTZTypeInfo tsTZTypeInfo;

  @Override
  public void initialize(Configuration configuration, Properties tableProperties, Properties partitionProperties)
      throws SerDeException {
    super.initialize(configuration, tableProperties, partitionProperties);

    tsTZTypeInfo = new TimestampLocalTZTypeInfo(configuration.get(HiveConf.ConfVars.HIVE_LOCAL_TIME_ZONE.varname));
    // Druid query
    final String druidQuery = properties.getProperty(Constants.DRUID_QUERY_JSON, null);
    if (druidQuery != null && !druidQuery.isEmpty()) {
      initFromDruidQueryPlan(properties, druidQuery);
    } else {
      // No query. Either it is a CTAS, or we need to create a Druid meta data Query
      if (!org.apache.commons.lang3.StringUtils.isEmpty(properties.getProperty(serdeConstants.LIST_COLUMNS))
          && !org.apache.commons.lang3.StringUtils.isEmpty(properties.getProperty(serdeConstants.LIST_COLUMN_TYPES))) {
        // CASE CTAS statement
        initFromProperties(properties);
      } else {
        // Segment Metadata query that retrieves all columns present in
        // the data source (dimensions and metrics).
        initFromMetaDataQuery(configuration, properties);
      }
    }
    if (log.isDebugEnabled()) {
      log.debug("DruidSerDe initialized with\n"
          + "\t columns: "
          + Arrays.toString(columns)
          + "\n\t types: "
          + Arrays.toString(types));
    }
  }

  private void initFromMetaDataQuery(final Configuration configuration, final Properties properties)
      throws SerDeException {
    final List<String> columnNames = new ArrayList<>();
    final List<PrimitiveTypeInfo> columnTypes = new ArrayList<>();
    final List<ObjectInspector> inspectors = new ArrayList<>();

    String dataSource = properties.getProperty(Constants.DRUID_DATA_SOURCE);
    if (dataSource == null) {
      throw new SerDeException("Druid data source not specified; use "
          + Constants.DRUID_DATA_SOURCE
          + " in table properties");
    }
    SegmentMetadataQueryBuilder builder = new Druids.SegmentMetadataQueryBuilder();
    builder.dataSource(dataSource);
    builder.merge(true);
    builder.analysisTypes();
    SegmentMetadataQuery query = builder.build();

    // Execute query in Druid
    String address = HiveConf.getVar(configuration, HiveConf.ConfVars.HIVE_DRUID_BROKER_DEFAULT_ADDRESS);
    if (org.apache.commons.lang3.StringUtils.isEmpty(address)) {
      throw new SerDeException("Druid broker address not specified in configuration");
    }
    // Infer schema
    SegmentAnalysis schemaInfo;
    try {
      schemaInfo = submitMetadataRequest(address, query);
    } catch (IOException e) {
      throw new SerDeException(e);
    }
    for (Entry<String, ColumnAnalysis> columnInfo : schemaInfo.getColumns().entrySet()) {
      if (columnInfo.getKey().equals(DruidConstants.DEFAULT_TIMESTAMP_COLUMN)) {
        // Special handling for timestamp column
        columnNames.add(columnInfo.getKey()); // field name
        PrimitiveTypeInfo type = tsTZTypeInfo; // field type
        columnTypes.add(type);
        inspectors.add(PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(type));
        continue;
      }
      columnNames.add(columnInfo.getKey()); // field name
      PrimitiveTypeInfo type = DruidSerDeUtils.convertDruidToHiveType(columnInfo.getValue().getType()); // field type
      columnTypes.add(type instanceof TimestampLocalTZTypeInfo ? tsTZTypeInfo : type);
      inspectors.add(PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(type));
    }
    columns = columnNames.toArray(new String[0]);
    types = columnTypes.toArray(new PrimitiveTypeInfo[0]);
    inspector = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, inspectors);
  }

  private void initFromProperties(final Properties properties) throws SerDeException {

    final List<String> columnNames = new ArrayList<>(Utilities.getColumnNames(properties));
    if (!columnNames.contains(DruidConstants.DEFAULT_TIMESTAMP_COLUMN)) {
      throw new SerDeException("Timestamp column (' "
          + DruidConstants.DEFAULT_TIMESTAMP_COLUMN
          + "') not specified in create table; list of columns is : "
          + properties.getProperty(serdeConstants.LIST_COLUMNS));
    }
    final List<PrimitiveTypeInfo>
        columnTypes =
        Utilities.getColumnTypes(properties)
            .stream()
            .map(TypeInfoFactory::getPrimitiveTypeInfo)
            .collect(Collectors.toList())
            .stream()
            .map(e -> e instanceof TimestampLocalTZTypeInfo ? tsTZTypeInfo : e)
            .collect(Collectors.toList());
    final List<ObjectInspector>
        inspectors =
        columnTypes.stream()
            .map(PrimitiveObjectInspectorFactory::getPrimitiveJavaObjectInspector)
            .collect(Collectors.toList());
    columns = columnNames.toArray(new String[0]);
    types = columnTypes.toArray(new PrimitiveTypeInfo[0]);
    inspector = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, inspectors);
  }

  private void initFromDruidQueryPlan(Properties properties, String druidQuery) {
    Preconditions.checkNotNull(druidQuery, "Why Druid query is null");
    final List<ObjectInspector> inspectors = new ArrayList<>();
    final List<String> columnNames;
    final List<PrimitiveTypeInfo> columnTypes;
    final String
        fieldNamesProperty =
        Preconditions.checkNotNull(properties.getProperty(Constants.DRUID_QUERY_FIELD_NAMES, null));
    final String
        fieldTypesProperty =
        Preconditions.checkNotNull(properties.getProperty(Constants.DRUID_QUERY_FIELD_TYPES, null));
    if (fieldNamesProperty.isEmpty()) {
      // this might seem counter intuitive but some queries like query
      // SELECT YEAR(Calcs.date0) AS yr_date0_ok FROM druid_tableau.calcs Calcs WHERE (YEAR(Calcs.date0) IS NULL)
      // LIMIT 1
      // is planed in a way where we only push a filter down and keep the project of null as hive project. Thus empty
      // columns
      columnNames = Collections.emptyList();
      columnTypes = Collections.emptyList();
    } else {
      columnNames = Arrays.stream(fieldNamesProperty.trim().split(",")).collect(Collectors.toList());
      columnTypes =
          TypeInfoUtils.getTypeInfosFromTypeString(fieldTypesProperty)
              .stream()
              .map(e -> TypeInfoFactory.getPrimitiveTypeInfo(e.getTypeName()))
              .map(primitiveTypeInfo -> {
                if (primitiveTypeInfo instanceof TimestampLocalTZTypeInfo) {
                  return tsTZTypeInfo;
                }
                return primitiveTypeInfo;
              })
              .collect(Collectors.toList());
    }
    columns = new String[columnNames.size()];
    types = new PrimitiveTypeInfo[columnNames.size()];
    for (int i = 0; i < columnTypes.size(); ++i) {
      columns[i] = columnNames.get(i);
      types[i] = columnTypes.get(i);
      inspectors.add(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(types[i]));
    }
    inspector = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, inspectors);
  }

  /* Submits the request and returns */
  protected SegmentAnalysis submitMetadataRequest(String address, SegmentMetadataQuery query)
      throws SerDeException, IOException {
    InputStream response;
    try {
      response =
          DruidStorageHandlerUtils.submitRequest(DruidStorageHandler.getHttpClient(),
              DruidStorageHandlerUtils.createSmileRequest(address, query));
    } catch (Exception e) {
      throw new SerDeException(StringUtils.stringifyException(e));
    }

    // Retrieve results
    List<SegmentAnalysis> resultsList;
    try {
      // This will throw an exception in case of the response from druid is not an array
      // this case occurs if for instance druid query execution returns an exception instead of array of results.
      resultsList =
          DruidStorageHandlerUtils.SMILE_MAPPER.readValue(response, new TypeReference<List<SegmentAnalysis>>() {
          });
    } catch (Exception e) {
      response.close();
      throw new SerDeException(StringUtils.stringifyException(e));
    }
    if (resultsList == null || resultsList.isEmpty()) {
      throw new SerDeException("Connected to Druid but could not retrieve datasource information");
    }
    if (resultsList.size() != 1) {
      throw new SerDeException("Information about segments should have been merged");
    }

    return resultsList.get(0);
  }

  @Override public Class<? extends Writable> getSerializedClass() {
    return DruidWritable.class;
  }

  @Override public Writable serialize(Object o, ObjectInspector objectInspector) throws SerDeException {
    if (objectInspector.getCategory() != ObjectInspector.Category.STRUCT) {
      throw new SerDeException(getClass().toString()
          + " can only serialize struct types, but we got: "
          + objectInspector.getTypeName());
    }

    // Prepare the field ObjectInspectors
    StructObjectInspector soi = (StructObjectInspector) objectInspector;
    List<? extends StructField> fields = soi.getAllStructFieldRefs();
    List<Object> values = soi.getStructFieldsDataAsList(o);
    // We deserialize the result
    final Map<String, Object> value = new HashMap<>();
    for (int i = 0; i < columns.length; i++) {
      if (values.get(i) == null) {
        // null, we just add it
        value.put(columns[i], null);
        continue;
      }
      final Object res;
      switch (types[i].getPrimitiveCategory()) {
      case TIMESTAMP:
        res =
            ((TimestampObjectInspector) fields.get(i).getFieldObjectInspector()).getPrimitiveJavaObject(values.get(i))
                .toEpochMilli();
        break;
      case TIMESTAMPLOCALTZ:
        res =
            ((TimestampLocalTZObjectInspector) fields.get(i)
                .getFieldObjectInspector()).getPrimitiveJavaObject(values.get(i))
                .getZonedDateTime()
                .toInstant()
                .toEpochMilli();
        break;
      case BYTE:
        res = ((ByteObjectInspector) fields.get(i).getFieldObjectInspector()).get(values.get(i));
        break;
      case SHORT:
        res = ((ShortObjectInspector) fields.get(i).getFieldObjectInspector()).get(values.get(i));
        break;
      case INT:
        res = ((IntObjectInspector) fields.get(i).getFieldObjectInspector()).get(values.get(i));
        break;
      case LONG:
        res = ((LongObjectInspector) fields.get(i).getFieldObjectInspector()).get(values.get(i));
        break;
      case FLOAT:
        res = ((FloatObjectInspector) fields.get(i).getFieldObjectInspector()).get(values.get(i));
        break;
      case DOUBLE:
        res = ((DoubleObjectInspector) fields.get(i).getFieldObjectInspector()).get(values.get(i));
        break;
      case CHAR:
        res =
            ((HiveCharObjectInspector) fields.get(i).getFieldObjectInspector()).getPrimitiveJavaObject(values.get(i))
                .getValue();
        break;
      case VARCHAR:
        res =
            ((HiveVarcharObjectInspector) fields.get(i).getFieldObjectInspector()).getPrimitiveJavaObject(values.get(i))
                .getValue();
        break;
      case STRING:
        res = ((StringObjectInspector) fields.get(i).getFieldObjectInspector()).getPrimitiveJavaObject(values.get(i));
        break;
      case BOOLEAN:
        res = ((BooleanObjectInspector) fields.get(i).getFieldObjectInspector()).get(values.get(i)) ? 1L : 0L;
        break;
      default:
        throw new SerDeException("Unsupported type: " + types[i].getPrimitiveCategory());
      }
      value.put(columns[i], res);
    }
    //Extract the partitions keys segments granularity and partition key if any
    // First Segment Granularity has to be here.
    final int granularityFieldIndex = columns.length;
    assert values.size() > granularityFieldIndex;
    Preconditions.checkArgument(fields.get(granularityFieldIndex)
        .getFieldName()
        .equals(Constants.DRUID_TIMESTAMP_GRANULARITY_COL_NAME));

    Timestamp
        timestamp =
        ((TimestampObjectInspector) fields.get(granularityFieldIndex).getFieldObjectInspector()).getPrimitiveJavaObject(
            values.get(granularityFieldIndex));
    Preconditions.checkNotNull(timestamp, "Timestamp column cannot have null value");
    value.put(Constants.DRUID_TIMESTAMP_GRANULARITY_COL_NAME, timestamp.toEpochMilli());

    if (values.size() == columns.length + 2) {
      // Then partition number if any.
      final int partitionNumPos = granularityFieldIndex + 1;
      Preconditions.checkArgument(fields.get(partitionNumPos).getFieldName().equals(Constants.DRUID_SHARD_KEY_COL_NAME),
          String.format("expecting to encounter %s but was %s",
              Constants.DRUID_SHARD_KEY_COL_NAME,
              fields.get(partitionNumPos).getFieldName()));
      value.put(Constants.DRUID_SHARD_KEY_COL_NAME,
          ((LongObjectInspector) fields.get(partitionNumPos)
              .getFieldObjectInspector()).get(values.get(partitionNumPos)));
    }

    return new DruidWritable(value);
  }

  /**
   * @param writable Druid Writable to be deserialized.
   * @return List of Hive Writables.
   * @throws SerDeException if there is Serde issues.
   */
  @Override public Object deserialize(Writable writable) throws SerDeException {
    final DruidWritable input = (DruidWritable) writable;
    final List<Object> output = Lists.newArrayListWithExpectedSize(columns.length);
    for (int i = 0; i < columns.length; i++) {
      final Object value = input.isCompacted() ? input.getCompactedValue().get(i) : input.getValue().get(columns[i]);
      if (value == null) {
        output.add(null);
      } else {
        output.add(convertAsPrimitive(value, types[i]));
      }
    }
    return output;
  }

  /**
   * Function to convert Druid Primitive values to Hive Primitives. Main usage of this is to pipe data to VectorRow.
   * This has the exact same logic as {@link DruidSerDe#deserialize(Writable)}, any modification here should be done
   * there as well.
   * Reason to have 2 function is that no vectorized path expects writables.
   *
   * @param writable Druid Writable.
   * @param rowBoat   Rowboat used to carry columns values.
   * @throws SerDeException in case of deserialization errors.
   */
  public void deserializeAsPrimitive(Writable writable, final Object[] rowBoat) throws SerDeException {
    final DruidWritable input = (DruidWritable) writable;
    for (int i = 0; i < columns.length; i++) {
      final Object value = input.isCompacted() ? input.getCompactedValue().get(i) : input.getValue().get(columns[i]);
      rowBoat[i] = null;
      if (value != null) {
        rowBoat[i] = convertAsPrimitive(value, types[i]);
      }
    }
  }

  private static Object convertAsPrimitive(Object value, PrimitiveTypeInfo typeInfo) throws SerDeException {
    switch (typeInfo.getPrimitiveCategory()) {
    case TIMESTAMP:
      return Timestamp.ofEpochMilli(deserializeToMillis(value));
    case TIMESTAMPLOCALTZ:
      return new TimestampTZ(ZonedDateTime.ofInstant(Instant.ofEpochMilli(deserializeToMillis(value)),
          ((TimestampLocalTZTypeInfo) typeInfo).timeZone()));
    case DATE:
      return (Date.ofEpochMilli(deserializeToMillis(value)));
    case BYTE:
      return ((Number) value).byteValue();
    case SHORT:
      return ((Number) value).shortValue();
    case INT:
      if (value instanceof Number) {
        return ((Number) value).intValue();
      } else {
        // This is a corner case where we have an extract of time unit like day/month pushed as Extraction Fn
        //@TODO The best way to fix this is to add explicit output Druid types to Calcite Extraction Functions impls
        return (Integer.valueOf((String) value));
      }
    case LONG:
      return ((Number) value).longValue();
    case FLOAT:
      return ((Number) value).floatValue();
    case DOUBLE:
      return ((Number) value).doubleValue();
    case CHAR:
      return new HiveChar(value.toString(), ((CharTypeInfo) typeInfo).getLength());
    case VARCHAR:
      return new HiveVarchar(value.toString(), ((VarcharTypeInfo) typeInfo).getLength());
    case STRING:
      return value.toString();
    case BOOLEAN:
      if (value instanceof Number) {
        return (((Number) value).intValue() != 0);
      } else {
        return (Boolean.valueOf(value.toString()));
      }
    default:
      throw new SerDeException("Unknown type: " + typeInfo.getPrimitiveCategory());
    }
  }

  private static long deserializeToMillis(Object value) {
    long numberOfMillis;
    if (value instanceof Number) {
      numberOfMillis = ((Number) value).longValue();
    } else {
      // it is an extraction fn need to be parsed
      try {
        numberOfMillis = dateOptionalTimeParser().parseDateTime((String) value).getMillis();
      } catch (IllegalArgumentException e) {
        // we may not be able to parse the date if it already comes in Hive format,
        // we retry and otherwise fail
        numberOfMillis = Timestamp.valueOf((String) value).toEpochMilli();
      }
    }
    return numberOfMillis;
  }

  @Override public ObjectInspector getObjectInspector() {
    return inspector;
  }

  @Override public boolean shouldStoreFieldsInMetastore(Map<String, String> tableParams) {
    return true;
  }
}
