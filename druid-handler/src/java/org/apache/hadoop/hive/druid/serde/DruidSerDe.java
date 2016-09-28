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
package org.apache.hadoop.hive.druid.serde;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.calcite.adapter.druid.DruidTable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.druid.DruidStorageHandlerUtils;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeSpec;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Lists;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.HttpClientConfig;
import com.metamx.http.client.HttpClientInit;

import io.druid.query.Druids;
import io.druid.query.Druids.SegmentMetadataQueryBuilder;
import io.druid.query.Query;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.metadata.metadata.ColumnAnalysis;
import io.druid.query.metadata.metadata.SegmentAnalysis;
import io.druid.query.metadata.metadata.SegmentMetadataQuery;
import io.druid.query.select.SelectQuery;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.topn.TopNQuery;

/**
 * DruidSerDe that is used to  deserialize objects from a Druid data source.
 */
@SerDeSpec(schemaProps = {Constants.DRUID_DATA_SOURCE})
public class DruidSerDe extends AbstractSerDe {

  protected static final Logger LOG = LoggerFactory.getLogger(DruidSerDe.class);

  private String[] columns;
  private PrimitiveTypeInfo[] types;
  private ObjectInspector inspector;


  @Override
  public void initialize(Configuration configuration, Properties properties) throws SerDeException {
    final List<String> columnNames = new ArrayList<>();
    final List<PrimitiveTypeInfo> columnTypes = new ArrayList<>();
    List<ObjectInspector> inspectors = new ArrayList<>();

    // Druid query
    String druidQuery = properties.getProperty(Constants.DRUID_QUERY_JSON);
    if (druidQuery == null) {
      // No query. We need to create a Druid Segment Metadata query that retrieves all
      // columns present in the data source (dimensions and metrics).
      // Create Segment Metadata Query
      String dataSource = properties.getProperty(Constants.DRUID_DATA_SOURCE);
      if (dataSource == null) {
        throw new SerDeException("Druid data source not specified; use " +
                Constants.DRUID_DATA_SOURCE + " in table properties");
      }
      SegmentMetadataQueryBuilder builder = new Druids.SegmentMetadataQueryBuilder();
      builder.dataSource(dataSource);
      builder.merge(true);
      builder.analysisTypes();
      SegmentMetadataQuery query = builder.build();

      // Execute query in Druid
      String address = HiveConf.getVar(configuration,
              HiveConf.ConfVars.HIVE_DRUID_BROKER_DEFAULT_ADDRESS);
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
      for (Entry<String,ColumnAnalysis> columnInfo : schemaInfo.getColumns().entrySet()) {
        if (columnInfo.getKey().equals(DruidTable.DEFAULT_TIMESTAMP_COLUMN)) {
          // Special handling for timestamp column
          columnNames.add(columnInfo.getKey()); // field name
          PrimitiveTypeInfo type = TypeInfoFactory.timestampTypeInfo; // field type
          columnTypes.add(type);
          inspectors.add(PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(type));
          continue;
        }
        columnNames.add(columnInfo.getKey()); // field name
        PrimitiveTypeInfo type = DruidSerDeUtils.convertDruidToHiveType(
                columnInfo.getValue().getType()); // field type
        columnTypes.add(type);
        inspectors.add(PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(type));
      }
      columns = columnNames.toArray(new String[columnNames.size()]);
      types = columnTypes.toArray(new PrimitiveTypeInfo[columnTypes.size()]);
      inspector = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, inspectors);
    } else {
      // Query is specified, we can extract the results schema from the query
      Query<?> query;
      try {
        query = DruidStorageHandlerUtils.JSON_MAPPER.readValue(druidQuery, Query.class);
      } catch (Exception e) {
        throw new SerDeException(e);
      }

      switch (query.getType()) {
        case Query.TIMESERIES:
          inferSchema((TimeseriesQuery) query, columnNames, columnTypes);
          break;
        case Query.TOPN:
          inferSchema((TopNQuery) query, columnNames, columnTypes);
          break;
        case Query.SELECT:
          inferSchema((SelectQuery) query, columnNames, columnTypes);
          break;
        case Query.GROUP_BY:
          inferSchema((GroupByQuery) query, columnNames, columnTypes);
          break;
        default:
          throw new SerDeException("Not supported Druid query");
      }
    
      columns = new String[columnNames.size()];
      types = new PrimitiveTypeInfo[columnNames.size()];
      for (int i = 0; i < columnTypes.size(); ++i) {
        columns[i] = columnNames.get(i);
        types[i] = columnTypes.get(i);
        inspectors.add(PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(types[i]));
      }
      inspector = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, inspectors);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("DruidSerDe initialized with\n"
              + "\t columns: " + columnNames
              + "\n\t types: " + columnTypes);
    }
  }

  /* Submits the request and returns */
  protected SegmentAnalysis submitMetadataRequest(String address, SegmentMetadataQuery query)
          throws SerDeException, IOException {
    HttpClient client = HttpClientInit.createClient(HttpClientConfig.builder().build(), new Lifecycle());
    InputStream response;
    try {
      response = DruidStorageHandlerUtils.submitRequest(client,
              DruidStorageHandlerUtils.createRequest(address, query));
    } catch (Exception e) {
      throw new SerDeException(StringUtils.stringifyException(e));
    }

    // Retrieve results
    List<SegmentAnalysis> resultsList;
    try {
      resultsList = DruidStorageHandlerUtils.SMILE_MAPPER.readValue(response,
              new TypeReference<List<SegmentAnalysis>>() {});
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

  /* Timeseries query */
  private void inferSchema(TimeseriesQuery query, List<String> columnNames,
          List<PrimitiveTypeInfo> columnTypes) {
    // Timestamp column
    columnNames.add(DruidTable.DEFAULT_TIMESTAMP_COLUMN);
    columnTypes.add(TypeInfoFactory.timestampTypeInfo);
    // Aggregator columns
    for (AggregatorFactory af : query.getAggregatorSpecs()) {
      columnNames.add(af.getName());
      columnTypes.add(DruidSerDeUtils.convertDruidToHiveType(af.getTypeName()));
    }
    // Post-aggregator columns
    for (PostAggregator pa : query.getPostAggregatorSpecs()) {
      columnNames.add(pa.getName());
      columnTypes.add(TypeInfoFactory.floatTypeInfo);
    }
  }

  /* TopN query */
  private void inferSchema(TopNQuery query, List<String> columnNames, List<PrimitiveTypeInfo> columnTypes) {
    // Timestamp column
    columnNames.add(DruidTable.DEFAULT_TIMESTAMP_COLUMN);
    columnTypes.add(TypeInfoFactory.timestampTypeInfo);
    // Dimension column
    columnNames.add(query.getDimensionSpec().getOutputName());
    columnTypes.add(TypeInfoFactory.stringTypeInfo);
    // Aggregator columns
    for (AggregatorFactory af : query.getAggregatorSpecs()) {
      columnNames.add(af.getName());
      columnTypes.add(DruidSerDeUtils.convertDruidToHiveType(af.getTypeName()));
    }
    // Post-aggregator columns
    for (PostAggregator pa : query.getPostAggregatorSpecs()) {
      columnNames.add(pa.getName());
      columnTypes.add(TypeInfoFactory.floatTypeInfo);
    }
  }

  /* Select query */
  private void inferSchema(SelectQuery query, List<String> columnNames,
          List<PrimitiveTypeInfo> columnTypes) {
    // Timestamp column
    columnNames.add(DruidTable.DEFAULT_TIMESTAMP_COLUMN);
    columnTypes.add(TypeInfoFactory.timestampTypeInfo);
    // Dimension columns
    for (DimensionSpec ds : query.getDimensions()) {
      columnNames.add(ds.getOutputName());
      columnTypes.add(TypeInfoFactory.stringTypeInfo);
    }
    // Metric columns
    for (String metric : query.getMetrics()) {
      columnNames.add(metric);
      columnTypes.add(TypeInfoFactory.floatTypeInfo);
    }
  }

  /* GroupBy query */
  private void inferSchema(GroupByQuery query, List<String> columnNames, List<PrimitiveTypeInfo> columnTypes) {
    // Timestamp column
    columnNames.add(DruidTable.DEFAULT_TIMESTAMP_COLUMN);
    columnTypes.add(TypeInfoFactory.timestampTypeInfo);
    // Dimension columns
    for (DimensionSpec ds : query.getDimensions()) {
      columnNames.add(ds.getOutputName());
      columnTypes.add(TypeInfoFactory.stringTypeInfo);
    }
    // Aggregator columns
    for (AggregatorFactory af : query.getAggregatorSpecs()) {
      columnNames.add(af.getName());
      columnTypes.add(DruidSerDeUtils.convertDruidToHiveType(af.getTypeName()));
    }
    // Post-aggregator columns
    for (PostAggregator pa : query.getPostAggregatorSpecs()) {
      columnNames.add(pa.getName());
      columnTypes.add(TypeInfoFactory.floatTypeInfo);
    }
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return NullWritable.class;
  }

  @Override
  public Writable serialize(Object o, ObjectInspector objectInspector) throws SerDeException {
    return NullWritable.get();
  }

  @Override
  public SerDeStats getSerDeStats() {
    throw new UnsupportedOperationException("SerdeStats not supported.");
  }

  @Override
  public Object deserialize(Writable writable) throws SerDeException {
    DruidWritable input = (DruidWritable) writable;
    List<Object> output = Lists.newArrayListWithExpectedSize(columns.length);
    for (int i = 0; i < columns.length; i++) {
      final Object value = input.getValue().get(columns[i]);
      if (value == null) {
        output.add(null);
        continue;
      }
      switch (types[i].getPrimitiveCategory()) {
        case TIMESTAMP:
          output.add(new TimestampWritable(new Timestamp((Long)value)));
          break;
        case LONG:
          output.add(new LongWritable(((Number)value).longValue()));
          break;
        case FLOAT:
          output.add(new FloatWritable(((Number)value).floatValue()));
          break;
        case STRING:
          output.add(new Text(value.toString()));
          break;
        default:
          throw new SerDeException("Unknown type: " + types[i].getPrimitiveCategory());
      }
    }
    return output;
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return inspector;
  }

}
