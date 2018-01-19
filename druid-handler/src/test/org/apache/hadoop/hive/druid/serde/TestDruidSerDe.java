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

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.util.concurrent.SettableFuture;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.response.HttpResponseHandler;
import io.druid.data.input.Row;
import io.druid.query.Result;
import io.druid.query.select.SelectResultValue;
import io.druid.query.timeseries.TimeseriesResultValue;
import io.druid.query.topn.TopNResultValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.common.type.TimestampTZ;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.druid.DruidStorageHandlerUtils;
import org.apache.hadoop.hive.druid.QTestDruidSerDe;
import org.apache.hadoop.hive.druid.io.DruidQueryBasedInputFormat;
import org.apache.hadoop.hive.druid.io.HiveDruidSplit;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
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
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import io.druid.query.Query;

/**
 * Basic tests for Druid SerDe. The examples are taken from Druid 0.9.1.1
 * documentation.
 */
public class TestDruidSerDe {
  // Timeseries query
  private static final String TIMESERIES_QUERY =
          "{  \"queryType\": \"timeseries\", "
                  + " \"dataSource\": \"sample_datasource\", "
                  + " \"granularity\": \"day\", "
                  + " \"descending\": \"true\", "
                  + " \"filter\": {  "
                  + "  \"type\": \"and\",  "
                  + "  \"fields\": [   "
                  + "   { \"type\": \"selector\", \"dimension\": \"sample_dimension1\", \"value\": \"sample_value1\" },   "
                  + "   { \"type\": \"or\",    "
                  + "    \"fields\": [     "
                  + "     { \"type\": \"selector\", \"dimension\": \"sample_dimension2\", \"value\": \"sample_value2\" },     "
                  + "     { \"type\": \"selector\", \"dimension\": \"sample_dimension3\", \"value\": \"sample_value3\" }    "
                  + "    ]   "
                  + "   }  "
                  + "  ] "
                  + " }, "
                  + " \"aggregations\": [  "
                  + "  { \"type\": \"longSum\", \"name\": \"sample_name1\", \"fieldName\": \"sample_fieldName1\" },  "
                  + "  { \"type\": \"doubleSum\", \"name\": \"sample_name2\", \"fieldName\": \"sample_fieldName2\" } "
                  + " ], "
                  + " \"postAggregations\": [  "
                  + "  { \"type\": \"arithmetic\",  "
                  + "    \"name\": \"sample_divide\",  "
                  + "    \"fn\": \"/\",  "
                  + "    \"fields\": [   "
                  + "     { \"type\": \"fieldAccess\", \"name\": \"postAgg__sample_name1\", \"fieldName\": \"sample_name1\" },   "
                  + "     { \"type\": \"fieldAccess\", \"name\": \"postAgg__sample_name2\", \"fieldName\": \"sample_name2\" }  "
                  + "    ]  "
                  + "  } "
                  + " ], "
                  + " \"intervals\": [ \"2012-01-01T00:00:00.000/2012-01-03T00:00:00.000\" ]}";

  // Timeseries query results
  private static final String TIMESERIES_QUERY_RESULTS =
          "[  "
                  + "{   "
                  + " \"timestamp\": \"2012-01-01T00:00:00.000Z\",   "
                  + " \"result\": { \"sample_name1\": 0, \"sample_name2\": 1.0, \"sample_divide\": 2.2222 }   "
                  + "},  "
                  + "{   "
                  + " \"timestamp\": \"2012-01-02T00:00:00.000Z\",   "
                  + " \"result\": { \"sample_name1\": 2, \"sample_name2\": 3.32, \"sample_divide\": 4 }  "
                  + "}]";

  private byte[] tsQueryResults;
  private byte[] topNQueryResults;
  private byte[] groupByQueryResults;
  private byte[] groupByTimeExtractQueryResults;
  private byte[] selectQueryResults;
  private byte[] groupByMonthExtractQueryResults;


  // Timeseries query results as records
  private static final Object[][] TIMESERIES_QUERY_RESULTS_RECORDS = new Object[][] {
          new Object[] { new TimestampLocalTZWritable(new TimestampTZ(Instant.ofEpochMilli(1325376000000L).atZone(ZoneOffset.UTC))), new LongWritable(0),
                  new FloatWritable(1.0F), new FloatWritable(2.2222F) },
          new Object[] { new TimestampLocalTZWritable(new TimestampTZ(Instant.ofEpochMilli(1325462400000L).atZone(ZoneOffset.UTC))), new LongWritable(2),
                  new FloatWritable(3.32F), new FloatWritable(4F) }
  };

  // Timeseries query results as records (types defined by metastore)
  private static final String TIMESERIES_COLUMN_NAMES = "__time,sample_name1,sample_name2,sample_divide";
  private static final String TIMESERIES_COLUMN_TYPES = "timestamp with local time zone,smallint,double,float";
  private static final Object[][] TIMESERIES_QUERY_RESULTS_RECORDS_2 = new Object[][] {
    new Object[] { new TimestampLocalTZWritable(new TimestampTZ(Instant.ofEpochMilli(1325376000000L).atZone(ZoneOffset.UTC))), new ShortWritable((short) 0),
        new DoubleWritable(1.0d), new FloatWritable(2.2222F) },
    new Object[] { new TimestampLocalTZWritable(new TimestampTZ(Instant.ofEpochMilli(1325462400000L).atZone(ZoneOffset.UTC))), new ShortWritable((short) 2),
        new DoubleWritable(3.32d), new FloatWritable(4F) }
  };

  // TopN query
  private static final String TOPN_QUERY =
          "{  \"queryType\": \"topN\", "
                  + " \"dataSource\": \"sample_data\", "
                  + " \"dimension\": \"sample_dim\", "
                  + " \"threshold\": 5, "
                  + " \"metric\": \"count\", "
                  + " \"granularity\": \"all\", "
                  + " \"filter\": {  "
                  + "  \"type\": \"and\",  "
                  + "  \"fields\": [   "
                  + "   {    "
                  + "    \"type\": \"selector\",    "
                  + "    \"dimension\": \"dim1\",    "
                  + "    \"value\": \"some_value\"   "
                  + "   },   "
                  + "   {    "
                  + "    \"type\": \"selector\",    "
                  + "    \"dimension\": \"dim2\",    "
                  + "    \"value\": \"some_other_val\"   "
                  + "   }  "
                  + "  ] "
                  + " }, "
                  + " \"aggregations\": [  "
                  + "  {   "
                  + "   \"type\": \"longSum\",   "
                  + "   \"name\": \"count\",   "
                  + "   \"fieldName\": \"count\"  "
                  + "  },  "
                  + "  {   "
                  + "   \"type\": \"doubleSum\",   "
                  + "   \"name\": \"some_metric\",   "
                  + "   \"fieldName\": \"some_metric\"  "
                  + "  } "
                  + " ], "
                  + " \"postAggregations\": [  "
                  + "  {   "
                  + "   \"type\": \"arithmetic\",   "
                  + "   \"name\": \"sample_divide\",   "
                  + "   \"fn\": \"/\",   "
                  + "   \"fields\": [    "
                  + "    {     "
                  + "     \"type\": \"fieldAccess\",     "
                  + "     \"name\": \"some_metric\",     "
                  + "     \"fieldName\": \"some_metric\"    "
                  + "    },    "
                  + "    {     "
                  + "     \"type\": \"fieldAccess\",     "
                  + "     \"name\": \"count\",     "
                  + "     \"fieldName\": \"count\"    "
                  + "    }   "
                  + "   ]  "
                  + "  } "
                  + " ], "
                  + " \"intervals\": [  "
                  + "  \"2013-08-31T00:00:00.000/2013-09-03T00:00:00.000\" "
                  + " ]}";

  // TopN query results
  private static final String TOPN_QUERY_RESULTS =
          "[ "
                  + " {  "
                  + "  \"timestamp\": \"2013-08-31T00:00:00.000Z\",  "
                  + "  \"result\": [   "
                  + "   {   "
                  + "     \"sample_dim\": \"dim1_val\",   "
                  + "     \"count\": 111,   "
                  + "     \"some_metric\": 10669,   "
                  + "     \"sample_divide\": 96.11711711711712   "
                  + "   },   "
                  + "   {   "
                  + "     \"sample_dim\": \"another_dim1_val\",   "
                  + "     \"count\": 88,   "
                  + "     \"some_metric\": 28344,   "
                  + "     \"sample_divide\": 322.09090909090907   "
                  + "   },   "
                  + "   {   "
                  + "     \"sample_dim\": \"dim1_val3\",   "
                  + "     \"count\": 70,   "
                  + "     \"some_metric\": 871,   "
                  + "     \"sample_divide\": 12.442857142857143   "
                  + "   },   "
                  + "   {   "
                  + "     \"sample_dim\": \"dim1_val4\",   "
                  + "     \"count\": 62,   "
                  + "     \"some_metric\": 815,   "
                  + "     \"sample_divide\": 13.14516129032258   "
                  + "   },   "
                  + "   {   "
                  + "     \"sample_dim\": \"dim1_val5\",   "
                  + "     \"count\": 60,   "
                  + "     \"some_metric\": 2787,   "
                  + "     \"sample_divide\": 46.45   "
                  + "   }  "
                  + "  ] "
                  + " }]";

  // TopN query results as records
  private static final Object[][] TOPN_QUERY_RESULTS_RECORDS = new Object[][] {
          new Object[] { new TimestampLocalTZWritable(new TimestampTZ(Instant.ofEpochMilli(1377907200000L).atZone(ZoneOffset.UTC))),
                  new Text("dim1_val"), new LongWritable(111), new FloatWritable(10669F),
                  new FloatWritable(96.11711711711712F) },
          new Object[] { new TimestampLocalTZWritable(new TimestampTZ(Instant.ofEpochMilli(1377907200000L).atZone(ZoneOffset.UTC))),
                  new Text("another_dim1_val"), new LongWritable(88), new FloatWritable(28344F),
                  new FloatWritable(322.09090909090907F) },
          new Object[] { new TimestampLocalTZWritable(new TimestampTZ(Instant.ofEpochMilli(1377907200000L).atZone(ZoneOffset.UTC))),
                  new Text("dim1_val3"), new LongWritable(70), new FloatWritable(871F),
                  new FloatWritable(12.442857142857143F) },
          new Object[] { new TimestampLocalTZWritable(new TimestampTZ(Instant.ofEpochMilli(1377907200000L).atZone(ZoneOffset.UTC))),
                  new Text("dim1_val4"), new LongWritable(62), new FloatWritable(815F),
                  new FloatWritable(13.14516129032258F) },
          new Object[] { new TimestampLocalTZWritable(new TimestampTZ(Instant.ofEpochMilli(1377907200000L).atZone(ZoneOffset.UTC))),
                  new Text("dim1_val5"), new LongWritable(60), new FloatWritable(2787F),
                  new FloatWritable(46.45F) }
  };

  // TopN query results as records (types defined by metastore)
  private static final String TOPN_COLUMN_NAMES = "__time,sample_dim,count,some_metric,sample_divide";
  private static final String TOPN_COLUMN_TYPES = "timestamp with local time zone,string,bigint,double,float";
  private static final Object[][] TOPN_QUERY_RESULTS_RECORDS_2 = new Object[][] {
          new Object[] { new TimestampLocalTZWritable(new TimestampTZ(Instant.ofEpochMilli(1377907200000L).atZone(ZoneOffset.UTC))),
                  new Text("dim1_val"), new LongWritable(111), new DoubleWritable(10669d),
                  new FloatWritable(96.11711711711712F) },
          new Object[] { new TimestampLocalTZWritable(new TimestampTZ(Instant.ofEpochMilli(1377907200000L).atZone(ZoneOffset.UTC))),
                  new Text("another_dim1_val"), new LongWritable(88), new DoubleWritable(28344d),
                  new FloatWritable(322.09090909090907F) },
          new Object[] { new TimestampLocalTZWritable(new TimestampTZ(Instant.ofEpochMilli(1377907200000L).atZone(ZoneOffset.UTC))),
                  new Text("dim1_val3"), new LongWritable(70), new DoubleWritable(871d),
                  new FloatWritable(12.442857142857143F) },
          new Object[] { new TimestampLocalTZWritable(new TimestampTZ(Instant.ofEpochMilli(1377907200000L).atZone(ZoneOffset.UTC))),
                  new Text("dim1_val4"), new LongWritable(62), new DoubleWritable(815d),
                  new FloatWritable(13.14516129032258F) },
          new Object[] { new TimestampLocalTZWritable(new TimestampTZ(Instant.ofEpochMilli(1377907200000L).atZone(ZoneOffset.UTC))),
                  new Text("dim1_val5"), new LongWritable(60), new DoubleWritable(2787d),
                  new FloatWritable(46.45F) }
  };

  // GroupBy query
  private static final String GROUP_BY_QUERY =
          "{ "
                  + " \"queryType\": \"groupBy\", "
                  + " \"dataSource\": \"sample_datasource\", "
                  + " \"granularity\": \"day\", "
                  + " \"dimensions\": [\"country\", \"device\"], "
                  + " \"limitSpec\": {"
                  + " \"type\": \"default\","
                  + " \"limit\": 5000,"
                  + " \"columns\": [\"country\", \"data_transfer\"] }, "
                  + " \"filter\": {  "
                  + "  \"type\": \"and\",  "
                  + "  \"fields\": [   "
                  + "   { \"type\": \"selector\", \"dimension\": \"carrier\", \"value\": \"AT&T\" },   "
                  + "   { \"type\": \"or\",     "
                  + "    \"fields\": [     "
                  + "     { \"type\": \"selector\", \"dimension\": \"make\", \"value\": \"Apple\" },     "
                  + "     { \"type\": \"selector\", \"dimension\": \"make\", \"value\": \"Samsung\" }    "
                  + "    ]   "
                  + "   }  "
                  + "  ] "
                  + " }, "
                  + " \"aggregations\": [  "
                  + "  { \"type\": \"longSum\", \"name\": \"total_usage\", \"fieldName\": \"user_count\" },  "
                  + "  { \"type\": \"doubleSum\", \"name\": \"data_transfer\", \"fieldName\": \"data_transfer\" } "
                  + " ], "
                  + " \"postAggregations\": [  "
                  + "  { \"type\": \"arithmetic\",  "
                  + "    \"name\": \"avg_usage\",  "
                  + "    \"fn\": \"/\",  "
                  + "    \"fields\": [   "
                  + "     { \"type\": \"fieldAccess\", \"fieldName\": \"data_transfer\" },   "
                  + "     { \"type\": \"fieldAccess\", \"fieldName\": \"total_usage\" }  "
                  + "    ]  "
                  + "  } "
                  + " ], "
                  + " \"intervals\": [ \"2012-01-01T00:00:00.000/2012-01-03T00:00:00.000\" ], "
                  + " \"having\": {  "
                  + "  \"type\": \"greaterThan\",  "
                  + "  \"aggregation\": \"total_usage\",  "
                  + "  \"value\": 100 "
                  + " }}";

  // GroupBy query results
  private static final String GROUP_BY_QUERY_RESULTS =
          "[  "
                  + " {  "
                  + "  \"version\" : \"v1\",  "
                  + "  \"timestamp\" : \"2012-01-01T00:00:00.000Z\",  "
                  + "  \"event\" : {   "
                  + "   \"country\" : \"India\",   "
                  + "   \"device\" : \"phone\",   "
                  + "   \"total_usage\" : 88,   "
                  + "   \"data_transfer\" : 29.91233453,   "
                  + "   \"avg_usage\" : 60.32  "
                  + "  } "
                  + " },  "
                  + " {  "
                  + "  \"version\" : \"v1\",  "
                  + "  \"timestamp\" : \"2012-01-01T00:00:12.000Z\",  "
                  + "  \"event\" : {   "
                  + "   \"country\" : \"Spain\",   "
                  + "   \"device\" : \"pc\",   "
                  + "   \"total_usage\" : 16,   "
                  + "   \"data_transfer\" : 172.93494959,   "
                  + "   \"avg_usage\" : 6.333333  "
                  + "  } "
                  + " }]";

  private static final String GB_TIME_EXTRACTIONS = "{\"queryType\":\"groupBy\",\"dataSource\":\"sample_datasource\","
          + "\"granularity\":\"all\",\"dimensions\":"
          + "[{\"type\":\"extraction\",\"dimension\":\"__time\",\"outputName\":\"extract\",\"extractionFn\":"
          + "{\"type\":\"timeFormat\",\"format\":\"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\",\"timeZone\":\"UTC\"}}],"
          + "\"limitSpec\":{\"type\":\"default\"},"
          + "\"aggregations\":[{\"type\":\"count\",\"name\":\"$f1\"}],"
          + "\"intervals\":[\"1900-01-01T00:00:00.000/3000-01-01T00:00:00.000\"]}";

  private static final String GB_TIME_EXTRACTIONS_RESULTS = "[  "
          + " {  "
          + "  \"version\" : \"v1\",  "
          + "  \"timestamp\" : \"2012-01-01T00:00:00.000Z\",  "
          + "  \"event\" : {   "
          + "   \"extract\" : \"2012-01-01T00:00:00.000Z\",   "
          + "   \"$f1\" : 200"
          + "  } "
          + " },  "
          + " {  "
          + "  \"version\" : \"v1\",  "
          + "  \"timestamp\" : \"2012-01-01T00:00:12.000Z\",  "
          + "  \"event\" : {   "
          + "   \"extract\" : \"2012-01-01T00:00:12.000Z\",   "
          + "   \"$f1\" : 400"
          + "  }  "
          + " }]";

  private static final String GB_MONTH_EXTRACTIONS_RESULTS = "[  "
          + " {  "
          + "  \"version\" : \"v1\",  "
          + "  \"timestamp\" : \"2012-01-01T00:00:00.000Z\",  "
          + "  \"event\" : {   "
          + "   \"extract_month\" : \"01\",   "
          + "   \"$f1\" : 200"
          + "  } "
          + " },  "
          + " {  "
          + "  \"version\" : \"v1\",  "
          + "  \"timestamp\" : \"2012-01-01T00:00:12.000Z\",  "
          + "  \"event\" : {   "
          + "   \"extract_month\" : \"01\",   "
          + "   \"$f1\" : 400"
          + "  }  "
          + " }]";


  private final static String GB_MONTH_EXTRACTIONS = "{\"queryType\":\"groupBy\",\"dataSource\":\"sample_datasource\","
          + "\"granularity\":\"all\","
          + "\"dimensions\":[{\"type\":\"extraction\",\"dimension\":\"__time\",\"outputName\":\"extract_month\","
          + "\"extractionFn\":{\"type\":\"timeFormat\",\"format\":\"M\",\"timeZone\":\"UTC\",\"locale\":\"en-US\"}}],"
          + "\"limitSpec\":{\"type\":\"default\"},\"aggregations\":[{\"type\":\"count\",\"name\":\"$f1\"}],"
          + "\"intervals\":[\"1900-01-01T00:00:00.000/3000-01-01T00:00:00.000\"]}";

  // GroupBy query results as records
  private static final Object[][] GROUP_BY_QUERY_EXTRACTION_RESULTS_RECORDS = new Object[][] {
          new Object[] { new TimestampLocalTZWritable(new TimestampTZ(Instant.ofEpochMilli(1325376000000L).atZone(ZoneOffset.UTC))),
              new TimestampLocalTZWritable(new TimestampTZ(Instant.ofEpochMilli(1325376000000L).atZone(ZoneOffset.UTC))),
                  new LongWritable(200) },
          new Object[] { new TimestampLocalTZWritable(new TimestampTZ(Instant.ofEpochMilli(1325376012000L).atZone(ZoneOffset.UTC))),
              new TimestampLocalTZWritable(new TimestampTZ(Instant.ofEpochMilli(1325376012000L).atZone(ZoneOffset.UTC))),
                  new LongWritable(400) }
  };

  private static final Object[][] GROUP_BY_QUERY_RESULTS_RECORDS = new Object[][] {
          new Object[] { new TimestampLocalTZWritable(new TimestampTZ(Instant.ofEpochMilli(1325376000000L).atZone(ZoneOffset.UTC))), new Text("India"),
                  new Text("phone"), new LongWritable(88), new FloatWritable(29.91233453F),
                  new FloatWritable(60.32F) },
          new Object[] { new TimestampLocalTZWritable(new TimestampTZ(Instant.ofEpochMilli(1325376012000L).atZone(ZoneOffset.UTC))), new Text("Spain"),
                  new Text("pc"), new LongWritable(16), new FloatWritable(172.93494959F),
                  new FloatWritable(6.333333F) }
  };

  private static final Object[][] GB_MONTH_EXTRACTION_RESULTS_RECORDS = new Object[][] {
          new Object[] { new TimestampLocalTZWritable(new TimestampTZ(Instant.ofEpochMilli(1325376000000L).atZone(ZoneOffset.UTC))),
                  new IntWritable(1),
                  new LongWritable(200) },
          new Object[] { new TimestampLocalTZWritable(new TimestampTZ(Instant.ofEpochMilli(1325376012000L).atZone(ZoneOffset.UTC))), new IntWritable(1),
                  new LongWritable(400) }
  };

  // GroupBy query results as records (types defined by metastore)
  private static final String GROUP_BY_COLUMN_NAMES = "__time,country,device,total_usage,data_transfer,avg_usage";
  private static final String GROUP_BY_COLUMN_TYPES = "timestamp with local time zone,string,string,int,double,float";
  private static final Object[][] GROUP_BY_QUERY_RESULTS_RECORDS_2 = new Object[][] {
    new Object[] { new TimestampLocalTZWritable(new TimestampTZ(Instant.ofEpochMilli(1325376000000L).atZone(ZoneOffset.UTC))), new Text("India"),
            new Text("phone"), new IntWritable(88), new DoubleWritable(29.91233453),
            new FloatWritable(60.32F) },
    new Object[] { new TimestampLocalTZWritable(new TimestampTZ(Instant.ofEpochMilli(1325376012000L).atZone(ZoneOffset.UTC))), new Text("Spain"),
            new Text("pc"), new IntWritable(16), new DoubleWritable(172.93494959),
            new FloatWritable(6.333333F) }
  };

  // Select query
  private static final String SELECT_QUERY =
          "{   \"queryType\": \"select\",  "
                  + " \"dataSource\": \"wikipedia\",   \"descending\": \"false\",  "
                  + " \"dimensions\":[\"robot\",\"namespace\",\"anonymous\",\"unpatrolled\",\"page\",\"language\",\"newpage\",\"user\"],  "
                  + " \"metrics\":[\"count\",\"added\",\"delta\",\"variation\",\"deleted\"],  "
                  + " \"granularity\": \"all\",  "
                  + " \"intervals\": [     \"2013-01-01/2013-01-02\"   ],  "
                  + " \"pagingSpec\":{\"pagingIdentifiers\": {}, \"threshold\":5} }";

  // Select query results
  private static final String SELECT_QUERY_RESULTS =
          "[{ "
                  + " \"timestamp\" : \"2013-01-01T00:00:00.000Z\", "
                  + " \"result\" : {  "
                  + "  \"pagingIdentifiers\" : {   "
                  + "   \"wikipedia_2012-12-29T00:00:00.000Z_2013-01-10T08:00:00.000Z_2013-01-10T08:13:47.830Z_v9\" : 4    }, "
                  + "   \"events\" : [ {  "
                  + "    \"segmentId\" : \"wikipedia_editstream_2012-12-29T00:00:00.000Z_2013-01-10T08:00:00.000Z_2013-01-10T08:13:47.830Z_v9\",  "
                  + "    \"offset\" : 0,  "
                  + "    \"event\" : {   "
                  + "     \"timestamp\" : \"2013-01-01T00:00:00.000Z\",   "
                  + "     \"robot\" : \"1\",   "
                  + "     \"namespace\" : \"article\",   "
                  + "     \"anonymous\" : \"0\",   "
                  + "     \"unpatrolled\" : \"0\",   "
                  + "     \"page\" : \"11._korpus_(NOVJ)\",   "
                  + "     \"language\" : \"sl\",   "
                  + "     \"newpage\" : \"0\",   "
                  + "     \"user\" : \"EmausBot\",   "
                  + "     \"count\" : 1.0,   "
                  + "     \"added\" : 39.0,   "
                  + "     \"delta\" : 39.0,   "
                  + "     \"variation\" : 39.0,   "
                  + "     \"deleted\" : 0.0  "
                  + "    } "
                  + "   }, {  "
                  + "    \"segmentId\" : \"wikipedia_2012-12-29T00:00:00.000Z_2013-01-10T08:00:00.000Z_2013-01-10T08:13:47.830Z_v9\",  "
                  + "    \"offset\" : 1,  "
                  + "    \"event\" : {   "
                  + "     \"timestamp\" : \"2013-01-01T00:00:00.000Z\",   "
                  + "     \"robot\" : \"0\",   "
                  + "     \"namespace\" : \"article\",   "
                  + "     \"anonymous\" : \"0\",   "
                  + "     \"unpatrolled\" : \"0\",   "
                  + "     \"page\" : \"112_U.S._580\",   "
                  + "     \"language\" : \"en\",   "
                  + "     \"newpage\" : \"1\",   "
                  + "     \"user\" : \"MZMcBride\",   "
                  + "     \"count\" : 1.0,   "
                  + "     \"added\" : 70.0,   "
                  + "     \"delta\" : 70.0,   "
                  + "     \"variation\" : 70.0,   "
                  + "     \"deleted\" : 0.0  "
                  + "    } "
                  + "   }, {  "
                  + "    \"segmentId\" : \"wikipedia_2012-12-29T00:00:00.000Z_2013-01-10T08:00:00.000Z_2013-01-10T08:13:47.830Z_v9\",  "
                  + "    \"offset\" : 2,  "
                  + "    \"event\" : {   "
                  + "     \"timestamp\" : \"2013-01-01T00:00:12.000Z\",   "
                  + "     \"robot\" : \"0\",   "
                  + "     \"namespace\" : \"article\",   "
                  + "     \"anonymous\" : \"0\",   "
                  + "     \"unpatrolled\" : \"0\",   "
                  + "     \"page\" : \"113_U.S._243\",   "
                  + "     \"language\" : \"en\",   "
                  + "     \"newpage\" : \"1\",   "
                  + "     \"user\" : \"MZMcBride\",   "
                  + "     \"count\" : 1.0,   "
                  + "     \"added\" : 77.0,   "
                  + "     \"delta\" : 77.0,   "
                  + "     \"variation\" : 77.0,   "
                  + "     \"deleted\" : 0.0  "
                  + "    } "
                  + "   }, {  "
                  + "    \"segmentId\" : \"wikipedia_2012-12-29T00:00:00.000Z_2013-01-10T08:00:00.000Z_2013-01-10T08:13:47.830Z_v9\",  "
                  + "    \"offset\" : 3,  "
                  + "    \"event\" : {   "
                  + "     \"timestamp\" : \"2013-01-01T00:00:12.000Z\",   "
                  + "     \"robot\" : \"0\",   "
                  + "     \"namespace\" : \"article\",   "
                  + "     \"anonymous\" : \"0\",   "
                  + "     \"unpatrolled\" : \"0\",   "
                  + "     \"page\" : \"113_U.S._73\",   "
                  + "     \"language\" : \"en\",   "
                  + "     \"newpage\" : \"1\",   "
                  + "     \"user\" : \"MZMcBride\",   "
                  + "     \"count\" : 1.0,   "
                  + "     \"added\" : 70.0,   "
                  + "     \"delta\" : 70.0,   "
                  + "     \"variation\" : 70.0,   "
                  + "     \"deleted\" : 0.0  "
                  + "    } "
                  + "   }, {  "
                  + "    \"segmentId\" : \"wikipedia_2012-12-29T00:00:00.000Z_2013-01-10T08:00:00.000Z_2013-01-10T08:13:47.830Z_v9\",  "
                  + "    \"offset\" : 4,  "
                  + "    \"event\" : {   "
                  + "     \"timestamp\" : \"2013-01-01T00:00:12.000Z\",   "
                  + "     \"robot\" : \"0\",   "
                  + "     \"namespace\" : \"article\",   "
                  + "     \"anonymous\" : \"0\",   "
                  + "     \"unpatrolled\" : \"0\",   "
                  + "     \"page\" : \"113_U.S._756\",   "
                  + "     \"language\" : \"en\",   "
                  + "     \"newpage\" : \"1\",   "
                  + "     \"user\" : \"MZMcBride\",   "
                  + "     \"count\" : 1.0,   "
                  + "     \"added\" : 68.0,   "
                  + "     \"delta\" : 68.0,   "
                  + "     \"variation\" : 68.0,   "
                  + "     \"deleted\" : 0.0  "
                  + "    } "
                  + "   } ]  }} ]";

  // Select query results as records
  private static final Object[][] SELECT_QUERY_RESULTS_RECORDS = new Object[][] {
          new Object[] { new TimestampLocalTZWritable(new TimestampTZ(Instant.ofEpochMilli(1356998400000L).atZone(ZoneOffset.UTC))), new Text("1"),
                  new Text("article"), new Text("0"), new Text("0"),
                  new Text("11._korpus_(NOVJ)"), new Text("sl"), new Text("0"),
                  new Text("EmausBot"),
                  new FloatWritable(1.0F), new FloatWritable(39.0F), new FloatWritable(39.0F),
                  new FloatWritable(39.0F), new FloatWritable(0.0F) },
          new Object[] { new TimestampLocalTZWritable(new TimestampTZ(Instant.ofEpochMilli(1356998400000L).atZone(ZoneOffset.UTC))), new Text("0"),
                  new Text("article"), new Text("0"), new Text("0"),
                  new Text("112_U.S._580"), new Text("en"), new Text("1"), new Text("MZMcBride"),
                  new FloatWritable(1.0F), new FloatWritable(70.0F), new FloatWritable(70.0F),
                  new FloatWritable(70.0F), new FloatWritable(0.0F) },
          new Object[] { new TimestampLocalTZWritable(new TimestampTZ(Instant.ofEpochMilli(1356998412000L).atZone(ZoneOffset.UTC))), new Text("0"),
                  new Text("article"), new Text("0"), new Text("0"),
                  new Text("113_U.S._243"), new Text("en"), new Text("1"), new Text("MZMcBride"),
                  new FloatWritable(1.0F), new FloatWritable(77.0F), new FloatWritable(77.0F),
                  new FloatWritable(77.0F), new FloatWritable(0.0F) },
          new Object[] { new TimestampLocalTZWritable(new TimestampTZ(Instant.ofEpochMilli(1356998412000L).atZone(ZoneOffset.UTC))), new Text("0"),
                  new Text("article"), new Text("0"), new Text("0"),
                  new Text("113_U.S._73"), new Text("en"), new Text("1"), new Text("MZMcBride"),
                  new FloatWritable(1.0F), new FloatWritable(70.0F), new FloatWritable(70.0F),
                  new FloatWritable(70.0F), new FloatWritable(0.0F) },
          new Object[] { new TimestampLocalTZWritable(new TimestampTZ(Instant.ofEpochMilli(1356998412000L).atZone(ZoneOffset.UTC))), new Text("0"),
                  new Text("article"), new Text("0"), new Text("0"),
                  new Text("113_U.S._756"), new Text("en"), new Text("1"), new Text("MZMcBride"),
                  new FloatWritable(1.0F), new FloatWritable(68.0F), new FloatWritable(68.0F),
                  new FloatWritable(68.0F), new FloatWritable(0.0F) }
  };

  // Select query results as records (types defined by metastore)
  private static final String SELECT_COLUMN_NAMES = "__time,robot,namespace,anonymous,unpatrolled,page,language,newpage,user,count,added,delta,variation,deleted";
  private static final String SELECT_COLUMN_TYPES = "timestamp with local time zone,string,string,string,string,string,string,string,string,double,double,float,float,float";
  private static final Object[][] SELECT_QUERY_RESULTS_RECORDS_2 = new Object[][] {
          new Object[] { new TimestampLocalTZWritable(new TimestampTZ(Instant.ofEpochMilli(1356998400000L).atZone(ZoneOffset.UTC))), new Text("1"),
                  new Text("article"), new Text("0"), new Text("0"),
                  new Text("11._korpus_(NOVJ)"), new Text("sl"), new Text("0"),
                  new Text("EmausBot"),
                  new DoubleWritable(1.0d), new DoubleWritable(39.0d), new FloatWritable(39.0F),
                  new FloatWritable(39.0F), new FloatWritable(0.0F) },
          new Object[] { new TimestampLocalTZWritable(new TimestampTZ(Instant.ofEpochMilli(1356998400000L).atZone(ZoneOffset.UTC))), new Text("0"),
                  new Text("article"), new Text("0"), new Text("0"),
                  new Text("112_U.S._580"), new Text("en"), new Text("1"), new Text("MZMcBride"),
                  new DoubleWritable(1.0d), new DoubleWritable(70.0d), new FloatWritable(70.0F),
                  new FloatWritable(70.0F), new FloatWritable(0.0F) },
          new Object[] { new TimestampLocalTZWritable(new TimestampTZ(Instant.ofEpochMilli(1356998412000L).atZone(ZoneOffset.UTC))), new Text("0"),
                  new Text("article"), new Text("0"), new Text("0"),
                  new Text("113_U.S._243"), new Text("en"), new Text("1"), new Text("MZMcBride"),
                  new DoubleWritable(1.0d), new DoubleWritable(77.0d), new FloatWritable(77.0F),
                  new FloatWritable(77.0F), new FloatWritable(0.0F) },
          new Object[] { new TimestampLocalTZWritable(new TimestampTZ(Instant.ofEpochMilli(1356998412000L).atZone(ZoneOffset.UTC))), new Text("0"),
                  new Text("article"), new Text("0"), new Text("0"),
                  new Text("113_U.S._73"), new Text("en"), new Text("1"), new Text("MZMcBride"),
                  new DoubleWritable(1.0d), new DoubleWritable(70.0d), new FloatWritable(70.0F),
                  new FloatWritable(70.0F), new FloatWritable(0.0F) },
          new Object[] { new TimestampLocalTZWritable(new TimestampTZ(Instant.ofEpochMilli(1356998412000L).atZone(ZoneOffset.UTC))), new Text("0"),
                  new Text("article"), new Text("0"), new Text("0"),
                  new Text("113_U.S._756"), new Text("en"), new Text("1"), new Text("MZMcBride"),
                  new DoubleWritable(1.0d), new DoubleWritable(68.0d), new FloatWritable(68.0F),
                  new FloatWritable(68.0F), new FloatWritable(0.0F) }
  };

  @Before
  public void setup() throws IOException {
    tsQueryResults = DruidStorageHandlerUtils.SMILE_MAPPER.writeValueAsBytes(DruidStorageHandlerUtils.JSON_MAPPER.readValue(TIMESERIES_QUERY_RESULTS, new TypeReference<List<Result<TimeseriesResultValue>>>() {
    }));

    topNQueryResults = DruidStorageHandlerUtils.SMILE_MAPPER
            .writeValueAsBytes(DruidStorageHandlerUtils.JSON_MAPPER.readValue(TOPN_QUERY_RESULTS,  new TypeReference<List<Result<TopNResultValue>>>() {
            }));
    groupByQueryResults = DruidStorageHandlerUtils.SMILE_MAPPER
            .writeValueAsBytes(DruidStorageHandlerUtils.JSON_MAPPER.readValue(GROUP_BY_QUERY_RESULTS,
                    new TypeReference<List<Row>>() {
                    }));
    groupByTimeExtractQueryResults = DruidStorageHandlerUtils.SMILE_MAPPER
            .writeValueAsBytes(DruidStorageHandlerUtils.JSON_MAPPER.readValue(GB_TIME_EXTRACTIONS_RESULTS, new TypeReference<List<Row>>() {
            }));
    groupByMonthExtractQueryResults = DruidStorageHandlerUtils.SMILE_MAPPER
            .writeValueAsBytes(DruidStorageHandlerUtils.JSON_MAPPER.readValue(GB_MONTH_EXTRACTIONS_RESULTS, new TypeReference<List<Row>>() {
            }));
    selectQueryResults = DruidStorageHandlerUtils.SMILE_MAPPER
            .writeValueAsBytes(DruidStorageHandlerUtils.JSON_MAPPER.readValue(SELECT_QUERY_RESULTS, new TypeReference<List<Result<SelectResultValue>>>() {
            }));
  }

  /**
   * Test the default behavior of the objects and object inspectors.
   * @throws IOException
   * @throws IllegalAccessException
   * @throws IllegalArgumentException
   * @throws SecurityException
   * @throws NoSuchFieldException
   * @throws JsonMappingException
   * @throws JsonParseException
   * @throws InvocationTargetException
   * @throws NoSuchMethodException
   */
  @Test
  public void testDruidDeserializer() throws SerDeException, NoSuchFieldException,
          SecurityException, IllegalArgumentException, IllegalAccessException,
          IOException, InterruptedException, NoSuchMethodException, InvocationTargetException {
    // Create, initialize, and test the SerDe
    QTestDruidSerDe serDe = new QTestDruidSerDe();
    Configuration conf = new Configuration();
    Properties tbl;
    // Timeseries query
    tbl = createPropertiesQuery("sample_datasource", Query.TIMESERIES, TIMESERIES_QUERY);
    SerDeUtils.initializeSerDe(serDe, conf, tbl, null);
    deserializeQueryResults(serDe, Query.TIMESERIES, TIMESERIES_QUERY,
            tsQueryResults, TIMESERIES_QUERY_RESULTS_RECORDS
    );
    // Timeseries query (simulating column types from metastore)
    tbl.setProperty(serdeConstants.LIST_COLUMNS, TIMESERIES_COLUMN_NAMES);
    tbl.setProperty(serdeConstants.LIST_COLUMN_TYPES, TIMESERIES_COLUMN_TYPES);
    SerDeUtils.initializeSerDe(serDe, conf, tbl, null);
    deserializeQueryResults(serDe, Query.TIMESERIES, TIMESERIES_QUERY,
            tsQueryResults, TIMESERIES_QUERY_RESULTS_RECORDS_2
    );
    // TopN query
    tbl = createPropertiesQuery("sample_data", Query.TOPN, TOPN_QUERY);
    SerDeUtils.initializeSerDe(serDe, conf, tbl, null);
    deserializeQueryResults(serDe, Query.TOPN, TOPN_QUERY,
            topNQueryResults, TOPN_QUERY_RESULTS_RECORDS
    );
    // TopN query (simulating column types from metastore)
    tbl.setProperty(serdeConstants.LIST_COLUMNS, TOPN_COLUMN_NAMES);
    tbl.setProperty(serdeConstants.LIST_COLUMN_TYPES, TOPN_COLUMN_TYPES);
    SerDeUtils.initializeSerDe(serDe, conf, tbl, null);
    deserializeQueryResults(serDe, Query.TOPN, TOPN_QUERY,
            topNQueryResults, TOPN_QUERY_RESULTS_RECORDS_2
    );
    // GroupBy query
    tbl = createPropertiesQuery("sample_datasource", Query.GROUP_BY, GROUP_BY_QUERY);
    SerDeUtils.initializeSerDe(serDe, conf, tbl, null);
    deserializeQueryResults(serDe, Query.GROUP_BY, GROUP_BY_QUERY,
            groupByQueryResults, GROUP_BY_QUERY_RESULTS_RECORDS
    );
    // GroupBy query (simulating column types from metastore)
    tbl.setProperty(serdeConstants.LIST_COLUMNS, GROUP_BY_COLUMN_NAMES);
    tbl.setProperty(serdeConstants.LIST_COLUMN_TYPES, GROUP_BY_COLUMN_TYPES);
    SerDeUtils.initializeSerDe(serDe, conf, tbl, null);
    deserializeQueryResults(serDe, Query.GROUP_BY, GROUP_BY_QUERY,
            groupByQueryResults, GROUP_BY_QUERY_RESULTS_RECORDS_2
    );
    tbl = createPropertiesQuery("sample_datasource", Query.GROUP_BY, GB_TIME_EXTRACTIONS);
    SerDeUtils.initializeSerDe(serDe, conf, tbl, null);
    deserializeQueryResults(serDe, Query.GROUP_BY, GB_TIME_EXTRACTIONS,
            groupByTimeExtractQueryResults, GROUP_BY_QUERY_EXTRACTION_RESULTS_RECORDS
    );

    tbl = createPropertiesQuery("sample_datasource", Query.GROUP_BY, GB_MONTH_EXTRACTIONS);
    SerDeUtils.initializeSerDe(serDe, conf, tbl, null);
    deserializeQueryResults(serDe, Query.GROUP_BY, GB_MONTH_EXTRACTIONS,
            groupByMonthExtractQueryResults, GB_MONTH_EXTRACTION_RESULTS_RECORDS
    );
    // Select query
    tbl = createPropertiesQuery("wikipedia", Query.SELECT, SELECT_QUERY);
    SerDeUtils.initializeSerDe(serDe, conf, tbl, null);
    deserializeQueryResults(serDe, Query.SELECT, SELECT_QUERY,
            selectQueryResults, SELECT_QUERY_RESULTS_RECORDS
    );
    // Select query (simulating column types from metastore)
    tbl.setProperty(serdeConstants.LIST_COLUMNS, SELECT_COLUMN_NAMES);
    tbl.setProperty(serdeConstants.LIST_COLUMN_TYPES, SELECT_COLUMN_TYPES);
    SerDeUtils.initializeSerDe(serDe, conf, tbl, null);
    deserializeQueryResults(serDe, Query.SELECT, SELECT_QUERY,
            selectQueryResults, SELECT_QUERY_RESULTS_RECORDS_2
    );
  }

  private static Properties createPropertiesQuery(String dataSource, String queryType,
          String jsonQuery
  ) {
    Properties tbl = new Properties();

    // Set the configuration parameters
    tbl.setProperty(Constants.DRUID_DATA_SOURCE, dataSource);
    tbl.setProperty(Constants.DRUID_QUERY_JSON, jsonQuery);
    tbl.setProperty(Constants.DRUID_QUERY_TYPE, queryType);
    return tbl;
  }

  @SuppressWarnings("unchecked")
  private void deserializeQueryResults(DruidSerDe serDe, String queryType, String jsonQuery,
          byte[] resultString, Object[][] records
  ) throws SerDeException, IOException, NoSuchFieldException, SecurityException,
          IllegalArgumentException, IllegalAccessException, InterruptedException,
          NoSuchMethodException, InvocationTargetException {

    // Initialize
    HttpClient httpClient = mock(HttpClient.class);
    SettableFuture<InputStream> futureResult = SettableFuture.create();
    futureResult.set(new ByteArrayInputStream(resultString));
    when(httpClient.go(anyObject(), any(HttpResponseHandler.class))).thenReturn(futureResult);
    DruidQueryRecordReader<?, ?> reader = DruidQueryBasedInputFormat.getDruidQueryReader(queryType);

    final HiveDruidSplit split = new HiveDruidSplit(jsonQuery,
            new Path("empty"),
            new String[] { "testing_host" }
    );

    reader.initialize(split, new Configuration(), DruidStorageHandlerUtils.JSON_MAPPER,
            DruidStorageHandlerUtils.SMILE_MAPPER, httpClient
    );
    StructObjectInspector oi = (StructObjectInspector) serDe.getObjectInspector();
    List<? extends StructField> fieldRefs = oi.getAllStructFieldRefs();

    // Check mapred
    DruidWritable writable = new DruidWritable();
    int pos = 0;
    while (reader.next(NullWritable.get(), writable)) {
      List<Object> row = (List<Object>) serDe.deserialize(writable);
      Object[] expectedFieldsData = records[pos];
      assertEquals(expectedFieldsData.length, fieldRefs.size());
      for (int i = 0; i < fieldRefs.size(); i++) {
        assertEquals("Field " + i + " type", expectedFieldsData[i].getClass(),
                row.get(i).getClass()
        );
        Object fieldData = oi.getStructFieldData(row, fieldRefs.get(i));
        assertEquals("Field " + i, expectedFieldsData[i], fieldData);
      }
      pos++;
    }
    assertEquals(pos, records.length);


    // Check mapreduce path
    futureResult = SettableFuture.create();
    futureResult.set(new ByteArrayInputStream(resultString));
    when(httpClient.go(anyObject(), any(HttpResponseHandler.class))).thenReturn(futureResult);
    reader = DruidQueryBasedInputFormat.getDruidQueryReader(queryType);
    reader.initialize(split, new Configuration(), DruidStorageHandlerUtils.JSON_MAPPER,
            DruidStorageHandlerUtils.SMILE_MAPPER, httpClient
    );

     pos = 0;
    while (reader.nextKeyValue()) {
      List<Object> row = (List<Object>) serDe.deserialize(reader.getCurrentValue());
      Object[] expectedFieldsData = records[pos];
      assertEquals(expectedFieldsData.length, fieldRefs.size());
      for (int i = 0; i < fieldRefs.size(); i++) {
        assertEquals("Field " + i + " type", expectedFieldsData[i].getClass(), row.get(i).getClass());
        Object fieldData = oi.getStructFieldData(row, fieldRefs.get(i));
        assertEquals("Field " + i, expectedFieldsData[i], fieldData);
      }
      pos++;
    }
    assertEquals(pos, records.length);
  }


  private static final String COLUMN_NAMES = "__time,c0,c1,c2,c3,c4,c5,c6,c7,c8,c9";
  private static final String COLUMN_TYPES = "timestamp with local time zone,string,char(6),varchar(8),double,float,decimal(38,18),bigint,int,smallint,tinyint";
  private static final Object[] ROW_OBJECT = new Object[] {
      new TimestampLocalTZWritable(new TimestampTZ(Instant.ofEpochMilli(1377907200000L).atZone(ZoneOffset.UTC))),
      new Text("dim1_val"),
      new HiveCharWritable(new HiveChar("dim2_v", 6)),
      new HiveVarcharWritable(new HiveVarchar("dim3_val", 8)),
      new DoubleWritable(10669.3D),
      new FloatWritable(10669.45F),
      new HiveDecimalWritable(HiveDecimal.create(1064.34D)),
      new LongWritable(1113939),
      new IntWritable(1112123),
      new ShortWritable((short) 12),
      new ByteWritable((byte) 0),
      new TimestampWritable(new Timestamp(1377907200000L)) // granularity
  };
  private static final DruidWritable DRUID_WRITABLE = new DruidWritable(
      ImmutableMap.<String, Object>builder()
          .put("__time", 1377907200000L)
          .put("c0", "dim1_val")
          .put("c1", "dim2_v")
          .put("c2", "dim3_val")
          .put("c3", 10669.3D)
          .put("c4", 10669.45F)
          .put("c5", 1064.34D)
          .put("c6", 1113939L)
          .put("c7", 1112123)
          .put("c8", (short) 12)
          .put("c9", (byte) 0)
          .put("__time_granularity", 1377907200000L)
          .build());

  /**
   * Test the default behavior of the objects and object inspectors.
   * @throws IOException
   * @throws IllegalAccessException
   * @throws IllegalArgumentException
   * @throws SecurityException
   * @throws NoSuchFieldException
   * @throws JsonMappingException
   * @throws JsonParseException
   * @throws InvocationTargetException
   * @throws NoSuchMethodException
   */
  @Test
  public void testDruidObjectSerializer()
          throws SerDeException, JsonParseException, JsonMappingException,
          NoSuchFieldException, SecurityException, IllegalArgumentException,
          IllegalAccessException, IOException, InterruptedException,
          NoSuchMethodException, InvocationTargetException {
    // Create, initialize, and test the SerDe
    DruidSerDe serDe = new DruidSerDe();
    Configuration conf = new Configuration();
    Properties tbl;
    // Mixed source (all types)
    tbl = createPropertiesSource(COLUMN_NAMES, COLUMN_TYPES);
    SerDeUtils.initializeSerDe(serDe, conf, tbl, null);
    serializeObject(tbl, serDe, ROW_OBJECT, DRUID_WRITABLE);
  }

  private static Properties createPropertiesSource(String columnNames, String columnTypes) {
    Properties tbl = new Properties();

    // Set the configuration parameters
    tbl.setProperty(serdeConstants.LIST_COLUMNS, columnNames);
    tbl.setProperty(serdeConstants.LIST_COLUMN_TYPES, columnTypes);
    return tbl;
  }

  private static void serializeObject(Properties properties, DruidSerDe serDe,
      Object[] rowObject, DruidWritable druidWritable) throws SerDeException {
    // Build OI with timestamp granularity column
    final List<String> columnNames = new ArrayList<>();
    final List<PrimitiveTypeInfo> columnTypes = new ArrayList<>();
    List<ObjectInspector> inspectors = new ArrayList<>();
    columnNames.addAll(Utilities.getColumnNames(properties));
    columnNames.add(Constants.DRUID_TIMESTAMP_GRANULARITY_COL_NAME);
    columnTypes.addAll(Lists.transform(Utilities.getColumnTypes(properties),
            new Function<String, PrimitiveTypeInfo>() {
              @Override
              public PrimitiveTypeInfo apply(String type) {
                return TypeInfoFactory.getPrimitiveTypeInfo(type);
              }
            }
    ));
    columnTypes.add(TypeInfoFactory.getPrimitiveTypeInfo("timestamp"));
    inspectors.addAll(Lists.transform(columnTypes,
            new Function<PrimitiveTypeInfo, ObjectInspector>() {
              @Override
              public ObjectInspector apply(PrimitiveTypeInfo type) {
                return PrimitiveObjectInspectorFactory
                        .getPrimitiveWritableObjectInspector(type);
              }
            }
    ));
    ObjectInspector inspector = ObjectInspectorFactory
            .getStandardStructObjectInspector(columnNames, inspectors);
    // Serialize
    DruidWritable writable = (DruidWritable) serDe.serialize(rowObject, inspector);
    // Check result
    assertEquals(druidWritable.getValue().size(), writable.getValue().size());
    for (Entry<String, Object> e: druidWritable.getValue().entrySet()) {
      assertEquals(e.getValue(), writable.getValue().get(e.getKey()));
    }
  }

  private static final Object[] ROW_OBJECT_2 = new Object[] {
      new TimestampLocalTZWritable(new TimestampTZ(Instant.ofEpochMilli(1377907200000L).atZone(ZoneOffset.UTC))),
      new Text("dim1_val"),
      new HiveCharWritable(new HiveChar("dim2_v", 6)),
      new HiveVarcharWritable(new HiveVarchar("dim3_val", 8)),
      new DoubleWritable(10669.3D),
      new FloatWritable(10669.45F),
      new HiveDecimalWritable(HiveDecimal.create(1064.34D)),
      new LongWritable(1113939),
      new IntWritable(1112123),
      new ShortWritable((short) 12),
      new ByteWritable((byte) 0)
  };
  private static final DruidWritable DRUID_WRITABLE_2 = new DruidWritable(
      ImmutableMap.<String, Object>builder()
          .put("__time", 1377907200000L)
          .put("c0", "dim1_val")
          .put("c1", "dim2_v")
          .put("c2", "dim3_val")
          .put("c3", 10669.3D)
          .put("c4", 10669.45F)
          .put("c5", 1064.34D)
          .put("c6", 1113939L)
          .put("c7", 1112123)
          .put("c8", (short) 12)
          .put("c9", (byte) 0)
          .build());

  @Test
  public void testDruidObjectDeserializer()
          throws SerDeException, JsonParseException, JsonMappingException,
          NoSuchFieldException, SecurityException, IllegalArgumentException,
          IllegalAccessException, IOException, InterruptedException,
          NoSuchMethodException, InvocationTargetException {
    // Create, initialize, and test the SerDe
    DruidSerDe serDe = new DruidSerDe();
    Configuration conf = new Configuration();
    Properties tbl;
    // Mixed source (all types)
    tbl = createPropertiesSource(COLUMN_NAMES, COLUMN_TYPES);
    SerDeUtils.initializeSerDe(serDe, conf, tbl, null);
    deserializeObject(tbl, serDe, ROW_OBJECT_2, DRUID_WRITABLE_2);
  }

  @SuppressWarnings("unchecked")
  private static void deserializeObject(Properties properties, DruidSerDe serDe,
      Object[] rowObject, DruidWritable druidWritable) throws SerDeException {
    // Deserialize
    List<Object> object = (List<Object>) serDe.deserialize(druidWritable);
    // Check result
    assertEquals(rowObject.length, object.size());
    for (int i = 0; i < rowObject.length; i++) {
      assertEquals(rowObject[i].getClass(), object.get(i).getClass());
      assertEquals(rowObject[i], object.get(i));
    }
  }
}
