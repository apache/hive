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
package org.apache.hadoop.hive.druid;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.druid.serde.DruidGroupByQueryRecordReader;
import org.apache.hadoop.hive.druid.serde.DruidQueryRecordReader;
import org.apache.hadoop.hive.druid.serde.DruidSelectQueryRecordReader;
import org.apache.hadoop.hive.druid.serde.DruidSerDe;
import org.apache.hadoop.hive.druid.serde.DruidTimeseriesQueryRecordReader;
import org.apache.hadoop.hive.druid.serde.DruidTopNQueryRecordReader;
import org.apache.hadoop.hive.druid.serde.DruidWritable;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
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
import org.junit.Test;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import io.druid.data.input.Row;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.Query;
import io.druid.query.Result;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.select.SelectQuery;
import io.druid.query.select.SelectResultValue;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.timeseries.TimeseriesResultValue;
import io.druid.query.topn.TopNQuery;
import io.druid.query.topn.TopNResultValue;

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

  // Timeseries query results as records
  private static final Object[][] TIMESERIES_QUERY_RESULTS_RECORDS = new Object[][] {
          new Object[] { new TimestampWritable(new Timestamp(1325376000000L)), new LongWritable(0),
                  new FloatWritable(1.0F), new FloatWritable(2.2222F) },
          new Object[] { new TimestampWritable(new Timestamp(1325462400000L)), new LongWritable(2),
                  new FloatWritable(3.32F), new FloatWritable(4F) }
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
          new Object[] { new TimestampWritable(new Timestamp(1377907200000L)), new Text("dim1_val"),
                  new LongWritable(111), new FloatWritable(10669F),
                  new FloatWritable(96.11711711711712F) },
          new Object[] { new TimestampWritable(new Timestamp(1377907200000L)),
                  new Text("another_dim1_val"), new LongWritable(88), new FloatWritable(28344F),
                  new FloatWritable(322.09090909090907F) },
          new Object[] { new TimestampWritable(new Timestamp(1377907200000L)),
                  new Text("dim1_val3"), new LongWritable(70), new FloatWritable(871F),
                  new FloatWritable(12.442857142857143F) },
          new Object[] { new TimestampWritable(new Timestamp(1377907200000L)),
                  new Text("dim1_val4"), new LongWritable(62), new FloatWritable(815F),
                  new FloatWritable(13.14516129032258F) },
          new Object[] { new TimestampWritable(new Timestamp(1377907200000L)),
                  new Text("dim1_val5"), new LongWritable(60), new FloatWritable(2787F),
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

  // GroupBy query results as records
  private static final Object[][] GROUP_BY_QUERY_RESULTS_RECORDS = new Object[][] {
          new Object[] { new TimestampWritable(new Timestamp(1325376000000L)), new Text("India"),
                  new Text("phone"), new LongWritable(88), new FloatWritable(29.91233453F),
                  new FloatWritable(60.32F) },
          new Object[] { new TimestampWritable(new Timestamp(1325376012000L)), new Text("Spain"),
                  new Text("pc"), new LongWritable(16), new FloatWritable(172.93494959F),
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
          new Object[] { new TimestampWritable(new Timestamp(1356998400000L)), new Text("1"),
                  new Text("article"), new Text("0"), new Text("0"),
                  new Text("11._korpus_(NOVJ)"), new Text("sl"), new Text("0"),
                  new Text("EmausBot"),
                  new FloatWritable(1.0F), new FloatWritable(39.0F), new FloatWritable(39.0F),
                  new FloatWritable(39.0F), new FloatWritable(0.0F) },
          new Object[] { new TimestampWritable(new Timestamp(1356998400000L)), new Text("0"),
                  new Text("article"), new Text("0"), new Text("0"),
                  new Text("112_U.S._580"), new Text("en"), new Text("1"), new Text("MZMcBride"),
                  new FloatWritable(1.0F), new FloatWritable(70.0F), new FloatWritable(70.0F),
                  new FloatWritable(70.0F), new FloatWritable(0.0F) },
          new Object[] { new TimestampWritable(new Timestamp(1356998412000L)), new Text("0"),
                  new Text("article"), new Text("0"), new Text("0"),
                  new Text("113_U.S._243"), new Text("en"), new Text("1"), new Text("MZMcBride"),
                  new FloatWritable(1.0F), new FloatWritable(77.0F), new FloatWritable(77.0F),
                  new FloatWritable(77.0F), new FloatWritable(0.0F) },
          new Object[] { new TimestampWritable(new Timestamp(1356998412000L)), new Text("0"),
                  new Text("article"), new Text("0"), new Text("0"),
                  new Text("113_U.S._73"), new Text("en"), new Text("1"), new Text("MZMcBride"),
                  new FloatWritable(1.0F), new FloatWritable(70.0F), new FloatWritable(70.0F),
                  new FloatWritable(70.0F), new FloatWritable(0.0F) },
          new Object[] { new TimestampWritable(new Timestamp(1356998412000L)), new Text("0"),
                  new Text("article"), new Text("0"), new Text("0"),
                  new Text("113_U.S._756"), new Text("en"), new Text("1"), new Text("MZMcBride"),
                  new FloatWritable(1.0F), new FloatWritable(68.0F), new FloatWritable(68.0F),
                  new FloatWritable(68.0F), new FloatWritable(0.0F) }
  };

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
  public void testDruidDeserializer()
          throws SerDeException, JsonParseException, JsonMappingException,
          NoSuchFieldException, SecurityException, IllegalArgumentException,
          IllegalAccessException, IOException, InterruptedException,
          NoSuchMethodException, InvocationTargetException {
    // Create, initialize, and test the SerDe
    QTestDruidSerDe serDe = new QTestDruidSerDe();
    Configuration conf = new Configuration();
    Properties tbl;
    // Timeseries query
    tbl = createPropertiesQuery("sample_datasource", Query.TIMESERIES, TIMESERIES_QUERY);
    SerDeUtils.initializeSerDe(serDe, conf, tbl, null);
    deserializeQueryResults(serDe, Query.TIMESERIES, TIMESERIES_QUERY,
            TIMESERIES_QUERY_RESULTS, TIMESERIES_QUERY_RESULTS_RECORDS
    );
    // TopN query
    tbl = createPropertiesQuery("sample_data", Query.TOPN, TOPN_QUERY);
    SerDeUtils.initializeSerDe(serDe, conf, tbl, null);
    deserializeQueryResults(serDe, Query.TOPN, TOPN_QUERY,
            TOPN_QUERY_RESULTS, TOPN_QUERY_RESULTS_RECORDS
    );
    // GroupBy query
    tbl = createPropertiesQuery("sample_datasource", Query.GROUP_BY, GROUP_BY_QUERY);
    SerDeUtils.initializeSerDe(serDe, conf, tbl, null);
    deserializeQueryResults(serDe, Query.GROUP_BY, GROUP_BY_QUERY,
            GROUP_BY_QUERY_RESULTS, GROUP_BY_QUERY_RESULTS_RECORDS
    );
    // Select query
    tbl = createPropertiesQuery("wikipedia", Query.SELECT, SELECT_QUERY);
    SerDeUtils.initializeSerDe(serDe, conf, tbl, null);
    deserializeQueryResults(serDe, Query.SELECT, SELECT_QUERY,
            SELECT_QUERY_RESULTS, SELECT_QUERY_RESULTS_RECORDS
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

  private static void deserializeQueryResults(DruidSerDe serDe, String queryType, String jsonQuery,
          String resultString, Object[][] records
  ) throws SerDeException, JsonParseException,
          JsonMappingException, IOException, NoSuchFieldException, SecurityException,
          IllegalArgumentException, IllegalAccessException, InterruptedException,
          NoSuchMethodException, InvocationTargetException {

    // Initialize
    Query<?> query = null;
    DruidQueryRecordReader<?, ?> reader = null;
    List<?> resultsList = null;
    ObjectMapper mapper = new DefaultObjectMapper();
    switch (queryType) {
      case Query.TIMESERIES:
        query = mapper.readValue(jsonQuery, TimeseriesQuery.class);
        reader = new DruidTimeseriesQueryRecordReader();
        resultsList = mapper.readValue(resultString,
                new TypeReference<List<Result<TimeseriesResultValue>>>() {
                }
        );
        break;
      case Query.TOPN:
        query = mapper.readValue(jsonQuery, TopNQuery.class);
        reader = new DruidTopNQueryRecordReader();
        resultsList = mapper.readValue(resultString,
                new TypeReference<List<Result<TopNResultValue>>>() {
                }
        );
        break;
      case Query.GROUP_BY:
        query = mapper.readValue(jsonQuery, GroupByQuery.class);
        reader = new DruidGroupByQueryRecordReader();
        resultsList = mapper.readValue(resultString,
                new TypeReference<List<Row>>() {
                }
        );
        break;
      case Query.SELECT:
        query = mapper.readValue(jsonQuery, SelectQuery.class);
        reader = new DruidSelectQueryRecordReader();
        resultsList = mapper.readValue(resultString,
                new TypeReference<List<Result<SelectResultValue>>>() {
                }
        );
        break;
    }

    // Set query and fields access
    Field field1 = DruidQueryRecordReader.class.getDeclaredField("query");
    field1.setAccessible(true);
    field1.set(reader, query);
    if (reader instanceof DruidGroupByQueryRecordReader) {
      Method method1 = DruidGroupByQueryRecordReader.class.getDeclaredMethod("initExtractors");
      method1.setAccessible(true);
      method1.invoke(reader);
    }
    Field field2 = DruidQueryRecordReader.class.getDeclaredField("results");
    field2.setAccessible(true);

    // Get the row structure
    StructObjectInspector oi = (StructObjectInspector) serDe.getObjectInspector();
    List<? extends StructField> fieldRefs = oi.getAllStructFieldRefs();

    // Check mapred
    Iterator<?> results = resultsList.iterator();
    field2.set(reader, results);
    DruidWritable writable = new DruidWritable();
    int pos = 0;
    while (reader.next(NullWritable.get(), writable)) {
      Object row = serDe.deserialize(writable);
      Object[] expectedFieldsData = records[pos];
      assertEquals(expectedFieldsData.length, fieldRefs.size());
      for (int i = 0; i < fieldRefs.size(); i++) {
        Object fieldData = oi.getStructFieldData(row, fieldRefs.get(i));
        assertEquals("Field " + i, expectedFieldsData[i], fieldData);
      }
      pos++;
    }
    assertEquals(pos, records.length);

    // Check mapreduce
    results = resultsList.iterator();
    field2.set(reader, results);
    pos = 0;
    while (reader.nextKeyValue()) {
      Object row = serDe.deserialize(reader.getCurrentValue());
      Object[] expectedFieldsData = records[pos];
      assertEquals(expectedFieldsData.length, fieldRefs.size());
      for (int i = 0; i < fieldRefs.size(); i++) {
        Object fieldData = oi.getStructFieldData(row, fieldRefs.get(i));
        assertEquals("Field " + i, expectedFieldsData[i], fieldData);
      }
      pos++;
    }
    assertEquals(pos, records.length);
  }


  private static final String COLUMN_NAMES = "__time,c0,c1,c2,c3,c4,c5,c6,c7";
  private static final String COLUMN_TYPES = "timestamp,string,double,float,decimal(38,18),bigint,int,smallint,tinyint";
  private static final Object[] ROW_OBJECT = new Object[] {
      new TimestampWritable(new Timestamp(1377907200000L)),
      new Text("dim1_val"),
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
          .put("c1", 10669.3D)
          .put("c2", 10669.45F)
          .put("c3", 1064.34D)
          .put("c4", 1113939L)
          .put("c5", 1112123)
          .put("c6", (short) 12)
          .put("c7", (byte) 0)
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
  public void testDruidSerializer()
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
    assertEquals(DRUID_WRITABLE.getValue().size(), writable.getValue().size());
    for (Entry<String, Object> e: DRUID_WRITABLE.getValue().entrySet()) {
      assertEquals(e.getValue(), writable.getValue().get(e.getKey()));
    }
  }
}
