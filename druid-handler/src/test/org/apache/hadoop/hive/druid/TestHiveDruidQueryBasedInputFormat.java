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
package org.apache.hadoop.hive.druid;

import java.lang.reflect.Method;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.druid.io.DruidQueryBasedInputFormat;
import org.apache.hadoop.hive.druid.io.HiveDruidSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.druid.query.Query;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 * Test Class.
 */
@SuppressWarnings("SameParameterValue") public class TestHiveDruidQueryBasedInputFormat {

  private static final String
      TIMESERIES_QUERY =
      "{  \"queryType\": \"timeseries\", "
          + " \"dataSource\": \"sample_datasource\", "
          + " \"granularity\": \"DAY\", "
          + " \"descending\": \"true\", "
          + " \"intervals\": [ \"2012-01-01T00:00:00.000-08:00/2012-01-03T00:00:00.000-08:00\" ]}";
  private static final String
      TIMESERIES_QUERY_SPLIT =
      "[HiveDruidSplit{{\"queryType\":\"timeseries\","
          + "\"dataSource\":{\"type\":\"table\",\"name\":\"sample_datasource\"},"
          + "\"intervals\":{\"type\":\"LegacySegmentSpec\",\"intervals\":"
          + "[\"2012-01-01T08:00:00.000Z/2012-01-03T08:00:00.000Z\"]},"
          + "\"descending\":true,"
          + "\"virtualColumns\":[],"
          + "\"filter\":null,"
          + "\"granularity\":\"DAY\","
          + "\"aggregations\":[],"
          + "\"postAggregations\":[],"
          + "\"limit\":2147483647,"
          + "\"context\":{\"queryId\":\"\"}}, [localhost:8082]}]";

  private static final String
      TOPN_QUERY =
      "{  \"queryType\": \"topN\", "
          + " \"dataSource\": \"sample_data\", "
          + " \"dimension\": \"sample_dim\", "
          + " \"threshold\": 5, "
          + " \"metric\": \"count\", "
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
          + " \"granularity\": \"all\", "
          + " \"intervals\": [  "
          + "  \"2013-08-31T00:00:00.000-07:00/2013-09-03T00:00:00.000-07:00\" "
          + " ]}";
  private static final String
      TOPN_QUERY_SPLIT =
      "[HiveDruidSplit{{\"queryType\":\"topN\","
          + "\"dataSource\":{\"type\":\"table\",\"name\":\"sample_data\"},"
          + "\"virtualColumns\":[],"
          + "\"dimension\":{\"type\":\"LegacyDimensionSpec\",\"dimension\":\"sample_dim\","
          + "\"outputName\":\"sample_dim\",\"outputType\":\"STRING\"},"
          + "\"metric\":{\"type\":\"LegacyTopNMetricSpec\",\"metric\":\"count\"},"
          + "\"threshold\":5,"
          + "\"intervals\":{\"type\":\"LegacySegmentSpec\",\"intervals\":[\"2013-08-31T07:00:00"
          + ".000Z/2013-09-03T07:00:00.000Z\"]},"
          + "\"filter\":null,"
          + "\"granularity\":{\"type\":\"all\"},"
          + "\"aggregations\":[{\"type\":\"longSum\",\"name\":\"count\",\"fieldName\":\"count\",\"expression\":null},"
          + "{\"type\":\"doubleSum\",\"name\":\"some_metric\",\"fieldName\":\"some_metric\",\"expression\":null}],"
          + "\"postAggregations\":[],"
          + "\"context\":{\"queryId\":\"\"},"
          + "\"descending\":false}, [localhost:8082]}]";

  private static final String
      GROUP_BY_QUERY =
      "{  \"queryType\": \"groupBy\", "
          + " \"dataSource\": \"sample_datasource\", "
          + " \"granularity\": \"day\", "
          + " \"dimensions\": [\"country\", \"device\"], "
          + " \"limitSpec\": {"
          + " \"type\": \"default\","
          + " \"limit\": 5000,"
          + " \"columns\": [\"country\", \"data_transfer\"] }, "
          + " \"aggregations\": [  "
          + "  { \"type\": \"longSum\", \"name\": \"total_usage\", \"fieldName\": \"user_count\" },  "
          + "  { \"type\": \"doubleSum\", \"name\": \"data_transfer\", \"fieldName\": \"data_transfer\" } "
          + " ], "
          + " \"intervals\": [ \"2012-01-01T00:00:00.000-08:00/2012-01-03T00:00:00.000-08:00\" ]"
          + " }";
  private static final String
      GROUP_BY_QUERY_SPLIT =
      "[HiveDruidSplit{{\"queryType\":\"groupBy\","
          + "\"dataSource\":{\"type\":\"table\",\"name\":\"sample_datasource\"},"
          + "\"intervals\":{\"type\":\"LegacySegmentSpec\",\"intervals\":[\"2012-01-01T08:00:00"
          + ".000Z/2012-01-03T08:00:00.000Z\"]},"
          + "\"virtualColumns\":[],"
          + "\"filter\":null,"
          + "\"granularity\":\"DAY\","
          + "\"dimensions\":[{\"type\":\"LegacyDimensionSpec\",\"dimension\":\"country\",\"outputName\":\"country\","
          + "\"outputType\":\"STRING\"},"
          + "{\"type\":\"LegacyDimensionSpec\",\"dimension\":\"device\",\"outputName\":\"device\","
          + "\"outputType\":\"STRING\"}],"
          + "\"aggregations\":[{\"type\":\"longSum\",\"name\":\"total_usage\",\"fieldName\":\"user_count\","
          + "\"expression\":null},"
          + "{\"type\":\"doubleSum\",\"name\":\"data_transfer\",\"fieldName\":\"data_transfer\",\"expression\":null}],"
          + "\"postAggregations\":[],"
          + "\"having\":null,"
          + "\"limitSpec\":{\"type\":\"default\",\"columns\":[{\"dimension\":\"country\",\"direction\":\"ascending\","
          + "\"dimensionOrder\":{\"type\":\"lexicographic\"}},"
          + "{\"dimension\":\"data_transfer\",\"direction\":\"ascending\","
          + "\"dimensionOrder\":{\"type\":\"lexicographic\"}}],\"limit\":5000},"
          + "\"context\":{\"queryId\":\"\"},"
          + "\"descending\":false}, [localhost:8082]}]";

  @Test
  public void testTimeZone() throws Exception {
    DruidQueryBasedInputFormat input = new DruidQueryBasedInputFormat();

    Method method1 = DruidQueryBasedInputFormat.class.getDeclaredMethod("getInputSplits", Configuration.class);
    method1.setAccessible(true);

    // Create, initialize, and test
    Configuration conf = createPropertiesQuery("sample_datasource", Query.TIMESERIES, TIMESERIES_QUERY);
    HiveDruidSplit[] resultSplits = (HiveDruidSplit[]) method1.invoke(input, conf);
    assertEquals(TIMESERIES_QUERY_SPLIT, Arrays.toString(resultSplits));

    conf = createPropertiesQuery("sample_datasource", Query.TOPN, TOPN_QUERY);
    resultSplits = (HiveDruidSplit[]) method1.invoke(input, conf);
    assertEquals(TOPN_QUERY_SPLIT, Arrays.toString(resultSplits));

    conf = createPropertiesQuery("sample_datasource", Query.GROUP_BY, GROUP_BY_QUERY);
    resultSplits = (HiveDruidSplit[]) method1.invoke(input, conf);
    assertEquals(GROUP_BY_QUERY_SPLIT, Arrays.toString(resultSplits));

  }

  private static Configuration createPropertiesQuery(String dataSource, String queryType, String jsonQuery) {
    Configuration conf = new Configuration();
    // Set the configuration parameters
    conf.set(FileInputFormat.INPUT_DIR, "/my/dir");
    conf.set(HiveConf.ConfVars.HIVE_DRUID_BROKER_DEFAULT_ADDRESS.varname, "localhost:8082");
    conf.set(Constants.DRUID_DATA_SOURCE, dataSource);
    conf.set(Constants.DRUID_QUERY_JSON, jsonQuery);
    conf.set(Constants.DRUID_QUERY_TYPE, queryType);
    return conf;
  }

}
