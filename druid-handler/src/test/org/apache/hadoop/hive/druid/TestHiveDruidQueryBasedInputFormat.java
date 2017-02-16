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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.druid.io.DruidQueryBasedInputFormat;
import org.apache.hadoop.hive.druid.io.HiveDruidSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.joda.time.Interval;
import org.joda.time.chrono.ISOChronology;
import org.junit.Test;

import io.druid.query.Query;
import junit.framework.TestCase;

public class TestHiveDruidQueryBasedInputFormat extends TestCase {

  @SuppressWarnings("unchecked")
  @Test
  public void testCreateSplitsIntervals() throws Exception {
    DruidQueryBasedInputFormat input = new DruidQueryBasedInputFormat();

    Method method1 = DruidQueryBasedInputFormat.class.getDeclaredMethod("createSplitsIntervals",
            List.class, int.class
    );
    method1.setAccessible(true);

    List<Interval> intervals;
    List<List<Interval>> resultList;
    List<List<Interval>> expectedResultList;

    // Test 1 : single split, create 4
    intervals = new ArrayList<>();
    intervals.add(new Interval(1262304000000L, 1293840000000L, ISOChronology.getInstanceUTC()));
    resultList = (List<List<Interval>>) method1.invoke(input, intervals, 4);
    expectedResultList = new ArrayList<>();
    expectedResultList.add(Arrays
            .asList(new Interval(1262304000000L, 1270188000000L, ISOChronology.getInstanceUTC())));
    expectedResultList.add(Arrays
            .asList(new Interval(1270188000000L, 1278072000000L, ISOChronology.getInstanceUTC())));
    expectedResultList.add(Arrays
            .asList(new Interval(1278072000000L, 1285956000000L, ISOChronology.getInstanceUTC())));
    expectedResultList.add(Arrays
            .asList(new Interval(1285956000000L, 1293840000000L, ISOChronology.getInstanceUTC())));
    assertEquals(expectedResultList, resultList);

    // Test 2 : two splits, create 4
    intervals = new ArrayList<>();
    intervals.add(new Interval(1262304000000L, 1293840000000L, ISOChronology.getInstanceUTC()));
    intervals.add(new Interval(1325376000000L, 1356998400000L, ISOChronology.getInstanceUTC()));
    resultList = (List<List<Interval>>) method1.invoke(input, intervals, 4);
    expectedResultList = new ArrayList<>();
    expectedResultList.add(Arrays
            .asList(new Interval(1262304000000L, 1278093600000L, ISOChronology.getInstanceUTC())));
    expectedResultList.add(Arrays
            .asList(new Interval(1278093600000L, 1293840000000L, ISOChronology.getInstanceUTC()),
                    new Interval(1325376000000L, 1325419200000L, ISOChronology.getInstanceUTC())
            ));
    expectedResultList.add(Arrays
            .asList(new Interval(1325419200000L, 1341208800000L, ISOChronology.getInstanceUTC())));
    expectedResultList.add(Arrays
            .asList(new Interval(1341208800000L, 1356998400000L, ISOChronology.getInstanceUTC())));
    assertEquals(expectedResultList, resultList);

    // Test 3 : two splits, create 5
    intervals = new ArrayList<>();
    intervals.add(new Interval(1262304000000L, 1293840000000L, ISOChronology.getInstanceUTC()));
    intervals.add(new Interval(1325376000000L, 1356998400000L, ISOChronology.getInstanceUTC()));
    resultList = (List<List<Interval>>) method1.invoke(input, intervals, 5);
    expectedResultList = new ArrayList<>();
    expectedResultList.add(Arrays
            .asList(new Interval(1262304000000L, 1274935680000L, ISOChronology.getInstanceUTC())));
    expectedResultList.add(Arrays
            .asList(new Interval(1274935680000L, 1287567360000L, ISOChronology.getInstanceUTC())));
    expectedResultList.add(Arrays
            .asList(new Interval(1287567360000L, 1293840000000L, ISOChronology.getInstanceUTC()),
                    new Interval(1325376000000L, 1331735040000L, ISOChronology.getInstanceUTC())
            ));
    expectedResultList.add(Arrays
            .asList(new Interval(1331735040000L, 1344366720000L, ISOChronology.getInstanceUTC())));
    expectedResultList.add(Arrays
            .asList(new Interval(1344366720000L, 1356998400000L, ISOChronology.getInstanceUTC())));
    assertEquals(expectedResultList, resultList);

    // Test 4 : three splits, different ranges, create 6
    intervals = new ArrayList<>();
    intervals.add(new Interval(1199145600000L, 1201824000000L,
            ISOChronology.getInstanceUTC()
    )); // one month
    intervals.add(new Interval(1325376000000L, 1356998400000L,
            ISOChronology.getInstanceUTC()
    )); // one year
    intervals.add(new Interval(1407283200000L, 1407888000000L,
            ISOChronology.getInstanceUTC()
    )); // 7 days
    resultList = (List<List<Interval>>) method1.invoke(input, intervals, 6);
    expectedResultList = new ArrayList<>();
    expectedResultList.add(Arrays
            .asList(new Interval(1199145600000L, 1201824000000L, ISOChronology.getInstanceUTC()),
                    new Interval(1325376000000L, 1328515200000L, ISOChronology.getInstanceUTC())
            ));
    expectedResultList.add(Arrays
            .asList(new Interval(1328515200000L, 1334332800000L, ISOChronology.getInstanceUTC())));
    expectedResultList.add(Arrays
            .asList(new Interval(1334332800000L, 1340150400000L, ISOChronology.getInstanceUTC())));
    expectedResultList.add(Arrays
            .asList(new Interval(1340150400000L, 1345968000000L, ISOChronology.getInstanceUTC())));
    expectedResultList.add(Arrays
            .asList(new Interval(1345968000000L, 1351785600000L, ISOChronology.getInstanceUTC())));
    expectedResultList.add(Arrays
            .asList(new Interval(1351785600000L, 1356998400000L, ISOChronology.getInstanceUTC()),
                    new Interval(1407283200000L, 1407888000000L, ISOChronology.getInstanceUTC())
            ));
    assertEquals(expectedResultList, resultList);
  }

  private static final String TIMESERIES_QUERY =
      "{  \"queryType\": \"timeseries\", "
          + " \"dataSource\": \"sample_datasource\", "
          + " \"granularity\": \"day\", "
          + " \"descending\": \"true\", "
          + " \"intervals\": [ \"2012-01-01T00:00:00.000/2012-01-03T00:00:00.000\" ]}";
  private static final String TIMESERIES_QUERY_SPLIT =
      "[HiveDruidSplit{{\"queryType\":\"timeseries\","
          + "\"dataSource\":{\"type\":\"table\",\"name\":\"sample_datasource\"},"
          + "\"intervals\":{\"type\":\"LegacySegmentSpec\",\"intervals\":[\"2012-01-01T00:00:00.000-08:00/2012-01-03T00:00:00.000-08:00\"]},"
          + "\"descending\":true,"
          + "\"filter\":null,"
          + "\"granularity\":{\"type\":\"duration\",\"duration\":86400000,\"origin\":\"1969-12-31T16:00:00.000-08:00\"},"
          + "\"aggregations\":[],"
          + "\"postAggregations\":[],"
          + "\"context\":null}, [localhost:8082]}]";

  private static final String TOPN_QUERY =
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
          + "  \"2013-08-31T00:00:00.000/2013-09-03T00:00:00.000\" "
          + " ]}";
  private static final String TOPN_QUERY_SPLIT =
      "[HiveDruidSplit{{\"queryType\":\"topN\","
          + "\"dataSource\":{\"type\":\"table\",\"name\":\"sample_data\"},"
          + "\"dimension\":{\"type\":\"LegacyDimensionSpec\",\"dimension\":\"sample_dim\",\"outputName\":\"sample_dim\"},"
          + "\"metric\":{\"type\":\"LegacyTopNMetricSpec\",\"metric\":\"count\"},"
          + "\"threshold\":5,"
          + "\"intervals\":{\"type\":\"LegacySegmentSpec\",\"intervals\":[\"2013-08-31T00:00:00.000-07:00/2013-09-03T00:00:00.000-07:00\"]},"
          + "\"filter\":null,"
          + "\"granularity\":{\"type\":\"all\"},"
          + "\"aggregations\":[{\"type\":\"longSum\",\"name\":\"count\",\"fieldName\":\"count\"},"
          + "{\"type\":\"doubleSum\",\"name\":\"some_metric\",\"fieldName\":\"some_metric\"}],"
          + "\"postAggregations\":[],"
          + "\"context\":null,"
          + "\"descending\":false}, [localhost:8082]}]";

  private static final String GROUP_BY_QUERY =
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
          + " \"intervals\": [ \"2012-01-01T00:00:00.000/2012-01-03T00:00:00.000\" ]"
          + " }";
  private static final String GROUP_BY_QUERY_SPLIT =
      "[HiveDruidSplit{{\"queryType\":\"groupBy\","
          + "\"dataSource\":{\"type\":\"table\",\"name\":\"sample_datasource\"},"
          + "\"intervals\":{\"type\":\"LegacySegmentSpec\",\"intervals\":[\"2012-01-01T00:00:00.000-08:00/2012-01-03T00:00:00.000-08:00\"]},"
          + "\"filter\":null,"
          + "\"granularity\":{\"type\":\"duration\",\"duration\":86400000,\"origin\":\"1969-12-31T16:00:00.000-08:00\"},"
          + "\"dimensions\":[{\"type\":\"LegacyDimensionSpec\",\"dimension\":\"country\",\"outputName\":\"country\"},"
          + "{\"type\":\"LegacyDimensionSpec\",\"dimension\":\"device\",\"outputName\":\"device\"}],"
          + "\"aggregations\":[{\"type\":\"longSum\",\"name\":\"total_usage\",\"fieldName\":\"user_count\"},"
          + "{\"type\":\"doubleSum\",\"name\":\"data_transfer\",\"fieldName\":\"data_transfer\"}],"
          + "\"postAggregations\":[],"
          + "\"having\":null,"
          + "\"limitSpec\":{\"type\":\"default\",\"columns\":[{\"dimension\":\"country\",\"direction\":\"ascending\",\"dimensionOrder\":{\"type\":\"lexicographic\"}},"
          + "{\"dimension\":\"data_transfer\",\"direction\":\"ascending\",\"dimensionOrder\":{\"type\":\"lexicographic\"}}],\"limit\":5000},"
          + "\"context\":null,"
          + "\"descending\":false}, [localhost:8082]}]";

  private static final String SELECT_QUERY =
      "{   \"queryType\": \"select\",  "
          + " \"dataSource\": \"wikipedia\",   \"descending\": \"false\",  "
          + " \"dimensions\":[\"robot\",\"namespace\",\"anonymous\",\"unpatrolled\",\"page\",\"language\",\"newpage\",\"user\"],  "
          + " \"metrics\":[\"count\",\"added\",\"delta\",\"variation\",\"deleted\"],  "
          + " \"granularity\": \"all\",  "
          + " \"intervals\": [     \"2013-01-01/2013-01-02\"   ],  "
          + " \"pagingSpec\":{\"pagingIdentifiers\": {}, \"threshold\":5}, "
          + " \"context\":{\"druid.query.fetch\":true}}";
  private static final String SELECT_QUERY_SPLIT =
      "[HiveDruidSplit{{\"queryType\":\"select\","
          + "\"dataSource\":{\"type\":\"table\",\"name\":\"wikipedia\"},"
          + "\"intervals\":{\"type\":\"LegacySegmentSpec\",\"intervals\":[\"2013-01-01T00:00:00.000-08:00/2013-01-02T00:00:00.000-08:00\"]},"
          + "\"descending\":false,"
          + "\"filter\":null,"
          + "\"granularity\":{\"type\":\"all\"},"
          + "\"dimensions\":[{\"type\":\"LegacyDimensionSpec\",\"dimension\":\"robot\",\"outputName\":\"robot\"},"
          + "{\"type\":\"LegacyDimensionSpec\",\"dimension\":\"namespace\",\"outputName\":\"namespace\"},"
          + "{\"type\":\"LegacyDimensionSpec\",\"dimension\":\"anonymous\",\"outputName\":\"anonymous\"},"
          + "{\"type\":\"LegacyDimensionSpec\",\"dimension\":\"unpatrolled\",\"outputName\":\"unpatrolled\"},"
          + "{\"type\":\"LegacyDimensionSpec\",\"dimension\":\"page\",\"outputName\":\"page\"},"
          + "{\"type\":\"LegacyDimensionSpec\",\"dimension\":\"language\",\"outputName\":\"language\"},"
          + "{\"type\":\"LegacyDimensionSpec\",\"dimension\":\"newpage\",\"outputName\":\"newpage\"},"
          + "{\"type\":\"LegacyDimensionSpec\",\"dimension\":\"user\",\"outputName\":\"user\"}],"
          + "\"metrics\":[\"count\",\"added\",\"delta\",\"variation\",\"deleted\"],"
          + "\"pagingSpec\":{\"pagingIdentifiers\":{},\"threshold\":5,\"fromNext\":false},"
          + "\"context\":{\"druid.query.fetch\":true}}, [localhost:8082]}]";

  @Test
  public void testTimeZone() throws Exception {
    DruidQueryBasedInputFormat input = new DruidQueryBasedInputFormat();

    Method method1 = DruidQueryBasedInputFormat.class.getDeclaredMethod(
            "getInputSplits", Configuration.class);
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

    conf = createPropertiesQuery("sample_datasource", Query.SELECT, SELECT_QUERY);
    resultSplits = (HiveDruidSplit[]) method1.invoke(input, conf);
    assertEquals(SELECT_QUERY_SPLIT, Arrays.toString(resultSplits));
  }

  private static Configuration createPropertiesQuery(String dataSource, String queryType,
          String jsonQuery) {
    Configuration conf = new Configuration();
    // Set the configuration parameters
    conf.set(FileInputFormat.INPUT_DIR, "/my/dir");
    conf.set(HiveConf.ConfVars.HIVE_DRUID_BROKER_DEFAULT_ADDRESS.varname, "localhost:8082");
    conf.set(Constants.DRUID_DATA_SOURCE, dataSource);
    conf.set(Constants.DRUID_QUERY_JSON, jsonQuery);
    conf.set(Constants.DRUID_QUERY_TYPE, queryType);
    conf.setBoolean(HiveConf.ConfVars.HIVE_DRUID_SELECT_DISTRIBUTE.varname, false);
    return conf;
  }

}
