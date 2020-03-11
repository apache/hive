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

package org.apache.hive.jdbc;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.commons.collections4.MultiSet;
import org.apache.commons.collections4.multiset.HashMultiSet;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.LlapArrowRowInputFormat;
import org.apache.hadoop.hive.llap.LlapBaseInputFormat;
import org.apache.hadoop.hive.llap.Row;
import org.apache.hadoop.hive.ql.io.arrow.ArrowWrapperWritable;
import org.apache.hadoop.hive.ql.io.arrow.RootAllocatorFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * TestJdbcWithMiniLlap for Arrow format - uses batch record reader.
 * We can obtain arrow batches and compare the results.
 *
 */
public class TestJdbcWithMiniLlapVectorArrowBatch extends BaseJdbcWithMiniLlap {

  private final DateTimestampTestIO dateTimestampTestIO = new DateTimestampTestIO();

  @BeforeClass public static void beforeTest() throws Exception {
    HiveConf conf = defaultConf();
    conf.setBoolVar(ConfVars.LLAP_OUTPUT_FORMAT_ARROW, true);
    conf.setBoolVar(ConfVars.HIVE_VECTORIZATION_FILESINK_ARROW_NATIVE_ENABLED, true);
    BaseJdbcWithMiniLlap.beforeTest(conf);
  }

  @Override protected InputFormat<NullWritable, Row> getInputFormat() {
    return new LlapArrowRowInputFormat(Long.MAX_VALUE);
  }

  private static class DateTimestampTestIO {

    final List<String> inputDates;
    final MultiSet<List<Object>> expectedHybridDates;

    final List<String> inputTimestamps;
    final MultiSet<List<Object>> expectedHybridTimestamps;

    public DateTimestampTestIO() {
      // date stuff
      this.inputDates = Arrays
          .asList("2012-02-21", "2014-02-11", "1947-02-11", "8200-02-11", "1012-02-21", "1014-02-11", "0947-02-11",
              "0200-02-11", "0001-01-01");

      expectedHybridDates = new HashMultiSet<>();
      expectedHybridDates.add(Lists.newArrayList(15391));
      expectedHybridDates.add(Lists.newArrayList(16112));
      expectedHybridDates.add(Lists.newArrayList(-8360));
      expectedHybridDates.add(Lists.newArrayList(2275502));
      expectedHybridDates.add(Lists.newArrayList(-349846));
      expectedHybridDates.add(Lists.newArrayList(-349125));
      expectedHybridDates.add(Lists.newArrayList(-373597));
      expectedHybridDates.add(Lists.newArrayList(-646439));
      expectedHybridDates.add(Lists.newArrayList(-719164));

      // timestamp stuff

      inputTimestamps = Arrays.asList("2012-02-21 07:08:09.123", "2014-02-11 07:08:09.123", "1947-02-11 07:08:09.123",
          "8200-02-11 07:08:09.123", "1012-02-21 07:15:11.123", "1014-02-11 07:15:11.123", "947-02-11 07:15:11.123",
          "0200-02-11 07:15:11.123", "0001-01-01 00:00:00");
      expectedHybridTimestamps = new HashMultiSet<>();
      expectedHybridTimestamps.add(Lists.newArrayList(1329808089123000L));
      expectedHybridTimestamps.add(Lists.newArrayList(1392102489123000L));
      expectedHybridTimestamps.add(Lists.newArrayList(-722278310877000L));
      expectedHybridTimestamps.add(Lists.newArrayList(196603398489123000L));
      expectedHybridTimestamps.add(Lists.newArrayList(-30226668288877000L));
      expectedHybridTimestamps.add(Lists.newArrayList(-30164373888877000L));
      expectedHybridTimestamps.add(Lists.newArrayList(-32278754688877000L));
      expectedHybridTimestamps.add(Lists.newArrayList(-55852303488877000L));
      expectedHybridTimestamps.add(Lists.newArrayList(-62135769600000000L));

    }

    private String getInsertQuery(String tableName, boolean dateQuery) {
      String format = "INSERT INTO %s VALUES %s";
      List<String> inputs = dateQuery ? inputDates : inputTimestamps;
      String values = inputs.stream().map(d -> String.format("('%s')", d)).collect(Collectors.joining(","));
      return String.format(format, tableName, values);
    }
  }

  @Test public void testDates() throws Exception {
    String tableName = "test_dates";
    testDateTimestampWithStorageFormat(tableName, "orc",
        ImmutableMap.of(ConfVars.LLAP_EXTERNAL_CLIENT_USE_HYBRID_CALENDAR.toString(), "true"), true);
    testDateTimestampWithStorageFormat(tableName, "parquet",
        ImmutableMap.of(ConfVars.LLAP_EXTERNAL_CLIENT_USE_HYBRID_CALENDAR.toString(), "true"), true);
  }

  @Test public void testTimestamps() throws Exception {
    String tableName = "test_timestamps";
    testDateTimestampWithStorageFormat(tableName, "orc",
        ImmutableMap.of(ConfVars.LLAP_EXTERNAL_CLIENT_USE_HYBRID_CALENDAR.toString(), "true"), false);
    testDateTimestampWithStorageFormat(tableName, "parquet",
        ImmutableMap.of(ConfVars.LLAP_EXTERNAL_CLIENT_USE_HYBRID_CALENDAR.toString(), "true"), false);
  }

  private void testDateTimestampWithStorageFormat(String tableName, String storageFormat,
      Map<String, String> extraHiveConfs, boolean testDate) throws Exception {

    String createTableQuery =
        String.format("create table %s (d %s) stored as %s", tableName, testDate ? "date" : "timestamp", storageFormat);

    executeSQL(createTableQuery, dateTimestampTestIO.getInsertQuery(tableName, testDate));

    MultiSet<List<Object>> llapResult = runQueryUsingLlapArrowBatchReader("select * from " + tableName, extraHiveConfs);
    assertEquals(testDate ? dateTimestampTestIO.expectedHybridDates : dateTimestampTestIO.expectedHybridTimestamps,
        llapResult);
    executeSQL("drop table " + tableName);
  }

  private MultiSet<List<Object>> executeQuerySQL(String query) throws SQLException {
    MultiSet<List<Object>> rows = new HashMultiSet<>();
    try (Statement stmt = hs2Conn.createStatement()) {
      ResultSet rs = stmt.executeQuery(query);
      int columnCount = rs.getMetaData().getColumnCount();
      while (rs.next()) {
        List<Object> oneRow = new ArrayList<>();
        for (int i = 1; i <= columnCount; i++) {
          oneRow.add(rs.getObject(i));
        }
        rows.add(oneRow);
      }
    }
    return rows;
  }

  private void executeSQL(String query, String... moreQueries) throws SQLException {
    try (Statement stmt = hs2Conn.createStatement()) {
      stmt.execute(query);
      if (moreQueries != null) {
        for (String q : moreQueries) {
          stmt.execute(q);
        }
      }
    }
  }

  private MultiSet<List<Object>> runQueryUsingLlapArrowBatchReader(String query, Map<String, String> extraHiveConfs)
      throws Exception {
    String url = miniHS2.getJdbcURL();

    if (extraHiveConfs != null) {
      url = url + "?" + extraHiveConfs.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue())
          .collect(Collectors.joining(";"));
    }

    String user = System.getProperty("user.name");
    String pwd = user;
    String handleId = UUID.randomUUID().toString();

    // Get splits
    JobConf job = new JobConf(conf);
    job.set(LlapBaseInputFormat.URL_KEY, url);
    job.set(LlapBaseInputFormat.USER_KEY, user);
    job.set(LlapBaseInputFormat.PWD_KEY, pwd);
    job.set(LlapBaseInputFormat.QUERY_KEY, query);
    job.set(LlapBaseInputFormat.HANDLE_ID, handleId);
    job.set(LlapBaseInputFormat.USE_NEW_SPLIT_FORMAT, "false");

    BufferAllocator allocator = RootAllocatorFactory.INSTANCE.getOrCreateRootAllocator(Long.MAX_VALUE)
        .newChildAllocator(UUID.randomUUID().toString(), 0, Long.MAX_VALUE);

    LlapBaseInputFormat llapBaseInputFormat = new LlapBaseInputFormat(true, allocator);
    InputSplit[] splits = llapBaseInputFormat.getSplits(job, 1);

    assertTrue(splits.length > 0);

    MultiSet<List<Object>> queryResult = new HashMultiSet<>();
    for (InputSplit split : splits) {
      System.out.println("Processing split " + Arrays.toString(split.getLocations()));
      RecordReader<NullWritable, ArrowWrapperWritable> reader = llapBaseInputFormat.getRecordReader(split, job, null);
      ArrowWrapperWritable wrapperWritable = new ArrowWrapperWritable();
      while (reader.next(NullWritable.get(), wrapperWritable)) {
        queryResult.addAll(collectResultFromArrowVector(wrapperWritable));
      }
      reader.close();
    }
    LlapBaseInputFormat.close(handleId);
    return queryResult;
  }

  private MultiSet<List<Object>> collectResultFromArrowVector(ArrowWrapperWritable wrapperWritable) {
    List<FieldVector> fieldVectors = wrapperWritable.getVectorSchemaRoot().getFieldVectors();
    MultiSet<List<Object>> result = new HashMultiSet<>();
    int valueCount = fieldVectors.get(0).getValueCount();
    for (int recordIndex = 0; recordIndex < valueCount; recordIndex++) {
      List<Object> row = new ArrayList<>();
      for (FieldVector fieldVector : fieldVectors) {
        row.add(fieldVector.getObject(recordIndex));
      }
      result.add(row);
    }
    return result;
  }

  @Override public void testLlapInputFormatEndToEnd() throws Exception {
    // to be implemented for this reader
  }

  @Override public void testNonAsciiStrings() throws Exception {
    // to be implemented for this reader
  }

  @Override public void testEscapedStrings() throws Exception {
    // to be implemented for this reader
  }

  @Override public void testDataTypes() throws Exception {
    // to be implemented for this reader
  }

  @Override public void testComplexQuery() throws Exception {
    // to be implemented for this reader
  }

  @Override public void testKillQuery() throws Exception {
    // to be implemented for this reader
  }

}

