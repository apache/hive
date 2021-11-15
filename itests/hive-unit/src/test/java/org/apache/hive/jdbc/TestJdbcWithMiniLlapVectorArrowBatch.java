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
import org.apache.hadoop.hive.common.type.CalendarUtils;
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
import org.junit.Ignore;
import org.junit.Test;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Map;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_AVRO_PROLEPTIC_GREGORIAN_DEFAULT;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_PARQUET_DATE_PROLEPTIC_GREGORIAN_DEFAULT;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.LLAP_EXTERNAL_CLIENT_USE_HYBRID_CALENDAR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * TestJdbcWithMiniLlap for Arrow format - uses batch record reader.
 * We can obtain arrow batches and compare the results.
 *
 */
public class TestJdbcWithMiniLlapVectorArrowBatch extends BaseJdbcWithMiniLlap {

  private final MultiSet<List<Object>> legacyDateExpectedOutput = getLegacyDateExpectedOutput();
  private final MultiSet<List<Object>> hybridMixedDateExpectedOutput = getHybridMixedDateExpectedOutput();

  private final MultiSet<List<Object>> legacyTimestampExpectedOutput = getLegacyTimestampExpectedOutput();
  private final MultiSet<List<Object>> convertedLegacyTimestampExpectedOutput =
      getConvertedLegacyTimestampExpectedOutput();
  private final MultiSet<List<Object>> hybridMixedTimestampExpectedOutput = getHybridMixedTimestampExpectedOutput();

  @BeforeClass public static void beforeTest() throws Exception {
    HiveConf conf = defaultConf();
    conf.setBoolVar(ConfVars.LLAP_OUTPUT_FORMAT_ARROW, true);
    conf.setBoolVar(ConfVars.HIVE_VECTORIZATION_FILESINK_ARROW_NATIVE_ENABLED, true);
    BaseJdbcWithMiniLlap.beforeTest(conf);
  }

  @Override protected InputFormat<NullWritable, Row> getInputFormat() {
    return new LlapArrowRowInputFormat(Long.MAX_VALUE);
  }

  private MultiSet<List<Object>> getHybridMixedDateExpectedOutput() {
    MultiSet<List<Object>> expected = new HashMultiSet<>();
    expected.add(Lists.newArrayList("2012-02-21"));
    expected.add(Lists.newArrayList("2014-02-11"));
    expected.add(Lists.newArrayList("1947-02-11"));
    expected.add(Lists.newArrayList("8200-02-11"));
    expected.add(Lists.newArrayList("1012-02-21"));
    expected.add(Lists.newArrayList("1014-02-11"));
    expected.add(Lists.newArrayList("0947-02-11"));
    expected.add(Lists.newArrayList("0200-02-11"));
    return expected;
  }

  private MultiSet<List<Object>> getLegacyDateExpectedOutput() {
    MultiSet<List<Object>> expected = new HashMultiSet<>();
    expected.add(Lists.newArrayList("2012-02-21"));
    expected.add(Lists.newArrayList("2014-02-11"));
    expected.add(Lists.newArrayList("1947-02-11"));
    expected.add(Lists.newArrayList("8200-02-11"));
    expected.add(Lists.newArrayList("1012-02-27"));
    expected.add(Lists.newArrayList("1014-02-17"));
    expected.add(Lists.newArrayList("0947-02-16"));
    expected.add(Lists.newArrayList("0200-02-10"));
    return expected;
  }

  private MultiSet<List<Object>> getHybridMixedTimestampExpectedOutput() {
    MultiSet<List<Object>> expected = new HashMultiSet<>();
    expected.add(Lists.newArrayList("2012-02-21 07:08:09.123"));
    expected.add(Lists.newArrayList("2014-02-11 07:08:09.123"));
    expected.add(Lists.newArrayList("1947-02-11 07:08:09.123"));
    expected.add(Lists.newArrayList("8200-02-11 07:08:09.123"));
    expected.add(Lists.newArrayList("1012-02-21 07:08:09.123"));
    expected.add(Lists.newArrayList("1014-02-11 07:08:09.123"));
    expected.add(Lists.newArrayList("0947-02-11 07:08:09.123"));
    expected.add(Lists.newArrayList("0200-02-11 07:08:09.123"));
    return expected;
  }

  private MultiSet<List<Object>> getLegacyTimestampExpectedOutput() {
    MultiSet<List<Object>> expected = new HashMultiSet<>();
    expected.add(Lists.newArrayList("2012-02-21 07:08:09.123"));
    expected.add(Lists.newArrayList("2014-02-11 07:08:09.123"));
    expected.add(Lists.newArrayList("1947-02-11 07:08:09.123"));
    expected.add(Lists.newArrayList("8200-02-11 07:08:09.123"));
    expected.add(Lists.newArrayList("1012-02-21 07:08:09.123"));
    expected.add(Lists.newArrayList("1014-02-11 07:08:09.123"));
    expected.add(Lists.newArrayList("0947-02-11 07:08:09.123"));
    expected.add(Lists.newArrayList("0200-02-11 07:08:09.123"));
    return expected;
  }

  private MultiSet<List<Object>> getConvertedLegacyTimestampExpectedOutput() {
    MultiSet<List<Object>> expected = new HashMultiSet<>();
    expected.add(Lists.newArrayList("2012-02-21 07:08:09.123"));
    expected.add(Lists.newArrayList("2014-02-11 07:08:09.123"));
    expected.add(Lists.newArrayList("1947-02-11 07:08:09.123"));
    expected.add(Lists.newArrayList("8200-02-11 07:08:09.123"));
    expected.add(Lists.newArrayList("1012-02-27 07:08:09.123"));
    expected.add(Lists.newArrayList("1014-02-17 07:08:09.123"));
    expected.add(Lists.newArrayList("0947-02-16 07:08:09.123"));
    expected.add(Lists.newArrayList("0200-02-10 07:08:09.123"));
    return expected;
  }

  // test newly inserted orc records which have calendar info in orc footer.
  // similar to ql/src/test/queries/clientpositive/orc_hybrid_mixed_date.q
  @Test public void testOrcHybridMixedDates() throws Exception {

    final String tableName = "testOrcHybridMixedDates";
    executeSQL("create table " + tableName + " (d date) stored as orc");
    executeSQL("INSERT INTO " + tableName + " VALUES " + "('2012-02-21'), " + "('2014-02-11'), " + "('1947-02-11'), "
        + "('8200-02-11'), " + "('1012-02-21'), " + "('1014-02-11'), " + "('0947-02-11'), " + "('0200-02-11')");

    final String query = "select * from " + tableName;

    testDateQueries(query, "orc.proleptic.gregorian.default", hybridMixedDateExpectedOutput,
        hybridMixedDateExpectedOutput);
  }

  // test with legacy orc files
  // similar to ql/src/test/queries/clientpositive/orc_legacy_mixed_date.q
  @Test public void testOrcLegacyMixedDates() throws Exception {

    final String tableName = "testOrcLegacyMixedDates";
    executeSQL("create table " + tableName + " (d date) stored as orc");
    executeSQL("load data local inpath '../../data/files/orc_legacy_mixed_dates.orc' into table " + tableName);

    final String query = "select * from " + tableName;

    // ORC properties (here orc.proleptic.gregorian.default) are not propogated to LLAP as of now
    // and hence the expected output hybridMixedDateExpectedOutput otherwise it should be hybridMixedDateExpectedOutput ideally
    testDateQueries(query, "orc.proleptic.gregorian.default", hybridMixedDateExpectedOutput,
        hybridMixedDateExpectedOutput);
  }

  // test newly inserted orc records which have calendar info in orc footer.
  // similar to ql/src/test/queries/clientpositive/orc_hybrid_mixed_timestamp.q
  @Test public void testOrcHybridMixedTimestamps() throws Exception {

    final String tableName = "testOrcHybridMixedTimestamps";
    executeSQL("create table " + tableName + " (d timestamp) stored as orc");
    executeSQL("INSERT INTO " + tableName + " VALUES ('2012-02-21 07:08:09.123')," + "('2014-02-11 07:08:09.123'),"
        + "('1947-02-11 07:08:09.123')," + "('8200-02-11 07:08:09.123')," + "('1012-02-21 07:08:09.123'),"
        + "('1014-02-11 07:08:09.123')," + "('0947-02-11 07:08:09.123')," + "('0200-02-11 07:08:09.123')");

    final String query = "select * from " + tableName;

    testTimestampQueries(query, "orc.proleptic.gregorian.default", hybridMixedTimestampExpectedOutput,
        hybridMixedTimestampExpectedOutput);
  }

  // test with legacy parquet files
  // similar to ql/src/test/queries/clientpositive/orc_legacy_mixed_timestamp.q
  @Test public void testOrcLegacyMixedTimestamps() throws Exception {

    final String tableName = "testOrcLegacyMixedTimestamps";
    executeSQL("create table " + tableName + " (ts timestamp) stored as orc");
    executeSQL("load data local inpath '../../data/files/orc_legacy_mixed_timestamps.orc' into table " + tableName);

    final String query = "select * from " + tableName;

    // ORC properties (here orc.proleptic.gregorian.default) are not propogated to LLAP as of now
    testTimestampQueries(query, "orc.proleptic.gregorian.default", legacyTimestampExpectedOutput,
        legacyTimestampExpectedOutput);
  }

  // test with new parquet files
  // similar to ql/src/test/queries/clientpositive/parquet_hybrid_mixed_date.q
  @Test public void testParquetHybridMixedDates() throws Exception {

    final String tableName = "testParquetHybrcidMixedDates";
    executeSQL("create table " + tableName + " (d date) stored as parquet");
    executeSQL("INSERT INTO " + tableName + " VALUES " + "('2012-02-21'), " + "('2014-02-11'), " + "('1947-02-11'), "
        + "('8200-02-11'), " + "('1012-02-21'), " + "('1014-02-11'), " + "('0947-02-11'), " + "('0200-02-11')");

    final String query = "select * from " + tableName;

    testDateQueries(query, HIVE_PARQUET_DATE_PROLEPTIC_GREGORIAN_DEFAULT.toString(), hybridMixedDateExpectedOutput,
        hybridMixedDateExpectedOutput);
  }

  // test with legacy parquet files
  // similar to ql/src/test/queries/clientpositive/parquet_legacy_mixed_date.q
  @Test public void testParquetLegacyMixedDates() throws Exception {

    final String tableName = "testParquetLegacyMixedDates";

    executeSQL("create table " + tableName + " (d date) stored as parquet");
    executeSQL("load data local inpath '../../data/files/parquet_legacy_mixed_dates.parq' into table " + tableName);

    final String query = "select * from " + tableName;

    testDateQueries(query, HIVE_PARQUET_DATE_PROLEPTIC_GREGORIAN_DEFAULT.toString(), hybridMixedDateExpectedOutput,
        legacyDateExpectedOutput);
  }

  // test newly inserted parquet records.
  // similar to ql/src/test/queries/clientpositive/parquet_hybrid_mixed_timestamp.q
  @Test public void testParquetHybridMixedTimestamps() throws Exception {

    final String tableName = "testParquetHybridMixedTimestamps";
    executeSQL("create table " + tableName + " (ts timestamp) stored as parquet");
    executeSQL("INSERT INTO " + tableName + " VALUES ('2012-02-21 07:08:09.123')," + "('2014-02-11 07:08:09.123'),"
        + "('1947-02-11 07:08:09.123')," + "('8200-02-11 07:08:09.123')," + "('1012-02-21 07:08:09.123'),"
        + "('1014-02-11 07:08:09.123')," + "('0947-02-11 07:08:09.123')," + "('0200-02-11 07:08:09.123')");

    final String query = "select * from " + tableName;

    testTimestampQueries(query, HIVE_PARQUET_DATE_PROLEPTIC_GREGORIAN_DEFAULT.toString(),
        hybridMixedTimestampExpectedOutput, hybridMixedTimestampExpectedOutput);
  }

  // test with legacy parquet files
  // similar to ql/src/test/queries/clientpositive/parquet_legacy_mixed_timestamp.q
  @Test public void testParquetLegacyMixedTimestamps() throws Exception {

    final String tableName = "testParquetLegacyMixedTimestamps";
    executeSQL("create table " + tableName + " (d timestamp) stored as parquet");
    executeSQL(
        "load data local inpath '../../data/files/parquet_legacy_mixed_timestamps.parq' into table " + tableName);

    final String query = "select * from " + tableName;

    testTimestampQueries(query, HIVE_PARQUET_DATE_PROLEPTIC_GREGORIAN_DEFAULT.toString(),
        hybridMixedTimestampExpectedOutput, hybridMixedTimestampExpectedOutput);
  }

  // test with new avro files
  // similar to ql/src/test/queries/clientpositive/avro_hybrid_mixed_date.q
  @Test public void testAvroHybridMixedDates() throws Exception {

    final String tableName = "testAvroHybridMixedDates";
    executeSQL("create table " + tableName + " (d date) stored as avro");
    executeSQL("INSERT INTO " + tableName + " VALUES " + "('2012-02-21'), " + "('2014-02-11'), " + "('1947-02-11'), "
        + "('8200-02-11'), " + "('1012-02-21'), " + "('1014-02-11'), " + "('0947-02-11'), " + "('0200-02-11')");

    final String query = "select * from " + tableName;

    testDateQueries(query, HIVE_AVRO_PROLEPTIC_GREGORIAN_DEFAULT.toString(), hybridMixedDateExpectedOutput,
        hybridMixedDateExpectedOutput);
  }

  // test with legacy parquet files
  // similar to ql/src/test/queries/clientpositive/avro_legacy_mixed_date.q
  @Test public void testAvroLegacyMixedDates() throws Exception {

    final String tableName = "testAvroLegacyMixedDates";

    executeSQL("create table " + tableName + " (d date) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' "
        + "COLLECTION ITEMS TERMINATED BY ',' MAP KEYS TERMINATED BY ':' stored as avro");
    executeSQL("load data local inpath '../../data/files/avro_legacy_mixed_dates.avro' into table " + tableName);

    final String query = "select * from " + tableName;

    testDateQueries(query, HIVE_AVRO_PROLEPTIC_GREGORIAN_DEFAULT.toString(), hybridMixedDateExpectedOutput,
        legacyDateExpectedOutput);
  }

  // test newly inserted avro records
  // similar to ql/src/test/queries/clientpositive/avro_hybrid_mixed_timestamp.q
  @Test public void testAvroHybridMixedTimestamps() throws Exception {

    final String tableName = "testAvroHybridMixedTimestamps";
    executeSQL("create table " + tableName + " (d timestamp) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' "
        + "COLLECTION ITEMS TERMINATED BY ',' MAP KEYS TERMINATED BY ':' stored as avro");
    executeSQL("INSERT INTO " + tableName + " VALUES ('2012-02-21 07:08:09.123')," + "('2014-02-11 07:08:09.123'),"
        + "('1947-02-11 07:08:09.123')," + "('8200-02-11 07:08:09.123')," + "('1012-02-21 07:08:09.123'),"
        + "('1014-02-11 07:08:09.123')," + "('0947-02-11 07:08:09.123')," + "('0200-02-11 07:08:09.123')");

    final String query = "select * from " + tableName;

    testTimestampQueries(query, HIVE_AVRO_PROLEPTIC_GREGORIAN_DEFAULT.toString(), hybridMixedTimestampExpectedOutput,
        hybridMixedTimestampExpectedOutput);
  }

  // test with legacy avro files
  // similar to ql/src/test/queries/clientpositive/avro_legacy_mixed_timestamp.q
  @Test public void testAvroLegacyMixedTimestamps() throws Exception {

    final String tableName = "testAvroLegacyMixedTimestamps";
    executeSQL("create table " + tableName + "(d timestamp) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'"
        + "COLLECTION ITEMS TERMINATED BY ',' MAP KEYS TERMINATED BY ':' stored as avro");
    executeSQL("load data local inpath '../../data/files/avro_legacy_mixed_timestamps.avro' into table " + tableName);

    final String query = "select * from " + tableName;

    testTimestampQueries(query, HIVE_AVRO_PROLEPTIC_GREGORIAN_DEFAULT.toString(), hybridMixedTimestampExpectedOutput,
        convertedLegacyTimestampExpectedOutput);
  }

  private void testDateQueries(final String query, final String fileReaderLevelCalendarProp,
      MultiSet<List<Object>> newHybridExpected, MultiSet<List<Object>> legacyExpected) throws Exception {

    MultiSet<List<Object>> llapResultTransformed = transformLlapDateResultSet(runQueryUsingLlapArrowBatchReader(query,
        ImmutableMap.of(LLAP_EXTERNAL_CLIENT_USE_HYBRID_CALENDAR.toString(), "true")), false);
    assertEquals(newHybridExpected, llapResultTransformed);

    // test hybrid output with fileReaderLevelCalendarProp
    llapResultTransformed = transformLlapDateResultSet(runQueryUsingLlapArrowBatchReader(query, ImmutableMap
        .of(LLAP_EXTERNAL_CLIENT_USE_HYBRID_CALENDAR.toString(), "true", fileReaderLevelCalendarProp, "true")), false);
    assertEquals(legacyExpected, llapResultTransformed);
  }

  private void testTimestampQueries(final String query, final String fileReaderLevelCalendarProp,
      MultiSet<List<Object>> newHybridExpected, MultiSet<List<Object>> legacyExpected) throws Exception {

    MultiSet<List<Object>> llapResultTransformed = transformLlapTimestampResultSet(
        runQueryUsingLlapArrowBatchReader(query,
            ImmutableMap.of(LLAP_EXTERNAL_CLIENT_USE_HYBRID_CALENDAR.toString(), "true")), false);
    assertEquals(newHybridExpected, llapResultTransformed);

    // test hybrid output with fileReaderLevelCalendarProp
    llapResultTransformed = transformLlapTimestampResultSet(runQueryUsingLlapArrowBatchReader(query, ImmutableMap
        .of(LLAP_EXTERNAL_CLIENT_USE_HYBRID_CALENDAR.toString(), "true", fileReaderLevelCalendarProp, "true")), false);
    assertEquals(legacyExpected, llapResultTransformed);
  }

  private MultiSet<List<Object>> transformLlapDateResultSet(MultiSet<List<Object>> llapResult,
      final boolean useProleptic) {
    MultiSet<List<Object>> llapResultTransformed = new HashMultiSet<>();
    for (List<Object> list : llapResult) {
      llapResultTransformed.add(
          list.stream().map(ele -> CalendarUtils.formatDate((int) ele, useProleptic)).collect(Collectors.toList()));
    }
    return llapResultTransformed;
  }

  private MultiSet<List<Object>> transformLlapTimestampResultSet(MultiSet<List<Object>> llapResult,
      final boolean useProleptic) {
    MultiSet<List<Object>> llapResultTransformed = new HashMultiSet<>();
    for (List<Object> list : llapResult) {
      llapResultTransformed.add(list.stream().map(ele -> CalendarUtils.formatTimestamp((long) ele / 1000, useProleptic))
          .collect(Collectors.toList()));
    }
    return llapResultTransformed;
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

  @Override
  @Ignore
  public void testMultipleBatchesOfComplexTypes() {
    // ToDo: FixMe
  }

  @Override
  @Ignore
  public void testLlapInputFormatEndToEndWithMultipleBatches() {
    // ToDo: FixMe
  }

}

