/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.mr.hive;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBetween;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFIn;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNot;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotNull;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNull;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFVariantGet;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.mr.hive.test.TestTables.TestTableType;
import org.apache.iceberg.mr.mapreduce.IcebergSplitContainer;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.iceberg.types.Types;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * File-level pruning of shredded VARIANT values during split planning.
 */
public class TestHiveIcebergVariantFilePruning extends HiveIcebergStorageHandlerWithEngineBase {

  private static final Schema SCHEMA = new Schema(
      required(1, "id", Types.IntegerType.get()),
      required(2, "region", Types.StringType.get()),
      optional(3, "payload", Types.VariantType.get()));

  @Parameters(name = "fileFormat={0}, catalog={1}, isVectorized={2}, formatVersion={3}")
  public static Collection<Object[]> parameters() {
    return HiveIcebergStorageHandlerWithEngineBase.getParameters(p ->
        p.fileFormat() == FileFormat.PARQUET &&
        p.testTableType() == TestTableType.HIVE_CATALOG &&
        p.formatVersion() == 3);
  }

  @Test
  public void testSelectivePredicatePlansOnlyMatchingFile() throws Exception {
    // split planning does not depend on the execution mode
    Assume.assumeFalse(isVectorized);
    String table = "variant_file_pruning";
    createTable(table, PartitionSpec.unpartitioned(), true);
    insert(table, events(1, 10, "eu"));
    String second = insert(table, events(11, 20, "eu"));
    insert(table, events(21, 30, "eu"));

    assertThat(plannedFiles(table, eq(eventId("int"), intLit(15))))
        .containsExactly(second);
  }

  @Test
  public void testExtractPredicatesAndTheirNegationsPrune() throws Exception {
    Assume.assumeFalse(isVectorized);
    String table = "variant_file_pruning_conjuncts";
    createTable(table, PartitionSpec.unpartitioned(), true);
    String first = insert(table, events(1, 10, "eu"));
    String second = insert(table, events(11, 20, "eu"));
    String third = insert(table, events(21, 30, "eu"));
    List<String> all = sorted(first, second, third);

    assertThat(plannedFiles(table, and(gt(id(), intLit(0)), eq(eventId("int"), intLit(15)))))
        .containsExactly(second);
    assertThat(plannedFiles(table, lt(eventId("int"), intLit(11))))
        .containsExactly(first);
    assertThat(plannedFiles(table, lt(eventId("int"), intLit(100))))
        .isEqualTo(all);
    assertThat(plannedFiles(table, between(false, eventId("int"), intLit(12), intLit(14))))
        .containsExactly(second);
    assertThat(plannedFiles(table, in(eventId("int"), intLit(15), intLit(16))))
        .containsExactly(second);
    assertThat(plannedFiles(table, in(eventId("int"), intLit(5), intLit(25))))
        .isEqualTo(sorted(first, third));
    assertThat(plannedFiles(table, or(eq(eventId("int"), intLit(15)), eq(eventId("int"), intLit(25)))))
        .isEqualTo(all);
    assertThat(plannedFiles(table, isNull(eventId("int"))))
        .isEqualTo(all);

    // Hive's search argument writes >, >=, <> and IS NOT NULL as NOT of <=, <, = and IS NULL
    assertThat(plannedFiles(table, gt(eventId("int"), intLit(25))))
        .containsExactly(third);
    assertThat(plannedFiles(table, gtEq(eventId("int"), intLit(20))))
        .isEqualTo(sorted(second, third));
    assertThat(plannedFiles(table, not(lt(eventId("int"), intLit(11)))))
        .isEqualTo(sorted(second, third));
    // a file can be dropped only if its one value is 15, or in the list
    assertThat(plannedFiles(table, notEq(eventId("int"), intLit(15))))
        .isEqualTo(all);
    assertThat(plannedFiles(table, not(in(eventId("int"), intLit(15), intLit(16)))))
        .isEqualTo(all);
    // only the variant column's null count is known, and no file holds only NULL variants
    assertThat(plannedFiles(table, isNotNull(eventId("int"))))
        .isEqualTo(all);
    // the search argument pushes NOT below OR: NOT(a OR b) is NOT a AND NOT b
    assertThat(plannedFiles(table, not(or(lt(eventId("int"), intLit(11)), eq(eventId("int"), intLit(25))))))
        .isEqualTo(sorted(second, third));
    // ... and NOT(a AND b) becomes an OR, which is ignored, as is NOT over a BETWEEN
    assertThat(plannedFiles(table, not(and(lt(eventId("int"), intLit(11)), gt(id(), intLit(0))))))
        .isEqualTo(all);
    assertThat(plannedFiles(table, between(true, eventId("int"), intLit(1), intLit(10))))
        .isEqualTo(all);

    // nothing pruned: the files are combined into the same splits as without the pruner
    assertThat(plannedSplits(table, lt(eventId("int"), intLit(100))))
        .isEqualTo(plannedSplits(table, lt(id(), intLit(100))))
        .hasSize(1);
  }

  @Test
  public void testIsNotNullDropsFileOfNullVariants() throws Exception {
    Assume.assumeFalse(isVectorized);
    String table = "variant_file_pruning_null_variants";
    createTable(table, PartitionSpec.unpartitioned(), true);
    String events = insert(table, events(1, 10, "eu"));
    String nulls = insertInto(table, "(id, region)", "(11, 'eu'), (12, 'eu')");

    assertThat(plannedFiles(table, isNotNull(eventId("int"))))
        .containsExactly(events);
    assertThat(plannedFiles(table, isNull(eventId("int"))))
        .isEqualTo(sorted(events, nulls));
  }

  @Test
  public void testFilesWithoutVariantBoundsAreKept() throws Exception {
    Assume.assumeFalse(isVectorized);
    String table = "variant_file_pruning_no_bounds";
    createTable(table, PartitionSpec.unpartitioned(), true);
    String shredded = insert(table, events(1, 10, "eu"));
    String unshredded = insertUnshredded(table, "(11, 'eu', parse_json('{\"event_id\": 15}'))");
    String fallback = insert(table,
        "(21, 'eu', parse_json('{\"event_id\": 41}')), (22, 'eu', parse_json('{\"event_id\": \"x\"}'))");

    assertThat(filesWithVariantBounds(table)).containsExactly(shredded);
    assertThat(plannedFiles(table, eq(eventId("int"), intLit(15))))
        .isEqualTo(sorted(unshredded, fallback));
  }

  @Test
  public void testLossyCastKeepsFile() throws Exception {
    Assume.assumeFalse(isVectorized);
    String table = "variant_file_pruning_casts";
    createTable(table, PartitionSpec.unpartitioned(), true);
    String ints = insert(table, events(1, 10, "eu"));
    String doubles = insert(table, "(11, 'eu', parse_json('{\"event_id\": 1.57e1}'))");
    String decimals = insert(table, "(12, 'eu', parse_json('{\"event_id\": 15.70}'))");
    String strings = insert(table, "(13, 'eu', parse_json('{\"event_id\": \"15\"}'))");
    String moreInts = insert(table, events(14, 20, "eu"));

    assertThat(filesWithVariantBounds(table)).containsExactlyInAnyOrder(ints, doubles, decimals, strings, moreInts);
    // Hive casts 15.7, 15.70 and "15" to the int 15
    assertThat(plannedFiles(table, eq(eventId("int"), intLit(15))))
        .isEqualTo(sorted(doubles, decimals, strings, moreInts));
    assertThat(plannedFiles(table, eq(eventId("double"), doubleLit(15.0))))
        .isEqualTo(sorted(ints, decimals, strings, moreInts));
    assertThat(plannedFiles(table, eq(eventId("string"), stringLit("16"))))
        .isEqualTo(sorted(ints, doubles, decimals, moreInts));
  }

  @Test
  public void testPartitionedTable() throws Exception {
    Assume.assumeFalse(isVectorized);
    String table = "variant_file_pruning_partitioned";
    createTable(table, PartitionSpec.builderFor(SCHEMA).identity("region").build(), true);
    String first = insert(table, events(1, 10, "eu"));
    String second = insert(table, events(11, 20, "us"));
    insert(table, events(21, 30, "eu"));

    // grouping by "region" plans files and groups them, instead of planning tasks
    List<Consumer<JobConf>> paths =
        List.of(job -> { }, job -> job.set(TableScanDesc.GROUPING_PARTITION_COLUMNS, "region"));
    for (Consumer<JobConf> path : paths) {
      assertThat(plannedFiles(table, path, eq(eventId("int"), intLit(15))))
          .containsExactly(second);
      assertThat(plannedFiles(table, path, lt(eventId("int"), intLit(11))))
          .containsExactly(first);
    }
  }

  @Test
  public void testIncrementalAppendScan() throws Exception {
    Assume.assumeFalse(isVectorized);
    String table = "variant_file_pruning_incremental";
    createTable(table, PartitionSpec.unpartitioned(), true);
    insert(table, events(1, 10, "eu"));
    long first = testTables.loadTable(TableIdentifier.of("default", table)).currentSnapshot().snapshotId();
    String second = insert(table, events(11, 20, "eu"));
    String third = insert(table, events(21, 30, "eu"));
    Consumer<JobConf> appendedAfterFirst = job -> job.set(TableScanDesc.FROM_VERSION, String.valueOf(first));

    assertThat(plannedFiles(table, appendedAfterFirst, lt(id(), intLit(100))))
        .isEqualTo(sorted(second, third));
    assertThat(plannedFiles(table, appendedAfterFirst, eq(eventId("int"), intLit(15))))
        .containsExactly(second);
    assertThat(plannedFiles(table, appendedAfterFirst, eq(eventId("int"), intLit(5))))
        .isEmpty();
  }

  @Test
  public void testNestedPath() throws Exception {
    String shredded = "variant_file_pruning_nested";
    String plain = "variant_file_pruning_nested_plain";
    createTable(shredded, PartitionSpec.unpartitioned(), true);
    createTable(plain, PartitionSpec.unpartitioned(), false);
    String smallRows = "(1, 'eu', parse_json('{\"a\": {\"b\": 5}}')), (2, 'eu', parse_json('{\"a\": {\"b\": 6}}'))";
    String largeRows = "(3, 'eu', parse_json('{\"a\": {\"b\": 50}}'))";
    String small = insert(shredded, smallRows);
    String large = insert(shredded, largeRows);
    insert(plain, smallRows);
    insert(plain, largeRows);

    ExprNodeDesc ab = variantGet("$.a.b", "int");
    assertThat(plannedFiles(shredded, eq(ab, intLit(50)))).containsExactly(large);
    assertThat(plannedFiles(shredded, lt(ab, intLit(10)))).containsExactly(small);

    String predicate = "variant_get(payload, '$.a.b', 'int')";
    for (String query : List.of(predicate + " = 50", predicate + " = 5", predicate + " > 5")) {
      assertThat(ids(shredded, query)).as(query).isEqualTo(ids(plain, query));
    }
    assertThat(ids(shredded, predicate + " = 50")).containsExactly(3);
  }

  @Test
  public void testQueryReadsOnlyMatchingFiles() throws Exception {
    String table = "variant_file_pruning_missing_files";
    createTable(table, PartitionSpec.unpartitioned(), true);
    String first = insert(table, events(1, 10, "eu"));
    insert(table, events(11, 20, "eu"));
    String third = insert(table, events(21, 30, "eu"));
    // a query that plans a non-matching file now fails reading it
    assertThat(new File(new URI(first)).delete()).isTrue();
    assertThat(new File(new URI(third)).delete()).isTrue();

    String eventId = "variant_get(payload, '$.event_id', 'int')";
    assertThat(ids(table, eventId + " = 15")).containsExactly(15);
    assertThat(ids(table, eventId + " IN (15, 16)")).containsExactly(15, 16);
    assertThat(ids(table, eventId + " > 12 AND " + eventId + " < 14")).containsExactly(13);
    assertThatThrownBy(() -> ids(table, eventId + " = 15 OR id < 0"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testResultsMatchUnshreddedTable() {
    String shredded = "variant_results_shredded";
    String plain = "variant_results_plain";
    createTable(shredded, PartitionSpec.unpartitioned(), true);
    createTable(plain, PartitionSpec.unpartitioned(), false);
    for (String table : List.of(shredded, plain)) {
      insert(table, events(1, 10, "eu"));
      insert(table, events(11, 20, "eu"));
      insert(table, events(21, 30, "eu"));
      String unshreddedRows = events(31, 32, "eu") + ", (33, 'eu', parse_json('{\"event_id\": 15}'))";
      if (table.equals(shredded)) {
        insertUnshredded(table, unshreddedRows);
      } else {
        insert(table, unshreddedRows);
      }
      insert(table, "(41, 'eu', parse_json('{\"event_id\": 41}')), " +
          "(42, 'eu', parse_json('{\"event_id\": \"x\"}')), (43, 'eu', parse_json('{\"event_id\": \"15\"}'))");
      insert(table, "(51, 'eu', parse_json('{\"kind\": \"z\"}')), (52, 'eu', parse_json('null'))");
      shell.executeStatement(String.format("INSERT INTO default.%s (id, region) VALUES (53, 'eu')", table));
      insert(table,
          "(61, 'eu', parse_json('{\"event_id\": \"15\"}')), (62, 'eu', parse_json('{\"event_id\": \"16\"}'))");
    }

    String eventId = "variant_get(payload, '$.event_id', 'int')";
    assertThat(ids(shredded, eventId + " = 15")).containsExactly(15, 33, 43, 61);
    List<String> predicates = List.of(
        eventId + " = 15",
        eventId + " < 11",
        eventId + " > 0",
        eventId + " = 15 OR " + eventId + " = 25",
        "NOT (" + eventId + " < 11)",
        eventId + " > 25",
        eventId + " >= 20",
        eventId + " <> 15",
        eventId + " IN (15, 16, 25)",
        eventId + " NOT IN (15, 16)",
        "variant_get(payload, '$.kind', 'string') IS NULL",
        "variant_get(payload, '$.kind', 'string') IS NOT NULL",
        "id > 5 AND " + eventId + " BETWEEN 12 AND 14",
        "variant_get(payload, '$.event_id', 'string') = '16'",
        "variant_get(payload, '$.event_id', 'double') = 15.0");
    for (String predicate : predicates) {
      assertThat(ids(shredded, predicate)).as(predicate).isEqualTo(ids(plain, predicate));
    }
  }

  @Test
  public void testTimeTravelBeforeVariantColumnExisted() {
    Assume.assumeFalse(isVectorized);
    String table = "variant_file_pruning_time_travel";
    testTables.createTable(shell, table, new Schema(required(1, "id", Types.IntegerType.get())),
        PartitionSpec.unpartitioned(), fileFormat, ImmutableList.of(), formatVersion,
        ImmutableMap.of(InputFormatConfig.VARIANT_SHREDDING_ENABLED, "true"));
    shell.executeStatement(String.format("INSERT INTO default.%s VALUES (1)", table));
    long before = testTables.loadTable(TableIdentifier.of("default", table)).currentSnapshot().snapshotId();
    shell.executeStatement(String.format("ALTER TABLE default.%s ADD COLUMNS (payload variant)", table));
    shell.executeStatement(
        String.format("INSERT INTO default.%s VALUES (2, parse_json('{\"event_id\": 15}'))", table));

    String predicate = "variant_get(payload, '$.event_id', 'int') = 15";
    assertThat(ids(table, predicate)).containsExactly(2);
    assertThat(ids(table + " FOR SYSTEM_VERSION AS OF " + before, predicate)).isEmpty();
  }

  private void createTable(String table, PartitionSpec spec, boolean shredded) {
    testTables.createTable(shell, table, SCHEMA, spec, fileFormat, ImmutableList.of(), formatVersion,
        ImmutableMap.of(InputFormatConfig.VARIANT_SHREDDING_ENABLED, String.valueOf(shredded)));
  }

  /**
   * Inserts the rows in one statement and returns the location of the single data file it adds.
   */
  private String insert(String table, String rows) {
    return insertInto(table, "", rows);
  }

  private String insertInto(String table, String columns, String rows) {
    shell.executeStatement(String.format("INSERT INTO default.%s %s VALUES %s", table, columns, rows));
    Table icebergTable = testTables.loadTable(TableIdentifier.of("default", table));
    List<DataFile> added = Lists.newArrayList(icebergTable.currentSnapshot().addedDataFiles(icebergTable.io()));
    assertThat(added).hasSize(1);
    return added.getFirst().location();
  }

  /**
   * Inserts into a shredded table with shredding switched off for the insert.
   */
  private String insertUnshredded(String table, String rows) {
    setShredding(table, false);
    String file = insert(table, rows);
    setShredding(table, true);
    return file;
  }

  private void setShredding(String table, boolean enabled) {
    shell.executeStatement(String.format("ALTER TABLE default.%s SET TBLPROPERTIES ('%s'='%s')",
        table, InputFormatConfig.VARIANT_SHREDDING_ENABLED, enabled));
  }

  private static String events(int from, int to, String region) {
    return IntStream.rangeClosed(from, to)
        .mapToObj(id -> String.format("(%d, '%s', parse_json('{\"event_id\": %d}'))", id, region, id))
        .collect(Collectors.joining(", "));
  }

  private List<Integer> ids(String table, String predicate) {
    return shell.executeStatement(String.format("SELECT id FROM default.%s WHERE %s ORDER BY id", table, predicate))
        .stream()
        .map(row -> ((Number) row[0]).intValue())
        .toList();
  }

  private Set<String> filesWithVariantBounds(String table) throws IOException {
    Table icebergTable = testTables.loadTable(TableIdentifier.of("default", table));
    int payloadId = icebergTable.schema().findField("payload").fieldId();
    try (CloseableIterable<FileScanTask> tasks = icebergTable.newScan().includeColumnStats().planFiles()) {
      return Streams.stream(tasks)
          .map(FileScanTask::file)
          .filter(file -> file.lowerBounds() != null && file.lowerBounds().containsKey(payloadId))
          .map(DataFile::location)
          .collect(Collectors.toSet());
    }
  }

  private List<String> plannedFiles(String table, ExprNodeGenericFuncDesc filter) throws Exception {
    return plannedFiles(table, job -> { }, filter);
  }

  private List<String> plannedFiles(String table, Consumer<JobConf> settings, ExprNodeGenericFuncDesc filter)
      throws Exception {
    return sorted(plannedSplits(table, settings, filter).stream()
        .flatMap(List::stream)
        .toArray(String[]::new));
  }

  private List<List<String>> plannedSplits(String table, ExprNodeGenericFuncDesc filter) throws Exception {
    return plannedSplits(table, job -> { }, filter);
  }

  /**
   * Plans splits the way Hive does for a table scan: job properties from the storage handler, the pushed-down Hive
   * predicate and the given scan settings. Returns the sorted data file locations of each split.
   */
  private List<List<String>> plannedSplits(String table, Consumer<JobConf> settings,
      ExprNodeGenericFuncDesc filter) throws Exception {
    TableDesc tableDesc = Utilities.getTableDesc(
        new org.apache.hadoop.hive.ql.metadata.Table(shell.metastore().getTable("default", table)));
    HiveIcebergStorageHandler storageHandler = new HiveIcebergStorageHandler();
    storageHandler.setConf(shell.getHiveConf());
    Map<String, String> jobProperties = Maps.newHashMap();
    storageHandler.configureInputJobProperties(tableDesc, jobProperties);
    tableDesc.setJobProperties(jobProperties);

    JobConf job = new JobConf(shell.getHiveConf());
    Utilities.copyTableJobPropertiesToConf(tableDesc, job);
    job.set(TableScanDesc.FILTER_EXPR_CONF_STR, SerializationUtilities.serializeExpression(filter));
    settings.accept(job);

    InputSplit[] splits = new HiveIcebergInputFormat().getSplits(job, 0);
    return Arrays.stream(splits)
        .map(split -> sorted(((IcebergSplitContainer) split).icebergSplit().taskGroup().tasks().stream()
            .map(task -> task.file().location())
            .toArray(String[]::new)))
        .sorted(Comparator.comparing(List::getFirst))
        .toList();
  }

  private static List<String> sorted(String... files) {
    return Arrays.stream(files).sorted().toList();
  }

  private static ExprNodeDesc id() {
    return new ExprNodeColumnDesc(TypeInfoFactory.intTypeInfo, "id", "t", false);
  }

  private static ExprNodeDesc eventId(String type) {
    return variantGet("$.event_id", type);
  }

  private static ExprNodeDesc variantGet(String path, String type) {
    return func(new GenericUDFVariantGet(),
        new ExprNodeColumnDesc(TypeInfoFactory.getVariantTypeInfo(), "payload", "t", false),
        new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, path),
        new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, type));
  }

  private static ExprNodeConstantDesc intLit(int value) {
    return new ExprNodeConstantDesc(TypeInfoFactory.intTypeInfo, value);
  }

  private static ExprNodeConstantDesc doubleLit(double value) {
    return new ExprNodeConstantDesc(TypeInfoFactory.doubleTypeInfo, value);
  }

  private static ExprNodeConstantDesc stringLit(String value) {
    return new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, value);
  }

  private static ExprNodeGenericFuncDesc eq(ExprNodeDesc left, ExprNodeDesc right) {
    return func(new GenericUDFOPEqual(), left, right);
  }

  private static ExprNodeGenericFuncDesc notEq(ExprNodeDesc left, ExprNodeDesc right) {
    return func(new GenericUDFOPNotEqual(), left, right);
  }

  private static ExprNodeGenericFuncDesc lt(ExprNodeDesc left, ExprNodeDesc right) {
    return func(new GenericUDFOPLessThan(), left, right);
  }

  private static ExprNodeGenericFuncDesc gt(ExprNodeDesc left, ExprNodeDesc right) {
    return func(new GenericUDFOPGreaterThan(), left, right);
  }

  private static ExprNodeGenericFuncDesc gtEq(ExprNodeDesc left, ExprNodeDesc right) {
    return func(new GenericUDFOPEqualOrGreaterThan(), left, right);
  }

  private static ExprNodeGenericFuncDesc between(boolean invert, ExprNodeDesc expr, ExprNodeDesc low,
      ExprNodeDesc high) {
    return func(new GenericUDFBetween(),
        new ExprNodeConstantDesc(TypeInfoFactory.booleanTypeInfo, invert), expr, low, high);
  }

  private static ExprNodeGenericFuncDesc and(ExprNodeDesc left, ExprNodeDesc right) {
    return func(new GenericUDFOPAnd(), left, right);
  }

  private static ExprNodeGenericFuncDesc or(ExprNodeDesc left, ExprNodeDesc right) {
    return func(new GenericUDFOPOr(), left, right);
  }

  private static ExprNodeGenericFuncDesc not(ExprNodeDesc child) {
    return func(new GenericUDFOPNot(), child);
  }

  private static ExprNodeGenericFuncDesc isNull(ExprNodeDesc child) {
    return func(new GenericUDFOPNull(), child);
  }

  private static ExprNodeGenericFuncDesc in(ExprNodeDesc expr, ExprNodeDesc... values) {
    ExprNodeDesc[] children = new ExprNodeDesc[values.length + 1];
    children[0] = expr;
    System.arraycopy(values, 0, children, 1, values.length);
    return func(new GenericUDFIn(), children);
  }

  private static ExprNodeGenericFuncDesc isNotNull(ExprNodeDesc child) {
    return func(new GenericUDFOPNotNull(), child);
  }

  private static ExprNodeGenericFuncDesc func(GenericUDF udf, ExprNodeDesc... children) {
    try {
      return ExprNodeGenericFuncDesc.newInstance(udf, Lists.newArrayList(children));
    } catch (Exception e) {
      throw new IllegalArgumentException(e);
    }
  }
}
