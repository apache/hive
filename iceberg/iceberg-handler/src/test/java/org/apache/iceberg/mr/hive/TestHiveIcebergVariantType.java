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

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.stream.StreamSupport;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.mr.hive.test.TestTables.TestTableType;
import org.apache.iceberg.mr.hive.variant.VariantPathUtil;
import org.apache.iceberg.parquet.VariantParquetFilters;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHiveIcebergVariantType extends HiveIcebergStorageHandlerWithEngineBase {

  @Parameters(name = "fileFormat={0}, catalog={1}, isVectorized={2}, formatVersion={3}")
  public static Collection<Object[]> parameters() {
    return HiveIcebergStorageHandlerWithEngineBase.getParameters(p ->
        p.fileFormat() == FileFormat.PARQUET &&
        p.testTableType() == TestTableType.HIVE_CATALOG &&
        p.formatVersion() == 3);
  }

  @Test
  public void testVariantShredAndProject() throws IOException {
    Schema schema = new Schema(
        required(1, "id", Types.IntegerType.get()),
        required(2, "payload", Types.VariantType.get()));

    TableIdentifier table = TableIdentifier.of("default", "variant_projection");

    testTables.createTable(shell, table.name(),
        schema, PartitionSpec.unpartitioned(), fileFormat, ImmutableList.of(), formatVersion,
        ImmutableMap.of("variant.shredding.enabled", "true"));

    shell.executeStatement(
        String.format(
            "INSERT INTO %s VALUES " +
                "(1, parse_json('null'))," +
                "(2, parse_json('{\"name\":\"Alice\",\"age\":30}'))," +
                "(3, parse_json('{\"name\":\"Bob\"}'))",
            table));

    String queryStr = "SELECT id, " +
        "variant_get(payload, '$.name') AS name, " +
        "try_variant_get(payload, '$.age', 'int') AS age " +
        "FROM %s ORDER BY id";

    if (isVectorized) {
      List<Object[]> explain =
          shell.executeStatement(String.format("EXPLAIN VECTORIZATION " + queryStr, table));
      Assert.assertTrue(
          "Expected map-side vectorization for variant_get(payload, ...) query",
          mapVectorized(explain));
    }

    List<Object[]> rows =
        shell.executeStatement(String.format(queryStr, table));

    Assert.assertEquals(3, rows.size());
    Assert.assertEquals(1, ((Number) rows.get(0)[0]).intValue());
    Assert.assertNull(rows.get(0)[1]);
    Assert.assertNull(rows.get(0)[2]);

    Assert.assertEquals(2, ((Number) rows.get(1)[0]).intValue());
    Assert.assertEquals("Alice", rows.get(1)[1]);
    Assert.assertEquals(30, ((Number) rows.get(1)[2]).intValue());

    Assert.assertEquals(3, ((Number) rows.get(2)[0]).intValue());
    Assert.assertEquals("Bob", rows.get(2)[1]);
    Assert.assertNull(rows.get(2)[2]);

    Table icebergTable = testTables.loadTable(table);
    Types.NestedField variantField = requiredField(icebergTable, "payload", "Variant column should exist");
    MessageType parquetSchema = readParquetSchema(firstDataFile(icebergTable));
    assertThat(hasTypedValue(parquetSchema, variantField.name())).isTrue();
  }

  @Test
  public void testVariantShreddingAppliedToStructField() throws IOException {
    Schema schema = new Schema(
        required(1, "id", Types.IntegerType.get()),
        required(2, "payload", Types.StructType.of(
            required(3, "info", Types.VariantType.get()))));

    TableIdentifier table = TableIdentifier.of("default", "variant_struct_shredding");

    testTables.createTable(shell, table.name(),
        schema, PartitionSpec.unpartitioned(), fileFormat, ImmutableList.of(), formatVersion,
        ImmutableMap.of("variant.shredding.enabled", "true"));

    shell.executeStatement(
        String.format(
            "INSERT INTO %s VALUES " +
                "(1, named_struct('info', parse_json('null')))," +
                "(2, named_struct('info', parse_json('{\"city\":\"Seattle\",\"state\":\"WA\"}')))",
            table));

    String queryStr = "SELECT id, " +
        "variant_get(payload.info, '$.city') AS city, " +
        "variant_get(payload.info, '$.state') AS state " +
        "FROM %s ORDER BY id";

    if (isVectorized) {
      List<Object[]> explain =
          shell.executeStatement(String.format("EXPLAIN VECTORIZATION " + queryStr, table));
      Assert.assertTrue(
          "Expected map-side vectorization for nested variant_get(payload.info, ...) query",
          mapVectorized(explain));
    }

    List<Object[]> rows =
        shell.executeStatement(String.format(queryStr, table));

    Assert.assertEquals(2, rows.size());
    Assert.assertEquals(1, ((Number) rows.get(0)[0]).intValue());
    Assert.assertNull(rows.get(0)[1]);
    Assert.assertNull(rows.get(0)[2]);
    Assert.assertEquals(2, ((Number) rows.get(1)[0]).intValue());
    Assert.assertEquals("Seattle", rows.get(1)[1]);
    Assert.assertEquals("WA", rows.get(1)[2]);

    Table icebergTable = testTables.loadTable(table);
    Types.NestedField payloadField = requiredField(icebergTable, "payload", "Struct column should exist");
    MessageType parquetSchema = readParquetSchema(firstDataFile(icebergTable));
    assertThat(hasTypedValue(parquetSchema, payloadField.name(), "info")).isTrue();
  }

  @Test
  public void testVariantInContainersIsNotShredded() throws IOException {
    Assume.assumeTrue(isVectorized);

    Schema schema = new Schema(
        required(1, "id", Types.IntegerType.get()),
        required(2, "arr", Types.ListType.ofRequired(3, Types.VariantType.get())),
        required(4, "mp", Types.MapType.ofRequired(5, 6,
            Types.StringType.get(), Types.VariantType.get())));

    TableIdentifier table = TableIdentifier.of("default", "variant_container_no_shredding");

    testTables.createTable(shell, table.name(),
        schema, PartitionSpec.unpartitioned(), fileFormat, ImmutableList.of(), formatVersion,
        ImmutableMap.of("variant.shredding.enabled", "true"));

    shell.executeStatement(
        String.format(
            "INSERT INTO %s VALUES " +
                "(1, array(parse_json('{\"a\":1}')), map('k', parse_json('{\"b\":2}')))",
            table));

    Table icebergTable = testTables.loadTable(table);
    MessageType parquetSchema = readParquetSchema(firstDataFile(icebergTable));
    // The element/value types should remain as the base VARIANT struct (no typed_value).
    assertThat(hasTypedValue(parquetSchema, "arr", "list", "element")).isFalse();
    assertThat(hasTypedValue(parquetSchema, "mp", "key_value", "value")).isFalse();
  }

  @Test
  public void testParquetRowGroupPruningWithVariantPredicate() {
    Schema schema = new Schema(
        required(1, "id", Types.IntegerType.get()),
        required(2, "payload", Types.VariantType.get()));

    TableIdentifier table = TableIdentifier.of("default", "variant_select");

    testTables.createTable(shell, table.name(),
        schema, PartitionSpec.unpartitioned(), fileFormat, ImmutableList.of(), formatVersion,
        ImmutableMap.of(
            "variant.shredding.enabled", "true",
            // Force multiple row groups so we can assert row-group pruning for variant predicates.
            "write.parquet.row-group-size-bytes", "65536"));

    // Insert enough data (with padding) to create multiple row groups, and group tiers so row groups can be pruned.
    int rowsPerTier = 100;
    int padLen = 2000;
    StringBuilder insert = new StringBuilder("INSERT INTO ")
        .append(table)
        .append(" VALUES ");
    for (int i = 1; i <= rowsPerTier * 2; i++) {
      String tier = i <= rowsPerTier ? "gold" : "silver";
      if (i > 1) {
        insert.append(",");
      }
      insert.append("(")
          .append(i)
          .append(", parse_json(concat('{\"tier\":\"")
          .append(tier)
          .append("\",\"pad\":\"', repeat('x', ")
          .append(padLen)
          .append("), '\"}')))");
    }
    shell.executeStatement(insert.toString());

    String queryStr = "SELECT id FROM %s " +
        "WHERE variant_get(payload, '$.tier') = 'gold' " +
        "ORDER BY id";

    if (isVectorized) {
      List<Object[]> explain =
          shell.executeStatement(String.format("EXPLAIN VECTORIZATION " + queryStr, table));
      Assert.assertTrue(
          "Expected map-side vectorization for variant predicate query",
          mapVectorized(explain));
    }

    List<Object[]> rows =
        shell.executeStatement(String.format(queryStr, table));

    Assert.assertEquals(rowsPerTier, rows.size());
    Assert.assertEquals(1, ((Number) rows.get(0)[0]).intValue());
    Assert.assertEquals(rowsPerTier, ((Number) rows.get(rows.size() - 1)[0]).intValue());

    // Assert the file has multiple row groups and that our Parquet reader row-group pruning will skip some.
    Table icebergTable = testTables.loadTable(table);
    DataFile dataFile = firstDataFile(icebergTable);
    Path parquetPath = new Path(dataFile.location());
    int rowGroupCount;
    try (ParquetFileReader reader =
          ParquetFileReader.open(HadoopInputFile.fromPath(parquetPath, shell.getHiveConf()))) {
      rowGroupCount = reader.getRowGroups().size();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    Assert.assertTrue(
        "Expected multiple row groups to validate pruning",
        rowGroupCount > 1);

    Expression filter = Expressions.equal(
        Expressions.extract("payload", "$.tier", "string"),
        "gold");
    if (isVectorized) {
      assertVectorizedParquetRowGroupsPruned(parquetPath, filter);
    } else {
      assertNonVectorizedParquetRowGroupsPruned(parquetPath, filter);
    }
  }

  @Test
  public void testVariantProjectionWithColumnRename() {
    Assume.assumeTrue(isVectorized);

    Schema schema = new Schema(
        required(1, "id", Types.IntegerType.get()),
        required(2, "payload", Types.VariantType.get()));

    TableIdentifier table = TableIdentifier.of("default", "variant_rename");

    testTables.createTable(shell, table.name(),
        schema, PartitionSpec.unpartitioned(), fileFormat, ImmutableList.of(), formatVersion,
        ImmutableMap.of("variant.shredding.enabled", "true"));

    shell.executeStatement(
        String.format(
            "INSERT INTO %s VALUES (1, parse_json('{\"a\": 100}'))",
            table));

    shell.executeStatement(
        String.format(
            "ALTER TABLE %s CHANGE COLUMN payload data VARIANT",
            table));

    List<Object[]> rows = shell.executeStatement(
        String.format(
            "SELECT id, try_variant_get(data, '$.a', 'int') FROM %s",
            table));

    Assert.assertEquals(1, rows.size());
    Assert.assertEquals(1, ((Number) rows.get(0)[0]).intValue());
    Assert.assertEquals(100, ((Number) rows.get(0)[1]).intValue());
  }

  private static boolean mapVectorized(List<Object[]> explain) {
    boolean insideMapSection = false;
    for (Object[] row : explain) {
      if (row == null || row.length == 0 || row[0] == null) {
        continue;
      }

      String line = row[0].toString().trim();
      // Enter Map Vectorization section
      if (line.startsWith("Map Vectorization:")) {
        insideMapSection = true;
        continue;
      }
      if (insideMapSection) {
        // Check for vectorized=true inside the section
        if (line.contains("vectorized: true")) {
          return true;
        }
        // Exit section when a Reduce vertex or Reduce Vectorization starts
        if (line.startsWith("Reducer") || line.startsWith("Reduce Vectorization:")) {
          insideMapSection = false;
        }
      }
    }
    return false;
  }

  private static void assertVectorizedParquetRowGroupsPruned(Path parquetPath, Expression filter) {
    assertParquetRowGroupsPruned(
        parquetPath, filter,
        (parquetMetadata, fileSchema, expr) ->
            // Simulate what HiveVectorizedReader.parquetRecordReader() does
            VariantParquetFilters
                .pruneVariantRowGroups(parquetMetadata, fileSchema, expr)
                .getBlocks()
                .size());
  }

  private static void assertNonVectorizedParquetRowGroupsPruned(Path parquetPath, Expression filter) {
    assertParquetRowGroupsPruned(
        parquetPath, filter,
        (parquetMetadata, fileSchema, expr) -> {
          // Simulate what ReadConf does - uses variantRowGroupMayMatch to compute shouldSkip array
          boolean[] mayMatch = VariantParquetFilters
              .variantRowGroupMayMatch(fileSchema, expr, parquetMetadata.getBlocks());
          int matching = 0;
          for (boolean match : mayMatch) {
            if (match) {
              matching++;
            }
          }
          return matching;
        });
  }

  @FunctionalInterface
  private interface RowGroupPruner {
    int prune(
        ParquetMetadata parquetMetadata,
        MessageType fileSchema,
        Expression filter) throws Exception;
  }

  private static void assertParquetRowGroupsPruned(
      Path parquetPath,
      Expression filter,
      RowGroupPruner rowGroupPruner) {
    try (ParquetFileReader reader =
          ParquetFileReader.open(HadoopInputFile.fromPath(parquetPath, shell.getHiveConf()))) {
      ParquetMetadata parquetMetadata = reader.getFooter();
      MessageType fileSchema = parquetMetadata.getFileMetaData().getSchema();
      int originalRowGroups = parquetMetadata.getBlocks().size();

      Assert.assertTrue(
          "Expected multiple row groups to validate pruning",
          originalRowGroups > 1);

      int matchingRowGroups = rowGroupPruner.prune(parquetMetadata, fileSchema, filter);

      Assert.assertTrue(
          "Expected at least one row group to be pruned",
          matchingRowGroups > 0 && matchingRowGroups < originalRowGroups);

    } catch (Exception e) {
      throw new RuntimeException("Unable to validate vectorized Parquet row-group pruning", e);
    }
  }

  private static Types.NestedField requiredField(Table table, String fieldName, String message) {
    Types.NestedField field = table.schema().findField(fieldName);
    Assert.assertNotNull(message, field);
    return field;
  }

  private static DataFile firstDataFile(Table table) {
    return StreamSupport.stream(table.currentSnapshot().addedDataFiles(table.io()).spliterator(), false)
        .findFirst()
        .orElseThrow(() -> new IllegalStateException("No data files written for test table"));
  }

  private MessageType readParquetSchema(DataFile dataFile) throws IOException {
    Path parquetPath = new Path(dataFile.location());
    try (ParquetFileReader reader =
          ParquetFileReader.open(HadoopInputFile.fromPath(parquetPath, shell.getHiveConf()))) {
      return reader.getFooter().getFileMetaData().getSchema();
    }
  }

  private static GroupType groupAt(MessageType parquetSchema, String... path) {
    org.apache.parquet.schema.Type type = parquetSchema.getType(path[0]);
    for (int i = 1; i < path.length; i++) {
      type = type.asGroupType().getType(path[i]);
    }
    return type.asGroupType();
  }

  private static boolean hasTypedValue(MessageType parquetSchema, String... pathToVariantGroup) {
    return groupAt(parquetSchema, pathToVariantGroup).containsField(
        VariantPathUtil.TYPED_VALUE);
  }
}
