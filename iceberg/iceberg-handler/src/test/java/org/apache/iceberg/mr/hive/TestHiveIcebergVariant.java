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
import java.util.List;
import java.util.stream.StreamSupport;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.junit.Assert;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assume.assumeTrue;

public class TestHiveIcebergVariant extends HiveIcebergStorageHandlerWithEngineBase {
  private static final String TYPED_VALUE_FIELD = "typed_value";

  @Test
  public void testVariantSelectProjection() throws IOException {
    assumeParquetNonVectorized();

    TableIdentifier table = TableIdentifier.of("default", "variant_projection");
    shell.executeStatement(String.format("DROP TABLE IF EXISTS %s", table));

    shell.executeStatement(
        String.format(
            "CREATE TABLE %s (id INT, payload VARIANT) STORED BY ICEBERG STORED AS %s %s %s",
            table,
            fileFormat,
            testTables.locationForCreateTableSQL(table),
            testTables.propertiesForCreateTableSQL(
                ImmutableMap.of("format-version", "3", "variant.shredding.enabled", "true"))));

    shell.executeStatement(
        String.format(
            "INSERT INTO %s VALUES " +
                "(1, parse_json('null'))," +
                "(2, parse_json('{\"name\":\"Alice\",\"age\":30}'))," +
                "(3, parse_json('{\"name\":\"Bob\"}'))",
            table));

    List<Object[]> rows =
        shell.executeStatement(
            String.format(
                "SELECT id, " +
                    "variant_get(payload, '$.name') AS name, " +
                    "try_variant_get(payload, '$.age', 'int') AS age " +
                    "FROM %s ORDER BY id",
                table));

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
  public void testVariantShreddingInStruct() throws IOException {
    assumeParquetNonVectorized();

    TableIdentifier table = TableIdentifier.of("default", "variant_struct_shredding");
    shell.executeStatement(String.format("DROP TABLE IF EXISTS %s", table));

    shell.executeStatement(
        String.format(
            "CREATE TABLE %s (id INT, payload STRUCT<info: VARIANT>) STORED BY ICEBERG STORED AS %s %s %s",
            table,
            fileFormat,
            testTables.locationForCreateTableSQL(table),
            testTables.propertiesForCreateTableSQL(
                ImmutableMap.of("format-version", "3", "variant.shredding.enabled", "true"))));

    shell.executeStatement(
        String.format(
            "INSERT INTO %s VALUES " +
                "(1, named_struct('info', parse_json('null')))," +
                "(2, named_struct('info', parse_json('{\"city\":\"Seattle\",\"state\":\"WA\"}')))",
            table));

    Table icebergTable = testTables.loadTable(table);
    Types.NestedField payloadField = requiredField(icebergTable, "payload", "Struct column should exist");
    MessageType parquetSchema = readParquetSchema(firstDataFile(icebergTable));
    assertThat(hasTypedValue(parquetSchema, payloadField.name(), "info")).isTrue();
  }

  @Test
  public void testVariantShreddingNotAppliedInArrayOrMap() throws IOException {
    assumeParquetNonVectorized();

    TableIdentifier table = TableIdentifier.of("default", "variant_container_no_shredding");
    shell.executeStatement(String.format("DROP TABLE IF EXISTS %s", table));

    shell.executeStatement(
        String.format(
            "CREATE TABLE %s (id INT, arr ARRAY<VARIANT>, mp MAP<STRING, VARIANT>) " +
                "STORED BY ICEBERG STORED AS %s %s %s",
            table,
            fileFormat,
            testTables.locationForCreateTableSQL(table),
            testTables.propertiesForCreateTableSQL(
                ImmutableMap.of("format-version", "3", "variant.shredding.enabled", "true"))));

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

  private void assumeParquetNonVectorized() {
    assumeTrue(fileFormat == FileFormat.PARQUET);
    assumeTrue(!isVectorized);
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
    return groupAt(parquetSchema, pathToVariantGroup).containsField(TYPED_VALUE_FIELD);
  }
}
