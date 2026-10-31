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

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.mr.hive.test.TestTables.TestTableType;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import static org.apache.iceberg.types.Types.NestedField.optional;

public class TestHiveIcebergUnknownType extends HiveIcebergStorageHandlerWithEngineBase {

  @Parameters(name = "fileFormat={0}, catalog={1}, isVectorized={2}, formatVersion={3}")
  public static Collection<Object[]> parameters() {
    return HiveIcebergStorageHandlerWithEngineBase.getParameters(p ->
        p.testTableType() == TestTableType.HIVE_CATALOG &&
        p.formatVersion() == 3);
  }

  private void createTable(TableIdentifier identifier, String columns) {
    shell.executeStatement(String.format(
        "CREATE EXTERNAL TABLE %s (%s) STORED BY ICEBERG " +
            "TBLPROPERTIES ('format-version'='3', '%s'='%s')",
        identifier, columns, TableProperties.DEFAULT_FILE_FORMAT, fileFormat));
  }

  @Test
  public void testUnknownTypeReadWrite() {
    Schema schema = new Schema(
        optional(1, "id", Types.IntegerType.get()),
        optional(2, "placeholder", Types.UnknownType.get()));

    TableIdentifier table = TableIdentifier.of("default", "unknown_type_basic");

    testTables.createTable(shell, table.name(),
        schema, org.apache.iceberg.PartitionSpec.unpartitioned(), fileFormat, ImmutableList.of(), formatVersion,
        ImmutableMap.of());

    shell.executeStatement(
        String.format("INSERT INTO %s VALUES (1, NULL), (2, NULL)", table));

    List<Object[]> rows = shell.executeStatement(
        String.format("SELECT id, placeholder FROM %s ORDER BY id", table));

    Assert.assertEquals(2, rows.size());
    Assert.assertEquals(1, rows.get(0)[0]);
    Assert.assertNull(rows.get(0)[1]);
    Assert.assertEquals(2, rows.get(1)[0]);
    Assert.assertNull(rows.get(1)[1]);
  }

  @Test
  public void testUnknownTypeDdl() {
    TableIdentifier table = TableIdentifier.of("default", "unknown_type_ddl");

    createTable(table, "id INT, placeholder UNKNOWN");

    shell.executeStatement(String.format("INSERT INTO %s VALUES (1, NULL)", table));

    List<Object[]> rows = shell.executeStatement(
        String.format("SELECT id, placeholder FROM %s", table));

    Assert.assertEquals(1, rows.size());
    Assert.assertEquals(1, rows.get(0)[0]);
    Assert.assertNull(rows.get(0)[1]);
  }

  @Test
  public void testPromoteUnknownColumnKeepsFieldId() {
    TableIdentifier identifier = TableIdentifier.of("default", "unknown_type_promote");

    createTable(identifier, "id INT, placeholder UNKNOWN");
    shell.executeStatement(String.format("INSERT INTO %s (id) VALUES (1)", identifier));
    shell.executeStatement(String.format("ALTER TABLE %s CREATE TAG before_promotion", identifier));

    Table before = testTables.loadTable(identifier);
    Types.NestedField unknownField = before.schema().findField("placeholder");
    int unknownSchemaId = before.schema().schemaId();

    shell.executeStatement(String.format(
        "ALTER TABLE %s CHANGE COLUMN placeholder placeholder INT", identifier));
    shell.executeStatement(String.format("INSERT INTO %s VALUES (2, 20)", identifier));

    Table after = testTables.loadTable(identifier);
    Types.NestedField promotedField = after.schema().findField("placeholder");
    Assert.assertEquals(unknownField.fieldId(), promotedField.fieldId());
    Assert.assertEquals(Types.IntegerType.get(), promotedField.type());
    Assert.assertTrue(promotedField.isOptional());
    Assert.assertNotEquals(unknownSchemaId, after.schema().schemaId());
    Assert.assertEquals(Types.UnknownType.get(),
        after.schemas().get(unknownSchemaId).findField(unknownField.fieldId()).type());

    List<Object[]> rows = shell.executeStatement(
        String.format("SELECT id, placeholder FROM %s ORDER BY id", identifier));

    Assert.assertEquals(2, rows.size());
    Assert.assertEquals(1, rows.get(0)[0]);
    Assert.assertNull(rows.get(0)[1]);
    Assert.assertEquals(2, rows.get(1)[0]);
    Assert.assertEquals(20, rows.get(1)[1]);

    rows = shell.executeStatement(String.format(
        "SELECT id, placeholder FROM %s FOR SYSTEM_VERSION AS OF 'before_promotion'", identifier));
    Assert.assertEquals(1, rows.size());
    Assert.assertArrayEquals(new Object[] {1, null}, rows.get(0));
  }

  @Test
  public void testUnknownTypeSchemaDdl() {
    TableIdentifier identifier = TableIdentifier.of("default", "unknown_type_schema_ddl");

    Assertions.assertThatThrownBy(() -> shell.executeStatement(String.format(
            "CREATE EXTERNAL TABLE %s (id INT, placeholder UNKNOWN) STORED BY ICEBERG " +
                "TBLPROPERTIES ('format-version'='2')", identifier)))
        .hasMessageContaining("unknown is not supported until v3");

    createTable(identifier, "id INT, placeholder UNKNOWN, extra UNKNOWN");
    shell.executeStatement(String.format("INSERT INTO %s (id) VALUES (1)", identifier));

    String createStatement = shell.executeStatement(String.format("SHOW CREATE TABLE %s", identifier)).stream()
        .map(row -> (String) row[0])
        .collect(Collectors.joining("\n"));
    Assertions.assertThat(createStatement).contains("`placeholder` unknown");

    shell.executeStatement(String.format("ALTER TABLE %s REPLACE COLUMNS (id INT, placeholder UNKNOWN)", identifier));
    Assert.assertNull(testTables.loadTable(identifier).schema().findField("extra"));

    List<Object[]> rows = shell.executeStatement(String.format("SELECT * FROM %s", identifier));
    Assert.assertEquals(1, rows.size());
    Assert.assertArrayEquals(new Object[] {1, null}, rows.get(0));
  }

  @Test
  public void testUnknownTypeColumnListInsertAndDml() {
    TableIdentifier identifier = TableIdentifier.of("default", "unknown_type_dml");

    createTable(identifier, "a INT, b UNKNOWN, c INT, s STRUCT<x:INT, u:UNKNOWN>");
    shell.executeStatement(String.format("INSERT INTO %s (a, c) VALUES (1, 10)", identifier));
    shell.executeStatement(String.format("INSERT INTO %s (c, a) VALUES (20, 2)", identifier));
    shell.executeStatement(String.format(
        "INSERT INTO %s SELECT 3, NULL, 30, named_struct('x', 5, 'u', NULL)", identifier));
    shell.executeStatement(String.format("UPDATE %s SET c = 11 WHERE a = 1", identifier));
    shell.executeStatement(String.format("DELETE FROM %s WHERE a = 2", identifier));

    List<Object[]> rows = shell.executeStatement(
        String.format("SELECT a, b, c, s.x, s.u FROM %s ORDER BY a", identifier));

    Assert.assertEquals(2, rows.size());
    Assert.assertArrayEquals(new Object[] {1, null, 11, null, null}, rows.get(0));
    Assert.assertArrayEquals(new Object[] {3, null, 30, 5, null}, rows.get(1));

    rows = shell.executeStatement(String.format(
        "SELECT count(*) FROM %s WHERE b IS NULL AND c > 10 AND s.u IS NULL", identifier));
    Assert.assertEquals(2L, rows.get(0)[0]);
  }

  @Test
  public void testUnknownTypeRejectedAsPartitionSourceOrNonNullDefault() {
    TableIdentifier identifier = TableIdentifier.of("default", "unknown_type_invalid");

    Assertions.assertThatThrownBy(() -> shell.executeStatement(String.format(
            "CREATE EXTERNAL TABLE %s (id INT, placeholder UNKNOWN) PARTITIONED BY SPEC (placeholder) " +
                "STORED BY ICEBERG TBLPROPERTIES ('format-version'='3')", identifier)))
        .hasMessageContaining("partitioning by a column of type unknown is not supported");

    Assertions.assertThatThrownBy(() -> createTable(identifier, "id INT, placeholder UNKNOWN DEFAULT 'abc'"))
        .hasMessageContaining("columns of type unknown only allow a NULL default");

    createTable(identifier, "id INT, placeholder UNKNOWN DEFAULT NULL");
    Assertions.assertThatThrownBy(() -> shell.executeStatement(String.format(
            "ALTER TABLE %s SET PARTITION SPEC (placeholder)", identifier)))
        .hasMessageContaining("partitioning by a column of type unknown is not supported");

    shell.executeStatement(String.format("INSERT INTO %s (id) VALUES (1)", identifier));
    List<Object[]> rows = shell.executeStatement(String.format("SELECT id, placeholder FROM %s", identifier));
    Assert.assertEquals(1, rows.size());
    Assert.assertArrayEquals(new Object[] {1, null}, rows.get(0));
  }
}
