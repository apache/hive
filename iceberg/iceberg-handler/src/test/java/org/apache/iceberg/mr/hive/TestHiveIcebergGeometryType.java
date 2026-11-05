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
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.mr.hive.test.TestTables.TestTableType;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import static org.apache.iceberg.types.Types.NestedField.optional;

public class TestHiveIcebergGeometryType extends HiveIcebergStorageHandlerWithEngineBase {

  // WKB for POINT (1 2) and POINT (3 4), little endian
  private static final String POINT_1_2 = "0101000000000000000000F03F0000000000000040";
  private static final String POINT_3_4 = "010100000000000000000008400000000000001040";

  @Parameters(name = "fileFormat={0}, catalog={1}, isVectorized={2}, formatVersion={3}")
  public static Collection<Object[]> parameters() {
    return HiveIcebergStorageHandlerWithEngineBase.getParameters(p ->
        p.fileFormat() == FileFormat.PARQUET &&
        p.testTableType() == TestTableType.HIVE_CATALOG &&
        p.formatVersion() == 3);
  }

  @Test
  public void testGeometryTypeReadWrite() {
    Schema schema = new Schema(
        optional(1, "id", Types.IntegerType.get()),
        optional(2, "geom", Types.GeometryType.crs84()),
        optional(3, "geom_3857", Types.GeometryType.of("EPSG:3857")));

    TableIdentifier table = TableIdentifier.of("default", "geometry_type_basic");

    testTables.createTable(shell, table.name(),
        schema, PartitionSpec.unpartitioned(), fileFormat, ImmutableList.of(), formatVersion,
        ImmutableMap.of());

    shell.executeStatement(String.format(
        "INSERT INTO %s VALUES " +
            "(1, CAST(unhex('%s') AS GEOMETRY), CAST(unhex('%s') AS GEOMETRY('EPSG:3857'))), " +
            "(2, unhex('%s'), NULL), " +
            "(3, NULL, NULL)",
        table, POINT_1_2, POINT_3_4, POINT_3_4));

    List<Object[]> rows = shell.executeStatement(String.format(
        "SELECT id, hex(CAST(geom AS BINARY)), hex(CAST(geom_3857 AS BINARY)) FROM %s ORDER BY id", table));

    Assert.assertEquals(3, rows.size());
    Assert.assertArrayEquals(new Object[] {1, POINT_1_2, POINT_3_4}, rows.get(0));
    Assert.assertArrayEquals(new Object[] {2, POINT_3_4, null}, rows.get(1));
    Assert.assertArrayEquals(new Object[] {3, null, null}, rows.get(2));

    rows = shell.executeStatement(String.format(
        "SELECT id FROM %s WHERE geom IS NOT NULL AND geom_3857 IS NULL", table));
    Assert.assertEquals(1, rows.size());
    Assert.assertEquals(2, rows.get(0)[0]);

    rows = shell.executeStatement(String.format(
        "SELECT ST_AsText(ST_GeomFromWKB(CAST(geom AS BINARY))) FROM %s WHERE id = 1", table));
    Assert.assertEquals(1, rows.size());
    Assert.assertEquals("POINT (1 2)", rows.get(0)[0]);
  }

  @Test
  public void testGeometryTypeDdl() {
    TableIdentifier table = TableIdentifier.of("default", "geometry_type_ddl");

    shell.executeStatement(String.format(
        "CREATE EXTERNAL TABLE %s (id INT, geom GEOMETRY, geom_3857 GEOMETRY('EPSG:3857')) STORED BY ICEBERG " +
            "TBLPROPERTIES ('format-version'='3')", table));

    Table icebergTable = testTables.loadTable(table);
    Assert.assertEquals(Types.GeometryType.crs84(), icebergTable.schema().findType("geom"));
    Assert.assertEquals(Types.GeometryType.of("EPSG:3857"), icebergTable.schema().findType("geom_3857"));

    List<Object[]> rows = shell.executeStatement("DESCRIBE " + table);
    Assert.assertEquals("geometry", rows.get(1)[1]);
    Assert.assertEquals("geometry('EPSG:3857')", rows.get(2)[1]);

    shell.executeStatement(String.format(
        "INSERT INTO %s VALUES (1, unhex('%s'), unhex('%s'))", table, POINT_1_2, POINT_3_4));

    rows = shell.executeStatement(String.format(
        "SELECT id, hex(CAST(geom AS BINARY)), hex(CAST(geom_3857 AS BINARY)) FROM %s", table));
    Assert.assertEquals(1, rows.size());
    Assert.assertArrayEquals(new Object[] {1, POINT_1_2, POINT_3_4}, rows.get(0));
  }

  @Test
  public void testGeometryTypeCtas() {
    TableIdentifier source = TableIdentifier.of("default", "geometry_type_source");
    TableIdentifier target = TableIdentifier.of("default", "geometry_type_target");

    shell.executeStatement(String.format(
        "CREATE EXTERNAL TABLE %s (id INT, geom GEOMETRY('EPSG:4326')) STORED BY ICEBERG " +
            "TBLPROPERTIES ('format-version'='3')", source));
    shell.executeStatement(String.format("INSERT INTO %s VALUES (1, unhex('%s'))", source, POINT_1_2));

    shell.executeStatement(String.format(
        "CREATE EXTERNAL TABLE %s STORED BY ICEBERG TBLPROPERTIES ('format-version'='3') AS SELECT * FROM %s",
        target, source));

    Table icebergTable = testTables.loadTable(target);
    Assert.assertEquals(Types.GeometryType.of("EPSG:4326"), icebergTable.schema().findType("geom"));

    List<Object[]> rows = shell.executeStatement(String.format(
        "SELECT id, hex(CAST(geom AS BINARY)) FROM %s", target));
    Assert.assertEquals(1, rows.size());
    Assert.assertArrayEquals(new Object[] {1, POINT_1_2}, rows.get(0));
  }

  @Test
  public void testGeometryTypeRejectsString() {
    TableIdentifier table = TableIdentifier.of("default", "geometry_type_string");

    shell.executeStatement(String.format(
        "CREATE EXTERNAL TABLE %s (id INT, geom GEOMETRY) STORED BY ICEBERG " +
            "TBLPROPERTIES ('format-version'='3')", table));

    for (String value : new String[] {"'POINT (1 2)'", "CAST('POINT (1 2)' AS GEOMETRY)"}) {
      IllegalArgumentException exception = Assert.assertThrows(IllegalArgumentException.class, () ->
          shell.executeStatement(String.format("INSERT INTO %s SELECT 1, %s", table, value)));
      Assert.assertTrue(exception.getMessage(), exception.getMessage().contains("Only binary data in the WKB format"));
    }
  }

  @Test
  public void testGeometryTypeRejectsOrc() {
    TableIdentifier orcTable = TableIdentifier.of("default", "geometry_type_orc");
    TableIdentifier parquetTable = TableIdentifier.of("default", "geometry_type_parquet");

    assertOrcRejected(String.format(
        "CREATE EXTERNAL TABLE %s (id INT, geom GEOMETRY) STORED BY ICEBERG STORED AS ORC " +
            "TBLPROPERTIES ('format-version'='3')", orcTable), "geom");
    assertOrcRejected(String.format(
        "CREATE EXTERNAL TABLE %s (id INT, s STRUCT<g:GEOMETRY('EPSG:3857')>) STORED BY ICEBERG " +
            "TBLPROPERTIES ('format-version'='3', 'write.format.default'='orc')", orcTable), "s.g");

    shell.executeStatement(String.format(
        "CREATE EXTERNAL TABLE %s (id INT) STORED BY ICEBERG STORED AS ORC " +
            "TBLPROPERTIES ('format-version'='3')", orcTable));
    assertOrcRejected(String.format("ALTER TABLE %s ADD COLUMNS (geom GEOMETRY)", orcTable), "geom");
    assertOrcRejected(String.format("ALTER TABLE %s REPLACE COLUMNS (id INT, geom GEOMETRY)", orcTable), "geom");
    Assert.assertNull(testTables.loadTable(orcTable).schema().findField("geom"));

    shell.executeStatement(String.format(
        "CREATE EXTERNAL TABLE %s (id INT, geom GEOMETRY) STORED BY ICEBERG " +
            "TBLPROPERTIES ('format-version'='3')", parquetTable));
    assertOrcRejected(String.format(
        "ALTER TABLE %s SET TBLPROPERTIES ('write.format.default'='orc')", parquetTable), "geom");
    Assert.assertNotEquals("orc",
        testTables.loadTable(parquetTable).properties().get(TableProperties.DEFAULT_FILE_FORMAT));
  }

  private void assertOrcRejected(String statement, String column) {
    IllegalArgumentException exception = Assert.assertThrows(IllegalArgumentException.class,
        () -> shell.executeStatement(statement));
    Assert.assertTrue(exception.getMessage(), exception.getMessage().contains(
        "Column " + column + " has type geometry"));
    Assert.assertTrue(exception.getMessage(), exception.getMessage().contains(
        "not supported by the ORC file format"));
  }

  @Test
  public void testGeometryTypeAvro() {
    TableIdentifier table = TableIdentifier.of("default", "geometry_type_avro");

    shell.executeStatement(String.format(
        "CREATE EXTERNAL TABLE %s (id INT, geom GEOMETRY('EPSG:3857')) STORED BY ICEBERG STORED AS AVRO " +
            "TBLPROPERTIES ('format-version'='3')", table));
    shell.executeStatement(String.format("INSERT INTO %s VALUES (1, unhex('%s')), (2, NULL)", table, POINT_1_2));

    Table icebergTable = testTables.loadTable(table);
    Assert.assertEquals(Types.GeometryType.of("EPSG:3857"), icebergTable.schema().findType("geom"));
    List<Object[]> rows = shell.executeStatement(String.format(
        "SELECT id, hex(CAST(geom AS BINARY)) FROM %s ORDER BY id", table));
    Assert.assertEquals(2, rows.size());
    Assert.assertArrayEquals(new Object[] {1, POINT_1_2}, rows.get(0));
    Assert.assertArrayEquals(new Object[] {2, null}, rows.get(1));
  }

  @Test
  public void testGeometryTypeValidatesWkb() {
    TableIdentifier table = TableIdentifier.of("default", "geometry_type_wkb");

    shell.executeStatement(String.format(
        "CREATE EXTERNAL TABLE %s (id INT, geom GEOMETRY) STORED BY ICEBERG " +
            "TBLPROPERTIES ('format-version'='3')", table));

    String truncated = POINT_1_2.substring(0, 20);
    String ewkbWithSrid = "0101000020E6100000000000000000F03F0000000000000040";
    for (String wkb : new String[] {truncated, ewkbWithSrid}) {
      IllegalArgumentException exception = Assert.assertThrows(IllegalArgumentException.class, () ->
          shell.executeStatement(String.format("INSERT INTO %s SELECT 1, unhex('%s')", table, wkb)));
      Assert.assertTrue(exception.getMessage(), exception.getMessage().contains("Invalid WKB"));
    }

    shell.executeStatement(String.format(
        "INSERT INTO %s SELECT 1, ST_AsBinary(ST_GeomFromText('POINT (1 2)'))", table));
    List<Object[]> rows = shell.executeStatement(String.format(
        "SELECT id, hex(CAST(geom AS BINARY)) FROM %s", table));
    Assert.assertEquals(1, rows.size());
    Assert.assertArrayEquals(new Object[] {1, "00000000013FF00000000000004000000000000000"}, rows.get(0));
  }

  @Test
  public void testGeometryTypeRejectsCrsChange() {
    TableIdentifier source = TableIdentifier.of("default", "geometry_type_crs_source");
    TableIdentifier target = TableIdentifier.of("default", "geometry_type_crs_target");

    shell.executeStatement(String.format(
        "CREATE EXTERNAL TABLE %s (id INT, geom GEOMETRY('EPSG:4326')) STORED BY ICEBERG " +
            "TBLPROPERTIES ('format-version'='3')", source));
    shell.executeStatement(String.format(
        "CREATE EXTERNAL TABLE %s (id INT, geom GEOMETRY('EPSG:3857')) STORED BY ICEBERG " +
            "TBLPROPERTIES ('format-version'='3')", target));
    shell.executeStatement(String.format("INSERT INTO %s VALUES (1, unhex('%s'))", source, POINT_1_2));

    for (String select : new String[] {"geom", "CAST(geom AS GEOMETRY('EPSG:3857'))"}) {
      IllegalArgumentException exception = Assert.assertThrows(IllegalArgumentException.class, () ->
          shell.executeStatement(String.format("INSERT INTO %s SELECT id, %s FROM %s", target, select, source)));
      Assert.assertTrue(exception.getMessage(), exception.getMessage().contains("coordinate reference systems differ"));
    }

    shell.executeStatement(String.format(
        "INSERT INTO %s SELECT id, CAST(CAST(geom AS BINARY) AS GEOMETRY('EPSG:3857')) FROM %s", target, source));
    List<Object[]> rows = shell.executeStatement(String.format(
        "SELECT id, hex(CAST(geom AS BINARY)) FROM %s", target));
    Assert.assertEquals(1, rows.size());
    Assert.assertArrayEquals(new Object[] {1, POINT_1_2}, rows.get(0));

    rows = shell.executeStatement(String.format(
        "SELECT hex(CAST(CAST(CAST(unhex('%s') AS GEOMETRY('EPSG:3857')) AS GEOMETRY('epsg:3857')) AS BINARY))",
        POINT_1_2));
    Assert.assertArrayEquals(new Object[] {POINT_1_2}, rows.get(0));
  }
}
