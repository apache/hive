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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/**
 * Tests table migration from native Hive tables to Iceberg backed tables (with the same underlying fileformat).
 * Migration on the original tables is done by setting HiveIcebergStorageHandler on them, contents later should be
 * verified by a select query.
 */
public class TestHiveIcebergMigration extends HiveIcebergStorageHandlerWithEngineBase {

  @Test
  public void testMigrateHiveTableWithPrimitiveTypeColumnsToIceberg() throws TException, InterruptedException {
    shell.setHiveSessionValue(InputFormatConfig.SCHEMA_AUTO_CONVERSION, "true");
    TableIdentifier identifier = TableIdentifier.of("default", "tbl_alltypes");
    shell.executeStatement(String.format("CREATE EXTERNAL TABLE %s (" +
        "a INT, " +
        "decimal_col DECIMAL(30, 3), " +
        "tinyint_col TINYINT, " +
        "boolean_col BOOLEAN, " +
        "float_col FLOAT, " +
        "bigint_col BIGINT, " +
        "double_col DOUBLE, " +
        "string_col STRING, " +
        "int_col INT, " +
        "smallint_col SMALLINT) " +
        "STORED AS %s %s %s",
        identifier.name(), fileFormat.name(), testTables.locationForCreateTableSQL(identifier),
        testTables.propertiesForCreateTableSQL(ImmutableMap.of())));

    shell.executeStatement(String.format("INSERT INTO TABLE %s VALUES(" +
        "1, " +
        "13.234, " +
        "8, " +
        "false, " +
        "0.7896, " +
        "543643275, " +
        "5462435243, " +
        "'wfewjifwejfoewfnvewokfow', " +
        "43221, " +
        "129 " +
        ")", identifier.name()));

    validateMigration(identifier.name());
  }

  @Test
  public void testMigrateHiveTableWithUnsupportedPrimitiveTypeColumnToIceberg() {
    // enough to test once
    Assume.assumeTrue(fileFormat == FileFormat.ORC && isVectorized &&
        testTableType == TestTables.TestTableType.HIVE_CATALOG);
    TableIdentifier identifier = TableIdentifier.of("default", "tbl_unsupportedtypes");
    shell.executeStatement(String.format("CREATE EXTERNAL TABLE %s (" +
        "char_col CHAR(10)) STORED AS %s %s %s", identifier.name(), fileFormat.name(),
        testTables.locationForCreateTableSQL(identifier), testTables.propertiesForCreateTableSQL(ImmutableMap.of())));
    AssertHelpers.assertThrows("should throw exception", IllegalArgumentException.class,
        "Cannot convert hive table to iceberg that", () -> {
          shell.executeStatement(String.format("ALTER TABLE %s Convert to iceberg", identifier.name()));
        });
  }

  @Test
  public void testMigrateHiveTableWithComplexTypeColumnsToIceberg() throws TException, InterruptedException {
    TableIdentifier identifier = TableIdentifier.of("default", "tbl_complex");
    shell.executeStatement(String.format("CREATE EXTERNAL TABLE %s (" +
        "a int, " +
        "arrayofprimitives array<string>, " +
        "arrayofarrays array<array<string>>, " +
        "arrayofmaps array<map<string, string>>, " +
        "arrayofstructs array<struct<something:string, someone:string, somewhere:string>>, " +
        "mapofprimitives map<string, string>, " +
        "mapofarrays map<string, array<string>>, " +
        "mapofmaps map<string, map<string, string>>, " +
        "mapofstructs map<string, struct<something:string, someone:string, somewhere:string>>, " +
        "structofprimitives struct<something:string, somewhere:string>, " +
        "structofarrays struct<names:array<string>, birthdays:array<string>>, " +
        "structofmaps struct<map1:map<string, string>, map2:map<string, string>>" +
        ") STORED AS %s %s %s", identifier.name(), fileFormat.name(),
        testTables.locationForCreateTableSQL(identifier),
        testTables.propertiesForCreateTableSQL(ImmutableMap.of())));

    shell.executeStatement(String.format("INSERT INTO %s VALUES (" +
        "1, " +
        "array('a','b','c'), " +
        "array(array('a'), array('b', 'c')), " +
        "array(map('a','b'), map('e','f')), " +
        "array(named_struct('something', 'a', 'someone', 'b', 'somewhere', 'c'), " +
        "named_struct('something', 'e', 'someone', 'f', 'somewhere', 'g')), " +
        "map('a', 'b'), " +
        "map('a', array('b','c')), " +
        "map('a', map('b','c')), " +
        "map('a', named_struct('something', 'b', 'someone', 'c', 'somewhere', 'd')), " +
        "named_struct('something', 'a', 'somewhere', 'b'), " +
        "named_struct('names', array('a', 'b'), 'birthdays', array('c', 'd', 'e')), " +
        "named_struct('map1', map('a', 'b'), 'map2', map('c', 'd')) " +
        ")", identifier.name()));

    validateMigration(identifier.name());
  }

  @Test
  public void testMigrateHiveTableToIcebergWithTBLPROPERTIES() throws TException, InterruptedException {
    String tableName = "tbl";
    String createQuery = "CREATE EXTERNAL TABLE " + tableName + " (a int) STORED AS " + fileFormat.name() + " " +
        testTables.locationForCreateTableSQL(TableIdentifier.of("default", tableName)) +
        testTables.propertiesForCreateTableSQL(Collections.singletonMap("random.prop", "random"));
    shell.executeStatement(createQuery);
    shell.executeStatement("INSERT INTO " + tableName + " VALUES (1), (2), (3)");
    Table hmsTable = validateMigration(tableName, "TBLPROPERTIES('external.table.purge'='true')");

    // Check the new property gets set.
    Assert.assertEquals("true", hmsTable.getParameters().get("external.table.purge"));
    // Check the exiting property stays as is.
    Assert.assertEquals("random", hmsTable.getParameters().get("random.prop"));

    // Check the new property gets translated to iceberg equivalent and gets set.
    org.apache.iceberg.Table icebergTable = testTables.loadTable(TableIdentifier.of("default", tableName));
    Assert.assertEquals("true", icebergTable.properties().get(TableProperties.GC_ENABLED));

    // Retry migration after table is already of iceberg type.
    AssertHelpers.assertThrows("Should throw exception", IllegalArgumentException.class,
        "Can not convert table to ICEBERG ,Table is already of that format", () -> {
          shell.executeStatement("ALTER TABLE " + tableName + " CONVERT TO ICEBERG");
        });
  }
  @Test
  public void testMigrateHiveTableToIceberg() throws TException, InterruptedException {
    String tableName = "tbl";
    String createQuery = "CREATE EXTERNAL TABLE " +  tableName + " (a int) STORED AS " + fileFormat.name() + " " +
        testTables.locationForCreateTableSQL(TableIdentifier.of("default", tableName)) +
        testTables.propertiesForCreateTableSQL(ImmutableMap.of());
    shell.executeStatement(createQuery);
    shell.executeStatement("INSERT INTO " + tableName + " VALUES (1), (2), (3)");
    validateMigration(tableName);
  }

  @Test
  public void testMigratePartitionedHiveTableToIceberg() throws TException, InterruptedException {
    String tableName = "tbl_part";
    shell.executeStatement("CREATE EXTERNAL TABLE " + tableName + " (a int) PARTITIONED BY (b string) STORED AS " +
        fileFormat.name() + " " + testTables.locationForCreateTableSQL(TableIdentifier.of("default", tableName)) +
        testTables.propertiesForCreateTableSQL(ImmutableMap.of()));
    shell.executeStatement("INSERT INTO " + tableName + " PARTITION (b='aaa') VALUES (1), (2), (3)");
    shell.executeStatement("INSERT INTO " + tableName + " PARTITION (b='bbb') VALUES (4), (5)");
    shell.executeStatement("INSERT INTO " + tableName + " PARTITION (b='ccc') VALUES (6)");
    shell.executeStatement("INSERT INTO " + tableName + " PARTITION (b='ddd') VALUES (7), (8), (9), (10)");
    validateMigration(tableName);
  }

  @Test
  public void testMigratePartitionedBucketedHiveTableToIceberg() throws TException, InterruptedException {
    String tableName = "tbl_part_bucketed";
    shell.executeStatement("CREATE EXTERNAL TABLE " + tableName + " (a int) PARTITIONED BY (b string) clustered by " +
        "(a) INTO 2 BUCKETS STORED AS " + fileFormat.name() + " " +
        testTables.locationForCreateTableSQL(TableIdentifier.of("default", tableName)) +
        testTables.propertiesForCreateTableSQL(ImmutableMap.of()));
    shell.executeStatement("INSERT INTO " + tableName + " PARTITION (b='aaa') VALUES (1), (2), (3)");
    shell.executeStatement("INSERT INTO " + tableName + " PARTITION (b='bbb') VALUES (4), (5)");
    shell.executeStatement("INSERT INTO " + tableName + " PARTITION (b='ccc') VALUES (6)");
    shell.executeStatement("INSERT INTO " + tableName + " PARTITION (b='ddd') VALUES (7), (8), (9), (10)");
    validateMigration(tableName);
  }

  @Test
  public void testRollbackMigrateHiveTableToIceberg() throws TException, InterruptedException {
    String tableName = "tbl_rollback";
    shell.executeStatement("CREATE EXTERNAL TABLE " +  tableName + " (a int) STORED AS " + fileFormat.name() + " " +
        testTables.locationForCreateTableSQL(TableIdentifier.of("default", tableName)) +
        testTables.propertiesForCreateTableSQL(ImmutableMap.of()));
    shell.executeStatement("INSERT INTO " + tableName + " VALUES (1), (2), (3)");
    validateMigrationRollback(tableName);
  }

  @Test
  public void testRollbackMigratePartitionedHiveTableToIceberg() throws TException, InterruptedException {
    String tableName = "tbl_rollback";
    shell.executeStatement("CREATE EXTERNAL TABLE " + tableName + " (a int) PARTITIONED BY (b string) STORED AS " +
        fileFormat.name() + " " + testTables.locationForCreateTableSQL(TableIdentifier.of("default", tableName)) +
        testTables.propertiesForCreateTableSQL(ImmutableMap.of()));
    shell.executeStatement("INSERT INTO " + tableName + " PARTITION (b='aaa') VALUES (1), (2), (3)");
    shell.executeStatement("INSERT INTO " + tableName + " PARTITION (b='bbb') VALUES (4), (5)");
    shell.executeStatement("INSERT INTO " + tableName + " PARTITION (b='ccc') VALUES (6)");
    shell.executeStatement("INSERT INTO " + tableName + " PARTITION (b='ddd') VALUES (7), (8), (9), (10)");
    validateMigrationRollback(tableName);
  }

  @Test
  public void testRollbackMultiPartitionedHiveTableToIceberg() throws TException, InterruptedException {
    String tableName = "tbl_rollback";
    shell.executeStatement("CREATE EXTERNAL TABLE " + tableName + " (a int) PARTITIONED BY (b string, c int) " +
        "STORED AS " + fileFormat.name() + " " +
        testTables.locationForCreateTableSQL(TableIdentifier.of("default", tableName)) +
        testTables.propertiesForCreateTableSQL(ImmutableMap.of()));
    shell.executeStatement("INSERT INTO " + tableName + " PARTITION (b='aaa', c='111') VALUES (1), (2), (3)");
    shell.executeStatement("INSERT INTO " + tableName + " PARTITION (b='bbb', c='111') VALUES (4), (5)");
    shell.executeStatement("INSERT INTO " + tableName + " PARTITION (b='aaa', c='222') VALUES (6)");
    shell.executeStatement("INSERT INTO " + tableName + " PARTITION (b='ccc', c='333') VALUES (7), (8), (9), (10)");
    validateMigrationRollback(tableName);
  }

  @Test
  public void testRollbackMigratePartitionedBucketedHiveTableToIceberg() throws TException, InterruptedException {
    String tableName = "tbl_part_bucketed";
    shell.executeStatement("CREATE EXTERNAL TABLE " + tableName + " (a int) PARTITIONED BY (b string) clustered by " +
        "(a) INTO 2 BUCKETS STORED AS " + fileFormat.name() + " " +
        testTables.locationForCreateTableSQL(TableIdentifier.of("default", tableName)) +
        testTables.propertiesForCreateTableSQL(ImmutableMap.of()));
    shell.executeStatement("INSERT INTO " + tableName + " PARTITION (b='aaa') VALUES (1), (2), (3)");
    shell.executeStatement("INSERT INTO " + tableName + " PARTITION (b='bbb') VALUES (4), (5)");
    shell.executeStatement("INSERT INTO " + tableName + " PARTITION (b='ccc') VALUES (6)");
    shell.executeStatement("INSERT INTO " + tableName + " PARTITION (b='ddd') VALUES (7), (8), (9), (10)");
    validateMigrationRollback(tableName);
  }

  @Test
  public void testMigrationFailsForUnsupportedSourceFileFormat() {
    // enough to test once
    Assume.assumeTrue(fileFormat == FileFormat.ORC && isVectorized &&
        testTableType == TestTables.TestTableType.HIVE_CATALOG);
    String tableName = "tbl_unsupported";
    List<String> formats = ImmutableList.of("TEXTFILE", "JSONFILE", "RCFILE", "SEQUENCEFILE");
    formats.forEach(format -> {
      shell.executeStatement("CREATE EXTERNAL TABLE " +  tableName + " (a int) STORED AS " + format + " " +
          testTables.locationForCreateTableSQL(TableIdentifier.of("default", tableName)) +
          testTables.propertiesForCreateTableSQL(ImmutableMap.of()));
      shell.executeStatement("INSERT INTO " + tableName + " VALUES (1), (2), (3)");
      AssertHelpers.assertThrows("Migrating a " + format + " table to Iceberg should have thrown an exception.",
          IllegalArgumentException.class, "Cannot convert hive table to iceberg with input format: ",
          () -> shell.executeStatement("ALTER TABLE " + tableName + " Convert to iceberg"));
      shell.executeStatement("DROP TABLE " + tableName);
    });
  }

  @Test
  public void testMigrationFailsForManagedTable() {
    // enough to test once
    Assume.assumeTrue(fileFormat == FileFormat.ORC && isVectorized &&
        testTableType == TestTables.TestTableType.HIVE_CATALOG);
    String tableName = "tbl_unsupported";
    shell.executeStatement("CREATE MANAGED TABLE " +  tableName + " (a int) STORED AS " + fileFormat + " " +
        testTables.locationForCreateTableSQL(TableIdentifier.of("default", tableName)) +
        testTables.propertiesForCreateTableSQL(ImmutableMap.of()));
    shell.executeStatement("INSERT INTO " + tableName + " VALUES (1), (2), (3)");
    AssertHelpers.assertThrows("Migrating a managed table to Iceberg should have thrown an exception.",
        IllegalArgumentException.class, "Converting non-external, temporary or transactional hive table to iceberg",
        () -> shell.executeStatement("ALTER TABLE " + tableName + " convert to iceberg"));
  }

  private Table validateMigration(String tableName) throws TException, InterruptedException {
    return validateMigration(tableName, null);
  }

  private Table validateMigration(String tableName, String tblProperties)
      throws TException, InterruptedException {
    List<Object[]> originalResult = shell.executeStatement("SELECT * FROM " + tableName + " ORDER BY a");
    String stmt = "ALTER TABLE " + tableName + " CONVERT TO ICEBERG";
    if (tblProperties != null) {
      stmt = stmt + " " + tblProperties;
    }
    shell.executeStatement(stmt);
    List<Object[]> alterResult = shell.executeStatement("SELECT * FROM " + tableName + " ORDER BY a");
    Assert.assertEquals(originalResult.size(), alterResult.size());
    for (int i = 0; i < originalResult.size(); i++) {
      Assert.assertEquals(originalResult.get(i).length, alterResult.get(i).length);
      for (int j = 0; j < originalResult.get(i).length; j++) {
        Assert.assertEquals(String.valueOf(originalResult.get(i)[j]), String.valueOf(alterResult.get(i)[j]));
      }
    }
    Table hmsTable = shell.metastore().getTable("default", tableName);
    validateSd(hmsTable, "iceberg");
    validateTblProps(hmsTable, true);
    validatePartitions(tableName);
    return hmsTable;
  }

  private void validatePartitions(String tableName) throws TException, InterruptedException {
    List<String> partitions = shell.metastore().run(client ->
        client.listPartitionNames("default", tableName, (short) -1));
    Assert.assertTrue(partitions.isEmpty());
  }

  private void validateMigrationRollback(String tableName) throws TException, InterruptedException {
    List<Object[]> originalResult = shell.executeStatement("SELECT * FROM " + tableName + " ORDER BY a");
    try (MockedStatic<HiveTableUtil> mockedTableUtil = Mockito.mockStatic(HiveTableUtil.class)) {
      mockedTableUtil.when(() -> HiveTableUtil.importFiles(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(),
          ArgumentMatchers.any(PartitionSpecProxy.class), ArgumentMatchers.anyList(),
          ArgumentMatchers.any(Properties.class), ArgumentMatchers.any(Configuration.class)))
          .thenThrow(new MetaException());
      try {
        shell.executeStatement("ALTER TABLE " + tableName + " CONVERT TO ICEBERG");
      } catch (IllegalArgumentException e) {
        Assert.assertTrue(e.getMessage().contains("Error occurred during hive table migration to iceberg."));
        Table hmsTable = shell.metastore().getTable("default", tableName);
        validateSd(hmsTable, fileFormat.name());
        validateTblProps(hmsTable, false);
        shell.executeStatement("MSCK REPAIR TABLE " + tableName);
        List<Object[]> alterResult = shell.executeStatement("SELECT * FROM " + tableName + " ORDER BY a");
        Assert.assertEquals(originalResult.size(), alterResult.size());
        for (int i = 0; i < originalResult.size(); i++) {
          Assert.assertTrue(Arrays.equals(originalResult.get(i), alterResult.get(i)));
        }
        return;
      }
      Assert.fail("Alter table operations should have thrown an exception.");
    }
  }

  private void validateSd(Table hmsTable, String format) {
    StorageDescriptor sd = hmsTable.getSd();
    Assert.assertTrue(sd.getSerdeInfo().getSerializationLib().toLowerCase().contains(format.toLowerCase()));
    Assert.assertTrue(sd.getInputFormat().toLowerCase().contains(format.toLowerCase()));
    Assert.assertTrue(sd.getOutputFormat().toLowerCase(Locale.ROOT).contains(format.toLowerCase()));
  }

  private void validateTblProps(Table hmsTable, boolean migrationSucceeded) {
    String migratedProp = hmsTable.getParameters().get(HiveIcebergMetaHook.MIGRATED_TO_ICEBERG);
    String tableTypeProp = hmsTable.getParameters().get(BaseMetastoreTableOperations.TABLE_TYPE_PROP);
    String nameMappingProp = hmsTable.getParameters().get(TableProperties.DEFAULT_NAME_MAPPING);
    if (migrationSucceeded) {
      Assert.assertTrue(Boolean.parseBoolean(migratedProp));
      Assert.assertEquals(BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE.toUpperCase(), tableTypeProp);
      Assert.assertTrue(nameMappingProp != null && !nameMappingProp.isEmpty());
    } else {
      Assert.assertNull(migratedProp);
      Assert.assertNotEquals(BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE.toUpperCase(), tableTypeProp);
      Assert.assertNull(nameMappingProp);
    }
  }
}
