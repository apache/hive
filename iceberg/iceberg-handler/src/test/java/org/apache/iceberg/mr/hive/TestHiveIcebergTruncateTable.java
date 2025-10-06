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
import java.util.Map;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.mr.TestHelper;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests truncate table feature on Iceberg tables.
 */
public class TestHiveIcebergTruncateTable extends HiveIcebergStorageHandlerWithEngineBase {

  @Test
  public void testTruncateTable() throws IOException, TException, InterruptedException {
    // Create an Iceberg table with some records in it then execute a truncate table command.
    // Then check if the data is deleted and the table statistics are reset to 0.
    String databaseName = "default";
    String tableName = "customers";
    Table icebergTable = testTables.createTable(shell, tableName, HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);
    testTruncateTable(databaseName, tableName, icebergTable, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS,
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, true, false);
  }

  @Test
  public void testTruncateEmptyTable() throws IOException, TException, InterruptedException {
    // Create an empty Iceberg table and execute a truncate table command on it.
    String databaseName = "default";
    String tableName = "customers";
    TableIdentifier identifier = TableIdentifier.of(databaseName, tableName);
    Table icebergTable = testTables.createTable(shell, tableName, HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        fileFormat, null);
    // Set the 'external.table.purge' table property on the table
    String alterTableCommand =
        "ALTER TABLE " + identifier + " SET TBLPROPERTIES('external.table.purge'='true')";
    shell.executeStatement(alterTableCommand);

    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS");

    shell.executeStatement("TRUNCATE " + identifier);

    icebergTable = testTables.loadTable(TableIdentifier.of(databaseName, tableName));
    Map<String, String> summary = icebergTable.currentSnapshot().summary();
    for (String key : STATS_MAPPING.values()) {
      Assert.assertEquals("0", summary.get(key));
    }
    List<Object[]> rows = shell.executeStatement("SELECT * FROM " + identifier);
    Assert.assertEquals(0, rows.size());
    validateBasicStats(icebergTable, databaseName, tableName);
  }

  @Test
  public void testMultipleTruncateTable() throws IOException, TException, InterruptedException {
    // Create an Iceberg table with come records in it, then execute a truncate table command
    // and check the result. Then insert some new data and run an other truncate table command.
    // The purpose of this test is to make sure that multiple truncate table commands can
    // run after each other without any issue (like issues with locking).
    String databaseName = "default";
    String tableName = "customers";
    Table icebergTable = testTables.createTable(shell, tableName, HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);
    testTruncateTable(databaseName, tableName, icebergTable, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS,
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, true, false);

    List<Record> newRecords = TestHelper.RecordsBuilder.newInstance(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
        .add(3L, "Jane", "Purple").add(4L, "Tim", "Grey").add(5L, "Eva", "Yellow").add(6L, "James", "White")
        .add(7L, "Jack", "Black").build();
    shell.executeStatement("INSERT INTO default.customers values (3, 'Jane', 'Purple'), (4, 'Tim', 'Grey')," +
        "(5, 'Eva', 'Yellow'), (6, 'James', 'White'), (7, 'Jack', 'Black')");

    icebergTable = testTables.loadTable(TableIdentifier.of(databaseName, tableName));
    testTruncateTable(databaseName, tableName, icebergTable, newRecords,
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, true, false);
  }

  @Test
  public void testTruncateTableExternalPurgeFalse() throws IOException, TException, InterruptedException {
    // Create an Iceberg table with some records and set the 'external.table.purge' table parameter to false.
    // Then execute a truncate table command which should run without any error, even without force.
    // Then check if the data is deleted from the table and the statistics are reset.
    String databaseName = "default";
    String tableName = "customers";
    Table icebergTable = testTables.createTable(shell, tableName, HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);
    testTruncateTable(databaseName, tableName, icebergTable, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS,
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, false, false);
  }

  @Test
  public void testTruncateTableForceExternalPurgeFalse() throws IOException, TException, InterruptedException {
    // Create an Iceberg table with some records and set the 'external.table.purge' table parameter to false.
    // Then execute a truncate table force command which should run without any error.
    // Then check if the data is deleted from the table and the statistics are reset.
    String databaseName = "default";
    String tableName = "customers";
    Table icebergTable = testTables.createTable(shell, tableName, HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);
    testTruncateTable(databaseName, tableName, icebergTable, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS,
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, false, true);
  }

  @Test
  public void testTruncateTableWithPartitionSpecOnUnpartitionedTable() throws IOException, TException,
      InterruptedException {
    // Create an Iceberg table with some record and try to run a truncate table command with partition
    // spec. The command should fail as the table is unpartitioned in Hive. Then check if the
    // initial data and the table statistics are not changed.
    String databaseName = "default";
    String tableName = "customers";
    TableIdentifier identifier = TableIdentifier.of(databaseName, tableName);
    Table icebergTable = testTables.createTable(shell, tableName, HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);
    shell.executeStatement("ALTER TABLE " + identifier + " SET TBLPROPERTIES('external.table.purge'='true')");
    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS");

    AssertHelpers.assertThrows("should throw exception", IllegalArgumentException.class,
        "Writing data into a partition fails when the Iceberg table is unpartitioned.",
        () -> {
          shell.executeStatement("TRUNCATE " + identifier + " PARTITION (customer_id=1)");
        });

    List<Object[]> rows = shell.executeStatement("SELECT * FROM " + identifier);
    HiveIcebergTestUtils.validateData(HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS,
        HiveIcebergTestUtils.valueForRow(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, rows), 0);
    icebergTable = testTables.loadTable(TableIdentifier.of(databaseName, tableName));
    validateBasicStats(icebergTable, databaseName, tableName);
  }

  @Test
  public void testTruncateTableWithPartitionSpecOnPartitionedTable() {
    // Create an Iceberg table with some record and try to run a truncate table command with partition
    // spec. The command should fail as the table is unpartitioned in Hive. Then check if the
    // initial data and the table statistics are not changed.
    String databaseName = "default";
    String tableName = "customers";
    TableIdentifier identifier = TableIdentifier.of(databaseName, tableName);
    PartitionSpec spec =
        PartitionSpec.builderFor(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA).identity("customer_id").build();
    testTables.createTable(shell, tableName, HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        spec, fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);
    shell.executeStatement("ALTER TABLE " + identifier + " SET TBLPROPERTIES('external.table.purge'='true')");
    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS");

    shell.executeStatement("TRUNCATE " + identifier + " PARTITION (customer_id=1)");

    List<Object[]> rows = shell.executeStatement("SELECT * FROM " + identifier);
    List<Record> truncatedRecords = TestHelper.RecordsBuilder
        .newInstance(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
        .add(0L, "Alice", "Brown")
        .add(2L, "Trudy", "Pink")
        .build();
    HiveIcebergTestUtils.validateData(truncatedRecords,
        HiveIcebergTestUtils.valueForRow(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, rows), 0);
  }

  @Test
  public void testTruncateTablePartitionedIcebergTable() throws TException, InterruptedException {
    // Create a partitioned Iceberg table with some initial data and run a truncate table command on this table.
    // Then check if the data is deleted and the table statistics are reset to 0.
    String databaseName = "default";
    String tableName = "customers";
    PartitionSpec spec =
        PartitionSpec.builderFor(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA).identity("last_name").build();
    List<Record> records = TestHelper.RecordsBuilder.newInstance(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
        .add(0L, "Alice", "Brown")
        .add(1L, "Bob", "Brown")
        .add(2L, "Trudy", "Green")
        .add(3L, "John", "Pink")
        .add(4L, "Jane", "Pink")
        .build();
    Table icebergTable = testTables.createTable(shell, tableName, HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        spec, fileFormat, records);
    testTruncateTable(databaseName, tableName, icebergTable, records,
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, true, false);
  }

  private void testTruncateTable(String databaseName, String tableName, Table icebergTable, List<Record> records,
      Schema schema, boolean externalTablePurge, boolean force) throws TException, InterruptedException {
    TableIdentifier identifier = TableIdentifier.of(databaseName, tableName);
    // Set the 'external.table.purge' table property on the table
    String alterTableCommand =
        "ALTER TABLE " + identifier + " SET TBLPROPERTIES('external.table.purge'='" + externalTablePurge + "')";
    shell.executeStatement(alterTableCommand);

    // Validate the initial data and the table statistics
    List<Object[]> rows = shell.executeStatement("SELECT * FROM " + identifier);
    HiveIcebergTestUtils.validateData(records, HiveIcebergTestUtils.valueForRow(schema, rows), 0);
    shell.executeStatement("ANALYZE TABLE " + identifier + " COMPUTE STATISTICS");
    validateBasicStats(icebergTable, databaseName, tableName);

    // Run a 'truncate table' or 'truncate table force' command
    String truncateCommand = "TRUNCATE " + identifier;
    if (force) {
      truncateCommand = truncateCommand + " FORCE";
    }
    shell.executeStatement(truncateCommand);

    // Validate if the data is deleted from the table and also that the table
    // statistics are reset to 0.
    Table table = testTables.loadTable(identifier);
    Map<String, String> summary = table.currentSnapshot().summary();
    for (String key : STATS_MAPPING.values()) {
      Assert.assertEquals("0", summary.get(key));
    }
    rows = shell.executeStatement("SELECT * FROM " + identifier);
    Assert.assertEquals(0, rows.size());
    validateBasicStats(table, databaseName, tableName);
  }

}
