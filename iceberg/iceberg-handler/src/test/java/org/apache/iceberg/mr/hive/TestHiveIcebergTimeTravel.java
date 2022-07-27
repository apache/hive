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
import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.iceberg.mr.hive.HiveIcebergTestUtils.timestampAfterSnapshot;

/**
 * Tests covering the time travel feature, aka reading from a table as of a certain snapshot.
 */
public class TestHiveIcebergTimeTravel extends HiveIcebergStorageHandlerWithEngineBase {

  @Test
  public void testSelectAsOfTimestamp() throws IOException, InterruptedException {
    Table table = testTables.createTableWithVersions(shell, "customers",
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, 2);

    List<Object[]> rows = shell.executeStatement(
        "SELECT * FROM customers FOR SYSTEM_TIME AS OF '" + timestampAfterSnapshot(table, 0) + "'");

    Assert.assertEquals(3, rows.size());

    rows = shell.executeStatement(
        "SELECT * FROM customers FOR SYSTEM_TIME AS OF '" + timestampAfterSnapshot(table, 1) + "'");

    Assert.assertEquals(4, rows.size());

    try {
      shell.executeStatement("SELECT * FROM customers FOR SYSTEM_TIME AS OF '1970-01-01 00:00:00'");
    } catch (Throwable e) {
      while (e.getCause() != null) {
        e = e.getCause();
      }
      Assert.assertTrue(e.getMessage().contains("Cannot find a snapshot older than 1970-01-01"));
    }
  }

  @Test
  public void testSelectAsOfVersion() throws IOException, InterruptedException {
    Table table = testTables.createTableWithVersions(shell, "customers",
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, 2);

    HistoryEntry first = table.history().get(0);
    List<Object[]> rows =
        shell.executeStatement("SELECT * FROM customers FOR SYSTEM_VERSION AS OF " + first.snapshotId());

    Assert.assertEquals(3, rows.size());

    HistoryEntry second = table.history().get(1);
    rows = shell.executeStatement("SELECT * FROM customers FOR SYSTEM_VERSION AS OF " + second.snapshotId());

    Assert.assertEquals(4, rows.size());

    try {
      shell.executeStatement("SELECT * FROM customers FOR SYSTEM_VERSION AS OF 1234");
    } catch (Throwable e) {
      while (e.getCause() != null) {
        e = e.getCause();
      }
      Assert.assertTrue(e.getMessage().contains("Cannot find snapshot with ID 1234"));
    }
  }

  @Test
  public void testCTASAsOfVersionAndTimestamp() throws IOException, InterruptedException {
    Table table = testTables.createTableWithVersions(shell, "customers",
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, fileFormat,
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, 3);

    shell.executeStatement("CREATE TABLE customers2 AS SELECT * FROM customers FOR SYSTEM_VERSION AS OF " +
        table.history().get(0).snapshotId());

    List<Object[]> rows = shell.executeStatement("SELECT * FROM customers2");
    Assert.assertEquals(3, rows.size());

    shell.executeStatement("INSERT INTO customers2 SELECT * FROM customers FOR SYSTEM_VERSION AS OF " +
        table.history().get(1).snapshotId());

    rows = shell.executeStatement("SELECT * FROM customers2");
    Assert.assertEquals(7, rows.size());

    shell.executeStatement("CREATE TABLE customers3 AS SELECT * FROM customers FOR SYSTEM_TIME AS OF '" +
        timestampAfterSnapshot(table, 1) + "'");

    rows = shell.executeStatement("SELECT * FROM customers3");
    Assert.assertEquals(4, rows.size());

    shell.executeStatement("INSERT INTO customers3 SELECT * FROM customers FOR SYSTEM_TIME AS OF '" +
        timestampAfterSnapshot(table, 0) + "'");

    rows = shell.executeStatement("SELECT * FROM customers3");
    Assert.assertEquals(7, rows.size());
  }

  @Test
  public void testAsOfWithJoins() throws IOException, InterruptedException {
    Table table = testTables.createTableWithVersions(shell, "customers",
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, fileFormat,
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, 4);

    List<Object[]> rows = shell.executeStatement("SELECT * FROM " +
        "customers FOR SYSTEM_TIME AS OF '" + timestampAfterSnapshot(table, 0) + "' fv, " +
        "customers FOR SYSTEM_TIME AS OF '" + timestampAfterSnapshot(table, 1) + "' sv " +
        "WHERE fv.first_name=sv.first_name");

    Assert.assertEquals(4, rows.size());

    rows = shell.executeStatement("SELECT * FROM " +
        "customers FOR SYSTEM_TIME AS OF '" + timestampAfterSnapshot(table, 1) + "' sv, " +
        "customers FOR SYSTEM_TIME AS OF '" + timestampAfterSnapshot(table, 2) + "' tv " +
        "WHERE sv.first_name=tv.first_name");

    Assert.assertEquals(8, rows.size());

    rows = shell.executeStatement("SELECT * FROM " +
        "customers FOR SYSTEM_TIME AS OF '" + timestampAfterSnapshot(table, 2) + "' sv, " +
        "customers lv " +
        "WHERE sv.first_name=lv.first_name");

    Assert.assertEquals(14, rows.size());

    rows = shell.executeStatement("SELECT * FROM " +
        "customers FOR SYSTEM_TIME AS OF '" + timestampAfterSnapshot(table, 1) + "' sv, " +
        "customers FOR SYSTEM_VERSION AS OF " + table.history().get(2).snapshotId() + " tv " +
        "WHERE sv.first_name=tv.first_name");

    Assert.assertEquals(8, rows.size());
  }

  @Test
  public void testAsOfWithJoinsAndSchemaEvolution() throws IOException, InterruptedException {
    TableIdentifier identifier = TableIdentifier.of("default", "customers");
    Table customers = testTables.createTableWithVersions(shell, identifier.name(),
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA_WITHOUT_COMMENTS, fileFormat,
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, 2);

    // add a new column
    shell.executeStatement(String.format("ALTER TABLE %s ADD COLUMNS (nickname string)",
        identifier.name()));
    // add some test data with the new column
    shell.executeStatement(
        String.format("INSERT INTO %s VALUES (11, 'Tony', 'Stark', 'IronMan')",
            identifier.name()));
    // drop the 'first_name' column
    shell.executeStatement(
        String.format("ALTER TABLE %s REPLACE COLUMNS (customer_id bigint, last_name string, nickname string)",
            identifier.name()));
    customers.refresh();
    // add some test date after the dropped column
    shell.executeStatement(
        String.format("INSERT INTO %s VALUES (12, 'Rogers', 'Captain America')", identifier.name()));

    // select on latest table version
    List<Object[]> latestRows = shell.executeStatement(String.format("SELECT * FROM %s", identifier.name()));
    Assert.assertEquals(6, latestRows.size());
    Assert.assertEquals(3, latestRows.get(0).length);

    // select on table version 2
    List<Object[]> version2Rows = shell.executeStatement(
        String.format("SELECT * FROM %s FOR SYSTEM_TIME AS OF '%s'", identifier.name(),
            timestampAfterSnapshot(customers, 2)));
    Assert.assertEquals(5, version2Rows.size());
    Assert.assertEquals(4, version2Rows.get(0).length);

    // select on table version 1
    List<Object[]> version1Rows = shell.executeStatement(
        String.format("SELECT * FROM %s FOR SYSTEM_VERSION AS OF %s", identifier.name(),
            customers.history().get(1).snapshotId()));
    Assert.assertEquals(4, version1Rows.size());
    Assert.assertEquals(3, version1Rows.get(0).length);

    // select on table version 0
    List<Object[]> version0Rows = shell.executeStatement(
        String.format("SELECT * FROM %s FOR SYSTEM_VERSION AS OF %s", identifier.name(),
            customers.history().get(0).snapshotId()));
    Assert.assertEquals(3, version0Rows.size());
    Assert.assertEquals(3, version0Rows.get(0).length);

    List<Object[]> joinRows = shell.executeStatement(String.format(
        "SELECT sv.nickname, lv.customer_id FROM %s FOR SYSTEM_TIME AS OF '%s' sv, " +
            "%s lv WHERE sv.last_name = lv.last_name", identifier.name(),
        timestampAfterSnapshot(customers, 2), identifier.name()));

    Assert.assertEquals(5, joinRows.size());
    Assert.assertEquals(2, joinRows.get(0).length);
  }
}
