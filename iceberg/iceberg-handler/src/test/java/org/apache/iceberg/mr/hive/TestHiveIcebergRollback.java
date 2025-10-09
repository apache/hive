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
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;


/**
 * Tests covering the rollback feature
 */
public class TestHiveIcebergRollback extends HiveIcebergStorageHandlerWithEngineBase {

  @Override
  protected void validateTestParams() {
    Assume.assumeTrue(fileFormat == FileFormat.PARQUET && isVectorized &&
        testTableType == TestTables.TestTableType.HIVE_CATALOG && formatVersion == 2);
  }

  @Test
  public void testRollbackToTimestamp() throws IOException, InterruptedException {
    TableIdentifier identifier = TableIdentifier.of("default", "source");
    Table table = testTables.createTableWithVersions(shell, identifier.name(),
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, fileFormat,
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, 3);
    /* TODO: re-add test case when Iceberg issue https://github.com/apache/iceberg/issues/5507 is resolved.
    shell.executeStatement("ALTER TABLE " + identifier.name() + " EXECUTE ROLLBACK('" +
        HiveIcebergTestUtils.timestampAfterSnapshot(table, 2) + "')");
    Assert.assertEquals(5, shell.executeStatement("SELECT * FROM " + identifier.name()).size());
    Assert.assertEquals(3, table.history().size());
    */
    shell.executeStatement("ALTER TABLE " + identifier.name() + " EXECUTE ROLLBACK('" +
        HiveIcebergTestUtils.timestampAfterSnapshot(table, 1) + "')");
    Assert.assertEquals(4, shell.executeStatement("SELECT * FROM " + identifier.name()).size());
    table.refresh();
    Assert.assertEquals(4, table.history().size());
    shell.executeStatement("ALTER TABLE " + identifier.name() + " EXECUTE ROLLBACK('" +
        HiveIcebergTestUtils.timestampAfterSnapshot(table, 0) + "')");
    Assert.assertEquals(3, shell.executeStatement("SELECT * FROM " + identifier.name()).size());
    table.refresh();
    Assert.assertEquals(5, table.history().size());
  }

  @Test
  public void testRollbackToVersion() throws IOException, InterruptedException {
    TableIdentifier identifier = TableIdentifier.of("default", "source");
    Table table = testTables.createTableWithVersions(shell, identifier.name(),
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, fileFormat,
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, 3);
    /* TODO: re-add test case when Iceberg issue https://github.com/apache/iceberg/issues/5507 is resolved.
    shell.executeStatement("ALTER TABLE " + identifier.name() + " EXECUTE ROLLBACK(" +
        table.history().get(2).snapshotId() + ")");
    Assert.assertEquals(5, shell.executeStatement("SELECT * FROM " + identifier.name()).size());
    table.refresh();
    Assert.assertEquals(3, table.history().size());
     */
    shell.executeStatement("ALTER TABLE " + identifier.name() + " EXECUTE ROLLBACK(" +
        table.history().get(1).snapshotId() + ")");
    Assert.assertEquals(4, shell.executeStatement("SELECT * FROM " + identifier.name()).size());
    table.refresh();
    Assert.assertEquals(4, table.history().size());
    shell.executeStatement("ALTER TABLE " + identifier.name() + " EXECUTE ROLLBACK(" +
        table.history().get(0).snapshotId() + ")");
    Assert.assertEquals(3, shell.executeStatement("SELECT * FROM " + identifier.name()).size());
    table.refresh();
    Assert.assertEquals(5, table.history().size());
  }

  @Test
  public void testRevertRollback() throws IOException, InterruptedException {
    Assume.assumeTrue("Rollback revert is only supported for tables from Hive Catalog",
        testTableType.equals(TestTables.TestTableType.HIVE_CATALOG));
    TableIdentifier identifier = TableIdentifier.of("default", "source");
    Table table = testTables.createTableWithVersions(shell, identifier.name(),
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, fileFormat,
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, 2);
    String metadataLocationBeforeRollback = ((BaseTable) table).operations().current().metadataFileLocation();
    shell.executeStatement("ALTER TABLE " + identifier.name() + " EXECUTE ROLLBACK(" +
        table.history().get(0).snapshotId() + ")");
    Assert.assertEquals(3, shell.executeStatement("SELECT * FROM " + identifier.name()).size());
    table.refresh();
    Assert.assertEquals(3, table.history().size());
    shell.executeStatement("ALTER TABLE " + identifier.name() + " SET TBLPROPERTIES('metadata_location'='" +
        metadataLocationBeforeRollback + "')");
    Assert.assertEquals(4, shell.executeStatement("SELECT * FROM " + identifier.name()).size());
    table.refresh();
    Assert.assertEquals(2, table.history().size());
  }

  @Test
  public void testInvalidRollbackToTimestamp() throws IOException, InterruptedException {
    TableIdentifier identifier = TableIdentifier.of("default", "source");
    testTables.createTableWithVersions(shell, identifier.name(), HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, 2);
    AssertHelpers.assertThrows("should throw exception", IllegalArgumentException.class,
        "Cannot roll back, no valid snapshot older than", () -> {
        shell.executeStatement("ALTER TABLE " + identifier.name() + " EXECUTE ROLLBACK('1970-01-01 00:00:00')");
        });
  }

  @Test
  public void testInvalidRollbackToVersion() throws IOException, InterruptedException {
    TableIdentifier identifier = TableIdentifier.of("default", "source");
    Table table = testTables.createTableWithVersions(shell, identifier.name(),
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, fileFormat,
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, 2);
    AssertHelpers.assertThrows("should throw exception", IllegalArgumentException.class,
        "Cannot roll back to unknown snapshot id", () -> {
        shell.executeStatement("ALTER TABLE " + identifier.name() + " EXECUTE ROLLBACK(1111)");
        });
    shell.executeStatement("ALTER TABLE " + identifier.name() + " EXECUTE ROLLBACK(" +
        table.history().get(0).snapshotId() + ")");
    AssertHelpers.assertThrows("should throw exception", IllegalArgumentException.class,
        "Cannot roll back to snapshot, not an ancestor of the current state", () -> {
        shell.executeStatement("ALTER TABLE " + identifier.name() + " EXECUTE ROLLBACK(" +
            table.history().get(1).snapshotId() + ")");
        });
  }

  @Test
  public void testNonIcebergRollback() {
    shell.executeStatement("CREATE TABLE non_ice (id int)");

    AssertHelpers.assertThrows("should throw exception", IllegalArgumentException.class,
        "ALTER EXECUTE is not supported for table", () -> {
        shell.executeStatement("ALTER TABLE non_ice EXECUTE ROLLBACK('2022-09-26 00:00:00')");
        });
  }
}
