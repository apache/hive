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
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.mr.hive.TestTables.TestTableType;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import static org.apache.iceberg.mr.hive.HiveIcebergTestUtils.timestampAfterSnapshot;

public class TestHiveIcebergBranchOperation extends HiveIcebergStorageHandlerWithEngineBase {

  @Override
  protected void validateTestParams() {
    Assume.assumeTrue(
        fileFormat == FileFormat.PARQUET &&
        testTableType == TestTableType.HIVE_CATALOG &&
        isVectorized && formatVersion == 2);
  }

  @Test
  public void testCreateBranchWithDefaultConfig() throws InterruptedException, IOException {
    Table table =
        testTables.createTableWithVersions(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
            fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, 2);

    String branchName = "test_branch_1";
    shell.executeStatement(String.format("ALTER TABLE customers CREATE BRANCH %s", branchName));
    table.refresh();
    SnapshotRef ref = table.refs().get(branchName);
    Assert.assertEquals(table.currentSnapshot().snapshotId(), ref.snapshotId());
    Assert.assertNull(ref.minSnapshotsToKeep());
    Assert.assertNull(ref.maxSnapshotAgeMs());
    Assert.assertNull(ref.maxRefAgeMs());

    // creating a branch which is already exists will fail
    try {
      shell.executeStatement(String.format("ALTER TABLE customers CREATE BRANCH %s", branchName));
    } catch (Throwable e) {
      while (e.getCause() != null) {
        e = e.getCause();
      }
      Assert.assertTrue(e.getMessage().contains("Ref test_branch_1 already exists"));
    }
  }

  @Test
  public void testCreateBranchWithSnapshotId() throws InterruptedException, IOException {
    Table table =
        testTables.createTableWithVersions(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
            fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, 2);

    String branchName = "test_branch_1";
    String branchName2 = "test_branch_2";
    Long snapshotId = table.history().get(0).snapshotId();
    shell.executeStatement(String.format("ALTER TABLE customers CREATE BRANCH %s FOR SYSTEM_VERSION AS OF %d",
        branchName, snapshotId));
    shell.executeStatement(
        String.format("CREATE BRANCH %s FROM customers FOR SYSTEM_VERSION AS OF %d", branchName2,
            snapshotId));
    table.refresh();
    SnapshotRef ref = table.refs().get(branchName);
    Assert.assertEquals(snapshotId.longValue(), ref.snapshotId());
    Assert.assertNull(ref.minSnapshotsToKeep());
    Assert.assertNull(ref.maxSnapshotAgeMs());
    Assert.assertNull(ref.maxRefAgeMs());
    Assert.assertEquals(snapshotId.longValue(), table.refs().get(branchName2).snapshotId());
  }

  @Test
  public void testCreateBranchWithTimeStamp() throws InterruptedException, IOException {
    Table table =
        testTables.createTableWithVersions(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
            fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, 2);

    String branchName = "test_branch_1";
    String branchName2 = "test_branch_2";
    Long snapshotId = table.history().get(0).snapshotId();

    shell.executeStatement(String.format("ALTER TABLE customers CREATE BRANCH %s FOR SYSTEM_TIME AS OF '%s'",
        branchName, timestampAfterSnapshot(table, 0)));
    shell.executeStatement(
        String.format("CREATE BRANCH %s FROM customers FOR SYSTEM_TIME AS OF '%s'", branchName2,
            timestampAfterSnapshot(table, 0)));
    table.refresh();
    SnapshotRef ref = table.refs().get(branchName);
    Assert.assertEquals(snapshotId.longValue(), ref.snapshotId());
    Assert.assertEquals(snapshotId.longValue(), table.refs().get(branchName2).snapshotId());
  }

  @Test
  public void testCreateBranchWithMaxRefAge() throws InterruptedException, IOException {
    Table table =
        testTables.createTableWithVersions(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
            fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, 2);

    String branchName = "test_branch_1";
    String branchName2 = "test_branch_2";
    long maxRefAge = 5L;
    shell.executeStatement(String.format("ALTER TABLE customers CREATE BRANCH %s RETAIN %d DAYS",
        branchName, maxRefAge));
    shell.executeStatement(
        String.format("CREATE BRANCH %s FROM customers RETAIN %d DAYS", branchName2, maxRefAge));
    table.refresh();
    SnapshotRef ref = table.refs().get(branchName);
    Assert.assertEquals(table.currentSnapshot().snapshotId(), ref.snapshotId());
    Assert.assertNull(ref.minSnapshotsToKeep());
    Assert.assertNull(ref.maxSnapshotAgeMs());
    Assert.assertEquals(TimeUnit.DAYS.toMillis(maxRefAge), ref.maxRefAgeMs().longValue());
    Assert.assertEquals(TimeUnit.DAYS.toMillis(maxRefAge), table.refs().get(branchName2).maxRefAgeMs().longValue());
  }

  @Test
  public void testCreateBranchWithMinSnapshotsToKeep() throws InterruptedException, IOException {
    Table table =
        testTables.createTableWithVersions(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
            fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, 2);

    String branchName = "test_branch_1";
    String branchName2 = "test_branch_2";
    Integer minSnapshotsToKeep = 2;
    shell.executeStatement(String.format("ALTER TABLE customers CREATE BRANCH %s WITH SNAPSHOT RETENTION %d SNAPSHOTS",
        branchName, minSnapshotsToKeep));
    shell.executeStatement(
        String.format("CREATE BRANCH %s  FROM customers WITH SNAPSHOT RETENTION %d SNAPSHOTS", branchName2,
            minSnapshotsToKeep));
    table.refresh();
    SnapshotRef ref = table.refs().get(branchName);
    Assert.assertEquals(table.currentSnapshot().snapshotId(), ref.snapshotId());
    Assert.assertEquals(minSnapshotsToKeep, ref.minSnapshotsToKeep());
    Assert.assertEquals(minSnapshotsToKeep, table.refs().get(branchName2).minSnapshotsToKeep());
    Assert.assertNull(ref.maxSnapshotAgeMs());
    Assert.assertNull(ref.maxRefAgeMs());
  }

  @Test
  public void testCreateBranchWithMinSnapshotsToKeepAndMaxSnapshotAge() throws InterruptedException, IOException {
    Table table =
        testTables.createTableWithVersions(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
            fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, 2);

    String branchName = "test_branch_1";
    String branchName2 = "test_branch_2";
    Integer minSnapshotsToKeep = 2;
    long maxSnapshotAge = 2L;
    shell.executeStatement(String.format("ALTER TABLE customers CREATE BRANCH %s WITH SNAPSHOT RETENTION %d SNAPSHOTS" +
        " %d DAYS", branchName, minSnapshotsToKeep, maxSnapshotAge));
    shell.executeStatement(
        String.format("CREATE BRANCH %s FROM customers WITH SNAPSHOT RETENTION %d SNAPSHOTS %d DAYS", branchName2,
            minSnapshotsToKeep, maxSnapshotAge));
    table.refresh();
    SnapshotRef ref = table.refs().get(branchName);
    Assert.assertEquals(table.currentSnapshot().snapshotId(), ref.snapshotId());
    Assert.assertEquals(minSnapshotsToKeep, ref.minSnapshotsToKeep());
    Assert.assertEquals(TimeUnit.DAYS.toMillis(maxSnapshotAge), ref.maxSnapshotAgeMs().longValue());
    Assert.assertEquals(TimeUnit.DAYS.toMillis(maxSnapshotAge),
        table.refs().get(branchName2).maxSnapshotAgeMs().longValue());
    Assert.assertNull(ref.maxRefAgeMs());
  }

  @Test
  public void testCreateBranchWithAllCustomConfig() throws IOException, InterruptedException {
    Table table =
        testTables.createTableWithVersions(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
            fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, 2);

    String branchName = "test_branch_1";
    String branchName2 = "test_branch_2";
    Long snapshotId = table.history().get(0).snapshotId();
    Integer minSnapshotsToKeep = 2;
    long maxSnapshotAge = 2L;
    long maxRefAge = 5L;
    shell.executeStatement(String.format("ALTER TABLE customers CREATE BRANCH %s FOR SYSTEM_VERSION AS OF %d RETAIN" +
            " %d DAYS WITH SNAPSHOT RETENTION %d SNAPSHOTS %d days", branchName, snapshotId, maxRefAge,
        minSnapshotsToKeep, maxSnapshotAge));
    shell.executeStatement(String.format("CREATE BRANCH %s  FROM customers FOR SYSTEM_VERSION AS OF %d RETAIN %d DAYS" +
            " WITH SNAPSHOT RETENTION %d SNAPSHOTS %d days", branchName2, snapshotId, maxRefAge, minSnapshotsToKeep,
        maxSnapshotAge));
    table.refresh();
    SnapshotRef ref = table.refs().get(branchName);
    SnapshotRef ref2 = table.refs().get(branchName2);
    Assert.assertEquals(snapshotId.longValue(), ref.snapshotId());
    Assert.assertEquals(minSnapshotsToKeep, ref.minSnapshotsToKeep());
    Assert.assertEquals(TimeUnit.DAYS.toMillis(maxSnapshotAge), ref.maxSnapshotAgeMs().longValue());
    Assert.assertEquals(TimeUnit.DAYS.toMillis(maxRefAge), ref.maxRefAgeMs().longValue());
    Assert.assertEquals(snapshotId.longValue(), ref2.snapshotId());
    Assert.assertEquals(TimeUnit.DAYS.toMillis(maxSnapshotAge), ref2.maxSnapshotAgeMs().longValue());
    Assert.assertEquals(TimeUnit.DAYS.toMillis(maxRefAge), ref2.maxRefAgeMs().longValue());
  }

  @Test
  public void testCreateBranchWithNonIcebergTable() {
    shell.executeStatement("create table nonice_tbl (id int, name string)");

    String branchName = "test_branch_1";
    try {
      shell.executeStatement(String.format("ALTER TABLE nonice_tbl CREATE BRANCH %s", branchName));
    } catch (Throwable e) {
      while (e.getCause() != null) {
        e = e.getCause();
      }
      Assert.assertTrue(e.getMessage().contains("Not an iceberg table"));
    }
  }

  @Test
  public void testDropBranch() throws InterruptedException, IOException {
    Table table =
        testTables.createTableWithVersions(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
            fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, 2);

    String branchName = "test_branch_1";
    shell.executeStatement(String.format("ALTER TABLE customers CREATE BRANCH %s", branchName));
    table.refresh();
    Assert.assertNotNull(table.refs().get(branchName));

    shell.executeStatement(String.format("ALTER TABLE customers DROP BRANCH IF EXISTS %s", branchName));
    table.refresh();
    Assert.assertNull(table.refs().get(branchName));
  }

  @Test
  public void testCreateBranchFromTag() throws IOException, InterruptedException {
    Table table =
        testTables.createTableWithVersions(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
            fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, 2);
    String branchName1 = "test_branch_1";
    String tagName = "test_tag";
    String branchName2 = "test_branch_2";
    String nonExistTag = "test_non_exist_tag";
    String branchName3 = "test_branch_3";
    shell.executeStatement(String.format("ALTER TABLE customers CREATE TAG %s", tagName));

    // Create a branch based on an existing tag.
    shell.executeStatement(String.format("ALTER TABLE customers CREATE BRANCH %s FOR TAG AS OF %s",
        branchName1, tagName));
    shell.executeStatement(
        String.format("CREATE BRANCH %s FROM customers FOR TAG AS OF %s", branchName3, tagName));
    table.refresh();
    SnapshotRef ref = table.refs().get(branchName1);
    Assert.assertEquals(table.currentSnapshot().snapshotId(), ref.snapshotId());
    Assert.assertEquals(table.currentSnapshot().snapshotId(), table.refs().get(branchName3).snapshotId());
    Assert.assertNull(ref.minSnapshotsToKeep());
    Assert.assertNull(ref.maxSnapshotAgeMs());
    Assert.assertNull(ref.maxRefAgeMs());

    // Create a branch based on a tag which doesn't exist will fail.
    Assertions.assertThatThrownBy(() -> shell.executeStatement(String.format(
        "ALTER TABLE customers CREATE BRANCH %s FOR TAG AS OF %s", branchName2, nonExistTag)))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("does not exist");

    // Create a branch based on a branch will fail.
    Assertions.assertThatThrownBy(() -> shell.executeStatement(String.format(
            "ALTER TABLE customers CREATE BRANCH %s FOR TAG AS OF %s", branchName2, branchName1)))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("does not exist");
  }

  @Test
  public void testCreateBranchWithNonLowerCase() throws InterruptedException, IOException {
    Table table =
        testTables.createTableWithVersions(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
            fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, 2);

    String branchName = "test_Branch_1";
    Long snapshotId = table.history().get(0).snapshotId();
    shell.executeStatement(
        String.format("ALTER TABLE customers CREATE BRANCH %s FOR SYSTEM_VERSION AS OF %d", branchName, snapshotId));
    // Select with non-lower case branch name shouldn't throw exception.
    shell.executeStatement(String.format("SELECT * FROM default.customers.branch_%s", branchName));
  }

  @Test
  public void testCreateOrReplaceBranchWithTag() throws InterruptedException, IOException {
    Table table =
        testTables.createTableWithVersions(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
            fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, 2);

    Long snapshotId = table.history().get(0).snapshotId();
    shell.executeStatement(
        String.format("ALTER TABLE customers CREATE TAG %s FOR SYSTEM_VERSION AS OF %d", "test1", snapshotId));

    Assertions.assertThatThrownBy(() -> shell.executeStatement("ALTER TABLE customers CREATE OR REPLACE BRANCH test1"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot complete replace branch operation on test1, as it exists as Tag");
  }

  @Test
  public void testCreateOrReplaceBranchWithEmptyTable() throws IOException {
    Table table =
        testTables.createTable(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, fileFormat, null,
            2);

    Assert.assertEquals(0, table.history().size());
    shell.executeStatement("ALTER TABLE customers CREATE BRANCH test1");
    Assertions.assertThatThrownBy(() -> shell.executeStatement("ALTER TABLE customers CREATE OR REPLACE BRANCH test1"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(" Cannot complete replace branch operation on test1, main has no snapshot");
  }
}
