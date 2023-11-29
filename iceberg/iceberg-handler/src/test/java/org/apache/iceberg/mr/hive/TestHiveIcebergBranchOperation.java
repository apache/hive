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
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import static org.apache.iceberg.mr.hive.HiveIcebergTestUtils.timestampAfterSnapshot;

public class TestHiveIcebergBranchOperation extends HiveIcebergStorageHandlerWithEngineBase {

  @Override
  protected void validateTestParams() {
    Assume.assumeTrue(fileFormat == FileFormat.PARQUET && isVectorized &&
        testTableType == TestTables.TestTableType.HIVE_CATALOG && formatVersion == 2);
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
    Long snapshotId = table.history().get(0).snapshotId();
    shell.executeStatement(String.format("ALTER TABLE customers CREATE BRANCH %s FOR SYSTEM_VERSION AS OF %d",
        branchName, snapshotId));
    table.refresh();
    SnapshotRef ref = table.refs().get(branchName);
    Assert.assertEquals(snapshotId.longValue(), ref.snapshotId());
    Assert.assertNull(ref.minSnapshotsToKeep());
    Assert.assertNull(ref.maxSnapshotAgeMs());
    Assert.assertNull(ref.maxRefAgeMs());
  }

  @Test
  public void testCreateBranchWithTimeStamp() throws InterruptedException, IOException {
    Table table =
        testTables.createTableWithVersions(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
            fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, 2);

    String branchName = "test_branch_1";
    Long snapshotId = table.history().get(0).snapshotId();

    shell.executeStatement(String.format("ALTER TABLE customers CREATE BRANCH %s FOR SYSTEM_TIME AS OF '%s'",
        branchName, timestampAfterSnapshot(table, 0)));
    table.refresh();
    SnapshotRef ref = table.refs().get(branchName);
    Assert.assertEquals(snapshotId.longValue(), ref.snapshotId());
  }

  @Test
  public void testCreateBranchWithMaxRefAge() throws InterruptedException, IOException {
    Table table =
        testTables.createTableWithVersions(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
            fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, 2);

    String branchName = "test_branch_1";
    long maxRefAge = 5L;
    shell.executeStatement(String.format("ALTER TABLE customers CREATE BRANCH %s RETAIN %d DAYS",
        branchName, maxRefAge));
    table.refresh();
    SnapshotRef ref = table.refs().get(branchName);
    Assert.assertEquals(table.currentSnapshot().snapshotId(), ref.snapshotId());
    Assert.assertNull(ref.minSnapshotsToKeep());
    Assert.assertNull(ref.maxSnapshotAgeMs());
    Assert.assertEquals(TimeUnit.DAYS.toMillis(maxRefAge), ref.maxRefAgeMs().longValue());
  }

  @Test
  public void testCreateBranchWithMinSnapshotsToKeep() throws InterruptedException, IOException {
    Table table =
        testTables.createTableWithVersions(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
            fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, 2);

    String branchName = "test_branch_1";
    Integer minSnapshotsToKeep = 2;
    shell.executeStatement(String.format("ALTER TABLE customers CREATE BRANCH %s WITH SNAPSHOT RETENTION %d SNAPSHOTS",
        branchName, minSnapshotsToKeep));
    table.refresh();
    SnapshotRef ref = table.refs().get(branchName);
    Assert.assertEquals(table.currentSnapshot().snapshotId(), ref.snapshotId());
    Assert.assertEquals(minSnapshotsToKeep, ref.minSnapshotsToKeep());
    Assert.assertNull(ref.maxSnapshotAgeMs());
    Assert.assertNull(ref.maxRefAgeMs());
  }

  @Test
  public void testCreateBranchWithMinSnapshotsToKeepAndMaxSnapshotAge() throws InterruptedException, IOException {
    Table table =
        testTables.createTableWithVersions(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
            fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, 2);

    String branchName = "test_branch_1";
    Integer minSnapshotsToKeep = 2;
    long maxSnapshotAge = 2L;
    shell.executeStatement(String.format("ALTER TABLE customers CREATE BRANCH %s WITH SNAPSHOT RETENTION %d SNAPSHOTS" +
        " %d DAYS", branchName, minSnapshotsToKeep, maxSnapshotAge));
    table.refresh();
    SnapshotRef ref = table.refs().get(branchName);
    Assert.assertEquals(table.currentSnapshot().snapshotId(), ref.snapshotId());
    Assert.assertEquals(minSnapshotsToKeep, ref.minSnapshotsToKeep());
    Assert.assertEquals(TimeUnit.DAYS.toMillis(maxSnapshotAge), ref.maxSnapshotAgeMs().longValue());
    Assert.assertNull(ref.maxRefAgeMs());
  }

  @Test
  public void testCreateBranchWithAllCustomConfig() throws IOException, InterruptedException {
    Table table =
        testTables.createTableWithVersions(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
            fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, 2);

    String branchName = "test_branch_1";
    Long snapshotId = table.history().get(0).snapshotId();
    Integer minSnapshotsToKeep = 2;
    long maxSnapshotAge = 2L;
    long maxRefAge = 5L;
    shell.executeStatement(String.format("ALTER TABLE customers CREATE BRANCH %s FOR SYSTEM_VERSION AS OF %d RETAIN" +
            " %d DAYS WITH SNAPSHOT RETENTION %d SNAPSHOTS %d days",
        branchName, snapshotId, maxRefAge, minSnapshotsToKeep, maxSnapshotAge));
    table.refresh();
    SnapshotRef ref = table.refs().get(branchName);
    Assert.assertEquals(snapshotId.longValue(), ref.snapshotId());
    Assert.assertEquals(minSnapshotsToKeep, ref.minSnapshotsToKeep());
    Assert.assertEquals(TimeUnit.DAYS.toMillis(maxSnapshotAge), ref.maxSnapshotAgeMs().longValue());
    Assert.assertEquals(TimeUnit.DAYS.toMillis(maxRefAge), ref.maxRefAgeMs().longValue());
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
    shell.executeStatement(String.format("ALTER TABLE customers CREATE TAG %s", tagName));

    // Create a branch based on an existing tag.
    shell.executeStatement(String.format("ALTER TABLE customers CREATE BRANCH %s FOR TAG AS OF %s",
        branchName1, tagName));
    table.refresh();
    SnapshotRef ref = table.refs().get(branchName1);
    Assert.assertEquals(table.currentSnapshot().snapshotId(), ref.snapshotId());
    Assert.assertNull(ref.minSnapshotsToKeep());
    Assert.assertNull(ref.maxSnapshotAgeMs());
    Assert.assertNull(ref.maxRefAgeMs());

    // Create a branch based on a tag which doesn't exist will fail.
    Assertions.assertThatThrownBy(() -> shell.executeStatement(String.format(
        "ALTER TABLE customers CREATE BRANCH %s FOR TAG AS OF %s", branchName2, nonExistTag)))
        .isInstanceOf(IllegalArgumentException.class).hasMessageEndingWith("does not exist");

    // Create a branch based on a branch will fail.
    Assertions.assertThatThrownBy(() -> shell.executeStatement(String.format(
            "ALTER TABLE customers CREATE BRANCH %s FOR TAG AS OF %s", branchName2, branchName1)))
        .isInstanceOf(IllegalArgumentException.class).hasMessageEndingWith("does not exist");
  }
}
