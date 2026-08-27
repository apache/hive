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
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.mr.hive.test.TestHiveShell;
import org.apache.iceberg.mr.hive.test.TestTables;
import org.apache.iceberg.mr.hive.test.utils.HiveIcebergStorageHandlerTestUtils;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.mr.hive.test.TestTables.TestTableType.HIVE_CATALOG;
import static org.junit.Assert.assertEquals;

public class TestHiveIcebergSnapshotOperations {

  private TestTables testTables;
  private TestHiveShell shell;
  private TemporaryFolder temp = new TemporaryFolder();

  @Before
  public void before() throws IOException {
    shell = HiveIcebergStorageHandlerTestUtils.shell();
    temp.create();
    testTables = HiveIcebergStorageHandlerTestUtils.testTables(shell, HIVE_CATALOG, temp);
    HiveIcebergStorageHandlerTestUtils.init(shell, testTables, temp);
  }

  @After
  public void after() throws Exception {
    HiveIcebergStorageHandlerTestUtils.close(shell);
  }

  @Test
  public void testCherryPick() {
    TableIdentifier identifier = TableIdentifier.of("default", "testCherryPick");
    shell.executeStatement(String.format("CREATE EXTERNAL TABLE %s (id INT) STORED BY iceberg  %s %s",
            identifier.name(),
            testTables.locationForCreateTableSQL(identifier),
            testTables.propertiesForCreateTableSQL(ImmutableMap.of())));

    shell.executeStatement(String.format("INSERT INTO TABLE %s VALUES(1),(2),(3),(4)", identifier.name()));

    org.apache.iceberg.Table icebergTable = testTables.loadTable(identifier);
    long id1 = icebergTable.currentSnapshot().snapshotId();

    // make 2 new inserts to the main branch
    shell.executeStatement(String.format("INSERT INTO TABLE %s VALUES(5),(6)", identifier.name()));
    shell.executeStatement(String.format("INSERT INTO TABLE %s VALUES(7),(8)", identifier.name()));

    icebergTable.refresh();
    long id2 = icebergTable.currentSnapshot().snapshotId();

    Assert.assertNotEquals(id1, id2);

    // Rollback the table to the previous state before the previous inserts.
    shell.executeStatement(
        "ALTER TABLE default.testCherryPick EXECUTE ROLLBACK (" + id1 + ")");
    // cherry-pick the last snapshot to test1 branch
    shell.executeStatement("ALTER TABLE default.testCherryPick EXECUTE CHERRY-PICK " + id2);

    List<Object[]> result = shell.executeStatement("SELECT COUNT(*) FROM " + identifier.name());
    assertEquals(6L, result.get(0)[0]);
  }

  @Test
  public void testReplaceBranchWithSnapshot() {
    TableIdentifier identifier = TableIdentifier.of("default", "testReplaceBranchWithSnapshot");
    shell.executeStatement(
        String.format("CREATE EXTERNAL TABLE %s (id INT) STORED BY iceberg  %s %s",
            identifier.name(),
            testTables.locationForCreateTableSQL(identifier),
            testTables.propertiesForCreateTableSQL(ImmutableMap.of())));

    shell.executeStatement(String.format("INSERT INTO TABLE %s VALUES(1),(2),(3),(4)", identifier.name()));

    org.apache.iceberg.Table icebergTable = testTables.loadTable(identifier);
    icebergTable.refresh();
    // Create a branch
    shell.executeStatement(String.format("ALTER TABLE %s create branch branch1", identifier.name()));
    // Make one new insert to the main branch
    shell.executeStatement(String.format("INSERT INTO TABLE %s VALUES(5),(6)", identifier.name()));
    icebergTable.refresh();
    long id = icebergTable.currentSnapshot().snapshotId();

    // Make another insert so that the commit isn't the last commit on the branch
    shell.executeStatement(String.format("INSERT INTO TABLE %s VALUES(7),(8)", identifier.name()));

    // Validate the original count on branch before replace
    List<Object[]> result =
        shell.executeStatement("SELECT COUNT(*) FROM default.testReplaceBranchWithSnapshot.branch_branch1");
    assertEquals(4L, result.get(0)[0]);
    // Perform replace branch with snapshot id.
    shell.executeStatement(
        String.format("ALTER TABLE %s replace branch branch1 as of system_version %s", identifier.name(), id));
    result = shell.executeStatement("SELECT COUNT(*) FROM default.testReplaceBranchWithSnapshot.branch_branch1");
    assertEquals(6L, result.get(0)[0]);
  }

  /**
   * Tests executing REWRITE_MANIFESTS on an unpartitioned table to verify that
   * multiple small manifests are successfully compacted into a single optimized manifest.
   */
  @Test
  public void testRewriteManifests() {
    TableIdentifier identifier = TableIdentifier.of("default", "testRewriteManifests");
    shell.executeStatement(
        String.format(
            "CREATE EXTERNAL TABLE %s (id INT, data STRING) STORED BY iceberg %s %s",
            identifier.name(),
            testTables.locationForCreateTableSQL(identifier),
            testTables.propertiesForCreateTableSQL(ImmutableMap.of("commit.manifest.min-count-to-compact", "2"))));

    // Create 5 manifests by executing 5 separate INSERT operations
    for (int i = 1; i <= 5; i++) {
      shell.executeStatement(
          String.format("INSERT INTO TABLE %s VALUES(%d, 'val')", identifier.name(), i));
    }

    int manifestCountAfterRewrite = generateManifestsAndRewrite(identifier, 5);
    assertEquals("Should have exactly 1 manifest after REWRITE_MANIFESTS", 1, manifestCountAfterRewrite);
  }

  /**
   * Tests executing REWRITE_MANIFESTS on a partitioned table to verify that multiple manifests
   * spread across different partitions are correctly clustered and compacted into fewer, optimized
   * manifests.
   */
  @Test
  public void testRewriteManifestsPartitioned() {
    TableIdentifier identifier = TableIdentifier.of("default", "testRewriteManifestsPartitioned");
    shell.executeStatement(
        String.format(
            "CREATE EXTERNAL TABLE %s (id INT, data STRING) PARTITIONED BY (part STRING) STORED BY iceberg %s %s",
            identifier.name(),
            testTables.locationForCreateTableSQL(identifier),
            testTables.propertiesForCreateTableSQL(
                ImmutableMap.of("commit.manifest.min-count-to-compact", "2"))));

    // Create 5 manifests by executing 5 separate INSERT operations across 2 partitions
    for (int i = 1; i <= 5; i++) {
      String partitionVal = (i % 2 == 0) ? "p2" : "p1";
      shell.executeStatement(
          String.format(
              "INSERT INTO TABLE %s VALUES(%d, 'val', '%s')", identifier.name(), i, partitionVal));
    }

    int manifestCountAfterRewrite = generateManifestsAndRewrite(identifier, 5);
    assertEquals("Should have exactly 1 manifest after REWRITE_MANIFESTS", 1, manifestCountAfterRewrite);
  }

  /**
   * Tests that executing REWRITE_MANIFESTS on an empty table (with no existing snapshots) safely
   * acts as a no-op without throwing any exceptions.
   */
  @Test
  public void testRewriteManifestsEmptyTable() {
    TableIdentifier identifier = TableIdentifier.of("default", "testRewriteManifestsEmptyTable");
    shell.executeStatement(
        String.format(
            "CREATE EXTERNAL TABLE %s (id INT) STORED BY iceberg %s %s",
            identifier.name(),
            testTables.locationForCreateTableSQL(identifier),
            testTables.propertiesForCreateTableSQL(ImmutableMap.of())));

    // Execute on a table with no snapshot
    shell.executeStatement(
        String.format("ALTER TABLE %s EXECUTE REWRITE_MANIFESTS", identifier.name()));

    org.apache.iceberg.Table icebergTable = testTables.loadTable(identifier);
    Assert.assertNull(icebergTable.currentSnapshot());
  }

  /**
   * Tests REWRITE_MANIFESTS with a very small target size to ensure that multiple output manifests
   * are created, proving the bucketing and splitting logic works.
   */
  @Test
  public void testRewriteManifestsMultipleOutputManifests() {
    TableIdentifier identifier =
        TableIdentifier.of("default", "testRewriteManifestsMultipleOutputs");
    shell.executeStatement(
        String.format(
            "CREATE EXTERNAL TABLE %s (id INT, data STRING) PARTITIONED BY (part STRING) STORED BY iceberg %s %s",
            identifier.name(),
            testTables.locationForCreateTableSQL(identifier),
            testTables.propertiesForCreateTableSQL(
                ImmutableMap.of(
                    "commit.manifest.min-count-to-compact", "100",
                    "commit.manifest.target-size-bytes",
                        "1")))); // Force multiple clusters and splits

    for (int i = 1; i <= 15; i++) {
      String partitionVal = "p" + (i % 5);
      shell.executeStatement(
          String.format(
              "INSERT INTO TABLE %s VALUES(%d, 'val', '%s')", identifier.name(), i, partitionVal));
    }

    int manifestCountAfterRewrite = generateManifestsAndRewrite(identifier, 15);
    Assert.assertTrue("Should have multiple manifests due to bucketing/splitting", manifestCountAfterRewrite > 1);
  }

  private int generateManifestsAndRewrite(TableIdentifier identifier, int expectedBefore) {
    org.apache.iceberg.Table icebergTable = testTables.loadTable(identifier);
    icebergTable.refresh();

    int manifestCountBefore = icebergTable.currentSnapshot().allManifests(icebergTable.io()).size();
    assertEquals(
        "Manifests keep accumulating for each insert", expectedBefore, manifestCountBefore);

    shell.executeStatement(
        String.format("ALTER TABLE %s EXECUTE REWRITE_MANIFESTS", identifier.name()));

    icebergTable.refresh();
    return icebergTable.currentSnapshot().allManifests(icebergTable.io()).size();
  }
}
