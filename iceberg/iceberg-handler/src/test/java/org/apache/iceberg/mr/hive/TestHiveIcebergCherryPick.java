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
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.mr.hive.TestTables.TestTableType.HIVE_CATALOG;
import static org.junit.Assert.assertEquals;

public class TestHiveIcebergCherryPick {

  private TestTables testTables;
  private TestHiveShell shell;
  private TemporaryFolder temp = new TemporaryFolder();

  @Before
  public void before() throws IOException {
    shell = HiveIcebergStorageHandlerTestUtils.shell();
    temp.create();
    testTables = HiveIcebergStorageHandlerTestUtils.testTables(shell, HIVE_CATALOG, temp);
    HiveIcebergStorageHandlerTestUtils.init(shell, testTables, temp, "tez");
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
}
