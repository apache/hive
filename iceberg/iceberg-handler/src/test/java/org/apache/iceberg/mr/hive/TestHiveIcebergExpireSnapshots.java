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
import org.apache.commons.collections4.IterableUtils;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests covering the rollback feature
 */
public class TestHiveIcebergExpireSnapshots extends HiveIcebergStorageHandlerWithEngineBase {

  @Test
  public void testExpireSnapshotsWithTimestamp() throws IOException, InterruptedException {
    TableIdentifier identifier = TableIdentifier.of("default", "source");
    Table table = testTables.createTableWithVersions(shell, identifier.name(),
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, fileFormat,
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, 5);
    Assert.assertEquals(5, table.history().size());
    shell.executeStatement("ALTER TABLE " + identifier.name() + " EXECUTE EXPIRE_SNAPSHOTS('" +
        HiveIcebergTestUtils.timestampAfterSnapshot(table, 2) + "')");
    table.refresh();
    Assert.assertEquals(2, table.history().size());
  }

  @Test
  public void testExpireSnapshotsWithSnapshotId() throws IOException, InterruptedException {
    TableIdentifier identifier = TableIdentifier.of("default", "source");
    Table table = testTables.createTableWithVersions(shell, identifier.name(),
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, fileFormat,
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, 10);
    Assert.assertEquals(10, IterableUtils.size(table.snapshots()));

    // Expire one snapshot
    shell.executeStatement(
        "ALTER TABLE " + identifier.name() + " EXECUTE EXPIRE_SNAPSHOTS" +
            "('" + table.history().get(2).snapshotId() + "')");
    table.refresh();
    Assert.assertEquals(9, IterableUtils.size(table.snapshots()));
    // Expire multiple snapshots
    shell.executeStatement(
        "ALTER TABLE " + identifier.name() + " EXECUTE EXPIRE_SNAPSHOTS('" + table.history().get(3).snapshotId() + "," +
            table.history().get(4).snapshotId() + "')");
    table.refresh();
    Assert.assertEquals(7,  IterableUtils.size(table.snapshots()));
  }
}
