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
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import org.apache.commons.collections4.IterableUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.functional.RemoteIterators;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import static org.apache.iceberg.TableProperties.MAX_SNAPSHOT_AGE_MS;
import static org.apache.iceberg.TableProperties.MIN_SNAPSHOTS_TO_KEEP;

/**
 * Tests covering the rollback feature
 */
public class TestHiveIcebergExpireSnapshots extends HiveIcebergStorageHandlerWithEngineBase {

  @Override
  protected void validateTestParams() {
    Assume.assumeTrue(fileFormat == FileFormat.PARQUET && isVectorized &&
        testTableType == TestTables.TestTableType.HIVE_CATALOG && formatVersion == 2);
  }

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

  @Test
    public void testExpireSnapshotsWithTimestampRange() throws IOException, InterruptedException {
    TableIdentifier identifier = TableIdentifier.of("default", "source");
    Table table = testTables.createTableWithVersions(shell, identifier.name(),
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, fileFormat,
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, 10);
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS000000");
    String fromTime = simpleDateFormat.format(new Date(table.history().get(5).timestampMillis()));
    String toTime = simpleDateFormat.format(new Date(table.history().get(8).timestampMillis()));
    shell.executeStatement("ALTER TABLE " + identifier.name() + " EXECUTE EXPIRE_SNAPSHOTS BETWEEN" +
        " '" + fromTime + "' AND '" + toTime + "'");
    table.refresh();
    Assert.assertEquals(6, IterableUtils.size(table.snapshots()));
  }

  @Test
  public void testExpireSnapshotsWithRetainLast() throws IOException, InterruptedException {
    TableIdentifier identifier = TableIdentifier.of("default", "source");
    Table table = testTables.createTableWithVersions(shell, identifier.name(),
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, fileFormat,
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, 10);
    // No snapshot should expire, since the max snapshot age to expire is by default 5 days
    shell.executeStatement("ALTER TABLE " + identifier.name() + " EXECUTE EXPIRE_SNAPSHOTS RETAIN LAST 5");
    table.refresh();
    Assert.assertEquals(10, IterableUtils.size(table.snapshots()));

    // Change max snapshot age to expire to 1 ms & re-execute, this time it should retain only 5
    shell.executeStatement(
        "ALTER TABLE " + identifier.name() + " SET TBLPROPERTIES('" + MAX_SNAPSHOT_AGE_MS + "'='1')");
    shell.executeStatement("ALTER TABLE " + identifier.name() + " EXECUTE EXPIRE_SNAPSHOTS RETAIN LAST 5");
    table.refresh();
    Assert.assertEquals(5, IterableUtils.size(table.snapshots()));
  }

  @Test
  public void testExpireSnapshotsWithDefaultParams() throws IOException, InterruptedException {
    TableIdentifier identifier = TableIdentifier.of("default", "source");
    Table table = testTables.createTableWithVersions(shell, identifier.name(),
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, fileFormat,
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, 10);
    // No snapshot should expire, since the max snapshot age to expire is by default 5 days
    shell.executeStatement("ALTER TABLE " + identifier.name() + " EXECUTE EXPIRE_SNAPSHOTS RETAIN LAST 5");
    table.refresh();
    Assert.assertEquals(10, IterableUtils.size(table.snapshots()));

    // Change max snapshot age to expire to 1 ms & min snapshots to keep as 3 & re-execute
    shell.executeStatement(
        "ALTER TABLE " + identifier.name() + " SET TBLPROPERTIES('" + MAX_SNAPSHOT_AGE_MS + "'='1'" + ",'" +
            MIN_SNAPSHOTS_TO_KEEP + "'='3')");
    shell.executeStatement("ALTER TABLE " + identifier.name() + " EXECUTE EXPIRE_SNAPSHOTS");
    table.refresh();
    Assert.assertEquals(3, IterableUtils.size(table.snapshots()));

    // Change the min snapshot to keep as 2
    shell.executeStatement(
        "ALTER TABLE " + identifier.name() + " SET TBLPROPERTIES('" + MIN_SNAPSHOTS_TO_KEEP + "'='2')");
    shell.executeStatement("ALTER TABLE " + identifier.name() + " EXECUTE EXPIRE_SNAPSHOTS");
    table.refresh();
    Assert.assertEquals(2, IterableUtils.size(table.snapshots()));

  }

  @Test
  public void testDeleteOrphanFiles() throws IOException, InterruptedException {
    TableIdentifier identifier = TableIdentifier.of("default", "source");
    Table table =
        testTables.createTableWithVersions(shell, identifier.name(), HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
            fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, 5);
    Assert.assertEquals(5, table.history().size());

    List<Object[]> rows = shell.executeStatement("SELECT * FROM " + identifier.name());
    List<Record> originalRecords = HiveIcebergTestUtils.valueForRow(table.schema(), rows);
    Path orphanDataFile = new Path(table.location(), "data/dataFile");
    Path orphanMetadataFile = new Path(table.location(), "metadata/metafile");
    FileSystem fs = orphanDataFile.getFileSystem(shell.getHiveConf());
    fs.create(orphanDataFile).close();
    fs.create(orphanMetadataFile).close();

    int numDataFiles = RemoteIterators.toList(fs.listFiles(new Path(table.location(), "data"), true)).size();
    int numMetadataFiles = RemoteIterators.toList(fs.listFiles(new Path(table.location(), "metadata"), true)).size();
    shell.executeStatement("ALTER TABLE " + identifier.name() + " EXECUTE DELETE ORPHAN-FILES");

    Assert.assertEquals(numDataFiles,
        RemoteIterators.toList(fs.listFiles(new Path(table.location(), "data"), true)).size());

    Assert.assertEquals(numMetadataFiles,
        RemoteIterators.toList(fs.listFiles(new Path(table.location(), "metadata"), true)).size());

    Assert.assertTrue(fs.exists(orphanDataFile));
    Assert.assertTrue(fs.exists(orphanDataFile));

    long time = System.currentTimeMillis() + 1000;
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS000000");
    String timeStamp = simpleDateFormat.format(new Date(time));
    shell.executeStatement(
        "ALTER TABLE " + identifier.name() + " EXECUTE DELETE ORPHAN-FILES OLDER THAN ('" + timeStamp + "')");

    Assert.assertEquals(numDataFiles - 1,
        RemoteIterators.toList(fs.listFiles(new Path(table.location(), "data"), true)).size());

    Assert.assertEquals(numMetadataFiles - 1,
        RemoteIterators.toList(fs.listFiles(new Path(table.location(), "metadata"), true)).size());

    Assert.assertFalse(fs.exists(orphanDataFile));
    Assert.assertFalse(fs.exists(orphanDataFile));
    table.refresh();

    rows = shell.executeStatement("SELECT * FROM " + identifier.name());
    List<Record> records = HiveIcebergTestUtils.valueForRow(table.schema(), rows);
    HiveIcebergTestUtils.validateData(originalRecords, records, 0);
  }
}
