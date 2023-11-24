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
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import static org.apache.iceberg.mr.hive.HiveIcebergTestUtils.timestampAfterSnapshot;

public class TestHiveIcebergTagOperation extends HiveIcebergStorageHandlerWithEngineBase {

  @Override
  protected void validateTestParams() {
    Assume.assumeTrue(fileFormat == FileFormat.PARQUET && isVectorized &&
        testTableType == TestTables.TestTableType.HIVE_CATALOG && formatVersion == 2);
  }

  @Test
  public void testCreateTagWithDefaultConfig() throws InterruptedException, IOException {
    Table table =
        testTables.createTableWithVersions(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
            fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, 2);

    String tagName = "test_tag_1";
    shell.executeStatement(String.format("ALTER TABLE customers CREATE TAG %s", tagName));
    table.refresh();
    SnapshotRef ref = table.refs().get(tagName);
    Assert.assertEquals(table.currentSnapshot().snapshotId(), ref.snapshotId());
    Assert.assertNull(ref.maxRefAgeMs());

    // creating a tag which is already exists will fail
    try {
      shell.executeStatement(String.format("ALTER TABLE customers CREATE TAG %s", tagName));
    } catch (Throwable e) {
      while (e.getCause() != null) {
        e = e.getCause();
      }
      Assert.assertTrue(e.getMessage().contains("Ref test_tag_1 already exists"));
    }
  }

  @Test
  public void testCreateTagWithSnapshotId() throws InterruptedException, IOException {
    Table table =
        testTables.createTableWithVersions(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
            fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, 2);

    String tagName = "test_tag_1";
    Long snapshotId = table.history().get(0).snapshotId();
    shell.executeStatement(String.format("ALTER TABLE customers CREATE TAG %s FOR SYSTEM_VERSION AS OF %d",
        tagName, snapshotId));
    table.refresh();
    SnapshotRef ref = table.refs().get(tagName);
    Assert.assertEquals(snapshotId.longValue(), ref.snapshotId());
    Assert.assertNull(ref.maxRefAgeMs());
  }

  @Test
  public void testCreateTagWithTimeStamp() throws InterruptedException, IOException {
    Table table =
        testTables.createTableWithVersions(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
            fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, 2);

    String tagName = "test_tag_1";
    Long snapshotId = table.history().get(0).snapshotId();

    shell.executeStatement(String.format("ALTER TABLE customers CREATE TAG %s FOR SYSTEM_TIME AS OF '%s'",
        tagName, timestampAfterSnapshot(table, 0)));
    table.refresh();
    SnapshotRef ref = table.refs().get(tagName);
    Assert.assertEquals(snapshotId.longValue(), ref.snapshotId());
  }

  @Test
  public void testCreateTagWithMaxRefAge() throws InterruptedException, IOException {
    Table table =
        testTables.createTableWithVersions(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
            fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, 2);

    String tagName = "test_tag_1";
    long maxRefAge = 5L;
    shell.executeStatement(String.format("ALTER TABLE customers CREATE TAG %s RETAIN %d DAYS", tagName, maxRefAge));
    table.refresh();
    SnapshotRef ref = table.refs().get(tagName);
    Assert.assertEquals(table.currentSnapshot().snapshotId(), ref.snapshotId());
    Assert.assertEquals(TimeUnit.DAYS.toMillis(maxRefAge), ref.maxRefAgeMs().longValue());
  }

  @Test
  public void testCreateTagWithAllCustomConfig() throws IOException, InterruptedException {
    Table table =
        testTables.createTableWithVersions(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
            fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, 2);

    String tagName = "test_tag_1";
    Long snapshotId = table.history().get(0).snapshotId();
    long maxRefAge = 5L;
    shell.executeStatement(String.format("ALTER TABLE customers CREATE TAG %s FOR SYSTEM_VERSION AS OF %d RETAIN" +
            " %d DAYS",
        tagName, snapshotId, maxRefAge));
    table.refresh();
    SnapshotRef ref = table.refs().get(tagName);
    Assert.assertEquals(snapshotId.longValue(), ref.snapshotId());
    Assert.assertEquals(TimeUnit.DAYS.toMillis(maxRefAge), ref.maxRefAgeMs().longValue());
  }

  @Test
  public void testCreateTagWithNonIcebergTable() {
    shell.executeStatement("create table nonice_tbl (id int, name string)");

    String tagName = "test_tag_1";
    try {
      shell.executeStatement(String.format("ALTER TABLE nonice_tbl CREATE TAG %s", tagName));
    } catch (Throwable e) {
      while (e.getCause() != null) {
        e = e.getCause();
      }
      Assert.assertTrue(e.getMessage().contains("Not an iceberg table"));
    }
  }

  @Test
  public void testQueryIcebergTag() throws IOException, InterruptedException {
    Table table = testTables.createTableWithVersions(shell, "customers",
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, 2);

    long firstSnapshotId = table.history().get(0).snapshotId();
    table.manageSnapshots().createTag("testtag", firstSnapshotId).commit();
    List<Object[]> rows =
        shell.executeStatement("SELECT * FROM default.customers.tag_testtag");

    Assert.assertEquals(3, rows.size());

    try {
      shell.executeStatement("insert into default.customers.tag_testtag values (0L, \"Alice\", \"Brown\")");
    } catch (Throwable e) {
      while (e.getCause() != null) {
        e = e.getCause();
      }
      Assert.assertTrue(e.getMessage().contains("Don't support write (insert/delete/update/merge) to iceberg tag"));
    }

    try {
      shell.executeStatement("delete from default.customers.tag_testtag where customer_id=0L");
    } catch (Throwable e) {
      while (e.getCause() != null) {
        e = e.getCause();
      }
      Assert.assertTrue(e.getMessage().contains("Don't support write (insert/delete/update/merge) to iceberg tag"));
    }

    try {
      shell.executeStatement("update default.customers.tag_testtag set customer_id=0L where customer_id=0L");
    } catch (Throwable e) {
      while (e.getCause() != null) {
        e = e.getCause();
      }
      Assert.assertTrue(e.getMessage().contains("Don't support write (insert/delete/update/merge) to iceberg tag"));
    }
  }

  @Test
  public void testDropTag() throws InterruptedException, IOException {
    Table table =
        testTables.createTableWithVersions(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
            fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, 2);

    String tagName = "test_tag_1";
    shell.executeStatement(String.format("ALTER TABLE customers CREATE TAG %s", tagName));
    table.refresh();
    Assert.assertNotNull(table.refs().get(tagName));

    shell.executeStatement(String.format("ALTER TABLE customers DROP TAG IF EXISTS %s", tagName));
    table.refresh();
    Assert.assertNull(table.refs().get(tagName));

    try {
      // drop a non-exist tag
      shell.executeStatement(String.format("ALTER TABLE customers DROP TAG %s", tagName));
    } catch (Throwable e) {
      while (e.getCause() != null) {
        e = e.getCause();
      }
      Assert.assertTrue(e.getMessage().contains("Tag does not exist: test_tag_1"));
    }
  }
}
