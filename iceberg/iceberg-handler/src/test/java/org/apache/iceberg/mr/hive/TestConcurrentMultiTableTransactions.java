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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.mr.TestHelper;
import org.apache.iceberg.mr.hive.test.TestTables.TestTableType;
import org.apache.iceberg.mr.hive.test.concurrent.WithMockedStorageHandler;
import org.apache.iceberg.mr.hive.test.utils.HiveIcebergStorageHandlerTestUtils;
import org.apache.iceberg.mr.hive.test.utils.HiveIcebergTestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

@WithMockedStorageHandler
public class TestConcurrentMultiTableTransactions extends HiveIcebergStorageHandlerWithEngineBase {

  public static final String XA_STORAGE_HANDLER_STUB =
      "'org.apache.iceberg.mr.hive.test.concurrent.HiveIcebergStorageHandlerTxnStub'";

  @Parameters(name = "fileFormat={0}, catalog={1}, isVectorized={2}, formatVersion={3}")
  public static Collection<Object[]> parameters() {
    return HiveIcebergStorageHandlerWithEngineBase.getParameters(p ->
        p.fileFormat() == FileFormat.PARQUET &&
            p.testTableType() == TestTableType.HIVE_CATALOG &&
            p.isVectorized());
  }

  @Before
  public void setUpTables() {
    PartitionSpec spec =
        PartitionSpec.builderFor(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
            .identity("last_name").bucket("customer_id", 16).build();

    testTables.createTable(shell, "t1", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        spec, fileFormat, HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_2,
        formatVersion, Collections.emptyMap(), XA_STORAGE_HANDLER_STUB);
    shell.executeStatement(testTables.getInsertQuery(
        HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_1,
        TableIdentifier.of("default", "t1"), false));

    testTables.createTable(shell, "t2", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        spec, fileFormat, HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_2,
        formatVersion, Collections.emptyMap(), XA_STORAGE_HANDLER_STUB);
    shell.executeStatement(testTables.getInsertQuery(
        HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_1,
        TableIdentifier.of("default", "t2"), false));
  }

  // ===== Multi-table Insert Tests =====

  @Test
  public void testConcurrentMultiTableInserts() throws Exception {
    String[][] sql = new String[][]{
        {
            "FROM (SELECT 100 as customer_id, 'NewA' as first_name, 'PartA' as last_name) s " +
                "INSERT INTO t1 SELECT * " +
                "INSERT INTO t2 SELECT *"
        }, {
            "FROM (SELECT 200 as customer_id, 'NewB' as first_name, 'PartB' as last_name) s " +
                "INSERT INTO t1 SELECT * " +
                "INSERT INTO t2 SELECT *"
        }};
    executeConcurrentlyNoRetry(sql);

    List<Record> expected = TestHelper.RecordsBuilder
        .newInstance(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
        .add(1L, "Joanna", "Pierce")
        .add(1L, "Sharon", "Taylor")
        .add(2L, "Bob", "Silver")
        .add(2L, "Jake", "Donnel")
        .add(2L, "Joanna", "Silver")
        .add(2L, "Susan", "Morrison")
        .add(3L, "Blake", "Burr")
        .add(3L, "Marci", "Barna")
        .add(3L, "Trudy", "Henderson")
        .add(3L, "Trudy", "Johnson")
        .add(4L, "Laci", "Zold")
        .add(5L, "Peti", "Rozsaszin")
        .add(100L, "NewA", "PartA")
        .add(200L, "NewB", "PartB")
        .build();

    List<Object[]> t1Rows = shell.executeStatement(
        "SELECT * FROM t1 ORDER BY customer_id, first_name, last_name");
    Assert.assertEquals(14, t1Rows.size());
    HiveIcebergTestUtils.validateData(expected,
        HiveIcebergTestUtils.valueForRow(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, t1Rows), 0);

    List<Object[]> t2Rows = shell.executeStatement(
        "SELECT * FROM t2 ORDER BY customer_id, first_name, last_name");
    Assert.assertEquals(14, t2Rows.size());
    HiveIcebergTestUtils.validateData(expected,
        HiveIcebergTestUtils.valueForRow(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, t2Rows), 0);
  }

  @Test
  public void testMultiTableInsertWithUpdate() throws Exception {
    Assume.assumeTrue(formatVersion >= 2);

    String[][] sql = new String[][]{
        {
            "FROM (SELECT 100 as customer_id, 'NewA' as first_name, 'PartA' as last_name) s " +
                "INSERT INTO t1 SELECT * " +
                "INSERT INTO t2 SELECT *"
        }, {
            "UPDATE t1 SET first_name='Changed' WHERE last_name='Taylor'"
        }};
    executeConcurrentlyNoRetry(sql);

    List<Object[]> t1Rows = shell.executeStatement(
        "SELECT * FROM t1 ORDER BY customer_id, first_name, last_name");
    Assert.assertEquals(13, t1Rows.size());
    List<Record> expectedT1 = TestHelper.RecordsBuilder
        .newInstance(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
        .add(1L, "Changed", "Taylor")
        .add(1L, "Joanna", "Pierce")
        .add(2L, "Bob", "Silver")
        .add(2L, "Jake", "Donnel")
        .add(2L, "Joanna", "Silver")
        .add(2L, "Susan", "Morrison")
        .add(3L, "Blake", "Burr")
        .add(3L, "Marci", "Barna")
        .add(3L, "Trudy", "Henderson")
        .add(3L, "Trudy", "Johnson")
        .add(4L, "Laci", "Zold")
        .add(5L, "Peti", "Rozsaszin")
        .add(100L, "NewA", "PartA")
        .build();
    HiveIcebergTestUtils.validateData(expectedT1,
        HiveIcebergTestUtils.valueForRow(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, t1Rows), 0);

    List<Object[]> t2Rows = shell.executeStatement(
        "SELECT * FROM t2 ORDER BY customer_id, first_name, last_name");
    Assert.assertEquals(13, t2Rows.size());
  }

  // ===== Multi-statement Transaction Tests =====

  @Test
  public void testMultiStatementSingleFilterUpdate() throws Exception {
    Assume.assumeTrue(formatVersion >= 2);

    String[][] stmts = new String[][]{
        {
            "START TRANSACTION",
            "UPDATE t1 SET first_name='Changed' WHERE last_name='Taylor'",
            "UPDATE t2 SET first_name='Changed' WHERE last_name='Donnel'",
            "COMMIT"
        },
        {
            "START TRANSACTION",
            "UPDATE t1 SET first_name='Changed' WHERE last_name='Henderson'",
            "UPDATE t2 SET first_name='Changed' WHERE last_name='Morrison'",
            "COMMIT"
        }
    };
    executeConcurrentlyNoRetry(stmts);

    List<Object[]> t1Rows = shell.executeStatement(
        "SELECT * FROM t1 ORDER BY customer_id, first_name, last_name");
    Assert.assertEquals(12, t1Rows.size());
    List<Record> expectedT1 = TestHelper.RecordsBuilder
        .newInstance(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
        .add(1L, "Changed", "Taylor")
        .add(1L, "Joanna", "Pierce")
        .add(2L, "Bob", "Silver")
        .add(2L, "Jake", "Donnel")
        .add(2L, "Joanna", "Silver")
        .add(2L, "Susan", "Morrison")
        .add(3L, "Blake", "Burr")
        .add(3L, "Changed", "Henderson")
        .add(3L, "Marci", "Barna")
        .add(3L, "Trudy", "Johnson")
        .add(4L, "Laci", "Zold")
        .add(5L, "Peti", "Rozsaszin")
        .build();
    HiveIcebergTestUtils.validateData(expectedT1,
        HiveIcebergTestUtils.valueForRow(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, t1Rows), 0);

    List<Object[]> t2Rows = shell.executeStatement(
        "SELECT * FROM t2 ORDER BY customer_id, first_name, last_name");
    Assert.assertEquals(12, t2Rows.size());
    List<Record> expectedT2 = TestHelper.RecordsBuilder
        .newInstance(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
        .add(1L, "Joanna", "Pierce")
        .add(1L, "Sharon", "Taylor")
        .add(2L, "Bob", "Silver")
        .add(2L, "Changed", "Donnel")
        .add(2L, "Changed", "Morrison")
        .add(2L, "Joanna", "Silver")
        .add(3L, "Blake", "Burr")
        .add(3L, "Marci", "Barna")
        .add(3L, "Trudy", "Henderson")
        .add(3L, "Trudy", "Johnson")
        .add(4L, "Laci", "Zold")
        .add(5L, "Peti", "Rozsaszin")
        .build();
    HiveIcebergTestUtils.validateData(expectedT2,
        HiveIcebergTestUtils.valueForRow(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, t2Rows), 0);
  }

  @Test
  public void testConflictingMultiStatementUpdates() {
    Assume.assumeTrue(formatVersion >= 2);

    String[][] stmts = new String[][]{
        {
            "START TRANSACTION",
            "UPDATE t1 SET first_name='Changed' WHERE last_name='Taylor'",
            "UPDATE t2 SET first_name='Changed' WHERE last_name='Taylor'",
            "COMMIT"
        },
        {
            "START TRANSACTION",
            "UPDATE t1 SET first_name='Changed' WHERE last_name='Taylor'",
            "UPDATE t2 SET first_name='Changed' WHERE last_name='Taylor'",
            "COMMIT"
        }
    };

    Assert.assertThrows(Exception.class, () -> executeConcurrentlyNoRetry(stmts));

    // First transaction succeeded
    List<Object[]> t1Rows = shell.executeStatement(
        "SELECT * FROM t1 ORDER BY customer_id, first_name, last_name");
    Assert.assertEquals(12, t1Rows.size());

    List<Object[]> t1Taylor = shell.executeStatement(
        "SELECT first_name FROM t1 WHERE last_name='Taylor'");
    Assert.assertEquals(1, t1Taylor.size());
    Assert.assertEquals("Changed", t1Taylor.getFirst()[0]);

    List<Object[]> t2Taylor = shell.executeStatement(
        "SELECT first_name FROM t2 WHERE last_name='Taylor'");
    Assert.assertEquals(1, t2Taylor.size());
    Assert.assertEquals("Changed", t2Taylor.getFirst()[0]);
  }

  @Test
  public void testMultiStatementInsertAndUpdate() throws Exception {
    Assume.assumeTrue(formatVersion >= 2);

    String[][] stmts = new String[][]{
        {
            "START TRANSACTION",
            "INSERT INTO t1 VALUES (100, 'NewA', 'PartA')",
            "UPDATE t2 SET first_name='Changed' WHERE last_name='Taylor'",
            "COMMIT"
        },
        {
            "START TRANSACTION",
            "INSERT INTO t1 VALUES (200, 'NewB', 'PartB')",
            "UPDATE t2 SET first_name='Changed' WHERE last_name='Donnel'",
            "COMMIT"
        }
    };
    executeConcurrentlyNoRetry(stmts);

    // t1: 12 original + 2 inserts = 14
    List<Object[]> t1Count = shell.executeStatement("SELECT count(*) FROM t1");
    Assert.assertEquals(14L, t1Count.getFirst()[0]);

    List<Object[]> newRows = shell.executeStatement(
        "SELECT customer_id FROM t1 WHERE last_name IN ('PartA', 'PartB') ORDER BY customer_id");
    Assert.assertEquals(2, newRows.size());
    Assert.assertEquals(100L, newRows.get(0)[0]);
    Assert.assertEquals(200L, newRows.get(1)[0]);

    // t2: 12 rows with updates
    Assert.assertEquals(12L,
        shell.executeStatement("SELECT count(*) FROM t2").getFirst()[0]);
    Assert.assertEquals("Changed",
        shell.executeStatement("SELECT first_name FROM t2 WHERE last_name='Taylor'").getFirst()[0]);
    Assert.assertEquals("Changed",
        shell.executeStatement("SELECT first_name FROM t2 WHERE last_name='Donnel'").getFirst()[0]);
  }

  @Test
  public void testMultiStatementDeleteAndUpdate() throws Exception {
    Assume.assumeTrue(formatVersion >= 2);

    String[][] stmts = new String[][]{
        {
            "START TRANSACTION",
            "DELETE FROM t1 WHERE last_name='Taylor'",
            "UPDATE t2 SET first_name='Changed' WHERE last_name='Taylor'",
            "COMMIT"
        },
        {
            "START TRANSACTION",
            "DELETE FROM t1 WHERE last_name='Donnel'",
            "UPDATE t2 SET first_name='Changed' WHERE last_name='Donnel'",
            "COMMIT"
        }
    };
    executeConcurrently(false, RETRY_STRATEGIES_WITHOUT_WRITE_CONFLICT, stmts);

    // t1: 12 - 1 (Taylor) - 1 (Donnel) = 10
    Assert.assertEquals(10L,
        shell.executeStatement("SELECT count(*) FROM t1").getFirst()[0]);
    Assert.assertEquals(0,
        shell.executeStatement("SELECT * FROM t1 WHERE last_name='Taylor'").size());
    Assert.assertEquals(0,
        shell.executeStatement("SELECT * FROM t1 WHERE last_name='Donnel'").size());

    // t2: 12 rows with updates
    Assert.assertEquals(12L,
        shell.executeStatement("SELECT count(*) FROM t2").getFirst()[0]);
    Assert.assertEquals("Changed",
        shell.executeStatement("SELECT first_name FROM t2 WHERE last_name='Taylor'").getFirst()[0]);
    Assert.assertEquals("Changed",
        shell.executeStatement("SELECT first_name FROM t2 WHERE last_name='Donnel'").getFirst()[0]);
  }

}
