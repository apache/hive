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
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

/**
 * Creates Iceberg tables using CTAS, and runs select queries against these new tables to verify table content.
 */
public class TestHiveIcebergCTAS extends HiveIcebergStorageHandlerWithEngineBase {

  @Test
  public void testCTASFromHiveTable() {
    Assume.assumeTrue(HiveIcebergSerDe.CTAS_EXCEPTION_MSG, testTableType == TestTables.TestTableType.HIVE_CATALOG);

    shell.executeStatement("CREATE TABLE source (id bigint, name string) PARTITIONED BY (dept string) STORED AS ORC");
    shell.executeStatement(testTables.getInsertQuery(
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, TableIdentifier.of("default", "source"), false));

    shell.executeStatement(String.format(
        "CREATE TABLE target STORED BY ICEBERG %s %s AS SELECT * FROM source",
        testTables.locationForCreateTableSQL(TableIdentifier.of("default", "target")),
        testTables.propertiesForCreateTableSQL(
            ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, fileFormat.toString()))));

    List<Object[]> objects = shell.executeStatement("SELECT * FROM target ORDER BY id");
    HiveIcebergTestUtils.validateData(HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS,
        HiveIcebergTestUtils.valueForRow(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, objects), 0);
  }

  @Test
  public void testCTASPartitionedFromHiveTable() throws TException, InterruptedException {
    Assume.assumeTrue(HiveIcebergSerDe.CTAS_EXCEPTION_MSG, testTableType == TestTables.TestTableType.HIVE_CATALOG);

    shell.executeStatement("CREATE TABLE source (id bigint, name string) PARTITIONED BY (dept string) STORED AS ORC");
    shell.executeStatement(testTables.getInsertQuery(
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, TableIdentifier.of("default", "source"), false));

    shell.executeStatement(String.format(
        "CREATE TABLE target PARTITIONED BY (dept, name) " +
            "STORED BY ICEBERG %s AS SELECT * FROM source",
        testTables.propertiesForCreateTableSQL(
            ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, fileFormat.toString()))));

    // check table can be read back correctly
    List<Object[]> objects = shell.executeStatement("SELECT id, name, dept FROM target ORDER BY id");
    HiveIcebergTestUtils.validateData(HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS,
        HiveIcebergTestUtils.valueForRow(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, objects), 0);

    // check HMS table has been created correctly (no partition cols, props pushed down)
    org.apache.hadoop.hive.metastore.api.Table hmsTable = shell.metastore().getTable("default", "target");
    Assert.assertEquals(3, hmsTable.getSd().getColsSize());
    Assert.assertTrue(hmsTable.getPartitionKeys().isEmpty());
    Assert.assertEquals(fileFormat.toString(), hmsTable.getParameters().get(TableProperties.DEFAULT_FILE_FORMAT));

    // check Iceberg table has correct partition spec
    Table table = testTables.loadTable(TableIdentifier.of("default", "target"));
    Assert.assertEquals(2, table.spec().fields().size());
    Assert.assertEquals("dept", table.spec().fields().get(0).name());
    Assert.assertEquals("name", table.spec().fields().get(1).name());
  }

  @Test
  public void testCTASTblPropsAndLocationClause() throws Exception {
    Assume.assumeTrue(HiveIcebergSerDe.CTAS_EXCEPTION_MSG, testTableType == TestTables.TestTableType.HIVE_CATALOG);

    shell.executeStatement("CREATE TABLE source (id bigint, name string) PARTITIONED BY (dept string) STORED AS ORC");
    shell.executeStatement(testTables.getInsertQuery(
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, TableIdentifier.of("default", "source"), false));

    String location = temp.newFolder().toURI().toString();
    shell.executeStatement(String.format(
        "CREATE TABLE target PARTITIONED BY (dept, name) " +
        "STORED BY ICEBERG STORED AS %s LOCATION '%s' TBLPROPERTIES ('customKey'='customValue') " +
        "AS SELECT * FROM source", fileFormat.toString(), location));

    // check table can be read back correctly
    List<Object[]> objects = shell.executeStatement("SELECT id, name, dept FROM target ORDER BY id");
    HiveIcebergTestUtils.validateData(HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS,
        HiveIcebergTestUtils.valueForRow(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, objects), 0);

    // check table is created at the correct location
    org.apache.hadoop.hive.metastore.api.Table tbl = shell.metastore().getTable("default", "target");
    Assert.assertEquals(location, tbl.getSd().getLocation() + "/" /* HMS trims the trailing dash */);

    // check if valid table properties are preserved, while serde props don't get preserved
    Assert.assertEquals("customValue", tbl.getParameters().get("customKey"));
    Assert.assertNull(tbl.getParameters().get(serdeConstants.LIST_COLUMNS));
    Assert.assertNull(tbl.getParameters().get(serdeConstants.LIST_PARTITION_COLUMNS));
  }

  @Test
  public void testCTASFailureRollback() throws IOException {
    Assume.assumeTrue(HiveIcebergSerDe.CTAS_EXCEPTION_MSG, testTableType == TestTables.TestTableType.HIVE_CATALOG);

    // force an execution error by passing in a committer class that Tez won't be able to load
    shell.setHiveSessionValue("hive.tez.mapreduce.output.committer.class", "org.apache.NotExistingClass");

    TableIdentifier target = TableIdentifier.of("default", "target");
    testTables.createTable(shell, "source", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);

    String[] partitioningSchemes = {"", "PARTITIONED BY (last_name)", "PARTITIONED BY (customer_id, last_name)"};
    for (String partitioning : partitioningSchemes) {
      AssertHelpers.assertThrows("Should fail while loading non-existent output committer class.",
          IllegalArgumentException.class, "org.apache.NotExistingClass",
          () -> shell.executeStatement(String.format(
              "CREATE TABLE target %s STORED BY ICEBERG AS SELECT * FROM source", partitioning)));
      // CTAS table should have been dropped by the lifecycle hook
      Assert.assertThrows(NoSuchTableException.class, () -> testTables.loadTable(target));
    }
  }
}
