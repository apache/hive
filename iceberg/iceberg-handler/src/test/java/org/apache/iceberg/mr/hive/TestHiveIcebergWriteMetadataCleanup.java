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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapper;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableProperties;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

import static org.apache.iceberg.TableMetadataParser.getFileExtension;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests Format specific features, such as reading/writing tables, using delete files, etc.
 */
public class TestHiveIcebergWriteMetadataCleanup {

  protected static TestHiveShell shell;

  protected TestTables testTables;

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Rule
  public Timeout timeout = new Timeout(500_000, TimeUnit.MILLISECONDS);

  @BeforeClass
  public static void beforeClass() {
    shell = HiveIcebergStorageHandlerTestUtils.shell();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    shell.stop();
  }

  @Before
  public void before() throws IOException {
    testTables = HiveIcebergStorageHandlerTestUtils.testTables(shell, TestTables.TestTableType.HIVE_CATALOG, temp);
    HiveIcebergStorageHandlerTestUtils.init(shell, testTables, temp);
    HiveConf.setBoolVar(shell.getHiveConf(), HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, true);
  }

  @After
  public void after() throws Exception {
    HiveIcebergStorageHandlerTestUtils.close(shell);
    // Mixing mr and tez jobs within the same JVM can cause problems. Mr jobs set the ExecMapper status to done=false
    // at the beginning and to done=true at the end. However, tez jobs also rely on this value to see if they should
    // proceed, but they do not reset it to done=false at the beginning. Therefore, without calling this after each test
    // case, any tez job that follows a completed mr job will erroneously read done=true and will not proceed.
    ExecMapper.setDone(false);
  }

  private void insertFirstFiveCustomers() {
    shell.executeStatement("insert into customers values (0, 'Alice', 'Brown')");
    shell.executeStatement("insert into customers values (1, 'Bob', 'Brown')");
    shell.executeStatement("insert into customers values (2, 'Charlie', 'Brown')");
    shell.executeStatement("insert into customers values (3, 'David', 'Brown')");
    shell.executeStatement("insert into customers values (4, 'Eve', 'Brown')");
  }

  private void insertNextFiveCustomers() {
    shell.executeStatement("insert into customers values (5, 'Frank', 'Brown')");
    shell.executeStatement("insert into customers values (6, 'Grace', 'Brown')");
    shell.executeStatement("insert into customers values (7, 'Heidi', 'Brown')");
    shell.executeStatement("insert into customers values (8, 'Ivan', 'Brown')");
    shell.executeStatement("insert into customers values (9, 'Judy', 'Brown')");
  }

  @Test
  public void testWriteMetadataCleanupEnabled() {

    // Enable write metadata cleanup on session level
    shell.getHiveConf().setBoolean(TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED, true);
    shell.getHiveConf().setInt(TableProperties.METADATA_PREVIOUS_VERSIONS_MAX, 4);

    Table table = testTables.createTable(shell, "customers",
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, PartitionSpec.unpartitioned(), FileFormat.ORC, null, 2);

    insertFirstFiveCustomers();
    assertMetadataFiles(table, 5 /* Iceberg keeps max metadata files + 1 */);

    // Override max previous versions on table level
    shell.executeStatement(String.format("alter table customers set tblproperties('%s'='%d')",
        TableProperties.METADATA_PREVIOUS_VERSIONS_MAX, 7));

    insertNextFiveCustomers();
    assertMetadataFiles(table, 8);
  }

  @Test
  public void testWriteMetadataCleanupDisabled() {

    // Disable write metadata cleanup on session level
    shell.getHiveConf().setBoolean(TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED, false);

    Table table = testTables.createTable(shell, "customers",
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, PartitionSpec.unpartitioned(), FileFormat.ORC, null, 2);

    insertFirstFiveCustomers();
    insertNextFiveCustomers();

    assertMetadataFiles(table, 11);
  }

  @Test
  public void testWriteMetadataCleanupEnabledOnSessionLevelDisabledOnTableLevel() {

    // Enable metadata cleanup configs on session level
    shell.getHiveConf().setBoolean(TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED, true);
    shell.getHiveConf().setInt(TableProperties.METADATA_PREVIOUS_VERSIONS_MAX, 4);

    Table table = testTables.createTable(shell, "customers",
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, PartitionSpec.unpartitioned(), FileFormat.ORC, null, 2);

    // Disable metadata cleanup configs on table level
    shell.executeStatement(String.format("alter table customers set tblproperties('%s'='%s')",
        TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED, "false"));

    insertFirstFiveCustomers();
    insertNextFiveCustomers();

    assertMetadataFiles(table, 12);
  }

  @Test
  public void testWriteMetadataCleanupDisabledOnSessionLevelEnabledOnTableLevel() {

    // Enable metadata cleanup configs on session level
    shell.getHiveConf().setBoolean(TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED, false);

    Table table = testTables.createTable(shell, "customers",
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, PartitionSpec.unpartitioned(), FileFormat.ORC, null, 2);

    // Disable metadata cleanup configs on table level
    shell.executeStatement(String.format("alter table customers set tblproperties('%s'='%s', '%s'='%d')",
        TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED, "true",
        TableProperties.METADATA_PREVIOUS_VERSIONS_MAX, 4));

    insertFirstFiveCustomers();
    insertNextFiveCustomers();

    assertMetadataFiles(table, 5);
  }

  private void assertMetadataFiles(Table table, int expectedCount) {
    List<String> metadataFiles =
        Arrays.stream(new File(table.location().replaceAll("^[a-zA-Z]+:", "") + "/metadata")
                .listFiles())
            .map(File::getAbsolutePath)
            .filter(f -> f.endsWith(getFileExtension(TableMetadataParser.Codec.NONE)))
            .toList();
    assertThat(metadataFiles).hasSize(expectedCount);
  }
}
