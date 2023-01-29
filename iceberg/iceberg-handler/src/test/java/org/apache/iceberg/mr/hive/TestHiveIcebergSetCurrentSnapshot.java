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
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests setting current snapshot feature
 */
public class TestHiveIcebergSetCurrentSnapshot extends HiveIcebergStorageHandlerWithEngineBase {

  @Test
  public void testSetCurrentSnapshot() throws IOException, InterruptedException {
    TableIdentifier identifier = TableIdentifier.of("default", "source");
    Table table =
        testTables.createTableWithVersions(shell, identifier.name(), HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
            fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, 5);
    Assert.assertEquals(5, table.history().size());
    List<Object[]> result4 = shell.executeStatement(
        "SELECT * from " + identifier.name() + " FOR SYSTEM_VERSION AS OF " + table.history().get(4).snapshotId());
    List<Object[]> result3 = shell.executeStatement(
        "SELECT * from " + identifier.name() + " FOR SYSTEM_VERSION AS OF " + table.history().get(3).snapshotId());
    shell.executeStatement(
        "ALTER TABLE " + identifier.name() + " EXECUTE SET_CURRENT_SNAPSHOT(" + table.history().get(3).snapshotId() +
            ")");
    List<Object[]> currentResult = shell.executeStatement("SELECT * from " + identifier.name());
    Assert.assertEquals(result3.size(), currentResult.size());
    HiveIcebergTestUtils.validateData(
        HiveIcebergTestUtils.valueForRow(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, currentResult),
        HiveIcebergTestUtils.valueForRow(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, result3), 0);

    shell.executeStatement(
        "ALTER TABLE " + identifier.name() + " EXECUTE SET_CURRENT_SNAPSHOT(" + table.history().get(4).snapshotId() +
            ")");
    currentResult = shell.executeStatement("SELECT * from " + identifier.name());
    Assert.assertEquals(result4.size(), currentResult.size());
    HiveIcebergTestUtils.validateData(
        HiveIcebergTestUtils.valueForRow(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, currentResult),
        HiveIcebergTestUtils.valueForRow(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, result4), 0);
  }
}
