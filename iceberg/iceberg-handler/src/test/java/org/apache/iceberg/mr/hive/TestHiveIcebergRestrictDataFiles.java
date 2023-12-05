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
import java.util.Collections;
import java.util.List;
import org.apache.commons.collections4.ListUtils;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_ICEBERG_ALLOW_DATAFILES_IN_TABLE_LOCATION_ONLY;

public class TestHiveIcebergRestrictDataFiles extends HiveIcebergStorageHandlerWithEngineBase {

  @BeforeClass
  public static void beforeClass() {
    shell = HiveIcebergStorageHandlerTestUtils.shell(
        Collections.singletonMap(HIVE_ICEBERG_ALLOW_DATAFILES_IN_TABLE_LOCATION_ONLY.varname, "true"));
  }

  @Test
  public void testRestrictDataFiles() throws IOException, InterruptedException {
    TableIdentifier table1 = TableIdentifier.of("default", "tab1");
    testTables.createTableWithVersions(shell, table1.name(), HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, 2);

    AssertHelpers.assertThrows("Should throw exception since there are files outside the table directory",
        IllegalArgumentException.class, "The table contains paths which are outside the table location",
        () -> shell.executeStatement("SELECT * FROM " + table1.name()));

    // Create another table with files within the table location
    TableIdentifier table2 = TableIdentifier.of("default", "tab2");
    testTables.createTableWithVersions(shell, table2.name(), HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        fileFormat, null, 0);

    shell.executeStatement(
        testTables.getInsertQuery(HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, table2, false));

    List<Object[]> result = shell.executeStatement("SELECT * FROM " + table2.name());

    HiveIcebergTestUtils.validateData(HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS,
        HiveIcebergTestUtils.valueForRow(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, result), 0);

    // Insert some more records to generate new Data file
    shell.executeStatement(
        testTables.getInsertQuery(HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_1, table2, false));

    result = shell.executeStatement("SELECT * FROM " + table2.name());

    HiveIcebergTestUtils.validateData(ListUtils.union(HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS,
            HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_1),
        HiveIcebergTestUtils.valueForRow(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, result), 0);
  }
}
