/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.util;

import static org.apache.hadoop.hive.ql.TxnCommandsBaseForTests.Table.ACIDTBL;
import static org.apache.hadoop.hive.ql.TxnCommandsBaseForTests.Table.ACIDTBLPART;
import static org.apache.hadoop.hive.ql.TxnCommandsBaseForTests.Table.NONACIDNONBUCKET;
import static org.apache.hadoop.hive.ql.TxnCommandsBaseForTests.Table.NONACIDORCTBL;
import static org.apache.hadoop.hive.ql.TxnCommandsBaseForTests.Table.NONACIDORCTBL2;

import java.io.File;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.TxnCommandsBaseForTests;
import org.junit.Assert;
import org.junit.Test;

public class TestHiveStrictManagedMigration extends TxnCommandsBaseForTests {
  private static final String TEST_DATA_DIR = new File(System.getProperty("java.io.tmpdir") +
  File.separator + TestHiveStrictManagedMigration.class.getCanonicalName() + "-" + System.currentTimeMillis()
          ).getPath().replaceAll("\\\\", "/");

  @Test
  public void testUpgrade() throws Exception {
    int[][] data = {{1, 2}, {3, 4}, {5, 6}};
    runStatementOnDriver("DROP TABLE IF EXISTS test.TAcid");
    runStatementOnDriver("DROP DATABASE IF EXISTS test");

    runStatementOnDriver("CREATE DATABASE test");
    runStatementOnDriver(
            "CREATE TABLE test.TAcid (a int, b int) CLUSTERED BY (b) INTO 2 BUCKETS STORED AS orc TBLPROPERTIES" +
                    " ('transactional'='true')");
    runStatementOnDriver("INSERT INTO test.TAcid" + makeValuesClause(data));

    runStatementOnDriver(
            "CREATE EXTERNAL TABLE texternal (a int, b int)");

    String oldWarehouse = getWarehouseDir();
    String[] args = {"--hiveconf", "hive.strict.managed.tables=true", "-m",  "automatic", "--modifyManagedTables",
            "--oldWarehouseRoot", oldWarehouse};
    HiveConf newConf = new HiveConf(hiveConf);
    File newWarehouseDir = new File(getTestDataDir(), "newWarehouse");
    newConf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, newWarehouseDir.getAbsolutePath());
    newConf.set("strict.managed.tables.migration.owner", System.getProperty("user.name"));
    HiveStrictManagedMigration.hiveConf = newConf;
    HiveStrictManagedMigration.scheme = "file";
    HiveStrictManagedMigration.main(args);

    Assert.assertTrue(newWarehouseDir.exists());
    Assert.assertTrue(new File(newWarehouseDir, ACIDTBL.toString().toLowerCase()).exists());
    Assert.assertTrue(new File(newWarehouseDir, ACIDTBLPART.toString().toLowerCase()).exists());
    Assert.assertTrue(new File(newWarehouseDir, NONACIDNONBUCKET.toString().toLowerCase()).exists());
    Assert.assertTrue(new File(newWarehouseDir, NONACIDORCTBL.toString().toLowerCase()).exists());
    Assert.assertTrue(new File(newWarehouseDir, NONACIDORCTBL2.toString().toLowerCase()).exists());
    Assert.assertTrue(new File(new File(newWarehouseDir, "test.db"), "tacid").exists());
    Assert.assertTrue(new File(oldWarehouse, "texternal").exists());
  }

  @Override
  protected String getTestDataDir() {
    return TEST_DATA_DIR;
  }
}
