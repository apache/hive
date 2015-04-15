/**
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

package org.apache.hadoop.hive.ql.security;

import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test cases focusing on drop table permission checks
 */
public class TestStorageBasedMetastoreAuthorizationReads extends StorageBasedMetastoreTestBase {

  @Test
  public void testReadTableSuccess() throws Exception {
    readTableByOtherUser("-rwxrwxrwx", true);
  }

  @Test
  public void testReadTableFailure() throws Exception {
    readTableByOtherUser("-rwxrwx---", false);
  }

  /**
   * @param perm dir permission for table dir
   * @param isSuccess if command was successful
   * @throws Exception
   */
  private void readTableByOtherUser(String perm, boolean isSuccess) throws Exception {
    String dbName = getTestDbName();
    String tblName = getTestTableName();
    setPermissions(clientHiveConf.getVar(ConfVars.METASTOREWAREHOUSE), "-rwxrwxrwx");

    CommandProcessorResponse resp = driver.run("create database " + dbName);
    Assert.assertEquals(0, resp.getResponseCode());
    Database db = msc.getDatabase(dbName);
    validateCreateDb(db, dbName);

    setPermissions(db.getLocationUri(), "-rwxrwxrwx");

    String dbDotTable = dbName + "." + tblName;
    resp = driver.run("create table " + dbDotTable + "(i int) partitioned by (`date` string)");
    Assert.assertEquals(0, resp.getResponseCode());
    Table tab = msc.getTable(dbName, tblName);
    setPermissions(tab.getSd().getLocation(), perm);

    InjectableDummyAuthenticator.injectMode(true);

    testCmd(driver, "DESCRIBE  " + dbDotTable, isSuccess);
    testCmd(driver, "DESCRIBE EXTENDED  " + dbDotTable, isSuccess);
    testCmd(driver, "SHOW PARTITIONS  " + dbDotTable, isSuccess);
    testCmd(driver, "SHOW COLUMNS IN " + tblName + " IN " + dbName, isSuccess);
    testCmd(driver, "use " + dbName, true);
    testCmd(driver, "SHOW TABLE EXTENDED LIKE " + tblName, isSuccess);

  }

  @Test
  public void testReadDbSuccess() throws Exception {
    readDbByOtherUser("-rwxrwxrwx", true);
  }

  @Test
  public void testReadDbFailure() throws Exception {
    readDbByOtherUser("-rwxrwx---", false);
  }


  /**
   * @param perm dir permission for database dir
   * @param isSuccess if command was successful
   * @throws Exception
   */
  private void readDbByOtherUser(String perm, boolean isSuccess) throws Exception {
    String dbName = getTestDbName();
    setPermissions(clientHiveConf.getVar(ConfVars.METASTOREWAREHOUSE), perm);

    CommandProcessorResponse resp = driver.run("create database " + dbName);
    Assert.assertEquals(0, resp.getResponseCode());
    Database db = msc.getDatabase(dbName);
    validateCreateDb(db, dbName);
    setPermissions(db.getLocationUri(), perm);

    InjectableDummyAuthenticator.injectMode(true);

    testCmd(driver, "DESCRIBE DATABASE " + dbName, isSuccess);
    testCmd(driver, "DESCRIBE DATABASE EXTENDED " + dbName, isSuccess);
    testCmd(driver, "SHOW TABLES IN " + dbName, isSuccess);
    driver.run("use " + dbName);
    testCmd(driver, "SHOW TABLES ", isSuccess);

  }

  private void testCmd(Driver driver, String cmd, boolean isSuccess)
      throws CommandNeedRetryException {
    CommandProcessorResponse resp = driver.run(cmd);
    Assert.assertEquals(isSuccess, resp.getResponseCode() == 0);
  }


}
