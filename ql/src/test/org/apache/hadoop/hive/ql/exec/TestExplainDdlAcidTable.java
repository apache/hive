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

package org.apache.hadoop.hive.ql.exec;

import java.io.File;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.TxnCommandsBaseForTests;
import org.junit.Test;

public class TestExplainDdlAcidTable extends TxnCommandsBaseForTests {

  private static final String TEST_DATA_DIR = new File(System.getProperty("java.io.tmpdir") +
      File.separator + TestExplainDdlAcidTable.class.getCanonicalName()
      + "-" + System.currentTimeMillis()
  ).getPath().replaceAll("\\\\", "/");

  private static final String EXPLAIN_DDL = "EXPLAIN DDL SELECT * FROM " + Table.ACIDTBL;

  @Override
  protected void initHiveConf() {
    super.initHiveConf();
    HiveConf.setBoolVar(hiveConf, HiveConf.ConfVars.HIVE_IN_TEST, false);
  }

  @Override
  protected String getTestDataDir() {
    return TEST_DATA_DIR;
  }

  @Test
  public void testExplainDdlAcidTableUnauthorized() throws Exception {
    runExplainDdl(hiveConf);
  }

  /**
   * {@link DDLPlanUtils} must use the query conf.
   * HIVE-29330 repoints thread-local {@link org.apache.hadoop.hive.ql.metadata.Hive} conf via
   * {@link org.apache.hadoop.hive.ql.security.authorization.plugin.HiveMetastoreClientFactoryImpl}
   */
  @Test
  public void testExplainDdlAcidTableAuthorized() throws Exception {
    HiveConf queryConf = new HiveConf(hiveConf);
    HiveConf.setBoolVar(queryConf, HiveConf.ConfVars.HIVE_AUTHORIZATION_ENABLED, true);
    runExplainDdl(queryConf);
  }

  private void runExplainDdl(HiveConf queryConf) throws Exception {
    Driver driver = new Driver(new QueryState.Builder().withHiveConf(queryConf).build(), null);
    driver.setMaxRows(10000);
    try {
      driver.run(EXPLAIN_DDL);
    } finally {
      driver.close();
      driver.destroy();
    }
  }
}
