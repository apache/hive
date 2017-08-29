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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hive.hcatalog.mapreduce;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.util.Shell;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

/**
 * Simplify writing HCatalog tests that require a HiveMetaStore.
 */
public class HCatBaseTest {
  protected static final Logger LOG = LoggerFactory.getLogger(HCatBaseTest.class);
  public static final String TEST_DATA_DIR = HCatUtil.makePathASafeFileName(System.getProperty("user.dir") +
          "/build/test/data/" + HCatBaseTest.class.getCanonicalName() + "-" + System.currentTimeMillis());
  protected static final String TEST_WAREHOUSE_DIR = TEST_DATA_DIR + "/warehouse";

  protected HiveConf hiveConf = null;
  protected Driver driver = null;
  protected HiveMetaStoreClient client = null;

  @BeforeClass
  public static void setUpTestDataDir() throws Exception {
    LOG.info("Using warehouse directory " + TEST_WAREHOUSE_DIR);
    File f = new File(TEST_WAREHOUSE_DIR);
    if (f.exists()) {
      FileUtil.fullyDelete(f);
    }
    Assert.assertTrue(new File(TEST_WAREHOUSE_DIR).mkdirs());
  }

  @Before
  public void setUp() throws Exception {
    if (driver == null) {
      setUpHiveConf();
      driver = new Driver(hiveConf);
      client = new HiveMetaStoreClient(hiveConf);
      SessionState.start(new CliSessionState(hiveConf));
    }
  }

  /**
   * Create a new HiveConf and set properties necessary for unit tests.
   */
  protected void setUpHiveConf() {
    hiveConf = new HiveConf(this.getClass());
    hiveConf.setVar(HiveConf.ConfVars.PREEXECHOOKS, "");
    hiveConf.setVar(HiveConf.ConfVars.POSTEXECHOOKS, "");
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    hiveConf.setVar(HiveConf.ConfVars.METASTOREWAREHOUSE, TEST_WAREHOUSE_DIR);
    hiveConf.setVar(HiveConf.ConfVars.HIVEMAPREDMODE, "nonstrict");
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVEOPTIMIZEMETADATAQUERIES, true);
    hiveConf
    .setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
  }

  protected void logAndRegister(PigServer server, String query) throws IOException {
    logAndRegister(server, query, 1);
  }
  protected void logAndRegister(PigServer server, String query, int lineNumber) throws IOException {
    assert lineNumber > 0 : "(lineNumber > 0) is false";
    LOG.info("Registering pig query: " + query);
    server.registerQuery(query, lineNumber);
  }

  /**
   * creates PigServer in LOCAL mode.
   * http://pig.apache.org/docs/r0.12.0/perf.html#error-handling
   * @param stopOnFailure equivalent of "-stop_on_failure" command line arg, setting to 'true' makes
   *                      debugging easier
   */
  public static PigServer createPigServer(boolean stopOnFailure) throws ExecException {
    if(stopOnFailure) {
      Properties p = new Properties();
      p.put("stop.on.failure", Boolean.TRUE.toString());
      return new PigServer(ExecType.LOCAL, p);
    }
    return new PigServer(ExecType.LOCAL);
  }
}
