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

package org.apache.hadoop.hive.metastore;

import java.security.Permission;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;
import org.junit.Before;

public class TestHiveMetaStoreGetMetaConf {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private static final Logger LOG = LoggerFactory.getLogger(TestHiveMetaStoreGetMetaConf.class);
  private static final String msPort = "20103";
  private static HiveConf hiveConf;
  private static SecurityManager securityManager;

  private HiveMetaStoreClient hmsc;

  public static class NoExitSecurityManager extends SecurityManager {

    @Override
    public void checkPermission(Permission perm) {
      // allow anything.
    }

    @Override
    public void checkPermission(Permission perm, Object context) {
      // allow anything.
    }

    @Override
    public void checkExit(int status) {
      super.checkExit(status);
      throw new RuntimeException("System.exit() was called. Raising exception.");
    }
  }

  private static class RunMS implements Runnable {

    @Override
    public void run() {
      try {
        HiveMetaStore.main(new String[]{"-v", "-p", msPort, "--hiveconf",
            "hive.metastore.expression.proxy=" + MockPartitionExpressionForMetastore.class.getCanonicalName(),
            "--hiveconf", "hive.metastore.try.direct.sql.ddl=false"});
      } catch (Throwable t) {
        LOG.error("Exiting. Got exception from metastore: ", t);
      }
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    LOG.info("Shutting down metastore.");
    System.setSecurityManager(securityManager);
  }

  @BeforeClass
  public static void startMetaStoreServer() throws Exception {

    securityManager = System.getSecurityManager();
    System.setSecurityManager(new NoExitSecurityManager());
    
    hiveConf = new HiveConf(TestHiveMetaStoreGetMetaConf.class);
    hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://localhost:"
        + msPort);
    hiveConf.setVar(HiveConf.ConfVars.PREEXECHOOKS, "");
    hiveConf.setVar(HiveConf.ConfVars.POSTEXECHOOKS, "");
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    hiveConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 10);

    System.setProperty(HiveConf.ConfVars.PREEXECHOOKS.varname, " ");
    System.setProperty(HiveConf.ConfVars.POSTEXECHOOKS.varname, " ");

    new Thread(new RunMS()).start();
  }

  @Before
  public void setup() throws MetaException {
    hmsc = new HiveMetaStoreClient(hiveConf);
  }

  @After
  public void closeClient() {
    if (hmsc != null) {
      hmsc.close();
    }
  }

  @Test
  public void testGetMetaConfDefault() throws MetaException, TException {
    HiveConf.ConfVars metaConfVar = HiveConf.ConfVars.METASTORE_TRY_DIRECT_SQL;
    String expected = metaConfVar.getDefaultValue();
    String actual = hmsc.getMetaConf(metaConfVar.toString());
    assertEquals(expected, actual);
  }

  @Test
  public void testGetMetaConfDefaultEmptyString() throws MetaException, TException {
    HiveConf.ConfVars metaConfVar = HiveConf.ConfVars.METASTORE_PARTITION_NAME_WHITELIST_PATTERN;
    String expected = "";
    String actual = hmsc.getMetaConf(metaConfVar.toString());
    assertEquals(expected, actual);
  }

  @Test
  public void testGetMetaConfOverridden() throws MetaException, TException {
    HiveConf.ConfVars metaConfVar = HiveConf.ConfVars.METASTORE_TRY_DIRECT_SQL_DDL;
    String expected = "false";
    String actual = hmsc.getMetaConf(metaConfVar.toString());
    assertEquals(expected, actual);
  }

  @Test
  public void testGetMetaConfUnknownPreperty() throws MetaException, TException {
    String unknownPropertyName = "hive.meta.foo.bar";
    thrown.expect(MetaException.class);
    thrown.expectMessage("Invalid configuration key " + unknownPropertyName);
    hmsc.getMetaConf(unknownPropertyName);
  }
}
