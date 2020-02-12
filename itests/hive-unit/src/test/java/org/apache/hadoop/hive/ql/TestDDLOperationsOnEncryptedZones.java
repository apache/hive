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
package org.apache.hadoop.hive.ql;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.messaging.json.gzip.GzipJSONMessageEncoder;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.TestReplicationScenarios;
import org.apache.hadoop.hive.ql.parse.WarehouseInstance;
import org.apache.hadoop.hive.shims.Utils;
import org.junit.BeforeClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTORE_AGGREGATE_STATS_CACHE_ENABLED;
import static org.apache.hadoop.hive.metastore.ReplChangeManager.SOURCE_OF_REPLICATION;

public class TestDDLOperationsOnEncryptedZones {
  private static String jksFile = System.getProperty("java.io.tmpdir") + "/test.jks";
  @Rule
  public final TestName testName = new TestName();

  protected static final Logger LOG = LoggerFactory.getLogger(TestReplicationScenarios.class);
  private static WarehouseInstance primary;
  private static String primaryDbName;
  private static Configuration conf;
  private static MiniDFSCluster miniDFSCluster;

  @BeforeClass
  public static void beforeClassSetup() throws Exception {
    conf = new Configuration();
    conf.set("dfs.client.use.datanode.hostname", "true");
    conf.set("hadoop.proxyuser." + Utils.getUGI().getShortUserName() + ".hosts", "*");
    conf.set("hadoop.security.key.provider.path", "jceks://file" + jksFile);
    conf.setBoolean("dfs.namenode.delegation.token.always-use", true);

    conf.setLong(HiveConf.ConfVars.HIVE_EXEC_COPYFILE_MAXSIZE.varname, 1);
    conf.setLong(HiveConf.ConfVars.HIVE_EXEC_COPYFILE_MAXNUMFILES.varname, 0);
    conf.setBoolean(METASTORE_AGGREGATE_STATS_CACHE_ENABLED.varname, false);

    miniDFSCluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).build();

    DFSTestUtil.createKey("test_key", miniDFSCluster, conf);
    primary = new WarehouseInstance(LOG, miniDFSCluster, new HashMap<String, String>() {{
      put(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS.varname, "false");
      put(HiveConf.ConfVars.REPL_INCLUDE_EXTERNAL_TABLES.varname, "true");
      put("hive.repl.bootstrap.dump.open.txn.timeout", "1s");
      put("hive.strict.checks.bucketing", "false");
      put("hive.mapred.mode", "nonstrict");
      put("mapred.input.dir.recursive", "true");
      put("hive.metastore.disallow.incompatible.col.type.changes", "false");
      put("hive.strict.managed.tables", "false");

      put(MetastoreConf.ConfVars.EVENT_MESSAGE_FACTORY.getHiveName(),
              GzipJSONMessageEncoder.class.getCanonicalName());
      put(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "true");
      put(HiveConf.ConfVars.HIVE_TXN_MANAGER.varname,
              "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager");
      put(MetastoreConf.ConfVars.CAPABILITY_CHECK.getHiveName(), "false");
      put(HiveConf.ConfVars.REPL_BOOTSTRAP_DUMP_OPEN_TXN_TIMEOUT.varname, "1s");
      put(HiveConf.ConfVars.HIVE_IN_TEST_REPL.varname, "true");
      put(HiveConf.ConfVars.HIVESTATSAUTOGATHER.varname, "true");

    }}, "test_key");
  }

  @AfterClass
  public static void classLevelTearDown() throws IOException {
    primary.close();
    FileUtils.deleteQuietly(new File(jksFile));
  }

  @Before
  public void setup() throws Throwable {
    primaryDbName = testName.getMethodName() + "_" + +System.currentTimeMillis();
    primary.run("create database " + primaryDbName + " WITH DBPROPERTIES ( '" +
            SOURCE_OF_REPLICATION + "' = '1,2,3')");
  }

  @Test
  public void createTableEncryptionZoneRoot() throws Throwable {
    boolean exceptionThrown = false;
    try {
      primary.run("use " + primaryDbName)
              .run("create table encrypted_table (id int, value string) location '" + primary.getWarehouseRoot() + "'");
    } catch (HiveException e) {
      exceptionThrown = true;
      Assert.assertEquals("Table Location cannot be set to encryption zone root dir", e.getMessage());
    }
    Assert.assertTrue(exceptionThrown);

  }

  @Test
  public void createExternalTableEncryptionZoneRoot() throws Throwable {
    boolean exceptionThrown = false;
    try {
      primary.run("use " + primaryDbName)
              .run("create external table encrypted_external_table (id int, value string) location '"
                      + primary.getWarehouseRoot() + "'");
    } catch (HiveException e) {
      exceptionThrown = true;
    } finally {
      primary.run("use " + primaryDbName)
              .run("drop table encrypted_external_table");
    }
    Assert.assertFalse(exceptionThrown);
  }

  @Test
  public void alterTableSetLocationEncryptionZoneRoot() throws Throwable {
    boolean exceptionThrown = false;
    try {
      primary.run("use " + primaryDbName)
              .run("create table encrypted_table (id int, value string) stored as orc TBLPROPERTIES('transactional'='true')");
      primary.run("use " + primaryDbName)
              .run("alter table encrypted_table set location '" + primary.getWarehouseRoot() + "'");
    } catch (HiveException e) {
      exceptionThrown = true;
      Assert.assertEquals("Table Location cannot be set to encryption zone root dir", e.getMessage());
    } finally {
      primary.run("use " + primaryDbName)
              .run("drop table encrypted_table");
    }
    Assert.assertTrue(exceptionThrown);
  }

  @Test
  public void alterExternalTableEncryptionZoneRoot() throws Throwable {
    boolean exceptionThrown = false;
    try {
      primary.run("use " + primaryDbName)
              .run("create external table encrypted_external_table (id int, value string)");
      primary.run("use " + primaryDbName)
              .run("alter table encrypted_external_table set location '"
                      + primary.getWarehouseRoot() + "'");
    } catch (HiveException e) {
      exceptionThrown = true;
    } finally {
      primary.run("use " + primaryDbName)
              .run("drop table encrypted_external_table");
    }
    Assert.assertFalse(exceptionThrown);
  }
}
