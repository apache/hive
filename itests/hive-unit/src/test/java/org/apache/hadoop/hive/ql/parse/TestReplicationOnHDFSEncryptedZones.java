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
package org.apache.hadoop.hive.ql.parse;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTORE_AGGREGATE_STATS_CACHE_ENABLED;

public class TestReplicationOnHDFSEncryptedZones {
  private static String jksFile = System.getProperty("java.io.tmpdir") + "/test.jks";
  @Rule
  public final TestName testName = new TestName();

  protected static final Logger LOG = LoggerFactory.getLogger(TestReplicationScenarios.class);
  private static WarehouseInstance primary;
  private static String primaryDbName, replicatedDbName;
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
      put(HiveConf.ConfVars.HIVE_IN_TEST.varname, "false");
      put(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS.varname, "false");
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
    replicatedDbName = "replicated_" + primaryDbName;
    primary.run("create database " + primaryDbName);
  }

  @Test
  public void targetAndSourceHaveDifferentEncryptionZoneKeys() throws Throwable {
    DFSTestUtil.createKey("test_key123", miniDFSCluster, conf);

    WarehouseInstance replica = new WarehouseInstance(LOG, miniDFSCluster,
        new HashMap<String, String>() {{
          put(HiveConf.ConfVars.HIVE_IN_TEST.varname, "false");
          put(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS.varname, "false");
        }}, "test_key123");

    WarehouseInstance.Tuple tuple =
        primary.run("use " + primaryDbName)
            .run("create table encrypted_table (id int, value string)")
            .run("insert into table encrypted_table values (1,'value1')")
            .run("insert into table encrypted_table values (2,'value2')")
            .dump(primaryDbName, null);

    replica
        .run("repl load " + replicatedDbName + " from '" + tuple.dumpLocation
            + "' with('hive.repl.add.raw.reserved.namespace'='true')")
        .run("use " + replicatedDbName)
        .run("repl status " + replicatedDbName)
        .verifyResult(tuple.lastReplicationId)
        .run("select value from encrypted_table")
        .verifyFailure(new String[] { "value1", "value2" });
  }

  @Ignore("this is ignored as minidfs cluster as of writing this test looked like did not copy the "
              + "files correctly")
  @Test
  public void targetAndSourceHaveSameEncryptionZoneKeys() throws Throwable {
    WarehouseInstance replica = new WarehouseInstance(LOG, miniDFSCluster,
        new HashMap<String, String>() {{
          put(HiveConf.ConfVars.HIVE_IN_TEST.varname, "false");
          put(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS.varname, "false");
          put(HiveConf.ConfVars.HIVE_DISTCP_DOAS_USER.varname,
              UserGroupInformation.getCurrentUser().getUserName());
        }}, "test_key");

    WarehouseInstance.Tuple tuple =
        primary.run("use " + primaryDbName)
            .run("create table encrypted_table (id int, value string)")
            .run("insert into table encrypted_table values (1,'value1')")
            .run("insert into table encrypted_table values (2,'value2')")
            .dump(primaryDbName, null);

    replica
        .run("repl load " + replicatedDbName + " from '" + tuple.dumpLocation
            + "' with('hive.repl.add.raw.reserved.namespace'='true')")
        .run("use " + replicatedDbName)
        .run("repl status " + replicatedDbName)
        .verifyResult(tuple.lastReplicationId)
        .run("select value from encrypted_table")
        .verifyResults(new String[] { "value1", "value2" });
  }
}
