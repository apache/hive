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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTORE_AGGREGATE_STATS_CACHE_ENABLED;
import static org.apache.hadoop.hive.common.repl.ReplConst.SOURCE_OF_REPLICATION;

public class TestReplicationOnHDFSEncryptedZones {
  private static String jksFile = System.getProperty("java.io.tmpdir") + "/test.jks";
  private static String jksFile2 = System.getProperty("java.io.tmpdir") + "/test2.jks";

  @Rule
  public final TestName testName = new TestName();

  protected static final Logger LOG = LoggerFactory.getLogger(TestReplicationScenarios.class);
  private static WarehouseInstance primary;
  private static String primaryDbName, replicatedDbName;
  private static Configuration conf;
  private static MiniDFSCluster miniDFSCluster;

  @BeforeClass
  public static void beforeClassSetup() throws Exception {
    System.setProperty("jceks.key.serialFilter", "java.lang.Enum;java.security.KeyRep;" +
        "java.security.KeyRep$Type;javax.crypto.spec.SecretKeySpec;" +
        "org.apache.hadoop.crypto.key.JavaKeyStoreProvider$KeyMetadata;!*");
    conf = getNewConf();
    conf.set(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE.varname, "mr");
    conf.set("dfs.client.use.datanode.hostname", "true");
    conf.set("hadoop.proxyuser." + Utils.getUGI().getShortUserName() + ".hosts", "*");
    conf.set("hadoop.security.key.provider.path", "jceks://file" + jksFile);
    conf.setBoolean("dfs.namenode.delegation.token.always-use", true);

    conf.setLong(HiveConf.ConfVars.HIVE_EXEC_COPYFILE_MAXSIZE.varname, 1);
    conf.setLong(HiveConf.ConfVars.HIVE_EXEC_COPYFILE_MAXNUMFILES.varname, 0);
    conf.setBoolean(METASTORE_AGGREGATE_STATS_CACHE_ENABLED.varname, false);

    miniDFSCluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(2).format(true).build();

    DFSTestUtil.createKey("test_key", miniDFSCluster, conf);
    primary = new WarehouseInstance(LOG, miniDFSCluster, new HashMap<String, String>() {{
      put(HiveConf.ConfVars.HIVE_IN_TEST.varname, "false");
      put(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS.varname, "false");
      put(HiveConf.ConfVars.REPL_INCLUDE_EXTERNAL_TABLES.varname, "true");
    }}, "test_key");
  }

  @AfterClass
  public static void classLevelTearDown() throws IOException {
    primary.close();
    FileUtils.deleteQuietly(new File(jksFile));
    FileUtils.deleteQuietly(new File(jksFile2));
  }

  @Before
  public void setup() throws Throwable {
    primaryDbName = testName.getMethodName() + "_" + +System.currentTimeMillis();
    replicatedDbName = "replicated_" + primaryDbName;
    primary.run("create database " + primaryDbName + " WITH DBPROPERTIES ( '" +
        SOURCE_OF_REPLICATION + "' = '1,2,3')");
  }

  @Test
  public void targetAndSourceHaveDifferentEncryptionZoneKeys() throws Throwable {
    String replicaBaseDir = Files.createTempDirectory("replica").toFile().getAbsolutePath();
    Configuration replicaConf = getNewConf();
    replicaConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, replicaBaseDir);
    replicaConf.set("dfs.client.use.datanode.hostname", "true");
    replicaConf.set("hadoop.proxyuser." + Utils.getUGI().getShortUserName() + ".hosts", "*");
    replicaConf.set("hadoop.security.key.provider.path", "jceks://file" + jksFile2);
    replicaConf.setBoolean("dfs.namenode.delegation.token.always-use", true);

    MiniDFSCluster miniReplicaDFSCluster =
        new MiniDFSCluster.Builder(replicaConf).numDataNodes(2).format(true).build();
    replicaConf.setBoolean(METASTORE_AGGREGATE_STATS_CACHE_ENABLED.varname, false);

    DFSTestUtil.createKey("test_key123", miniReplicaDFSCluster, replicaConf);

    WarehouseInstance replica = new WarehouseInstance(LOG, miniReplicaDFSCluster,
        new HashMap<String, String>() {{
          put(HiveConf.ConfVars.HIVE_IN_TEST.varname, "false");
          put(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS.varname, "false");
          put(HiveConf.ConfVars.HIVE_DISTCP_DOAS_USER.varname,
              UserGroupInformation.getCurrentUser().getUserName());
          put(HiveConf.ConfVars.REPL_DIR.varname, primary.repldDir);
        }}, "test_key123");

    //read should pass without raw-byte distcp
    List<String> dumpWithClause = Arrays.asList( "'" + HiveConf.ConfVars.REPL_EXTERNAL_TABLE_BASE_DIR.varname + "'='"
        + replica.externalTableWarehouseRoot + "'");
    WarehouseInstance.Tuple tuple =
        primary.run("use " + primaryDbName)
            .run("create external table encrypted_table2 (id int, value string)")
            .run("insert into table encrypted_table2 values (1,'value1')")
            .run("insert into table encrypted_table2 values (2,'value2')")
            .dump(primaryDbName, dumpWithClause);

    replica
        .run("repl load " + primaryDbName + " into " + replicatedDbName
            + " with('hive.repl.replica.external.table.base.dir'='" + replica.externalTableWarehouseRoot + "', "
            + "'hive.exec.copyfile.maxsize'='0', 'distcp.options.skipcrccheck'='')")
        .run("use " + replicatedDbName)
        .run("repl status " + replicatedDbName)
        .run("select value from encrypted_table2")
        .verifyResults(new String[] { "value1", "value2" });
  }

  @Test
  public void targetAndSourceHaveSameEncryptionZoneKeys() throws Throwable {
    String replicaBaseDir = Files.createTempDirectory("replica2").toFile().getAbsolutePath();
    Configuration replicaConf = getNewConf();
    replicaConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, replicaBaseDir);
    replicaConf.set("dfs.client.use.datanode.hostname", "true");
    replicaConf.set("hadoop.proxyuser." + Utils.getUGI().getShortUserName() + ".hosts", "*");
    replicaConf.set("hadoop.security.key.provider.path", "jceks://file" + jksFile);
    replicaConf.setBoolean("dfs.namenode.delegation.token.always-use", true);

    MiniDFSCluster miniReplicaDFSCluster =
        new MiniDFSCluster.Builder(replicaConf).numDataNodes(2).format(true).build();
    replicaConf.setBoolean(METASTORE_AGGREGATE_STATS_CACHE_ENABLED.varname, false);

    WarehouseInstance replica = new WarehouseInstance(LOG, miniReplicaDFSCluster,
        new HashMap<String, String>() {{
          put(HiveConf.ConfVars.HIVE_IN_TEST.varname, "false");
          put(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS.varname, "false");
          put(HiveConf.ConfVars.HIVE_DISTCP_DOAS_USER.varname,
              UserGroupInformation.getCurrentUser().getUserName());
          put(HiveConf.ConfVars.REPL_DIR.varname, primary.repldDir);
        }}, "test_key");

    List<String> dumpWithClause = Arrays.asList(
        "'hive.repl.add.raw.reserved.namespace'='true'",
        "'" + HiveConf.ConfVars.REPL_EXTERNAL_TABLE_BASE_DIR.varname + "'='"
            + replica.externalTableWarehouseRoot + "'",
        "'distcp.options.skipcrccheck'=''",
        "'" + HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS.varname + "'='false'",
        "'" + HiveConf.ConfVars.HIVE_DISTCP_DOAS_USER.varname + "'='"
            + UserGroupInformation.getCurrentUser().getUserName() +"'");

    WarehouseInstance.Tuple tuple =
        primary.run("use " + primaryDbName)
            .run("create table encrypted_table (id int, value string)")
            .run("insert into table encrypted_table values (1,'value1')")
            .run("insert into table encrypted_table values (2,'value2')")
            .dump(primaryDbName, dumpWithClause);

    replica
        .run("repl load " + primaryDbName + " into " + replicatedDbName
            + " with('hive.repl.add.raw.reserved.namespace'='true',"
            + "'" + HiveConf.ConfVars.REPL_EXTERNAL_TABLE_BASE_DIR.varname + "'='"
            +     replica.externalTableWarehouseRoot + "')")
        .run("use " + replicatedDbName)
        .run("repl status " + replicatedDbName)
        .verifyResult(tuple.lastReplicationId)
        .run("select value from encrypted_table")
        .verifyResults(new String[] { "value1", "value2" });
  }

  private static Configuration getNewConf() {
    Configuration conf = new Configuration();
    //TODO: HIVE-28044: Replication tests to run on Tez
    conf.set(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE.varname, "mr");
    return conf;
  }
}