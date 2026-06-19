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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConfForTest;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.shims.Utils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.hive.common.repl.ReplConst.SOURCE_OF_REPLICATION;

public class BaseReplicationAcrossInstances {
  @Rule
  public final TestName testName = new TestName();

  protected static final Logger LOG = LoggerFactory.getLogger(BaseReplicationAcrossInstances.class);
  static WarehouseInstance primary;
  static WarehouseInstance replica;
  String primaryDbName, replicatedDbName;
  static HiveConf conf; // for primary
  static HiveConf replicaConf;
  protected static final Path REPLICA_EXTERNAL_BASE = new Path("/replica_external_base");
  protected static String fullyQualifiedReplicaExternalBase;

  static void internalBeforeClassSetup(Map<String, String> overrides, Class clazz)
      throws Exception {
    conf = new HiveConfForTest(BaseReplicationAcrossInstances.class);
    //TODO: HIVE-28044: Replication tests to run on Tez
    conf.set(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE.varname, "mr");
    conf.set("dfs.client.use.datanode.hostname", "true");
    conf.set("hive.repl.cmrootdir", "/tmp/");
    conf.set("dfs.namenode.acls.enabled", "true");
    MiniDFSCluster miniDFSCluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(2).format(true).build();
    Map<String, String> localOverrides = new HashMap<String, String>() {{
      put("fs.defaultFS", miniDFSCluster.getFileSystem().getUri().toString());
      put(HiveConf.ConfVars.HIVE_IN_TEST_REPL.varname, "true");
      // Disable proxy authorization white-list for testing
      put(MetastoreConf.ConfVars.EVENT_DB_NOTIFICATION_API_AUTH.getVarname(), "false");
    }};
    localOverrides.putAll(overrides);
    setFullyQualifiedReplicaExternalTableBase(miniDFSCluster.getFileSystem());
    localOverrides.put(HiveConf.ConfVars.REPL_EXTERNAL_TABLE_BASE_DIR.varname, fullyQualifiedReplicaExternalBase);
     /* When 'hive.repl.retain.custom.db.locations.on.target' is enabled and a custom path for database is used on
       source i.e. non-default database path, on target the same path must be retained. Since in this constructor both
       source and target warehouse will be backed by same HDFS, essentially trying to create the same path on target
       would fail. Hence disabling this config to not to retain custom path on target.
     */
    localOverrides.put(HiveConf.ConfVars.REPL_RETAIN_CUSTOM_LOCATIONS_FOR_DB_ON_TARGET.varname, "false");
    primary = new WarehouseInstance(LOG, miniDFSCluster, localOverrides);
    localOverrides.put(MetastoreConf.ConfVars.REPLDIR.getHiveName(), primary.repldDir);
    replica = new WarehouseInstance(LOG, miniDFSCluster, localOverrides);
    replicaConf = conf;
  }

  static void internalBeforeClassSetupExclusiveReplica(Map<String, String> primaryOverrides,
                                                       Map<String, String> replicaOverrides, Class clazz)
          throws Exception {
    // Setup replica HDFS.
    String replicaBaseDir = Files.createTempDirectory("replica").toFile().getAbsolutePath();
    replicaConf = new HiveConfForTest(clazz);
    //TODO: HIVE-28044: Replication tests to run on Tez
    replicaConf.set(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE.varname, "mr");
    replicaConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, replicaBaseDir);
    replicaConf.set("dfs.client.use.datanode.hostname", "true");
    MiniDFSCluster miniReplicaDFSCluster =
            new MiniDFSCluster.Builder(replicaConf).numDataNodes(2).format(true).build();

    // Setup primary HDFS.
    String primaryBaseDir = Files.createTempDirectory("base").toFile().getAbsolutePath();
    conf = new HiveConfForTest(clazz);
    //TODO: HIVE-28044: Replication tests to run on Tez
    conf.set(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE.varname, "mr");
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, primaryBaseDir);
    conf.set("dfs.client.use.datanode.hostname", "true");
    MiniDFSCluster miniPrimaryDFSCluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).format(true).build();

    // Setup primary warehouse.
    setFullyQualifiedReplicaExternalTableBase(miniReplicaDFSCluster.getFileSystem());
    Map<String, String> localOverrides = new HashMap<>();
    localOverrides.put(HiveConf.ConfVars.HIVE_IN_TEST_REPL.varname, "true");
    localOverrides.put(HiveConf.ConfVars.REPL_EXTERNAL_TABLE_BASE_DIR.varname, fullyQualifiedReplicaExternalBase);
    localOverrides.put("fs.defaultFS", miniPrimaryDFSCluster.getFileSystem().getUri().toString());
    // Disable proxy authorization white-list for testing
    localOverrides.put(MetastoreConf.ConfVars.EVENT_DB_NOTIFICATION_API_AUTH.getVarname(), "false");
    localOverrides.putAll(primaryOverrides);
    primary = new WarehouseInstance(LOG, miniPrimaryDFSCluster, localOverrides);

    // Setup replica warehouse.
    localOverrides.clear();
    localOverrides.put(HiveConf.ConfVars.REPL_EXTERNAL_TABLE_BASE_DIR.varname, fullyQualifiedReplicaExternalBase);
    localOverrides.put("fs.defaultFS", miniReplicaDFSCluster.getFileSystem().getUri().toString());
    localOverrides.put(HiveConf.ConfVars.HIVE_IN_TEST_REPL.varname, "true");
    localOverrides.put(MetastoreConf.ConfVars.EVENT_DB_NOTIFICATION_API_AUTH.getVarname(), "false");
    localOverrides.putAll(replicaOverrides);
    replica = new WarehouseInstance(LOG, miniReplicaDFSCluster, localOverrides);
  }

  @AfterClass
  public static void classLevelTearDown() throws IOException {
    primary.close();
    replica.close();
    Hive.closeCurrent();
  }

  private static void setFullyQualifiedReplicaExternalTableBase(FileSystem fs) throws IOException {
    fs.mkdirs(REPLICA_EXTERNAL_BASE);
    fullyQualifiedReplicaExternalBase =  fs.getFileStatus(REPLICA_EXTERNAL_BASE).getPath().toString();
  }

  @Before
  public void setup() throws Throwable {
    primaryDbName = testName.getMethodName() + "_" + +System.currentTimeMillis();
    replicatedDbName = "replicated_" + primaryDbName;
    String mgdLocation = "/tmp/warehouse/managed/" + primaryDbName;
    String extLocation = "/tmp/warehouse/external/" + primaryDbName;
    primary.run("create database " + primaryDbName + " LOCATION '" + extLocation + "' MANAGEDLOCATION '" + mgdLocation + "' WITH DBPROPERTIES ( '" +
        SOURCE_OF_REPLICATION + "' = '1,2,3')");
  }

  @After
  public void tearDown() throws Throwable {
    primary.run("drop database if exists " + primaryDbName + " cascade");
    replica.run("drop database if exists " + replicatedDbName + " cascade");
  }

}
