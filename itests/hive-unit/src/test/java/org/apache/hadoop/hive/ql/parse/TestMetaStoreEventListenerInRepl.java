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

import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.events.AlterDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.CreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.shims.Utils;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.AfterClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.hive.common.repl.ReplConst.SOURCE_OF_REPLICATION;

/**
 * TestMetaStoreEventListenerInRepl - Test metastore events created by replication.
 */
public class TestMetaStoreEventListenerInRepl {

  @Rule
  public final TestName testName = new TestName();

  protected static final Logger LOG = LoggerFactory.getLogger(TestMetaStoreEventListenerInRepl.class);
  static WarehouseInstance primary;
  static WarehouseInstance replica;
  static HiveConf conf;
  String primaryDbName, replicatedDbName;

  @BeforeClass
  public static void internalBeforeClassSetup() throws Exception {
    TestMetaStoreEventListenerInRepl.conf = new HiveConf(TestMetaStoreEventListenerInRepl.class);
    TestMetaStoreEventListenerInRepl.conf.set("dfs.client.use.datanode.hostname", "true");
    TestMetaStoreEventListenerInRepl.conf.set("hadoop.proxyuser." + Utils.getUGI().getShortUserName() + ".hosts", "*");
    MiniDFSCluster miniDFSCluster =
        new MiniDFSCluster.Builder(TestMetaStoreEventListenerInRepl.conf).numDataNodes(2).format(true).build();

    Map<String, String> conf = new HashMap<String, String>() {{
	      put("fs.defaultFS", miniDFSCluster.getFileSystem().getUri().toString());
	      put("hive.support.concurrency", "true");
	      put("hive.txn.manager", "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager");
	      put("hive.metastore.client.capability.check", "false");
	      put("hive.repl.bootstrap.dump.open.txn.timeout", "1s");
	      put("hive.strict.checks.bucketing", "false");
	      put("hive.mapred.mode", "nonstrict");
	      put("mapred.input.dir.recursive", "true");
	      put("hive.metastore.disallow.incompatible.col.type.changes", "false");
	      put("hive.in.repl.test", "true");
	      put(MetastoreConf.ConfVars.EVENT_LISTENERS.getVarname(),
                ReplMetaStoreEventListenerTestImpl.class.getName());
    }};

    primary = new WarehouseInstance(LOG, miniDFSCluster, conf);
    conf.put(MetastoreConf.ConfVars.REPLDIR.getHiveName(), primary.repldDir);
    replica = new WarehouseInstance(LOG, miniDFSCluster, conf);
  }

  @AfterClass
  public static void classLevelTearDown() throws IOException {
    primary.close();
    replica.close();
  }

  @Before
  public void setup() throws Throwable {
    primaryDbName = testName.getMethodName() + "_" + +System.currentTimeMillis();
    replicatedDbName = "replicated_" + primaryDbName;
    primary.run("create database " + primaryDbName + " WITH DBPROPERTIES ( '" +
            SOURCE_OF_REPLICATION + "' = '1,2,3')");
  }

  @After
  public void tearDown() throws Throwable {
    primary.run("drop database if exists " + primaryDbName + " cascade");
    replica.run("drop database if exists " + replicatedDbName + " cascade");
  }

  private Map<String, Set<String>> prepareBootstrapData(String primaryDbName) throws Throwable {
    primary.run("use " + primaryDbName)
            .run("create table t1 (id int) clustered by(id) into 3 buckets stored as orc " +
            "tblproperties (\"transactional\"=\"true\")")
            .run("insert into t1 values(1)")
            .run("create table t2 (place string) partitioned by (country string) clustered by(place) " +
                    "into 3 buckets stored as orc tblproperties (\"transactional\"=\"true\")")
            .run("insert into t2 partition(country='india') values ('bangalore')")
            .run("create table t4 (id int)")
            .run("insert into t4 values(111), (222)");

    // Add expected events with associated tables, if any.
    Map<String, Set<String>> eventsMap = new HashMap<>();
    eventsMap.put(CreateDatabaseEvent.class.getName(), null);
    eventsMap.put(CreateTableEvent.class.getName(), new HashSet<>(Arrays.asList("t1", "t2", "t4")));
    eventsMap.put(AlterTableEvent.class.getName(), new HashSet<>(Arrays.asList("t1", "t2", "t4")));
    return eventsMap;
  }

  Map<String, Set<String>> prepareIncData(String dbName) throws Throwable {
    primary.run("use " + dbName)
            .run("create table t6 stored as orc tblproperties (\"transactional\"=\"true\")" +
                    " as select * from t1")
            .run("alter table t2 add columns (placetype string)")
            .run("update t2 set placetype = 'city'")
            .run("insert into t1 values (3)")
            .run("drop table t2")
            .run("alter database " + dbName + " set dbproperties('some.useless.property'='1')");

    // Add expected events with associated tables, if any.
    Map<String, Set<String>> eventsMap = new HashMap<>();
    eventsMap.put(CreateTableEvent.class.getName(), new HashSet<>(Arrays.asList("t6")));
    eventsMap.put(AlterTableEvent.class.getName(), new HashSet<>(Arrays.asList("t1", "t2", "t6")));
    eventsMap.put(DropTableEvent.class.getName(), new HashSet<>(Arrays.asList("t2")));

    return eventsMap;
  }

  Map<String, Set<String>> prepareInc2Data(String dbName) throws Throwable {
    primary.run("use " + dbName)
            .run("insert into t4 values (333)")
            .run("create table t7 (str string)")
            .run("insert into t7 values ('aaa')")
            .run("drop table t1");
    // Add expected events with associated tables, if any.
    Map<String, Set<String>> eventsMap = new HashMap<>();
    eventsMap.put(CreateTableEvent.class.getName(), new HashSet<>(Arrays.asList("t7")));
    eventsMap.put(AlterTableEvent.class.getName(), new HashSet<>(Arrays.asList("t4", "t7")));
    eventsMap.put(DropTableEvent.class.getName(), new HashSet<>(Arrays.asList("t1")));

    return eventsMap;
  }

  @Test
  public void testReplEvents() throws Throwable {
    Map<String, Set<String>> eventsMap = prepareBootstrapData(primaryDbName);
    primary.run("use " + primaryDbName)
            .dump(primaryDbName);
    replica.load(replicatedDbName, primaryDbName);
    ReplMetaStoreEventListenerTestImpl.checkEventSanity(eventsMap, replicatedDbName);
    ReplMetaStoreEventListenerTestImpl.clearSanityData();

    eventsMap = prepareIncData(primaryDbName);
    LOG.info(testName.getMethodName() + ": first incremental dump and load.");
    WarehouseInstance.Tuple incre = primary.run("use " + primaryDbName)
            .dump(primaryDbName);
    replica.load(replicatedDbName, primaryDbName);
    ReplMetaStoreEventListenerTestImpl.checkEventSanity(eventsMap, replicatedDbName);
    ReplMetaStoreEventListenerTestImpl.clearSanityData();

    // Second incremental, after bootstrap
    eventsMap = prepareInc2Data(primaryDbName);
    LOG.info(testName.getMethodName() + ": second incremental dump and load.");
    primary.run("use " + primaryDbName)
            .dump(primaryDbName);
    replica.load(replicatedDbName, primaryDbName);
    ReplMetaStoreEventListenerTestImpl.checkEventSanity(eventsMap, replicatedDbName);
    ReplMetaStoreEventListenerTestImpl.clearSanityData();
  }
}
