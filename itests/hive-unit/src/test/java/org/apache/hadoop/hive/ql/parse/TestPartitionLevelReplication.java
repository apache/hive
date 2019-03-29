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
import org.apache.hadoop.hive.metastore.messaging.json.gzip.GzipJSONMessageEncoder;
import org.apache.hadoop.security.UserGroupInformation;

import org.apache.hadoop.hive.shims.Utils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

/**
 * TestPartitionLevelReplication - test partition level replication.
 */
public class TestPartitionLevelReplication extends BaseReplicationScenariosAcidTables {
  private static String basicFilter = " 't0' where key0 < 5 and key0 > 0 or key0 = 100, 't1*' where key1 = 1, " +
          " '(t[2]+[0-9]*)|(t3)' where key2 > 2, 't4' where key4 != 1";
  private static final String REPLICA_EXTERNAL_BASE = "/replica_external_base";

  @BeforeClass
  public static void classLevelSetup() throws Exception {
    Map<String, String> overrides = new HashMap<>();
    overrides.put(MetastoreConf.ConfVars.EVENT_MESSAGE_FACTORY.getHiveName(),
        GzipJSONMessageEncoder.class.getCanonicalName());
    overrides.put(HiveConf.ConfVars.HIVE_DISTCP_DOAS_USER.varname,
            UserGroupInformation.getCurrentUser().getUserName());

    internalBeforeClassSetup(overrides, TestPartitionLevelReplication.class);
  }

  static void internalBeforeClassSetup(Map<String, String> overrides,
      Class clazz) throws Exception {

    conf = new HiveConf(clazz);
    conf.set("dfs.client.use.datanode.hostname", "true");
    conf.set("hadoop.proxyuser." + Utils.getUGI().getShortUserName() + ".hosts", "*");
    MiniDFSCluster miniDFSCluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).build();
    Map<String, String> acidEnableConf = new HashMap<String, String>() {{
        put("fs.defaultFS", miniDFSCluster.getFileSystem().getUri().toString());
        put("hive.support.concurrency", "true");
        put("hive.txn.manager", "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager");
        put("hive.metastore.client.capability.check", "false");
        put("hive.repl.bootstrap.dump.open.txn.timeout", "1s");
        put("hive.exec.dynamic.partition.mode", "nonstrict");
        put("hive.strict.checks.bucketing", "false");
        put("hive.mapred.mode", "nonstrict");
        put("mapred.input.dir.recursive", "true");
        put("hive.metastore.disallow.incompatible.col.type.changes", "false");
        put("hive.in.repl.test", "true");
      }};

    acidEnableConf.putAll(overrides);

    primary = new WarehouseInstance(LOG, miniDFSCluster, acidEnableConf);
    replica = new WarehouseInstance(LOG, miniDFSCluster, acidEnableConf);
  }

  @Before
  public void setup() throws Throwable {
    super.setup();
  }

  @After
  public void tearDown() throws Throwable {
    primary.run("drop database if exists " + primaryDbName + " cascade");
    replica.run("drop database if exists " + replicatedDbName + " cascade");
  }

  private void createTables(String txnProperty) throws Throwable {
    if (txnProperty == null) {
      txnProperty = "'transactional'='true'";
    }
    String tableProperty = "STORED AS ORC TBLPROPERTIES ( " + txnProperty + ")";
    primary.run("use " + primaryDbName)
            .run("CREATE TABLE t0(a int) partitioned by (key0 int) " + tableProperty)
            .run("CREATE TABLE t1(a int) partitioned by (key1 int) " + tableProperty)
            .run("CREATE TABLE t13(a int) partitioned by (key1 int) " + tableProperty)
            .run("CREATE TABLE t2(a int) partitioned by (key2 int) " + tableProperty)
            .run("CREATE TABLE t23(a int) partitioned by (key2 int) " + tableProperty)
            .run("CREATE TABLE t3(a int) partitioned by (key2 int) " + tableProperty)
            .run("CREATE TABLE t4(a int) partitioned by (key4 int) " + tableProperty)
            .run("CREATE TABLE t5(a int) partitioned by (key5 int) " + tableProperty)
            .run("CREATE TABLE t6(a int) " + tableProperty);
  }

  private void insertRecords(boolean isMultiStmtTxn) throws Throwable {
    String txnStrStart = "use " + primaryDbName;
    String txnStrCommit = "use " + primaryDbName;
    if (isMultiStmtTxn) {
      txnStrStart = "START TRANSACTION";
      txnStrCommit = "COMMIT";
    }

    primary.run("use " + primaryDbName).run(txnStrStart)
    .run("INSERT INTO t0 partition (key0 = 1) values (1) ")
    .run("INSERT INTO t0 partition (key0 = 2) values (2) ")
    .run("INSERT INTO t0 partition (key0 = 3) values (3) ")
    .run("INSERT INTO t0 partition (key0 = 5) values (5) ")
    .run("INSERT INTO t0 partition (key0 = 100) values (100) ")

    .run("INSERT INTO t1 partition (key1 = 1) values (1) ")
    .run("INSERT INTO t1 partition (key1 = 2) values (2) ")
    .run("INSERT INTO t13 partition (key1 = 1) values (1) ")

    .run("INSERT INTO t2 partition (key2 = 3) values (3) ")
    .run("INSERT INTO t23 partition (key2 = 3) values (3) ")
    .run("INSERT INTO t2 partition (key2 = 2) values (2) ")
    .run("INSERT INTO t23 partition (key2 = 1) values (4) ")
    .run("INSERT INTO t23 partition (key2 = 4) values (5) ")
    .run("INSERT INTO t3 partition (key2 = 3) values (3) ")

    .run("INSERT INTO t4 partition (key4 = 1) values (3) ")
    .run("INSERT INTO t5 partition (key5 = 1) values (3) ")
    .run("INSERT INTO t6 values (3) ")
    .run(txnStrCommit);
  }

  private void verifyTableContent() throws Throwable {
    //For table t0, partition with value 5 is not satisfying the filter condition, thus not replicated.
    replica.run("use " + replicatedDbName)
            .run("SELECT a from t0 order by a")
            .verifyResults(new String[] {"1", "2", "3", "100"})

    //For t1*, both t1 and t13 are filtered
            .run("SELECT a from t1")
            .verifyResults(new String[] {"1"})
            .run("SELECT a from t13")
            .verifyResults(new String[] {"1"})

    //For [t2*,t30], t2, t23 and t3 are filtered.
            .run("SELECT a from t2")
            .verifyResults(new String[] {"3"})
            .run("SELECT a from t23")
            .verifyResults(new String[] {"3", "5"})
            .run("SELECT a from t3")
            .verifyResults(new String[] {"3"})

    //For t4, none of the partition satisfies the filter condition.
            .run("SELECT a from t4")
            .verifyResults(new String[] {})

    //t5 and t6 are not part of the filter string, thus are not filtered.
            .run("SELECT a from t5")
            .verifyResults(new String[] {"3"})
            .run("SELECT a from t6")
            .verifyResults(new String[] {"3"});
  }

  @Test
  public void testPartLevelReplictionBootstrapAcidTable() throws Throwable {
    createTables(null);
    insertRecords(false);
    WarehouseInstance.Tuple bootStrapDump =
            primary.dump(primaryDbName, null, null, null, basicFilter);
    replica.loadWithoutExplain(replicatedDbName, bootStrapDump.dumpLocation)
            .run("REPL STATUS " + replicatedDbName)
            .verifyResult(bootStrapDump.lastReplicationId);
    verifyTableContent();
  }

  @Test
  public void testPartLevelReplictionBootstrapNonAcidTable() throws Throwable {
    createTables("'transactional'='false'");
    insertRecords(false);
    WarehouseInstance.Tuple bootStrapDump =
            primary.dump(primaryDbName, null, null, null, basicFilter);
    replica.loadWithoutExplain(replicatedDbName, bootStrapDump.dumpLocation)
            .run("REPL STATUS " + replicatedDbName)
            .verifyResult(bootStrapDump.lastReplicationId);
    verifyTableContent();
  }

  @Test
  public void testPartLevelReplictionBootstrapTableFilter() throws Throwable {
    createTables(null);
    insertRecords(false);
    WarehouseInstance.Tuple bootStrapDump =
            primary.dump(primaryDbName + ".'t0|t1'", null, null,
                    null, basicFilter);
    replica.loadWithoutExplain(replicatedDbName, bootStrapDump.dumpLocation)
            .run("REPL STATUS " + replicatedDbName)
            .verifyResult(bootStrapDump.lastReplicationId)
            .run("use " + replicatedDbName)
            .run("show tables")
            .verifyResults(new String[] {"t0", "t1"})
            .run("SELECT a from t0 order by a")
            .verifyResults(new String[] {"1", "2", "3", "100"})
            .run("SELECT a from t1")
            .verifyResults(new String[] {"1"});
  }

  @Test
  public void testPartLevelReplictionBootstrapDateTypeField() throws Throwable {
    String filter = "'acid_table' where year(dt) >= 2019 and month(dt) >= 6";
    WarehouseInstance.Tuple tuple = primary.run("use " + primaryDbName)
            .run("create table acid_table (a int) partitioned by (dt string) STORED AS " +
                    " ORC TBLPROPERTIES ('transactional'='true')")
            .run("insert into acid_table partition(dt='1970-01-01') values (1)")
            .run("insert into acid_table partition(dt='2019-06-01') values (2)")
            .run("insert into acid_table partition(dt='2018-12-01') values (3)")
            .run("insert into acid_table partition(dt='2019-01-01') values (4)")
            .dump(primaryDbName, null, null, null, filter);
    replica.load(replicatedDbName, tuple.dumpLocation)
            .run("use " + replicatedDbName)
            .run("select a from acid_table")
            .verifyResults(new String[] {"2"});
  }

  @Test
  public void testPartLevelReplictionBootstrapPatterns() throws Throwable {
    String filter = "'(a[0-9]+)|(b)' where (year(dt) >= 2019 and month(dt) >= 6) or (year(dt) == 1947)";
    WarehouseInstance.Tuple tuple = primary.run("use " + primaryDbName)
            .run("create table a1 (a int) partitioned by (dt string) STORED AS " +
                    " ORC TBLPROPERTIES ('transactional'='true')")
            .run("insert into a1 partition(dt='1970-01-01') values (1)")
            .run("insert into a1 partition(dt='2019-05-01') values (2)")
            .run("insert into a1 partition(dt='2019-07-01') values (3)")
            .run("insert into a1 partition(dt='1900-08-01') values (4)")
            .run("create table b (a int) partitioned by (dt string) STORED AS " +
                    " ORC TBLPROPERTIES ('transactional'='true')")
            .run("insert into b partition(dt='1983-01-01') values (1)")
            .run("insert into b partition(dt='1947-05-01') values (2)")
            .run("insert into b partition(dt='1947-07-01') values (3)")
            .run("create table b1 (a int) partitioned by (dt string) STORED AS " +
                    " ORC TBLPROPERTIES ('transactional'='true')")
            .run("insert into b1 partition(dt='1983-01-01') values (1)")
            .run("insert into b1 partition(dt='1947-05-01') values (2)")
            .run("insert into b1 partition(dt='2019-06-01') values (3)")
            .dump(primaryDbName, null, null, null, filter);
    replica.load(replicatedDbName, tuple.dumpLocation)
            .run("use " + replicatedDbName)
            .run("select a from a1") // a1 is filtered as per 'a[0-9]+'
            .verifyResults(new String[] {"3"})
            .run("select a from b") // b is filtered as per 'b'
            .verifyResults(new String[] {"2", "3"})
            .run("select a from b1") // b1 is not filtered
            .verifyResults(new String[] {"1", "2", "3"});
  }

  @Test
  public void testPartLevelReplictionBootstrapExternalTable() throws Throwable {
    List<String> loadWithClause = ReplicationTestUtils.externalTableBasePathWithClause(REPLICA_EXTERNAL_BASE, replica);
    List<String> dumpWithClause = Collections.singletonList(
            "'" + HiveConf.ConfVars.REPL_INCLUDE_EXTERNAL_TABLES.varname + "'='true'"
    );
    String filter = "'(a[0-9]+)|(b)' where (year(dt) >= 2019 and month(dt) >= 6) or (year(dt) == 1947)";
    WarehouseInstance.Tuple tuple = primary.run("use " + primaryDbName)
            .run("create external table a1 (a int) partitioned by (dt string)")
            .run("insert into a1 partition(dt='1970-01-01') values (1)")
            .run("insert into a1 partition(dt='1947-05-01') values (2)")
            .run("insert into a1 partition(dt='2019-07-01') values (3)")
            .run("insert into a1 partition(dt='1900-08-01') values (4)")
            .dump(primaryDbName, null, null, dumpWithClause, filter);
    replica.load(replicatedDbName, tuple.dumpLocation, loadWithClause)
            .run("use " + replicatedDbName)
            .run("select a from a1")
            .verifyResults(new String[] {"2", "3"});
  }
}
