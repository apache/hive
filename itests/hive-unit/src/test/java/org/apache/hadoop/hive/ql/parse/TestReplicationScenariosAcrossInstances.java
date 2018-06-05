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

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.repl.ReplUtils;
import org.apache.hadoop.hive.ql.parse.repl.PathBuilder;
import org.apache.hadoop.hive.ql.util.DependencyResolver;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.apache.hadoop.hive.metastore.ReplChangeManager.SOURCE_OF_REPLICATION;

public class TestReplicationScenariosAcrossInstances {
  @Rule
  public final TestName testName = new TestName();

  @Rule
  public TestRule replV1BackwardCompat;

  protected static final Logger LOG = LoggerFactory.getLogger(TestReplicationScenarios.class);
  private static WarehouseInstance primary, replica;
  private String primaryDbName, replicatedDbName;

  @BeforeClass
  public static void classLevelSetup() throws Exception {
    Configuration conf = new Configuration();
    conf.set("dfs.client.use.datanode.hostname", "true");
    conf.set("hadoop.proxyuser." + Utils.getUGI().getShortUserName() + ".hosts", "*");
    MiniDFSCluster miniDFSCluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).build();
    HashMap<String, String> overridesForHiveConf = new HashMap<String, String>() {{
      put("fs.defaultFS", miniDFSCluster.getFileSystem().getUri().toString());
      put(HiveConf.ConfVars.HIVE_IN_TEST_REPL.varname, "true");
    }};
    primary = new WarehouseInstance(LOG, miniDFSCluster, overridesForHiveConf);
    replica = new WarehouseInstance(LOG, miniDFSCluster, overridesForHiveConf);
  }

  @AfterClass
  public static void classLevelTearDown() throws IOException {
    primary.close();
    replica.close();
  }

  @Before
  public void setup() throws Throwable {
    replV1BackwardCompat = primary.getReplivationV1CompatRule(new ArrayList<>());
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

  @Test
  public void testCreateFunctionIncrementalReplication() throws Throwable {
    WarehouseInstance.Tuple bootStrapDump = primary.dump(primaryDbName, null);
    replica.load(replicatedDbName, bootStrapDump.dumpLocation)
        .run("REPL STATUS " + replicatedDbName)
        .verifyResult(bootStrapDump.lastReplicationId);

    primary.run("CREATE FUNCTION " + primaryDbName
        + ".testFunctionOne as 'hivemall.tools.string.StopwordUDF' "
        + "using jar  'ivy://io.github.myui:hivemall:0.4.0-2'");

    WarehouseInstance.Tuple incrementalDump =
        primary.dump(primaryDbName, bootStrapDump.lastReplicationId);
    replica.load(replicatedDbName, incrementalDump.dumpLocation)
        .run("REPL STATUS " + replicatedDbName)
        .verifyResult(incrementalDump.lastReplicationId)
        .run("SHOW FUNCTIONS LIKE '" + replicatedDbName + "*'")
        .verifyResult(replicatedDbName + ".testFunctionOne");

    // Test the idempotent behavior of CREATE FUNCTION
    replica.load(replicatedDbName, incrementalDump.dumpLocation)
        .run("REPL STATUS " + replicatedDbName)
        .verifyResult(incrementalDump.lastReplicationId)
        .run("SHOW FUNCTIONS LIKE '" + replicatedDbName + "*'")
        .verifyResult(replicatedDbName + ".testFunctionOne");
  }

  @Test
  public void testDropFunctionIncrementalReplication() throws Throwable {
    primary.run("CREATE FUNCTION " + primaryDbName
        + ".testFunctionAnother as 'hivemall.tools.string.StopwordUDF' "
        + "using jar  'ivy://io.github.myui:hivemall:0.4.0-2'");
    WarehouseInstance.Tuple bootStrapDump = primary.dump(primaryDbName, null);
    replica.load(replicatedDbName, bootStrapDump.dumpLocation)
        .run("REPL STATUS " + replicatedDbName)
        .verifyResult(bootStrapDump.lastReplicationId);

    primary.run("Drop FUNCTION " + primaryDbName + ".testFunctionAnother ");

    WarehouseInstance.Tuple incrementalDump =
        primary.dump(primaryDbName, bootStrapDump.lastReplicationId);
    replica.load(replicatedDbName, incrementalDump.dumpLocation)
        .run("REPL STATUS " + replicatedDbName)
        .verifyResult(incrementalDump.lastReplicationId)
        .run("SHOW FUNCTIONS LIKE '*testfunctionanother*'")
        .verifyResult(null);

    // Test the idempotent behavior of DROP FUNCTION
    replica.load(replicatedDbName, incrementalDump.dumpLocation)
        .run("REPL STATUS " + replicatedDbName)
        .verifyResult(incrementalDump.lastReplicationId)
        .run("SHOW FUNCTIONS LIKE '*testfunctionanother*'")
        .verifyResult(null);
  }

  @Test
  public void testBootstrapFunctionReplication() throws Throwable {
    primary.run("CREATE FUNCTION " + primaryDbName
        + ".testFunction as 'hivemall.tools.string.StopwordUDF' "
        + "using jar  'ivy://io.github.myui:hivemall:0.4.0-2'");
    WarehouseInstance.Tuple bootStrapDump = primary.dump(primaryDbName, null);

    replica.load(replicatedDbName, bootStrapDump.dumpLocation)
        .run("SHOW FUNCTIONS LIKE '" + replicatedDbName + "*'")
        .verifyResult(replicatedDbName + ".testFunction");
  }

  @Test
  public void testCreateFunctionWithFunctionBinaryJarsOnHDFS() throws Throwable {
    Dependencies dependencies = dependencies("ivy://io.github.myui:hivemall:0.4.0-2", primary);
    String jarSubString = dependencies.toJarSubSql();

    primary.run("CREATE FUNCTION " + primaryDbName
        + ".anotherFunction as 'hivemall.tools.string.StopwordUDF' "
        + "using " + jarSubString);

    WarehouseInstance.Tuple tuple = primary.dump(primaryDbName, null);

    replica.load(replicatedDbName, tuple.dumpLocation)
        .run("SHOW FUNCTIONS LIKE '" + replicatedDbName + "*'")
        .verifyResult(replicatedDbName + ".anotherFunction");

    FileStatus[] fileStatuses = replica.miniDFSCluster.getFileSystem().globStatus(
        new Path(
            replica.functionsRoot + "/" + replicatedDbName.toLowerCase() + "/anotherfunction/*/*")
        , path -> path.toString().endsWith("jar"));
    List<String> expectedDependenciesNames = dependencies.jarNames();
    assertThat(fileStatuses.length, is(equalTo(expectedDependenciesNames.size())));
    List<String> jars = Arrays.stream(fileStatuses).map(f -> {
      String[] splits = f.getPath().toString().split("/");
      return splits[splits.length - 1];
    }).collect(Collectors.toList());

    assertThat(jars, containsInAnyOrder(expectedDependenciesNames.toArray()));
  }

  static class Dependencies {
    private final List<Path> fullQualifiedJarPaths;

    Dependencies(List<Path> fullQualifiedJarPaths) {
      this.fullQualifiedJarPaths = fullQualifiedJarPaths;
    }

    private String toJarSubSql() {
      return StringUtils.join(
          fullQualifiedJarPaths.stream().map(p -> "jar '" + p + "'").collect(Collectors.toList()),
          ","
      );
    }

    private List<String> jarNames() {
      return fullQualifiedJarPaths.stream().map(p -> {
        String[] splits = p.toString().split("/");
        return splits[splits.length - 1];
      }).collect(Collectors.toList());
    }
  }

  private Dependencies dependencies(String ivyPath, WarehouseInstance onWarehouse)
      throws IOException, URISyntaxException, SemanticException {
    List<URI> localUris = new DependencyResolver().downloadDependencies(new URI(ivyPath));
    List<Path> remotePaths = onWarehouse.copyToHDFS(localUris);
    List<Path> collect =
        remotePaths.stream().map(r -> {
          try {
            return PathBuilder
                .fullyQualifiedHDFSUri(r, onWarehouse.miniDFSCluster.getFileSystem());

          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }).collect(Collectors.toList());
    return new Dependencies(collect);
  }

  /*
  From the hive logs(hive.log) we can also check for the info statement
  fgrep "Total Tasks" [location of hive.log]
  each line indicates one run of loadTask.
   */
  @Test
  public void testMultipleStagesOfReplicationLoadTask() throws Throwable {
    WarehouseInstance.Tuple tuple = primary
        .run("use " + primaryDbName)
        .run("create table t1 (id int)")
        .run("create table t2 (place string) partitioned by (country string)")
        .run("insert into table t2 partition(country='india') values ('bangalore')")
        .run("insert into table t2 partition(country='us') values ('austin')")
        .run("insert into table t2 partition(country='france') values ('paris')")
        .run("create table t3 (rank int)")
        .dump(primaryDbName, null);

    // each table creation itself takes more than one task, give we are giving a max of 1, we should hit multiple runs.
    replica.hiveConf.setIntVar(HiveConf.ConfVars.REPL_APPROX_MAX_LOAD_TASKS, 1);
    replica.load(replicatedDbName, tuple.dumpLocation)
        .run("use " + replicatedDbName)
        .run("show tables")
        .verifyResults(new String[] { "t1", "t2", "t3" })
        .run("repl status " + replicatedDbName)
        .verifyResult(tuple.lastReplicationId)
        .run("select country from t2 order by country")
        .verifyResults(new String[] { "france", "india", "us" });
  }

  @Test
  public void testParallelExecutionOfReplicationBootStrapLoad() throws Throwable {
    WarehouseInstance.Tuple tuple = primary
        .run("use " + primaryDbName)
        .run("create table t1 (id int)")
        .run("create table t2 (place string) partitioned by (country string)")
        .run("insert into table t2 partition(country='india') values ('bangalore')")
        .run("insert into table t2 partition(country='australia') values ('sydney')")
        .run("insert into table t2 partition(country='russia') values ('moscow')")
        .run("insert into table t2 partition(country='uk') values ('london')")
        .run("insert into table t2 partition(country='us') values ('sfo')")
        .run("insert into table t2 partition(country='france') values ('paris')")
        .run("insert into table t2 partition(country='japan') values ('tokyo')")
        .run("insert into table t2 partition(country='china') values ('hkg')")
        .run("create table t3 (rank int)")
        .dump(primaryDbName, null);

    replica.hiveConf.setBoolVar(HiveConf.ConfVars.EXECPARALLEL, true);
    replica.load(replicatedDbName, tuple.dumpLocation)
        .run("use " + replicatedDbName)
        .run("repl status " + replicatedDbName)
        .verifyResult(tuple.lastReplicationId)
        .run("show tables")
        .verifyResults(new String[] { "t1", "t2", "t3" })
        .run("select country from t2")
        .verifyResults(Arrays.asList("india", "australia", "russia", "uk", "us", "france", "japan",
            "china"));
    replica.hiveConf.setBoolVar(HiveConf.ConfVars.EXECPARALLEL, false);
  }

  @Test
  public void testMetadataBootstrapDump() throws Throwable {
    WarehouseInstance.Tuple tuple = primary.run("use " + primaryDbName)
        .run("create table  acid_table (key int, value int) partitioned by (load_date date) " +
            "clustered by(key) into 2 buckets stored as orc tblproperties ('transactional'='true')")
        .run("create table table1 (i int, j int)")
        .run("insert into table1 values (1,2)")
        .dump(primaryDbName, null, Arrays.asList("'hive.repl.dump.metadata.only'='true'",
            "'hive.repl.dump.include.acid.tables'='true'"));

    replica.load(replicatedDbName, tuple.dumpLocation)
        .run("use " + replicatedDbName)
        .run("show tables")
        .verifyResults(new String[] { "acid_table", "table1" })
        .run("select * from table1")
        .verifyResults(Collections.emptyList());
  }

  @Test
  public void testIncrementalMetadataReplication() throws Throwable {
    ////////////  Bootstrap   ////////////
    WarehouseInstance.Tuple bootstrapTuple = primary
        .run("use " + primaryDbName)
        .run("create table table1 (i int, j int)")
        .run("create table table2 (a int, city string) partitioned by (country string)")
        .run("create table table3 (i int, j int)")
        .run("insert into table1 values (1,2)")
        .dump(primaryDbName, null, Arrays.asList("'hive.repl.dump.metadata.only'='true'",
            "'hive.repl.dump.include.acid.tables'='true'"));

    replica.load(replicatedDbName, bootstrapTuple.dumpLocation)
        .run("use " + replicatedDbName)
        .run("show tables")
        .verifyResults(new String[] { "table1", "table2", "table3" })
        .run("select * from table1")
        .verifyResults(Collections.emptyList());

    ////////////  First Incremental ////////////
    WarehouseInstance.Tuple incrementalOneTuple =
        primary
            .run("use " + primaryDbName)
            .run("alter table table1 rename to renamed_table1")
            .run("insert into table2 partition(country='india') values (1,'mumbai') ")
            .run("create table table4 (i int, j int)")
            .dump(
                "repl dump " + primaryDbName + " from " + bootstrapTuple.lastReplicationId + " to "
                    + Long.parseLong(bootstrapTuple.lastReplicationId) + 100L + " limit 100 "
                    + "with ('hive.repl.dump.metadata.only'='true')"
            );

    replica.load(replicatedDbName, incrementalOneTuple.dumpLocation)
        .run("use " + replicatedDbName)
        .run("show tables")
        .verifyResults(new String[] { "renamed_table1", "table2", "table3", "table4" })
        .run("select * from renamed_table1")
        .verifyResults(Collections.emptyList())
        .run("select * from table2")
        .verifyResults(Collections.emptyList());

    ////////////  Second Incremental ////////////
    WarehouseInstance.Tuple secondIncremental = primary
        .run("alter table table2 add columns (zipcode int)")
        .run("alter table table3 change i a string")
        .run("alter table table3 set tblproperties('custom.property'='custom.value')")
        .run("drop table renamed_table1")
        .dump("repl dump " + primaryDbName + " from " + incrementalOneTuple.lastReplicationId
            + " with ('hive.repl.dump.metadata.only'='true')"
        );

    replica.load(replicatedDbName, secondIncremental.dumpLocation)
        .run("use " + replicatedDbName)
        .run("show tables")
        .verifyResults(new String[] { "table2", "table3", "table4" })
        .run("desc table3")
        .verifyResults(new String[] {
            "a                   \tstring              \t                    ",
            "j                   \tint                 \t                    "
        })
        .run("desc table2")
        .verifyResults(new String[] {
            "a                   \tint                 \t                    ",
            "city                \tstring              \t                    ",
            "country             \tstring              \t                    ",
            "zipcode             \tint                 \t                    ",
            "\t \t ",
            "# Partition Information\t \t ",
            "# col_name            \tdata_type           \tcomment             ",
            "country             \tstring              \t                    ",
        })
        .run("show tblproperties table3('custom.property')")
        .verifyResults(new String[] {
            "custom.value\t "
        });
  }

  @Test
  public void testBootStrapDumpOfWarehouse() throws Throwable {
    String randomOne = RandomStringUtils.random(10, true, false);
    String randomTwo = RandomStringUtils.random(10, true, false);
    String dbOne = primaryDbName + randomOne;
    String dbTwo = primaryDbName + randomTwo;
    primary.run("alter database default set dbproperties ('repl.source.for' = '1, 2, 3')");
    WarehouseInstance.Tuple tuple = primary
        .run("use " + primaryDbName)
        .run("create table t1 (i int, j int)")
        .run("create database " + dbOne + " WITH DBPROPERTIES ( '" +
                SOURCE_OF_REPLICATION + "' = '1,2,3')")
        .run("use " + dbOne)
    // TODO: this is wrong; this test sets up dummy txn manager and so it cannot create ACID tables.
    //       This used to work by accident, now this works due a test flag. The test needs to be fixed.
    //       Also applies for a couple more tests.
        .run("create table t1 (i int, j int) partitioned by (load_date date) "
            + "clustered by(i) into 2 buckets stored as orc tblproperties ('transactional'='true') ")
        .run("create database " + dbTwo + " WITH DBPROPERTIES ( '" +
                SOURCE_OF_REPLICATION + "' = '1,2,3')")
        .run("use " + dbTwo)
        .run("create table t1 (i int, j int)")
        .dump("`*`", null, Arrays.asList("'hive.repl.dump.metadata.only'='true'",
            "'hive.repl.dump.include.acid.tables'='true'"));

    /*
      Due to the limitation that we can only have one instance of Persistence Manager Factory in a JVM
      we are not able to create multiple embedded derby instances for two different MetaStore instances.
    */

    primary.run("drop database " + primaryDbName + " cascade");
    primary.run("drop database " + dbOne + " cascade");
    primary.run("drop database " + dbTwo + " cascade");

    /*
       End of additional steps
    */

    replica.run("show databases")
        .verifyFailure(new String[] { primaryDbName, dbOne, dbTwo })
        .load("", tuple.dumpLocation)
        .run("show databases")
        .verifyResults(new String[] { "default", primaryDbName, dbOne, dbTwo })
        .run("use " + primaryDbName)
        .run("show tables")
        .verifyResults(new String[] { "t1" })
        .run("use " + dbOne)
        .run("show tables")
        .verifyResults(new String[] { "t1" })
        .run("use " + dbTwo)
        .run("show tables")
        .verifyResults(new String[] { "t1" });
    /*
      Start of cleanup
    */

    replica.run("drop database " + primaryDbName + " cascade");
    replica.run("drop database " + dbOne + " cascade");
    replica.run("drop database " + dbTwo + " cascade");

    /*
       End of cleanup
    */
  }

  @Test
  public void testIncrementalDumpOfWarehouse() throws Throwable {
    String randomOne = RandomStringUtils.random(10, true, false);
    String randomTwo = RandomStringUtils.random(10, true, false);
    String dbOne = primaryDbName + randomOne;
    primary.run("alter database default set dbproperties ('repl.source.for' = '1, 2, 3')");
    WarehouseInstance.Tuple bootstrapTuple = primary
        .run("use " + primaryDbName)
        .run("create table t1 (i int, j int)")
        .run("create database " + dbOne + " WITH DBPROPERTIES ( '" +
                SOURCE_OF_REPLICATION + "' = '1,2,3')")
        .run("use " + dbOne)
        .run("create table t1 (i int, j int) partitioned by (load_date date) "
            + "clustered by(i) into 2 buckets stored as orc tblproperties ('transactional'='true') ")
        .dump("`*`", null, Arrays.asList("'hive.repl.dump.metadata.only'='true'",
            "'hive.repl.dump.include.acid.tables'='true'"));

    String dbTwo = primaryDbName + randomTwo;
    WarehouseInstance.Tuple incrementalTuple = primary
        .run("create database " + dbTwo + " WITH DBPROPERTIES ( '" +
                SOURCE_OF_REPLICATION + "' = '1,2,3')")
        .run("use " + dbTwo)
        .run("create table t1 (i int, j int)")
        .run("use " + dbOne)
        .run("create table t2 (a int, b int)")
        .dump("`*`", bootstrapTuple.lastReplicationId,
            Arrays.asList("'hive.repl.dump.metadata.only'='true'",
                "'hive.repl.dump.include.acid.tables'='true'"));

    /*
      Due to the limitation that we can only have one instance of Persistence Manager Factory in a JVM
      we are not able to create multiple embedded derby instances for two different MetaStore instances.
    */

    primary.run("drop database " + primaryDbName + " cascade");
    primary.run("drop database " + dbOne + " cascade");
    primary.run("drop database " + dbTwo + " cascade");

    /*
      End of additional steps
    */

    replica.run("show databases")
        .verifyFailure(new String[] { primaryDbName, dbOne, dbTwo })
        .load("", bootstrapTuple.dumpLocation)
        .run("show databases")
        .verifyResults(new String[] { "default", primaryDbName, dbOne })
        .run("use " + primaryDbName)
        .run("show tables")
        .verifyResults(new String[] { "t1" })
        .run("use " + dbOne)
        .run("show tables")
        .verifyResults(new String[] { "t1" });

    replica.load("", incrementalTuple.dumpLocation)
        .run("show databases")
        .verifyResults(new String[] { "default", primaryDbName, dbOne, dbTwo })
        .run("use " + dbTwo)
        .run("show tables")
        .verifyResults(new String[] { "t1" })
        .run("use " + dbOne)
        .run("show tables")
        .verifyResults(new String[] { "t1", "t2" });

    /*
       Start of cleanup
    */

    replica.run("drop database " + primaryDbName + " cascade");
    replica.run("drop database " + dbOne + " cascade");
    replica.run("drop database " + dbTwo + " cascade");

    /*
       End of cleanup
    */

  }

  @Test
  public void testReplLoadFromSourceUsingWithClause() throws Throwable {
    HiveConf replicaConf = replica.getConf();
    List<String> withConfigs = Arrays.asList(
            "'hive.metastore.warehouse.dir'='" + replicaConf.getVar(HiveConf.ConfVars.METASTOREWAREHOUSE) + "'",
            "'hive.metastore.uris'='" + replicaConf.getVar(HiveConf.ConfVars.METASTOREURIS) + "'",
            "'hive.repl.replica.functions.root.dir'='" + replicaConf.getVar(HiveConf.ConfVars.REPL_FUNCTIONS_ROOT_DIR) + "'");

    ////////////  Bootstrap   ////////////
    WarehouseInstance.Tuple bootstrapTuple = primary
            .run("use " + primaryDbName)
            .run("create table table1 (i int)")
            .run("create table table2 (id int) partitioned by (country string)")
            .run("insert into table1 values (1)")
            .dump(primaryDbName, null);

    // Run load on primary itself
    primary.load(replicatedDbName, bootstrapTuple.dumpLocation, withConfigs)
            .status(replicatedDbName, withConfigs)
            .verifyResult(bootstrapTuple.lastReplicationId);

    replica.run("use " + replicatedDbName)
            .run("show tables")
            .verifyResults(new String[] { "table1", "table2" })
            .run("select * from table1")
            .verifyResults(new String[]{ "1" });

    ////////////  First Incremental ////////////
    WarehouseInstance.Tuple incrementalOneTuple = primary
                    .run("use " + primaryDbName)
                    .run("alter table table1 rename to renamed_table1")
                    .run("insert into table2 partition(country='india') values (1) ")
                    .run("insert into table2 partition(country='usa') values (2) ")
                    .run("create table table3 (i int)")
                    .run("insert into table3 values(10)")
                    .run("create function " + primaryDbName
                      + ".testFunctionOne as 'hivemall.tools.string.StopwordUDF' "
                      + "using jar  'ivy://io.github.myui:hivemall:0.4.0-2'")
                    .dump(primaryDbName, bootstrapTuple.lastReplicationId);

    // Run load on primary itself
    primary.load(replicatedDbName, incrementalOneTuple.dumpLocation, withConfigs)
            .status(replicatedDbName, withConfigs)
            .verifyResult(incrementalOneTuple.lastReplicationId);

    replica.run("use " + replicatedDbName)
            .run("show tables")
            .verifyResults(new String[] { "renamed_table1", "table2", "table3" })
            .run("select * from renamed_table1")
            .verifyResults(new String[] { "1" })
            .run("select id from table2 order by id")
            .verifyResults(new String[] { "1", "2" })
            .run("select * from table3")
            .verifyResults(new String[] { "10" })
            .run("show functions like '" + replicatedDbName + "*'")
            .verifyResult(replicatedDbName + ".testFunctionOne");

    ////////////  Second Incremental ////////////
    WarehouseInstance.Tuple secondIncremental = primary
            .run("use " + primaryDbName)
            .run("alter table table2 add columns (zipcode int)")
            .run("alter table table3 set tblproperties('custom.property'='custom.value')")
            .run("drop table renamed_table1")
            .run("alter table table2 drop partition(country='usa')")
            .run("truncate table table3")
            .run("drop function " + primaryDbName + ".testFunctionOne ")
            .dump(primaryDbName, incrementalOneTuple.lastReplicationId);

    // Run load on primary itself
    primary.load(replicatedDbName, secondIncremental.dumpLocation, withConfigs)
            .status(replicatedDbName, withConfigs)
            .verifyResult(secondIncremental.lastReplicationId);

    replica.run("use " + replicatedDbName)
            .run("show tables")
            .verifyResults(new String[] { "table2", "table3"})
            .run("desc table2")
            .verifyResults(new String[] {
                    "id                  \tint                 \t                    ",
                    "country             \tstring              \t                    ",
                    "zipcode             \tint                 \t                    ",
                    "\t \t ",
                    "# Partition Information\t \t ",
                    "# col_name            \tdata_type           \tcomment             ",
                    "country             \tstring              \t                    ",
            })
            .run("show tblproperties table3('custom.property')")
            .verifyResults(new String[] { "custom.value\t " })
            .run("select id from table2 order by id")
            .verifyResults(new String[] { "1" })
            .run("select * from table3")
            .verifyResults(Collections.emptyList())
            .run("show functions like '" + replicatedDbName + "*'")
            .verifyResult(null);
  }

  @Test
  public void testIncrementalReplWithEventsBatchHavingDropCreateTable() throws Throwable {
    // Bootstrap dump with empty db
    WarehouseInstance.Tuple bootstrapTuple = primary.dump(primaryDbName, null);

    // Bootstrap load in replica
    replica.load(replicatedDbName, bootstrapTuple.dumpLocation)
            .status(replicatedDbName)
            .verifyResult(bootstrapTuple.lastReplicationId);

    // First incremental dump
    WarehouseInstance.Tuple firstIncremental = primary.run("use " + primaryDbName)
            .run("create table table1 (i int)")
            .run("create table table2 (id int) partitioned by (country string)")
            .run("insert into table1 values (1)")
            .run("insert into table2 partition(country='india') values(1)")
            .dump(primaryDbName, bootstrapTuple.lastReplicationId);

    // Second incremental dump
    WarehouseInstance.Tuple secondIncremental = primary.run("use " + primaryDbName)
            .run("drop table table1")
            .run("drop table table2")
            .run("create table table2 (id int) partitioned by (country string)")
            .run("alter table table2 add partition(country='india')")
            .run("alter table table2 drop partition(country='india')")
            .run("insert into table2 partition(country='us') values(2)")
            .run("create table table1 (i int)")
            .run("insert into table1 values (2)")
            .dump(primaryDbName, firstIncremental.lastReplicationId);

    // First incremental load
    replica.load(replicatedDbName, firstIncremental.dumpLocation)
            .status(replicatedDbName)
            .verifyResult(firstIncremental.lastReplicationId)
            .run("use " + replicatedDbName)
            .run("show tables")
            .verifyResults(new String[] {"table1", "table2"})
            .run("select * from table1")
            .verifyResults(new String[] {"1"})
            .run("select id from table2 order by id")
            .verifyResults(new String[] {"1"});

    // Second incremental load
    replica.load(replicatedDbName, secondIncremental.dumpLocation)
            .status(replicatedDbName)
            .verifyResult(secondIncremental.lastReplicationId)
            .run("use " + replicatedDbName)
            .run("show tables")
            .verifyResults(new String[] {"table1", "table2"})
            .run("select * from table1")
            .verifyResults(new String[] {"2"})
            .run("select id from table2 order by id")
            .verifyResults(new String[] {"2"});
  }

  @Test
  public void testIncrementalReplWithDropAndCreateTableDifferentPartitionTypeAndInsert() throws Throwable {
    // Bootstrap dump with empty db
    WarehouseInstance.Tuple bootstrapTuple = primary.dump(primaryDbName, null);

    // Bootstrap load in replica
    replica.load(replicatedDbName, bootstrapTuple.dumpLocation)
            .status(replicatedDbName)
            .verifyResult(bootstrapTuple.lastReplicationId);

    // First incremental dump
    WarehouseInstance.Tuple firstIncremental = primary.run("use " + primaryDbName)
            .run("create table table1 (id int) partitioned by (country string)")
            .run("create table table2 (id int)")
            .run("create table table3 (id int) partitioned by (country string)")
            .run("insert into table1 partition(country='india') values(1)")
            .run("insert into table2 values(2)")
            .run("insert into table3 partition(country='india') values(3)")
            .dump(primaryDbName, bootstrapTuple.lastReplicationId);

    // Second incremental dump
    WarehouseInstance.Tuple secondIncremental = primary.run("use " + primaryDbName)
            .run("drop table table1")
            .run("drop table table2")
            .run("drop table table3")
            .run("create table table1 (id int)")
            .run("insert into table1 values (10)")
            .run("create table table2 (id int) partitioned by (country string)")
            .run("insert into table2 partition(country='india') values(20)")
            .run("create table table3 (id int) partitioned by (name string, rank int)")
            .run("insert into table3 partition(name='adam', rank=100) values(30)")
            .dump(primaryDbName, firstIncremental.lastReplicationId);

    // First incremental load
    replica.load(replicatedDbName, firstIncremental.dumpLocation)
            .status(replicatedDbName)
            .verifyResult(firstIncremental.lastReplicationId)
            .run("use " + replicatedDbName)
            .run("select id from table1")
            .verifyResults(new String[] {"1"})
            .run("select * from table2")
            .verifyResults(new String[] {"2"})
            .run("select id from table3")
            .verifyResults(new String[] {"3"});

    // Second incremental load
    replica.load(replicatedDbName, secondIncremental.dumpLocation)
            .status(replicatedDbName)
            .verifyResult(secondIncremental.lastReplicationId)
            .run("use " + replicatedDbName)
            .run("select * from table1")
            .verifyResults(new String[] {"10"})
            .run("select id from table2")
            .verifyResults(new String[] {"20"})
            .run("select id from table3")
            .verifyResults(new String[] {"30"});
  }

  private void verifyIfCkptSet(Map<String, String> props, String dumpDir) {
    assertTrue(props.containsKey(ReplUtils.REPL_CHECKPOINT_KEY));
    assertTrue(props.get(ReplUtils.REPL_CHECKPOINT_KEY).equals(dumpDir));
  }

  @Test
  public void testIfCkptSetForObjectsByBootstrapReplLoad() throws Throwable {
    WarehouseInstance.Tuple tuple = primary
            .run("use " + primaryDbName)
            .run("create table t1 (id int)")
            .run("insert into table t1 values (10)")
            .run("create table t2 (place string) partitioned by (country string)")
            .run("insert into table t2 partition(country='india') values ('bangalore')")
            .run("insert into table t2 partition(country='uk') values ('london')")
            .run("insert into table t2 partition(country='us') values ('sfo')")
            .dump(primaryDbName, null);

    replica.load(replicatedDbName, tuple.dumpLocation)
            .run("use " + replicatedDbName)
            .run("repl status " + replicatedDbName)
            .verifyResult(tuple.lastReplicationId)
            .run("show tables")
            .verifyResults(new String[] { "t1", "t2" })
            .run("select country from t2")
            .verifyResults(Arrays.asList("india", "uk", "us"));

    Database db = replica.getDatabase(replicatedDbName);
    verifyIfCkptSet(db.getParameters(), tuple.dumpLocation);
    Table t1 = replica.getTable(replicatedDbName, "t1");
    verifyIfCkptSet(t1.getParameters(), tuple.dumpLocation);
    Table t2 = replica.getTable(replicatedDbName, "t2");
    verifyIfCkptSet(t2.getParameters(), tuple.dumpLocation);
    Partition india = replica.getPartition(replicatedDbName, "t2", Collections.singletonList("india"));
    verifyIfCkptSet(india.getParameters(), tuple.dumpLocation);
    Partition us = replica.getPartition(replicatedDbName, "t2", Collections.singletonList("us"));
    verifyIfCkptSet(us.getParameters(), tuple.dumpLocation);
    Partition uk = replica.getPartition(replicatedDbName, "t2", Collections.singletonList("uk"));
    verifyIfCkptSet(uk.getParameters(), tuple.dumpLocation);
  }

  @Test
  public void shouldNotCreateDirectoryForNonNativeTableInDumpDirectory() throws Throwable {
    String createTableQuery =
        "CREATE TABLE custom_serdes( serde_id bigint COMMENT 'from deserializer', name string "
            + "COMMENT 'from deserializer', slib string COMMENT 'from deserializer') "
            + "ROW FORMAT SERDE 'org.apache.hive.storage.jdbc.JdbcSerDe' "
            + "STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler' "
            + "WITH SERDEPROPERTIES ('serialization.format'='1') "
            + "TBLPROPERTIES ( "
            + "'hive.sql.database.type'='METASTORE', "
            + "'hive.sql.query'='SELECT \"SERDE_ID\", \"NAME\", \"SLIB\" FROM \"SERDES\"')";

    WarehouseInstance.Tuple bootstrapTuple = primary
        .run("use " + primaryDbName)
        .run(createTableQuery).dump(primaryDbName, null);
    Path cSerdesTableDumpLocation = new Path(
        new Path(bootstrapTuple.dumpLocation, primaryDbName),
        "custom_serdes");
    FileSystem fs = cSerdesTableDumpLocation.getFileSystem(primary.hiveConf);
    assertFalse(fs.exists(cSerdesTableDumpLocation));
  }

}
