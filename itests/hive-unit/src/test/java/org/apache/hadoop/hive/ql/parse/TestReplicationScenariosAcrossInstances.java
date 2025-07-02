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

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hive.common.repl.ReplConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.InjectableBehaviourObjectStore;
import org.apache.hadoop.hive.metastore.InjectableBehaviourObjectStore.BehaviourInjection;
import org.apache.hadoop.hive.metastore.InjectableBehaviourObjectStore.CallerArguments;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.messaging.json.gzip.GzipJSONMessageEncoder;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.WarehouseInstance.Tuple;
import org.apache.hadoop.hive.ql.exec.repl.incremental.IncrementalLoadTasksBuilder;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.parse.repl.PathBuilder;
import org.apache.hadoop.hive.ql.parse.repl.dump.Utils;
import org.apache.hadoop.hive.ql.util.DependencyResolver;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdfs.protocol.HdfsConstants.QUOTA_DONT_SET;
import static org.apache.hadoop.hdfs.protocol.HdfsConstants.QUOTA_RESET;
import static org.apache.hadoop.hive.common.repl.ReplConst.SOURCE_OF_REPLICATION;
import static org.apache.hadoop.hive.ql.exec.repl.ReplAck.LOAD_ACKNOWLEDGEMENT;
import static org.apache.hadoop.hive.ql.exec.repl.ReplAck.NON_RECOVERABLE_MARKER;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestReplicationScenariosAcrossInstances extends BaseReplicationAcrossInstances {
  private static final String NS_REMOTE = "nsRemote";
  @BeforeClass
  public static void classLevelSetup() throws Exception {
    HashMap<String, String> overrides = new HashMap<>();
    overrides.put(MetastoreConf.ConfVars.EVENT_MESSAGE_FACTORY.getHiveName(),
        GzipJSONMessageEncoder.class.getCanonicalName());
    overrides.put(HiveConf.ConfVars.HIVE_DISTCP_DOAS_USER.varname,
        UserGroupInformation.getCurrentUser().getUserName());
    overrides.put(MetastoreConf.ConfVars.HIVE_TXN_MANAGER.getVarname(),
        "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager");
    overrides.put(MetastoreConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.getVarname(),
        "true");
    overrides.put(HiveConf.ConfVars.REPL_BATCH_INCREMENTAL_EVENTS.varname, "false");
    internalBeforeClassSetup(overrides, TestReplicationScenariosAcrossInstances.class);
  }

  @Test
  public void testCreateFunctionIncrementalReplication() throws Throwable {
    WarehouseInstance.Tuple bootStrapDump = primary.dump(primaryDbName);
    replica.load(replicatedDbName, primaryDbName)
        .run("REPL STATUS " + replicatedDbName)
        .verifyResult(bootStrapDump.lastReplicationId);

    primary.run("CREATE FUNCTION " + primaryDbName
        + ".testFunctionOne as 'hivemall.tools.string.StopwordUDF' "
        + "using jar  'ivy://io.github.myui:hivemall:0.4.0-2'")
        .run("CREATE FUNCTION " + primaryDbName
            + ".testFunctionTwo as 'org.apache.hadoop.hive.ql.udf.generic.GenericUDAFMax'");

    //only testFunctionOne should be replicated, functions created without 'using' clause not supported
    WarehouseInstance.Tuple incrementalDump =
        primary.dump(primaryDbName);
    replica.load(replicatedDbName, primaryDbName)
        .run("REPL STATUS " + replicatedDbName)
        .verifyResult(incrementalDump.lastReplicationId)
        .run("SHOW FUNCTIONS LIKE '" + replicatedDbName + "%'")
        .verifyResults(new String[] { replicatedDbName + ".testFunctionOne"});

    // Test the idempotent behavior of CREATE FUNCTION
    replica.load(replicatedDbName, primaryDbName)
        .run("REPL STATUS " + replicatedDbName)
        .verifyResult(incrementalDump.lastReplicationId)
        .run("SHOW FUNCTIONS LIKE '" + replicatedDbName + "%'")
        .verifyResults(new String[] { replicatedDbName + ".testFunctionOne"});
  }

  @Test
  public void testCreateFunctionOnHDFSIncrementalReplication() throws Throwable {
    Path identityUdfLocalPath = new Path("../../data/files/identity_udf.jar");
    Path identityUdf1HdfsPath = new Path(primary.functionsRoot, "idFunc1" + File.separator + "identity_udf1.jar");
    Path identityUdf2HdfsPath = new Path(primary.functionsRoot, "idFunc2" + File.separator + "identity_udf2.jar");
    List<String> withClause = Arrays.asList("'" + HiveConf.ConfVars.REPL_RUN_DATA_COPY_TASKS_ON_TARGET.varname + "'='false'");
    setupUDFJarOnHDFS(identityUdfLocalPath, identityUdf1HdfsPath);
    setupUDFJarOnHDFS(identityUdfLocalPath, identityUdf2HdfsPath);

    primary.run("CREATE FUNCTION " + primaryDbName
            + ".idFunc1 as 'IdentityStringUDF' "
            + "using jar  '" + identityUdf1HdfsPath.toString() + "'");
    WarehouseInstance.Tuple bootStrapDump = primary.dump(primaryDbName, withClause);
    replica.load(replicatedDbName, primaryDbName, withClause)
            .run("REPL STATUS " + replicatedDbName)
            .verifyResult(bootStrapDump.lastReplicationId)
            .run("SHOW FUNCTIONS LIKE '" + replicatedDbName + "%'")
            .verifyResults(new String[] { replicatedDbName + ".idFunc1"})
            .run("SELECT " + replicatedDbName + ".idFunc1('MyName')")
            .verifyResults(new String[] { "MyName"});

    assertFunctionJarsOnTarget("idFunc1", Arrays.asList("identity_udf1.jar"));
    primary.run("CREATE FUNCTION " + primaryDbName
            + ".idFunc2 as 'IdentityStringUDF' "
            + "using jar  '" + identityUdf2HdfsPath.toString() + "'");

    WarehouseInstance.Tuple incrementalDump =
            primary.dump(primaryDbName, withClause);
    replica.load(replicatedDbName, primaryDbName, withClause)
            .run("REPL STATUS " + replicatedDbName)
            .verifyResult(incrementalDump.lastReplicationId)
            .run("SHOW FUNCTIONS LIKE '" + replicatedDbName + "%'")
            .verifyResults(new String[] { replicatedDbName + ".idFunc1",
                    replicatedDbName + ".idFunc2" })
            .run("SELECT " + replicatedDbName + ".idFunc2('YourName')")
            .verifyResults(new String[] { "YourName"});

    assertFunctionJarsOnTarget("idFunc1", Arrays.asList("identity_udf1.jar"));
    assertFunctionJarsOnTarget("idFunc2", Arrays.asList("identity_udf2.jar"));
  }

  @Test
  public void testCreateFunctionOnHDFSIncrementalReplicationLazyCopy() throws Throwable {
    Path identityUdfLocalPath = new Path("../../data/files/identity_udf.jar");
    Path identityUdf1HdfsPath = new Path(primary.functionsRoot, "idFunc1" + File.separator + "identity_udf1.jar");
    Path identityUdf2HdfsPath = new Path(primary.functionsRoot, "idFunc2" + File.separator + "identity_udf2.jar");
    setupUDFJarOnHDFS(identityUdfLocalPath, identityUdf1HdfsPath);
    setupUDFJarOnHDFS(identityUdfLocalPath, identityUdf2HdfsPath);
    List<String> withClause = Arrays.asList("'" + HiveConf.ConfVars.REPL_RUN_DATA_COPY_TASKS_ON_TARGET.varname + "'='true'");

    primary.run("CREATE FUNCTION " + primaryDbName
            + ".idFunc1 as 'IdentityStringUDF' "
            + "using jar  '" + identityUdf1HdfsPath.toString() + "'");
    WarehouseInstance.Tuple bootStrapDump = primary.dump(primaryDbName, withClause);
    replica.load(replicatedDbName, primaryDbName, withClause)
            .run("REPL STATUS " + replicatedDbName)
            .verifyResult(bootStrapDump.lastReplicationId)
            .run("SHOW FUNCTIONS LIKE '" + replicatedDbName + "%'")
            .verifyResults(new String[] { replicatedDbName + ".idFunc1"})
            .run("SELECT " + replicatedDbName + ".idFunc1('MyName')")
            .verifyResults(new String[] { "MyName"});

    assertFunctionJarsOnTarget("idFunc1", Arrays.asList("identity_udf1.jar"));

    primary.run("CREATE FUNCTION " + primaryDbName
            + ".idFunc2 as 'IdentityStringUDF' "
            + "using jar  '" + identityUdf2HdfsPath.toString() + "'");

    WarehouseInstance.Tuple incrementalDump =
            primary.dump(primaryDbName, withClause);
    replica.load(replicatedDbName, primaryDbName, withClause)
            .run("REPL STATUS " + replicatedDbName)
            .verifyResult(incrementalDump.lastReplicationId)
            .run("SHOW FUNCTIONS LIKE '" + replicatedDbName + "%'")
            .verifyResults(new String[] { replicatedDbName + ".idFunc1",
                    replicatedDbName + ".idFunc2" })
            .run("SELECT " + replicatedDbName + ".idFunc2('YourName')")
            .verifyResults(new String[] { "YourName"});

    assertFunctionJarsOnTarget("idFunc1", Arrays.asList("identity_udf1.jar"));
    assertFunctionJarsOnTarget("idFunc2", Arrays.asList("identity_udf2.jar"));
  }

  @Test
  public void testBootstrapReplLoadRetryAfterFailureForFunctions() throws Throwable {
    String funcName1 = "f1";
    String funcName2 = "f2";
    WarehouseInstance.Tuple tuple = primary.run("use " + primaryDbName)
            .run("CREATE FUNCTION " + primaryDbName + "." + funcName1 +
                    " as 'hivemall.tools.string.StopwordUDF' " +
                    "using jar  'ivy://io.github.myui:hivemall:0.4.0-2'")
            .run("CREATE FUNCTION " + primaryDbName + "." + funcName2 +
                    " as 'hivemall.tools.string.SplitWordsUDF' "+
                    "using jar  'ivy://io.github.myui:hivemall:0.4.0-1'")
            .dump(primaryDbName);

    // Allow create function only on f1. Create should fail for the second function.
    BehaviourInjection<CallerArguments, Boolean> callerVerifier
            = new BehaviourInjection<CallerArguments, Boolean>() {
              @Override
              public Boolean apply(CallerArguments args) {
                injectionPathCalled = true;
                if (!args.dbName.equalsIgnoreCase(replicatedDbName)) {
                  LOG.warn("Verifier - DB: " + String.valueOf(args.dbName));
                  return false;
                }
                if (args.funcName != null) {
                  LOG.debug("Verifier - Function: " + String.valueOf(args.funcName));
                  return args.funcName.equals(funcName1);
                }
                return true;
              }
            };
    InjectableBehaviourObjectStore.setCallerVerifier(callerVerifier);

    // Trigger bootstrap dump which just creates function f1 but not f2
    List<String> withConfigs = Arrays.asList("'hive.repl.approx.max.load.tasks'='1'",
            "'hive.in.repl.test.files.sorted'='true'");
    try {
      replica.loadFailure(replicatedDbName, primaryDbName, withConfigs);
      callerVerifier.assertInjectionsPerformed(true, false);
    } finally {
      InjectableBehaviourObjectStore.resetCallerVerifier(); // reset the behaviour
    }

    // Verify that only f1 got loaded
    replica.run("use " + replicatedDbName)
            .run("repl status " + replicatedDbName)
            .verifyResult("null")
            .run("show functions like '" + replicatedDbName + "%'")
            .verifyResult(replicatedDbName + "." + funcName1);

    // Verify no calls to load f1 only f2.
    callerVerifier = new BehaviourInjection<CallerArguments, Boolean>() {
      @Override
      public Boolean apply(CallerArguments args) {
        injectionPathCalled = true;
        if (!args.dbName.equalsIgnoreCase(replicatedDbName)) {
          LOG.warn("Verifier - DB: " + String.valueOf(args.dbName));
          return false;
        }
        if (args.funcName != null) {
          LOG.debug("Verifier - Function: " + String.valueOf(args.funcName));
          return args.funcName.equals(funcName2);
        }
        return true;
      }
    };
    InjectableBehaviourObjectStore.setCallerVerifier(callerVerifier);

    try {
      // Retry with same dump with which it was already loaded should resume the bootstrap load.
      // This time, it completes by adding just the function f2
      replica.load(replicatedDbName, primaryDbName);
      callerVerifier.assertInjectionsPerformed(true, false);
    } finally {
      InjectableBehaviourObjectStore.resetCallerVerifier(); // reset the behaviour
    }

    // Verify that both the functions are available.
    replica.run("use " + replicatedDbName)
            .run("repl status " + replicatedDbName)
            .verifyResult(tuple.lastReplicationId)
            .run("show functions like '" + replicatedDbName +"%'")
            .verifyResults(new String[] {replicatedDbName + "." + funcName1,
                                         replicatedDbName +"." +funcName2});
  }

  @Test
  public void testDropFunctionIncrementalReplication() throws Throwable {
    primary.run("CREATE FUNCTION " + primaryDbName
        + ".testFunctionAnother as 'hivemall.tools.string.StopwordUDF' "
        + "using jar  'ivy://io.github.myui:hivemall:0.4.0-2'");
    WarehouseInstance.Tuple bootStrapDump = primary.dump(primaryDbName);
    replica.load(replicatedDbName, primaryDbName)
        .run("REPL STATUS " + replicatedDbName)
        .verifyResult(bootStrapDump.lastReplicationId);

    primary.run("Drop FUNCTION " + primaryDbName + ".testFunctionAnother ");

    WarehouseInstance.Tuple incrementalDump =
        primary.dump(primaryDbName);
    replica.load(replicatedDbName, primaryDbName)
        .run("REPL STATUS " + replicatedDbName)
        .verifyResult(incrementalDump.lastReplicationId)
        .run("SHOW FUNCTIONS LIKE '%testfunctionanother%'")
        .verifyResult(null);

    // Test the idempotent behavior of DROP FUNCTION
    replica.load(replicatedDbName, primaryDbName)
        .run("REPL STATUS " + replicatedDbName)
        .verifyResult(incrementalDump.lastReplicationId)
        .run("SHOW FUNCTIONS LIKE '%testfunctionanother%'")
        .verifyResult(null);
  }

  @Test
  public void testBootstrapFunctionReplication() throws Throwable {
    primary.run("CREATE FUNCTION " + primaryDbName
        + ".testFunction as 'hivemall.tools.string.StopwordUDF' "
        + "using jar  'ivy://io.github.myui:hivemall:0.4.0-2'");
    WarehouseInstance.Tuple bootStrapDump = primary.dump(primaryDbName);

    replica.load(replicatedDbName, primaryDbName)
        .run("SHOW FUNCTIONS LIKE '" + replicatedDbName + "%'")
        .verifyResult(replicatedDbName + ".testFunction");
  }

  @Test
  public void testCreateFunctionWithFunctionBinaryJarsOnHDFS() throws Throwable {
    Dependencies dependencies = dependencies("ivy://io.github.myui:hivemall:0.4.0-2", primary);
    String jarSubString = dependencies.toJarSubSql();
    List<String> withClause = Arrays.asList("'" + HiveConf.ConfVars.REPL_RUN_DATA_COPY_TASKS_ON_TARGET.varname + "'='false'");

    primary.run("CREATE FUNCTION " + primaryDbName
        + ".anotherFunction as 'hivemall.tools.string.StopwordUDF' "
        + "using " + jarSubString);

    WarehouseInstance.Tuple tuple = primary.dump(primaryDbName, withClause);

    replica.load(replicatedDbName, primaryDbName, withClause)
        .run("SHOW FUNCTIONS LIKE '" + replicatedDbName + "%'")
        .verifyResult(replicatedDbName + ".anotherFunction");
    assertFunctionJarsOnTarget("anotherFunction", dependencies.jarNames());
  }

  @Test
  public void testBootstrapFunctionOnHDFSLazyCopy() throws Throwable {
    Dependencies dependencies = dependencies("ivy://io.github.myui:hivemall:0.4.0-2", primary);
    String jarSubString = dependencies.toJarSubSql();
    List<String> withClause = Arrays.asList("'" + HiveConf.ConfVars.REPL_RUN_DATA_COPY_TASKS_ON_TARGET.varname + "'='true'");

    primary.run("CREATE FUNCTION " + primaryDbName
            + ".anotherFunction as 'hivemall.tools.string.StopwordUDF' "
            + "using " + jarSubString);

    WarehouseInstance.Tuple tuple = primary.dump(primaryDbName, withClause);

    replica.load(replicatedDbName, primaryDbName, withClause)
            .run("SHOW FUNCTIONS LIKE '" + replicatedDbName + "%'")
            .verifyResult(replicatedDbName + ".anotherFunction");
    assertFunctionJarsOnTarget("anotherFunction", dependencies.jarNames());
  }

  @Test
  public void testIncrementalCreateFunctionWithFunctionBinaryJarsOnHDFS() throws Throwable {
    WarehouseInstance.Tuple bootStrapDump = primary.dump(primaryDbName);
    replica.load(replicatedDbName, primaryDbName)
            .run("REPL STATUS " + replicatedDbName)
            .verifyResult(bootStrapDump.lastReplicationId);

    Dependencies dependencies = dependencies("ivy://io.github.myui:hivemall:0.4.0-2", primary);
    String jarSubString = dependencies.toJarSubSql();

    primary.run("CREATE FUNCTION " + primaryDbName
            + ".anotherFunction as 'hivemall.tools.string.StopwordUDF' "
            + "using " + jarSubString);

    WarehouseInstance.Tuple tuple = primary.dump(primaryDbName);

    replica.load(replicatedDbName, primaryDbName)
            .run("SHOW FUNCTIONS LIKE '" + replicatedDbName + "%'")
            .verifyResult(replicatedDbName + ".anotherFunction");
    assertFunctionJarsOnTarget("anotherFunction", dependencies.jarNames());
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
        .run("insert into t1 values (1), (2)")
        .run("create table t2 (place string) partitioned by (country string)")
        .run("insert into table t2 partition(country='india') values ('bangalore')")
        .run("insert into table t2 partition(country='us') values ('austin')")
        .run("insert into table t2 partition(country='france') values ('paris')")
        .run("create table t3 (rank int)")
        .dump(primaryDbName);

    // each table creation itself takes more than one task, give we are giving a max of 1, we should hit multiple runs.
    List<String> withClause = Collections.singletonList(
        "'" + HiveConf.ConfVars.REPL_APPROX_MAX_LOAD_TASKS.varname + "'='1'");

    replica.load(replicatedDbName, primaryDbName, withClause)
        .run("use " + replicatedDbName)
        .run("show tables")
        .verifyResults(new String[] { "t1", "t2", "t3" })
        .run("repl status " + replicatedDbName)
        .verifyResult(tuple.lastReplicationId)
        .run("select country from t2 order by country")
        .verifyResults(new String[] { "france", "india", "us" });
  }

  @Test
  public void testMultipleStagesOfReplicationLoadTaskWithPartitionBatching() throws Throwable {
    WarehouseInstance.Tuple tuple = primary
      .run("use " + primaryDbName)
      .run("create table t1 (id int)")
      .run("insert into t1 values (1), (2)")
      .run("create table t2 (place string) partitioned by (country string)")
      .run("insert into table t2 partition(country='india') values ('bangalore')")
      .run("insert into table t2 partition(country='us') values ('austin')")
      .run("insert into table t2 partition(country='france') values ('paris')")
      .run("create table t3 (rank int)")
      .dump(primaryDbName);

    // each table creation itself takes more than one task, give we are giving a max of 1, we should hit multiple runs.
    List<String> withClause = new ArrayList<>();
    withClause.add("'" + HiveConf.ConfVars.REPL_APPROX_MAX_LOAD_TASKS.varname + "'='1'");
    withClause.add("'" + HiveConf.ConfVars.REPL_LOAD_PARTITIONS_WITH_DATA_COPY_BATCH_SIZE.varname + "'='1'");

    replica.load(replicatedDbName, primaryDbName, withClause)
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
        .dump(primaryDbName);

    replica.hiveConf.setBoolVar(HiveConf.ConfVars.EXEC_PARALLEL, true);
    replica.load(replicatedDbName, primaryDbName)
        .run("use " + replicatedDbName)
        .run("repl status " + replicatedDbName)
        .verifyResult(tuple.lastReplicationId)
        .run("show tables")
        .verifyResults(new String[] { "t1", "t2", "t3" })
        .run("select country from t2")
        .verifyResults(Arrays.asList("india", "australia", "russia", "uk", "us", "france", "japan",
            "china"));
    replica.hiveConf.setBoolVar(HiveConf.ConfVars.EXEC_PARALLEL, false);
  }

  @Test
  public void testMetadataBootstrapDump() throws Throwable {
    WarehouseInstance.Tuple tuple = primary.run("use " + primaryDbName)
        .run("create table  acid_table (key int, value int) partitioned by (load_date date) " +
            "clustered by(key) into 2 buckets stored as orc tblproperties ('transactional'='true')")
        .run("create table table1 (i int, j int)")
        .run("insert into table1 values (1,2)")
        .dump(primaryDbName, Collections.singletonList("'hive.repl.dump.metadata.only'='true'"));

    replica.load(replicatedDbName, primaryDbName)
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
        .dump(primaryDbName, Collections.singletonList("'hive.repl.dump.metadata.only'='true'"));

    replica.load(replicatedDbName, primaryDbName)
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
            .dumpWithCommand(
                "repl dump " + primaryDbName + " with ('hive.repl.dump.metadata.only'='true')"
            );

    replica.load(replicatedDbName, primaryDbName)
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
        .dumpWithCommand("repl dump " + primaryDbName + " with ('hive.repl.dump.metadata.only'='true')"
        );

    replica.load(replicatedDbName, primaryDbName)
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
            "custom.property\tcustom.value"
        });
  }

  @Test
  public void testNonReplDBMetadataReplication() throws Throwable {
    String dbName = primaryDbName + "_metadata";
    WarehouseInstance.Tuple tuple = primary
            .run("create database " + dbName)
            .run("use " + dbName)
            .run("create table table1 (i int, j int)")
            .run("create table table2 (a int, city string) partitioned by (country string)")
            .run("create table table3 (i int, j int)")
            .run("insert into table1 values (1,2)")
        .dump(dbName, Collections.singletonList("'hive.repl.dump.metadata.only'='true'"));

    replica.load(replicatedDbName, dbName)
            .run("use " + replicatedDbName)
            .run("show tables")
            .verifyResults(new String[]{"table1", "table2", "table3"})
            .run("select * from table1")
            .verifyResults(Collections.emptyList());

    tuple = primary
            .run("use " + dbName)
            .run("alter table table1 rename to renamed_table1")
            .run("insert into table2 partition(country='india') values (1,'mumbai') ")
            .run("create table table4 (i int, j int)")
        .dump(dbName, Collections.singletonList("'hive.repl.dump.metadata.only'='true'"));

    replica.load(replicatedDbName, dbName)
            .run("use " + replicatedDbName)
            .run("show tables")
            .verifyResults(new String[] { "renamed_table1", "table2", "table3", "table4" })
            .run("select * from renamed_table1")
            .verifyResults(Collections.emptyList())
            .run("select * from table2")
            .verifyResults(Collections.emptyList());
  }

    @Test
  public void testBootStrapDumpOfWarehouse() throws Throwable {
    //Clear the repl base dir
    Path replBootstrapDumpDir = new Path(primary.hiveConf.get(MetastoreConf.ConfVars.REPLDIR.getHiveName()), "*");
    replBootstrapDumpDir.getFileSystem(primary.hiveConf).delete(replBootstrapDumpDir, true);
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
        .dump("`*`", Collections.singletonList("'hive.repl.dump.metadata.only'='true'"));

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

    // Reset ckpt and last repl ID keys to empty set for allowing bootstrap load
    replica.run("show databases")
        .verifyFailure(new String[] { primaryDbName, dbOne, dbTwo })
        .run("alter database default set dbproperties ('hive.repl.ckpt.key'='', 'repl.last.id'='')");
    try {
      replica.load("", "`*`");
      Assert.fail();
    } catch (HiveException e) {
      assertEquals("REPL LOAD Target database name shouldn't be null", e.getMessage());
    }
  }

  @Test
  public void testReplLoadFromSourceUsingWithClause() throws Throwable {
    HiveConf replicaConf = replica.getConf();
    List<String> withConfigs = Arrays.asList(
            "'hive.metastore.warehouse.dir'='" + replicaConf.getVar(HiveConf.ConfVars.METASTORE_WAREHOUSE) + "'",
            "'hive.metastore.uris'='" + replicaConf.getVar(HiveConf.ConfVars.METASTORE_URIS) + "'",
            "'hive.repl.replica.functions.root.dir'='" + replicaConf.getVar(HiveConf.ConfVars.REPL_FUNCTIONS_ROOT_DIR) + "'");

    ////////////  Bootstrap   ////////////
    WarehouseInstance.Tuple bootstrapTuple = primary
            .run("use " + primaryDbName)
            .run("create table table1 (i int)")
            .run("create table table2 (id int) partitioned by (country string)")
            .run("insert into table1 values (1)")
            .dump(primaryDbName);

    // Run load on primary itself
    primary.load(replicatedDbName, primaryDbName, withConfigs)
            .status(replicatedDbName, withConfigs)
            .verifyResult(bootstrapTuple.lastReplicationId);

    replica.run("use " + replicatedDbName)
            .run("show tables")
            .verifyResults(new String[] { "table1", "table2" })
            .run("select * from table1")
            .verifyResults(new String[]{ "1" })
            .verifyReplTargetProperty(replicatedDbName);

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
                    .dump(primaryDbName, Collections.emptyList());

    // Run load on primary itself
    primary.load(replicatedDbName, primaryDbName, withConfigs)
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
            .run("show functions like '" + replicatedDbName + "%'")
            .verifyResult(replicatedDbName + ".testFunctionOne")
            .verifyReplTargetProperty(replicatedDbName);

    ////////////  Second Incremental ////////////
    WarehouseInstance.Tuple secondIncremental = primary
            .run("use " + primaryDbName)
            .run("alter table table2 add columns (zipcode int)")
            .run("alter table table3 set tblproperties('custom.property'='custom.value')")
            .run("drop table renamed_table1")
            .run("alter table table2 drop partition(country='usa')")
            .run("truncate table table3")
            .run("drop function " + primaryDbName + ".testFunctionOne ")
            .dump(primaryDbName, Collections.emptyList());

    // Run load on primary itself
    primary.load(replicatedDbName, primaryDbName, withConfigs)
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
            .verifyResults(new String[] { "custom.property\tcustom.value" })
            .run("select id from table2 order by id")
            .verifyResults(new String[] { "1" })
            .run("select * from table3")
            .verifyResults(Collections.emptyList())
            .run("show functions like '" + replicatedDbName + "%'")
            .verifyResult(null)
            .verifyReplTargetProperty(replicatedDbName);
  }

  @Test
  public void testIncrementalReplWithEventsBatchHavingDropCreateTable() throws Throwable {
    // Bootstrap dump with empty db
    WarehouseInstance.Tuple bootstrapTuple = primary.dump(primaryDbName);

    // Bootstrap load in replica
    replica.load(replicatedDbName, primaryDbName)
            .status(replicatedDbName)
            .verifyResult(bootstrapTuple.lastReplicationId);

    // First incremental dump
    WarehouseInstance.Tuple firstIncremental = primary.run("use " + primaryDbName)
            .run("create table table1 (i int)")
            .run("create table table2 (id int) partitioned by (country string)")
            .run("insert into table1 values (1)")
            .run("insert into table2 partition(country='india') values(1)")
            .dump(primaryDbName, Collections.emptyList());

    // First incremental load
    replica.load(replicatedDbName, primaryDbName)
            .status(replicatedDbName)
            .verifyResult(firstIncremental.lastReplicationId)
            .run("use " + replicatedDbName)
            .run("show tables")
            .verifyResults(new String[] {"table1", "table2"})
            .run("select * from table1")
            .verifyResults(new String[] {"1"})
            .run("select id from table2 order by id")
            .verifyResults(new String[] {"1"});

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
            .dump(primaryDbName, Collections.emptyList());

    // Second incremental load
    replica.load(replicatedDbName, primaryDbName)
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
    WarehouseInstance.Tuple bootstrapTuple = primary.dump(primaryDbName);

    // Bootstrap load in replica
    replica.load(replicatedDbName, primaryDbName)
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
        .dump(primaryDbName, Collections.emptyList());

    // First incremental load
    replica.load(replicatedDbName, primaryDbName)
            .status(replicatedDbName)
            .verifyResult(firstIncremental.lastReplicationId)
            .run("use " + replicatedDbName)
            .run("select id from table1")
            .verifyResults(new String[] {"1"})
            .run("select * from table2")
            .verifyResults(new String[] {"2"})
            .run("select id from table3")
            .verifyResults(new String[] {"3"});

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
        .dump(primaryDbName, Collections.emptyList());

    // Second incremental load
    replica.load(replicatedDbName, primaryDbName)
        .status(replicatedDbName)
        .verifyResult(secondIncremental.lastReplicationId)
        .run("use " + replicatedDbName)
        .run("select * from table1")
        .verifyResults(new String[] { "10" })
        .run("select id from table2")
        .verifyResults(new String[] { "20" })
        .run("select id from table3")
        .verifyResults(new String[] {"30"});
  }

  @Test
  public void testShouldNotCreateDirectoryForNonNativeTableInDumpDirectory() throws Throwable {
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
        .run(createTableQuery).dump(primaryDbName);
    Path cSerdesTableDumpLocation = new Path(
        new Path(bootstrapTuple.dumpLocation, primaryDbName),
        "custom_serdes");
    FileSystem fs = cSerdesTableDumpLocation.getFileSystem(primary.hiveConf);
    assertFalse(fs.exists(cSerdesTableDumpLocation));
  }

  @Test
  public void testShouldDumpMetaDataForNonNativeTableIfSetMeataDataOnly() throws Throwable {
    String tableName = testName.getMethodName() + "_table";
    String createTableQuery =
            "CREATE TABLE " + tableName + " ( serde_id bigint COMMENT 'from deserializer', name string "
                    + "COMMENT 'from deserializer', slib string COMMENT 'from deserializer') "
                    + "ROW FORMAT SERDE 'org.apache.hive.storage.jdbc.JdbcSerDe' "
                    + "STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler' "
                    + "WITH SERDEPROPERTIES ('serialization.format'='1') "
                    + "TBLPROPERTIES ( "
                    + "'hive.sql.database.type'='METASTORE', "
                    + "'hive.sql.query'='SELECT \"SERDE_ID\", \"NAME\", \"SLIB\" FROM \"SERDES\"')";

    WarehouseInstance.Tuple bootstrapTuple = primary
            .run("use " + primaryDbName)
            .run(createTableQuery)
            .dump(primaryDbName, Collections.singletonList("'hive.repl.dump.metadata.only'='true'"));

    // Bootstrap load in replica
    replica.load(replicatedDbName, primaryDbName)
            .status(replicatedDbName)
            .verifyResult(bootstrapTuple.lastReplicationId)
            .run("use " + replicatedDbName)
            .run("show tables")
            .verifyResult(tableName);
  }

  private void verifyIfCkptSet(Map<String, String> props, String dumpDir) {
    assertTrue(props.containsKey(ReplConst.REPL_TARGET_DB_PROPERTY));
    String hiveDumpDir = dumpDir + File.separator + ReplUtils.REPL_HIVE_BASE_DIR;
    assertTrue(props.get(ReplConst.REPL_TARGET_DB_PROPERTY).equals(hiveDumpDir));
  }

  private void verifyIfCkptPropMissing(Map<String, String> props) {
    assertFalse(props.containsKey(ReplConst.REPL_TARGET_DB_PROPERTY));
  }

  private void verifyIfSrcOfReplPropMissing(Map<String, String> props) {
    assertFalse(props.containsKey(SOURCE_OF_REPLICATION));
  }

  @Test
  public void testIncrementalDumpEmptyDumpDirectory() throws Throwable {
    WarehouseInstance.Tuple tuple = primary.dump(primaryDbName);

    replica.load(replicatedDbName, primaryDbName)
            .status(replicatedDbName)
            .verifyResult(tuple.lastReplicationId);

    tuple = primary.dump(primaryDbName, Collections.emptyList());

    replica.load(replicatedDbName, primaryDbName)
            .status(replicatedDbName)
            .verifyResult(tuple.lastReplicationId);

    // create events for some other database and then dump the primaryDbName to dump an empty directory.
    String testDbName = primaryDbName + "_test";
    tuple = primary.run(" create database " + testDbName)
            .run("create table " + testDbName + ".tbl (fld int)")
            .dump(primaryDbName, Collections.emptyList());

    // Incremental load to existing database with empty dump directory should set the repl id to the last event at src.
    replica.load(replicatedDbName, primaryDbName)
            .status(replicatedDbName)
            .verifyResult(tuple.lastReplicationId);

    // Bootstrap dump should fail if source database does not exist.
    String nonExistingDb = "someJunkDB";
    assertEquals (primary.getDatabase(nonExistingDb), null);
    try {
      primary.run("REPL DUMP " + nonExistingDb);
      assert false;
    } catch (Exception e) {
      assertEquals (ErrorMsg.REPL_SOURCE_DATABASE_NOT_FOUND.format(nonExistingDb), e.getMessage());
    }
    primary.run(" drop database if exists " + testDbName + " cascade");
  }

  @Test
  public void testIncrementalDumpMultiIteration() throws Throwable {
    WarehouseInstance.Tuple bootstrapTuple = primary.dump(primaryDbName);

    replica.load(replicatedDbName, primaryDbName)
            .status(replicatedDbName)
            .verifyResult(bootstrapTuple.lastReplicationId);

    WarehouseInstance.Tuple incremental = primary.run("use " + primaryDbName)
            .run("create table table1 (id int) partitioned by (country string)")
            .run("create table table2 (id int)")
            .run("create table table3 (id int) partitioned by (country string)")
            .run("insert into table1 partition(country='india') values(1)")
            .run("insert into table2 values(2)")
            .run("insert into table3 partition(country='india') values(3)")
            .dump(primaryDbName, Collections.emptyList());

    replica.load(replicatedDbName, primaryDbName,
        Collections.singletonList("'hive.repl.approx.max.load.tasks'='10'"))
            .status(replicatedDbName)
            .verifyResult(incremental.lastReplicationId)
            .run("use " + replicatedDbName)
            .run("select id from table1")
            .verifyResults(new String[] {"1" })
            .run("select * from table2")
            .verifyResults(new String[] {"2" })
            .run("select id from table3")
            .verifyResults(new String[] {"3" });
    assert(IncrementalLoadTasksBuilder.getNumIteration() > 1);

    incremental = primary.run("use " + primaryDbName)
            .run("create table  table5 (key int, value int) partitioned by (load_date date) " +
                    "clustered by(key) into 2 buckets stored as orc")
            .run("create table table4 (i int, j int)")
            .run("insert into table4 values (1,2)")
            .dump(primaryDbName, Collections.singletonList("'hive.repl.include.external.tables'='false'"));

    String hiveDumpDir = incremental.dumpLocation + File.separator + ReplUtils.REPL_HIVE_BASE_DIR;
    Path path = new Path(hiveDumpDir);
    FileSystem fs = path.getFileSystem(conf);
    FileStatus[] fileStatus = fs.listStatus(path);
    int numEvents = fileStatus.length - 3; //for _metadata, _finished_dump and _events_dump

    replica.load(replicatedDbName, primaryDbName,
        Arrays.asList("'hive.repl.approx.max.load.tasks'='1','hive.repl.include.external.tables'='false'"))
            .run("use " + replicatedDbName)
            .run("show tables")
            .verifyResults(new String[] {"table1", "table2", "table3", "table4", "table5" })
            .run("select i from table4")
            .verifyResult("1");
    Assert.assertEquals(IncrementalLoadTasksBuilder.getNumIteration(), numEvents);
  }

  @Test
  public void testIfCkptAndSourceOfReplPropsIgnoredByReplDump() throws Throwable {
    WarehouseInstance.Tuple tuplePrimary = primary
            .run("use " + primaryDbName)
            .run("create table t1 (place string) partitioned by (country string) "
                    + " tblproperties('custom.property'='custom.value')")
            .run("insert into table t1 partition(country='india') values ('bangalore')")
            .dump(primaryDbName);

    // Bootstrap Repl A -> B
    replica.load(replicatedDbName, primaryDbName)
            .run("repl status " + replicatedDbName)
            .verifyResult(tuplePrimary.lastReplicationId)
            .run("show tblproperties t1('custom.property')")
            .verifyResults(new String[] { "custom.property\tcustom.value" })
            .run("alter database " + replicatedDbName
                    + " set dbproperties ('" + SOURCE_OF_REPLICATION + "' = '1, 2, 3')");

    // do a empty incremental load to allow dump of replicatedDbName
    WarehouseInstance.Tuple temp = primary.dump(primaryDbName, Collections.emptyList());
    replica.load(replicatedDbName, primaryDbName); // first successful incremental load.

    // Bootstrap Repl B -> C
    WarehouseInstance.Tuple tupleReplica = replica.run("alter database " + replicatedDbName
            + " set dbproperties ('" + ReplConst.TARGET_OF_REPLICATION + "' = '')").dump(replicatedDbName);
    String replDbFromReplica = replicatedDbName + "_dupe";
    replica.load(replDbFromReplica, replicatedDbName)
            .run("use " + replDbFromReplica)
            .run("repl status " + replDbFromReplica)
            .verifyResult(tupleReplica.lastReplicationId)
            .run("show tables")
            .verifyResults(new String[] { "t1" })
            .run("select country from t1")
            .verifyResults(Arrays.asList("india"))
            .run("show tblproperties t1('custom.property')")
            .verifyResults(new String[] { "custom.property\tcustom.value" });

    // Check if DB/table/partition in C doesn't have repl.source.for props. Also ensure, ckpt property
    // is set to bootstrap dump location used in C.
    Database db = replica.getDatabase(replDbFromReplica);
    verifyIfSrcOfReplPropMissing(db.getParameters());
    verifyIfCkptSet(db.getParameters(), tupleReplica.dumpLocation);
    Table t1 = replica.getTable(replDbFromReplica, "t1");
    verifyIfCkptSet(t1.getParameters(), tupleReplica.dumpLocation);
    Partition india = replica.getPartition(replDbFromReplica, "t1", Collections.singletonList("india"));
    verifyIfCkptSet(india.getParameters(), tupleReplica.dumpLocation);

    // Perform alters in A for incremental replication
    WarehouseInstance.Tuple tuplePrimaryInc = primary.run("use " + primaryDbName)
            .run("alter database " + primaryDbName + " set dbproperties('dummy_key'='dummy_val')")
            .run("alter table t1 set tblproperties('dummy_key'='dummy_val')")
            .run("alter table t1 partition(country='india') set fileformat orc")
            .dump(primaryDbName, Collections.emptyList());

    // Incremental Repl A -> B with alters on db/table/partition
    WarehouseInstance.Tuple tupleReplicaInc = replica.load(replicatedDbName, primaryDbName)
            .run("repl status " + replicatedDbName)
            .verifyResult(tuplePrimaryInc.lastReplicationId)
            .run("alter database " + replicatedDbName
                    + " set dbproperties ('" + ReplConst.TARGET_OF_REPLICATION + "' = '')")
            .dump(replicatedDbName, Collections.emptyList());

    // Check if DB in B have ckpt property is set to bootstrap dump location used in B and missing for table/partition.
    db = replica.getDatabase(replicatedDbName);
    verifyIfCkptSet(db.getParameters(), tuplePrimary.dumpLocation);
    t1 = replica.getTable(replicatedDbName, "t1");
    verifyIfCkptPropMissing(t1.getParameters());
    india = replica.getPartition(replicatedDbName, "t1", Collections.singletonList("india"));
    verifyIfCkptPropMissing(india.getParameters());

    // Incremental Repl B -> C with alters on db/table/partition
    replica.load(replDbFromReplica, replicatedDbName)
            .run("use " + replDbFromReplica)
            .run("repl status " + replDbFromReplica)
            .verifyResult(tupleReplicaInc.lastReplicationId)
            .run("show tblproperties t1('custom.property')")
            .verifyResults(new String[] { "custom.property\tcustom.value" });

    // Check if DB/table/partition in C doesn't have repl.source.for props. Also ensure, ckpt property
    // in DB is set to bootstrap dump location used in C but for table/partition, it is missing.
    db = replica.getDatabase(replDbFromReplica);
    verifyIfCkptSet(db.getParameters(), tupleReplica.dumpLocation);
    verifyIfSrcOfReplPropMissing(db.getParameters());
    t1 = replica.getTable(replDbFromReplica, "t1");
    verifyIfCkptPropMissing(t1.getParameters());
    india = replica.getPartition(replDbFromReplica, "t1", Collections.singletonList("india"));
    verifyIfCkptPropMissing(india.getParameters());

    replica.run("drop database if exists " + replDbFromReplica + " cascade");
  }

  @Test
  public void testIfReplTargetSetInIncremental() throws Throwable {
    WarehouseInstance.Tuple tuplePrimary = primary
            .run("use " + primaryDbName)
            .run("create table t1 (place string) partitioned by (country string)")
            .run("insert into table t1 partition(country='india') values ('bangalore')")
            .dump(primaryDbName);

    // Bootstrap Repl A -> B
    replica.load(replicatedDbName, primaryDbName);

    //Perform empty dump and load
    primary.dump(primaryDbName);
    replica.load(replicatedDbName, primaryDbName);
    assertTrue(MetaStoreUtils.isTargetOfReplication(replica.getDatabase(replicatedDbName)));

    replica.run("ALTER DATABASE " + replicatedDbName + " Set DBPROPERTIES('repl.target.for' = '')");
    assertFalse(MetaStoreUtils.isTargetOfReplication(replica.getDatabase(replicatedDbName)));
    replica.dump(replicatedDbName);

    // do a empty incremental load to allow dump of replicatedDbName
    primary.run("ALTER DATABASE " + primaryDbName + " Set DBPROPERTIES('custom_property1' = 'custom_value1')")
            .dump(primaryDbName, Collections.emptyList());
    replica.load(replicatedDbName, primaryDbName);
    compareDbProperties(primary.getDatabase(primaryDbName).getParameters(),
            replica.getDatabase(replicatedDbName).getParameters());
    assertTrue(MetaStoreUtils.isTargetOfReplication(replica.getDatabase(replicatedDbName)));

  }

  @Test
  public void testIfCkptPropIgnoredByExport() throws Throwable {
    WarehouseInstance.Tuple tuplePrimary = primary
            .run("use " + primaryDbName)
            .run("create table t1 (place string) partitioned by (country string)")
            .run("insert into table t1 partition(country='india') values ('bangalore')")
            .dump(primaryDbName);

    // Bootstrap Repl A -> B and then export table t1
    String path = "hdfs:///tmp/" + replicatedDbName + "/";
    String exportPath = "'" + path + "1/'";
    replica.load(replicatedDbName, primaryDbName)
            .run("repl status " + replicatedDbName)
            .verifyResult(tuplePrimary.lastReplicationId)
            .run("use " + replicatedDbName)
            .run("export table t1 to " + exportPath);

    // Check if ckpt property set in table/partition in B after bootstrap load.
    Table t1 = replica.getTable(replicatedDbName, "t1");
    verifyIfCkptSet(t1.getParameters(), tuplePrimary.dumpLocation);
    Partition india = replica.getPartition(replicatedDbName, "t1", Collections.singletonList("india"));
    verifyIfCkptSet(india.getParameters(), tuplePrimary.dumpLocation);

    // Import table t1 to C
    String importDbFromReplica = replicatedDbName + "_dupe";
    replica.run("create database " + importDbFromReplica)
            .run("use " + importDbFromReplica)
            .run("import table t1 from " + exportPath)
            .run("select country from t1")
            .verifyResults(Collections.singletonList("india"));

    // Check if table/partition in C doesn't have ckpt property
    t1 = replica.getTable(importDbFromReplica, "t1");
    verifyIfCkptPropMissing(t1.getParameters());
    india = replica.getPartition(importDbFromReplica, "t1", Collections.singletonList("india"));
    verifyIfCkptPropMissing(india.getParameters());

    replica.run("drop database if exists " + importDbFromReplica + " cascade");
  }

  @Test
  public void testIfBootstrapReplLoadFailWhenRetryAfterBootstrapComplete() throws Throwable {
    WarehouseInstance.Tuple tuple = primary
            .run("use " + primaryDbName)
            .run("create table t1 (id int)")
            .run("insert into table t1 values (10)")
            .run("create table t2 (place string) partitioned by (country string)")
            .run("insert into table t2 partition(country='india') values ('bangalore')")
            .run("insert into table t2 partition(country='uk') values ('london')")
            .run("insert into table t2 partition(country='us') values ('sfo')")
            .dump(primaryDbName);

    replica.load(replicatedDbName, primaryDbName)
            .run("use " + replicatedDbName)
            .run("repl status " + replicatedDbName)
            .verifyResult(tuple.lastReplicationId)
            .run("show tables")
            .verifyResults(new String[] { "t1", "t2" })
            .run("select id from t1")
        .verifyResults(Collections.singletonList("10"))
            .run("select country from t2 order by country")
            .verifyResults(Arrays.asList("india", "uk", "us"));
    String hiveDumpLocation = tuple.dumpLocation + File.separator + ReplUtils.REPL_HIVE_BASE_DIR;
    replica.verifyIfCkptSet(replicatedDbName, hiveDumpLocation);

    // To retry with same dump delete the load ack
    new Path(tuple.dumpLocation).getFileSystem(conf).delete(new Path(
            hiveDumpLocation, LOAD_ACKNOWLEDGEMENT.toString()), true);
    // Retry with same dump with which it was already loaded also fails.
    replica.loadFailure(replicatedDbName, primaryDbName);

    // To retry with same dump delete the load ack
    new Path(tuple.dumpLocation).getFileSystem(conf).delete(new Path(
            hiveDumpLocation, LOAD_ACKNOWLEDGEMENT.toString()), true);
    // Retry from same dump when the database is empty is also not allowed.
    replica.run("drop table t1")
            .run("drop table t2")
            .loadFailure(replicatedDbName, primaryDbName);
  }

  @Test
  public void testBootstrapReplLoadRetryAfterFailureForTablesAndConstraints() throws Throwable {
    WarehouseInstance.Tuple tuple = primary
            .run("use " + primaryDbName)
            .run("create table t1(a string, b string, primary key (a, b) disable novalidate rely)")
            .run("create table t2(a string, b string, foreign key (a, b) references t1(a, b) disable novalidate)")
            .run("create table t3(a string, b string not null disable, unique (a) disable)")
            .dump(primaryDbName);

    // Need to drop the primary DB as metastore is shared by both primary/replica. So, constraints
    // conflict when loaded. Some issue with framework which needs to be relook into later.
    primary.run("drop database if exists " + primaryDbName + " cascade");

    // Allow create table only on t1. Create should fail for rest of the tables and hence constraints
    // also not loaded.
    BehaviourInjection<CallerArguments, Boolean> callerVerifier
            = new BehaviourInjection<CallerArguments, Boolean>() {
      @Override
      public Boolean apply(CallerArguments args) {
        injectionPathCalled = true;
        if (!args.dbName.equalsIgnoreCase(replicatedDbName) || (args.constraintTblName != null)) {
          LOG.warn("Verifier - DB: " + String.valueOf(args.dbName)
                  + " Constraint Table: " + String.valueOf(args.constraintTblName));
          return false;
        }
        if (args.tblName != null) {
          LOG.warn("Verifier - Table: " + String.valueOf(args.tblName));
          return args.tblName.equals("t1");
        }
        return true;
      }
    };
    InjectableBehaviourObjectStore.setCallerVerifier(callerVerifier);

    // Trigger bootstrap dump which just creates table t1 and other tables (t2, t3) and constraints not loaded.
    List<String> withConfigs = Arrays.asList("'hive.repl.approx.max.load.tasks'='1'");
    try {
      replica.loadFailure(replicatedDbName, primaryDbName, withConfigs);
      callerVerifier.assertInjectionsPerformed(true, false);
    } finally {
      InjectableBehaviourObjectStore.resetCallerVerifier(); // reset the behaviour
    }

    replica.run("use " + replicatedDbName)
            .run("repl status " + replicatedDbName)
            .verifyResult("null");
    assertEquals(0, replica.getPrimaryKeyList(replicatedDbName, "t1").size());
    assertEquals(0, replica.getUniqueConstraintList(replicatedDbName, "t3").size());
    assertEquals(0, replica.getNotNullConstraintList(replicatedDbName, "t3").size());
    assertEquals(0, replica.getForeignKeyList(replicatedDbName, "t2").size());

    // Verify if create table is not called on table t1 but called for t2 and t3.
    // Also, allow constraint creation only on t1 and t3. Foreign key creation on t2 fails.
    callerVerifier = new BehaviourInjection<CallerArguments, Boolean>() {
      @Override
      public Boolean apply(CallerArguments args) {
        injectionPathCalled = true;
        if (!args.dbName.equalsIgnoreCase(replicatedDbName) || (args.funcName != null)) {
          LOG.warn("Verifier - DB: " + String.valueOf(args.dbName) + " Func: " + String.valueOf(args.funcName));
          return false;
        }
        if (args.constraintTblName != null) {
          LOG.warn("Verifier - Constraint Table: " + String.valueOf(args.constraintTblName));
          return (args.constraintTblName.equals("t1") || args.constraintTblName.equals("t3"));
        }
        return true;
      }
    };
    InjectableBehaviourObjectStore.setCallerVerifier(callerVerifier);

    try {
      // Retry with same dump with which it was already loaded should resume the bootstrap load.
      // This time, it fails when try to load the foreign key constraints. All other constraints are loaded.
      replica.loadFailure(replicatedDbName, primaryDbName, withConfigs);
      callerVerifier.assertInjectionsPerformed(true, false);
    } finally {
      InjectableBehaviourObjectStore.resetCallerVerifier(); // reset the behaviour
    }

    replica.run("use " + replicatedDbName)
            .run("repl status " + replicatedDbName)
            .verifyResult("null")
            .run("show tables")
            .verifyResults(new String[] { "t1", "t2", "t3" });
    assertEquals(2, replica.getPrimaryKeyList(replicatedDbName, "t1").size());
    assertEquals(1, replica.getUniqueConstraintList(replicatedDbName, "t3").size());
    assertEquals(1, replica.getNotNullConstraintList(replicatedDbName, "t3").size());
    assertEquals(0, replica.getForeignKeyList(replicatedDbName, "t2").size());

    // Verify if no create table/function calls. Only add foreign key constraints on table t2.
    callerVerifier = new BehaviourInjection<CallerArguments, Boolean>() {
      @Override
      public Boolean apply(CallerArguments args) {
        injectionPathCalled = true;
        if (!args.dbName.equalsIgnoreCase(replicatedDbName) || (args.tblName != null)) {
          LOG.warn("Verifier - DB: " + String.valueOf(args.dbName)
                  + " Table: " + String.valueOf(args.tblName));
          return false;
        }
        if (args.constraintTblName != null) {
          LOG.warn("Verifier - Constraint Table: " + String.valueOf(args.constraintTblName));
          return args.constraintTblName.equals("t2");
        }
        return true;
      }
    };
    InjectableBehaviourObjectStore.setCallerVerifier(callerVerifier);

    try {
      // Retry with same dump with which it was already loaded should resume the bootstrap load.
      // This time, it completes by adding just foreign key constraints for table t2.
      replica.load(replicatedDbName, primaryDbName);
      callerVerifier.assertInjectionsPerformed(true, false);
    } finally {
      InjectableBehaviourObjectStore.resetCallerVerifier(); // reset the behaviour
    }

    replica.run("use " + replicatedDbName)
            .run("repl status " + replicatedDbName)
            .verifyResult(tuple.lastReplicationId)
            .run("show tables")
            .verifyResults(new String[] { "t1", "t2", "t3" });
    assertEquals(2, replica.getPrimaryKeyList(replicatedDbName, "t1").size());
    assertEquals(1, replica.getUniqueConstraintList(replicatedDbName, "t3").size());
    assertEquals(1, replica.getNotNullConstraintList(replicatedDbName, "t3").size());
    assertEquals(2, replica.getForeignKeyList(replicatedDbName, "t2").size());
  }

  @Test
  public void testBootstrapReplLoadRetryAfterFailureForPartitions() throws Throwable {
    WarehouseInstance.Tuple tuple = primary
            .run("use " + primaryDbName)
            .run("create table t2 (place string) partitioned by (country string)")
            .run("insert into table t2 partition(country='india') values ('bangalore')")
            .run("insert into table t2 partition(country='uk') values ('london')")
            .run("insert into table t2 partition(country='us') values ('sfo')")
            .run("CREATE FUNCTION " + primaryDbName
                    + ".testFunctionOne as 'hivemall.tools.string.StopwordUDF' "
                    + "using jar  'ivy://io.github.myui:hivemall:0.4.0-2'")
            .dump(primaryDbName);

    // Inject a behavior where REPL LOAD failed when try to load table "t2" and partition "uk".
    // So, table "t2" will exist and partition "india" will exist, rest failed as operation failed.
    BehaviourInjection<List<Partition>, Boolean> addPartitionStub
            = new BehaviourInjection<List<Partition>, Boolean>() {
      @Override
      public Boolean apply(List<Partition> ptns) {
        for (Partition ptn : ptns) {
          if (ptn.getValues().get(0).equals("uk")) {
            injectionPathCalled = true;
            LOG.warn("####getPartition Stub called");
            return false;
          }
        }
        return true;
      }
    };
    InjectableBehaviourObjectStore.setAddPartitionsBehaviour(addPartitionStub);

    // Make sure that there's some order in which the objects are loaded.
    List<String> withConfigs = Arrays.asList("'hive.repl.approx.max.load.tasks'='1'",
            "'hive.in.repl.test.files.sorted'='true'",
      "'" + HiveConf.ConfVars.REPL_LOAD_PARTITIONS_WITH_DATA_COPY_BATCH_SIZE + "' = '1'");
    replica.loadFailure(replicatedDbName, primaryDbName, withConfigs);
    InjectableBehaviourObjectStore.resetAddPartitionModifier(); // reset the behaviour
    addPartitionStub.assertInjectionsPerformed(true, false);

    replica.run("use " + replicatedDbName)
            .run("repl status " + replicatedDbName)
            .verifyResult("null")
            .run("show tables")
            .verifyResults(new String[] {"t2" })
            .run("select country from t2 order by country")
            .verifyResults(Collections.singletonList("india"));

    // Verify if no create table calls. Add partitions and create function calls expected.
    BehaviourInjection<CallerArguments, Boolean> callerVerifier
            = new BehaviourInjection<CallerArguments, Boolean>() {
      @Nullable
      @Override
      public Boolean apply(@Nullable CallerArguments args) {
        if (!args.dbName.equalsIgnoreCase(replicatedDbName) || (args.tblName != null)) {
          injectionPathCalled = true;
          LOG.warn("Verifier - DB: " + String.valueOf(args.dbName)
                  + " Table: " + String.valueOf(args.tblName));
          return false;
        }
        return true;
      }
    };
    InjectableBehaviourObjectStore.setCallerVerifier(callerVerifier);

    try {
      // Retry with same dump with which it was already loaded should resume the bootstrap load.
      // This time, it completes by adding remaining partitions and function.
      replica.load(replicatedDbName, primaryDbName);
      callerVerifier.assertInjectionsPerformed(false, false);
    } finally {
      InjectableBehaviourObjectStore.resetCallerVerifier(); // reset the behaviour
    }

    replica.run("use " + replicatedDbName)
            .run("repl status " + replicatedDbName)
            .verifyResult(tuple.lastReplicationId)
            .run("show tables")
            .verifyResults(new String[] { "t2" })
            .run("select country from t2 order by country")
            .verifyResults(Arrays.asList("india", "uk", "us"))
            .run("show functions like '" + replicatedDbName + "%'")
            .verifyResult(replicatedDbName + ".testFunctionOne");
  }

  @Test
  public void testMoveOptimizationBootstrapReplLoadRetryAfterFailure() throws Throwable {
    String replicatedDbName_CM = replicatedDbName + "_CM";
    WarehouseInstance.Tuple tuple = primary
            .run("use " + primaryDbName)
            .run("create table t2 (place string) partitioned by (country string)")
            .run("insert into table t2 partition(country='india') values ('bangalore')")
            .dump(primaryDbName);

    testMoveOptimization(primaryDbName, replicatedDbName, replicatedDbName_CM, "t2",
            "ADD_PARTITION", tuple);
  }

  @Test
  public void testMoveOptimizationIncrementalFailureAfterCopyReplace() throws Throwable {
    String replicatedDbName_CM = replicatedDbName + "_CM";
    WarehouseInstance.Tuple tuple = primary.run("use " + primaryDbName)
            .run("create table t2 (place string) partitioned by (country string)")
            .run("insert into table t2 partition(country='india') values ('bangalore')")
            .run("create table t1 (place string) partitioned by (country string)")
            .dump(primaryDbName);
    replica.load(replicatedDbName, primaryDbName);
    //delete load ack to reuse the dump
    new Path(tuple.dumpLocation).getFileSystem(conf).delete(new Path(tuple.dumpLocation
            + Path.SEPARATOR + ReplUtils.REPL_HIVE_BASE_DIR + Path.SEPARATOR
            + LOAD_ACKNOWLEDGEMENT.toString()), true);

    replica.load(replicatedDbName_CM, primaryDbName);
    replica.run("alter database " + replicatedDbName + " set DBPROPERTIES ('" + SOURCE_OF_REPLICATION + "' = '1,2,3')")
        .run("alter database " + replicatedDbName_CM + " set DBPROPERTIES ('" + SOURCE_OF_REPLICATION + "' = '1,2,3')");

    tuple = primary.run("use " + primaryDbName)
            .run("insert overwrite table t1 select * from t2")
            .dump(primaryDbName, Collections.emptyList());

    testMoveOptimization(primaryDbName, replicatedDbName, replicatedDbName_CM, "t1", "ADD_PARTITION",
            tuple);
  }

  @Test
  public void testMoveOptimizationIncrementalFailureAfterCopy() throws Throwable {
    String replicatedDbName_CM = replicatedDbName + "_CM";
    WarehouseInstance.Tuple bootstrapDump = primary.run("use " + primaryDbName)
            .run("create table t2 (place string) partitioned by (country string)")
            .run("ALTER TABLE t2 ADD PARTITION (country='india')")
            .dump(primaryDbName);
    replica.load(replicatedDbName, primaryDbName);
    //delete load ack to reuse the dump
    new Path(bootstrapDump.dumpLocation).getFileSystem(conf).delete(new Path(bootstrapDump.dumpLocation
            + Path.SEPARATOR + ReplUtils.REPL_HIVE_BASE_DIR + Path.SEPARATOR + LOAD_ACKNOWLEDGEMENT.toString()), true);
    replica.load(replicatedDbName_CM, primaryDbName);
    replica.run("alter database " + replicatedDbName + " set DBPROPERTIES ('" + SOURCE_OF_REPLICATION + "' = '1,2,3')")
        .run("alter database " + replicatedDbName_CM + " set DBPROPERTIES ('" + SOURCE_OF_REPLICATION + "' = '1,2,3')");

    WarehouseInstance.Tuple tuple = primary.run("use " + primaryDbName)
            .run("insert into table t2 partition(country='india') values ('bangalore')")
            .dump(primaryDbName, Collections.emptyList());

    testMoveOptimization(primaryDbName, replicatedDbName, replicatedDbName_CM, "t2", "INSERT", tuple);
  }

  private void testMoveOptimization(String primaryDb, String replicaDb, String replicatedDbName_CM,
                                    String tbl,  String eventType, WarehouseInstance.Tuple tuple) throws Throwable {
    // fail add notification for given event type.
    BehaviourInjection<NotificationEvent, Boolean> callerVerifier
            = new BehaviourInjection<NotificationEvent, Boolean>() {
      @Override
      public Boolean apply(NotificationEvent entry) {
        if (entry.getEventType().equalsIgnoreCase(eventType) && entry.getTableName().equalsIgnoreCase(tbl)) {
          injectionPathCalled = true;
          LOG.warn("Verifier - DB: " + String.valueOf(entry.getDbName())
                  + " Table: " + String.valueOf(entry.getTableName())
                  + " Event: " + String.valueOf(entry.getEventType()));
          return false;
        }
        return true;
      }
    };

    InjectableBehaviourObjectStore.setAddNotificationModifier(callerVerifier);
    try {
      replica.loadFailure(replicaDb, primaryDbName);
    } finally {
      InjectableBehaviourObjectStore.resetAddNotificationModifier();
    }

    callerVerifier.assertInjectionsPerformed(true, false);
    replica.load(replicaDb, primaryDbName);

    replica.run("use " + replicaDb)
            .run("select country from " + tbl + " where country == 'india'")
            .verifyResults(Arrays.asList("india"));

    primary.run("use " + primaryDb)
            .run("drop table " + tbl);

    //delete load ack to reuse the dump
    new Path(tuple.dumpLocation).getFileSystem(conf).delete(new Path(tuple.dumpLocation
            + Path.SEPARATOR + ReplUtils.REPL_HIVE_BASE_DIR + Path.SEPARATOR
            + LOAD_ACKNOWLEDGEMENT.toString()), true);


    InjectableBehaviourObjectStore.setAddNotificationModifier(callerVerifier);
    try {
      replica.loadFailure(replicatedDbName_CM, primaryDbName);
    } finally {
      InjectableBehaviourObjectStore.resetAddNotificationModifier();
    }

    callerVerifier.assertInjectionsPerformed(true, false);
    replica.load(replicatedDbName_CM, primaryDbName);

    replica.run("use " + replicatedDbName_CM)
            .run("select country from " + tbl + " where country == 'india'")
            .verifyResults(Arrays.asList("india"))
            .run(" drop database if exists " + replicatedDbName_CM + " cascade");
  }

  private void compareDbProperties(Map<String, String> primaryDbProps, Map<String, String> replicaDbProps){
    for (Map.Entry<String, String> prop : primaryDbProps.entrySet()) {
      if (prop.getKey().equals(SOURCE_OF_REPLICATION)) {
        continue;
      }
      assertTrue(replicaDbProps.containsKey(prop.getKey()));
      assertTrue(replicaDbProps.get(prop.getKey()).equals(prop.getValue()));
    }
  }

  // This requires the tables are loaded in a fixed sorted order.
  @Test
  public void testBootstrapLoadRetryAfterFailureForAlterTable() throws Throwable {
    WarehouseInstance.Tuple tuple = primary
            .run("use " + primaryDbName)
            .run("create table t1 (place string)")
            .run("insert into table t1 values ('testCheck')")
            .run("create table t2 (place string) partitioned by (country string)")
            .run("insert into table t2 partition(country='china') values ('shenzhen')")
            .run("insert into table t2 partition(country='india') values ('banaglore')")
            .dump(primaryDbName);

    // fail setting ckpt directory property for table t1.
    BehaviourInjection<CallerArguments, Boolean> callerVerifier
            = new BehaviourInjection<CallerArguments, Boolean>() {
      @Nullable
      @Override
      public Boolean apply(@Nullable CallerArguments args) {
        if (args.tblName.equalsIgnoreCase("t1") && args.dbName.equalsIgnoreCase(replicatedDbName)) {
          injectionPathCalled = true;
          LOG.warn("Verifier - DB : " + args.dbName + " TABLE : " + args.tblName);
          return false;
        }
        return true;
      }
    };

    // Fail repl load before the ckpt proeprty is set for t1 and after it is set for t2. So in the next run, for
    // t2 it goes directly to partion load with no task for table tracker and for t1 it loads the table
    // again from start.
    InjectableBehaviourObjectStore.setAlterTableModifier(callerVerifier);
    try {
      replica.loadFailure(replicatedDbName, primaryDbName);
      callerVerifier.assertInjectionsPerformed(true, false);
    } finally {
      InjectableBehaviourObjectStore.resetAlterTableModifier();
    }

    // Retry with same dump with which it was already loaded should resume the bootstrap load. Make sure that table t1,
    // is loaded before t2. So that scope is set to table in first iteration for table t1. In the next iteration, it
    // loads only remaining partitions of t2, so that the table tracker has no tasks.

    Path baseDumpDir = new Path(primary.hiveConf.getVar(HiveConf.ConfVars.REPL_DIR));
    Path nonRecoverablePath = getNonRecoverablePath(baseDumpDir, primaryDbName);
    if(nonRecoverablePath != null){
      baseDumpDir.getFileSystem(primary.hiveConf).delete(nonRecoverablePath, true);
    }


    List<String> withConfigs = Arrays.asList("'hive.in.repl.test.files.sorted'='true'");
    replica.load(replicatedDbName, primaryDbName, withConfigs);

    replica.run("use " + replicatedDbName)
            .run("repl status " + replicatedDbName)
            .verifyResult(tuple.lastReplicationId)
            .run("select country from t2 order by country")
            .verifyResults(Arrays.asList("china", "india"));
  }

  /*
  Can't test complete replication as mini ranger is not supported
  Testing just the configs and no impact on existing replication
   */
  @Test
  public void testRangerReplication() throws Throwable {
    List<String> clause = Arrays.asList("'hive.repl.include.authorization.metadata'='true'",
        "'hive.in.test'='true'", "'hive.repl.handle.ranger.deny.policy'='true'");
    primary.run("use " + primaryDbName)
        .run("create table  acid_table (key int, value int) partitioned by (load_date date) " +
            "clustered by(key) into 2 buckets stored as orc tblproperties ('transactional'='true')")
        .run("create table table1 (i String)")
        .run("insert into table1 values (1)")
        .run("insert into table1 values (2)")
        .dump(primaryDbName, clause);

    replica.load(replicatedDbName, primaryDbName, clause)
        .run("use " + replicatedDbName)
        .run("show tables")
        .verifyResults(new String[] {"acid_table", "table1"})
        .run("select * from table1")
        .verifyResults(new String[] {"1", "2"});
  }

  @Test
  public void testHdfsNameserviceLazyCopy() throws Throwable {
    List<String> clause = getHdfsNameserviceClause();
    clause.add("'" + HiveConf.ConfVars.REPL_DUMP_METADATA_ONLY_FOR_EXTERNAL_TABLE.varname + "'='true'");
    primary.run("use " + primaryDbName)
            .run("create table  acid_table (key int, value int) partitioned by (load_time timestamp) " +
                    "clustered by(key) into 2 buckets stored as orc tblproperties ('transactional'='true')")
            .run("insert into acid_table partition(load_time = '2012-02-21 07:08:09.123') values(1,2)")
            .run("insert into acid_table partition(load_time = '2012-02-21 07:08:09.124') values(1,3)")
            .run("insert into acid_table partition(load_time = '2012-02-21 07:08:09.125') values(1,4)")
            .run("insert into acid_table partition(load_time = '2012-02-21 07:08:09.126') values(1,5)")
            .run("show partitions acid_table")
            .verifyResults(new String[] {
                    "load_time=2012-02-21 07%3A08%3A09.123",
                    "load_time=2012-02-21 07%3A08%3A09.124",
                    "load_time=2012-02-21 07%3A08%3A09.125",
                    "load_time=2012-02-21 07%3A08%3A09.126"})
            .run("create table table1 (i int)")
            .run("insert into table1 values (1)")
            .run("insert into table1 values (2)")
            .run("create external table ext_table1 (id int)")
            .run("insert into ext_table1 values (3)")
            .run("insert into ext_table1 values (4)")
            .dump(primaryDbName, clause);

    try{
      replica.load(replicatedDbName, primaryDbName, clause);
      Assert.fail("Expected the UnknownHostException to be thrown.");
    } catch (IllegalArgumentException ex) {
      assertTrue(ex.getMessage().contains("java.net.UnknownHostException: nsRemote"));
    }
  }

  @Test
  public void testHdfsNSLazyCopyBootStrapExtTbls() throws Throwable {
    List<String> clause = getHdfsNameserviceClause();
    clause.add("'" + HiveConf.ConfVars.REPL_DUMP_METADATA_ONLY_FOR_EXTERNAL_TABLE.varname + "'='false'");
    Tuple tuple = primary.run("use " + primaryDbName)
            .run("create external table ext_table1 (id int)")
            .run("insert into ext_table1 values (3)")
            .run("insert into ext_table1 values (4)")
            .run("create external table  ext_table2 (key int, value int) partitioned by (load_time timestamp)")
            .run("insert into ext_table2 partition(load_time = '2012-02-21 07:08:09.123') values(1,2)")
            .run("insert into ext_table2 partition(load_time = '2012-02-21 07:08:09.124') values(1,3)")
            .run("show partitions ext_table2")
            .verifyResults(new String[] {
                    "load_time=2012-02-21 07%3A08%3A09.123",
                    "load_time=2012-02-21 07%3A08%3A09.124"})
            .dump(primaryDbName, clause);

    ReplicationTestUtils.assertExternalFileList(Arrays.asList("ext_table1", "ext_table2"), tuple.dumpLocation, primary);
    //SecurityException expected from DirCopyTask
    try{
      replica.load(replicatedDbName, primaryDbName, clause);
      Assert.fail("Expected the UnknownHostException to be thrown.");
    } catch (SecurityException ex) {
      assertTrue(ex.getMessage().contains("java.net.UnknownHostException: nsRemote"));
    }
  }

  @Test
  public void testHdfsNameserviceLazyCopyIncr() throws Throwable {
    List<String> clause = getHdfsNameserviceClause();
    primary.run("use " + primaryDbName)
            .run("create table  acid_table (key int, value int) partitioned by (load_time timestamp) " +
                    "clustered by(key) into 2 buckets stored as orc tblproperties ('transactional'='true')")
            .run("create table table1 (i String)")
            .run("insert into table1 values (1)")
            .run("insert into table1 values (2)")
            .dump(primaryDbName);

    replica.load(replicatedDbName, primaryDbName, clause)
            .run("use " + replicatedDbName)
            .run("show tables")
            .verifyResults(new String[] {"acid_table", "table1"})
            .run("select * from table1")
            .verifyResults(new String[] {"1", "2"});

    primary.run("use " + primaryDbName)
            .run("insert into table1 values (3)")
            .run("insert into acid_table partition(load_time = '2012-02-21 07:08:09.123') values(1,2)")
            .run("insert into acid_table partition(load_time = '2012-02-21 07:08:09.124') values(1,3)")
            .run("insert into acid_table partition(load_time = '2012-02-21 07:08:09.125') values(1,4)")
            .run("insert into acid_table partition(load_time = '2012-02-21 07:08:09.126') values(1,5)")
            .run("show partitions acid_table")
            .verifyResults(new String[] {
                    "load_time=2012-02-21 07%3A08%3A09.123",
                    "load_time=2012-02-21 07%3A08%3A09.124",
                    "load_time=2012-02-21 07%3A08%3A09.125",
                    "load_time=2012-02-21 07%3A08%3A09.126"})
            .dump(primaryDbName, clause);
    try{
      replica.load(replicatedDbName, primaryDbName, clause);
      Assert.fail("Expected the UnknownHostException to be thrown.");
    } catch (IllegalArgumentException ex) {
      assertTrue(ex.getMessage().contains("java.net.UnknownHostException: nsRemote"));
    }
  }

  @Test
  public void testHdfsNSLazyCopyIncrExtTbls() throws Throwable {
    List<String> clause = getHdfsNameserviceClause();
    clause.add("'" + HiveConf.ConfVars.REPL_DUMP_METADATA_ONLY_FOR_EXTERNAL_TABLE.varname + "'='false'");

    primary.run("use " + primaryDbName)
            .run("create table  acid_table (key int, value int) partitioned by (load_time timestamp) " +
                    "clustered by(key) into 2 buckets stored as orc tblproperties ('transactional'='true')")
            .run("create table table1 (i String)")
            .run("insert into table1 values (1)")
            .run("insert into table1 values (2)")
            .dump(primaryDbName);

    replica.load(replicatedDbName, primaryDbName, clause)
            .run("use " + replicatedDbName)
            .run("show tables")
            .verifyResults(new String[] {"acid_table", "table1"})
            .run("select * from table1")
            .verifyResults(new String[] {"1", "2"});

    Tuple tuple = primary.run("use " + primaryDbName)
            .run("create external table ext_table1 (id int)")
            .run("insert into ext_table1 values (3)")
            .run("insert into ext_table1 values (4)")
            .run("create external table  ext_table2 (key int, value int) partitioned by (load_time timestamp)")
            .run("insert into ext_table2 partition(load_time = '2012-02-21 07:08:09.123') values(1,2)")
            .run("insert into ext_table2 partition(load_time = '2012-02-21 07:08:09.124') values(1,3)")
            .run("show partitions ext_table2")
            .verifyResults(new String[] {
                    "load_time=2012-02-21 07%3A08%3A09.123",
                    "load_time=2012-02-21 07%3A08%3A09.124"})
            .dump(primaryDbName, clause);

    ReplicationTestUtils.assertExternalFileList(Arrays.asList("ext_table1", "ext_table2"), tuple.dumpLocation, primary);
    //SecurityException expected from DirCopyTask
    try{
      replica.load(replicatedDbName, primaryDbName, clause);
      Assert.fail("Expected the UnknownHostException to be thrown.");
    } catch (SecurityException ex) {
      assertTrue(ex.getMessage().contains("java.net.UnknownHostException: nsRemote"));
    }
  }

  @Test
  public void testHdfsNameserviceWithDataCopy() throws Throwable {
    List<String> clause = getHdfsNameserviceClause();
    //NS replacement parameters has no effect when data is also copied to staging
    clause.add("'" + HiveConf.ConfVars.REPL_RUN_DATA_COPY_TASKS_ON_TARGET + "'='false'");
    primary.run("use " + primaryDbName)
            .run("create table  acid_table (key int, value int) partitioned by (load_date date) " +
                    "clustered by(key) into 2 buckets stored as orc tblproperties ('transactional'='true')")
            .run("create table table1 (i String)")
            .run("insert into table1 values (1)")
            .run("insert into table1 values (2)")
            .dump(primaryDbName, clause);
    replica.load(replicatedDbName, primaryDbName, clause)
            .run("use " + replicatedDbName)
            .run("show tables")
            .verifyResults(new String[] {"acid_table", "table1"})
            .run("select * from table1")
            .verifyResults(new String[] {"1", "2"});

    primary.run("use " + primaryDbName)
            .run("insert into table1 values (3)")
            .dump(primaryDbName, clause);
    replica.load(replicatedDbName, primaryDbName, clause)
            .run("use " + replicatedDbName)
            .run("show tables")
            .verifyResults(new String[]{"acid_table", "table1"})
            .run("select * from table1")
            .verifyResults(new String[]{"1", "2", "3"});

    clause.add("'" + HiveConf.ConfVars.REPL_DUMP_METADATA_ONLY_FOR_EXTERNAL_TABLE.varname + "'='false'");
    primary.run("use " + primaryDbName)
            .run("create external table ext_table1 (id int)")
            .run("insert into ext_table1 values (3)")
            .run("insert into ext_table1 values (4)")
            .run("create external table  ext_table2 (key int, value int) partitioned by (load_time timestamp)")
            .run("insert into ext_table2 partition(load_time = '2012-02-21 07:08:09.123') values(1,2)")
            .run("insert into ext_table2 partition(load_time = '2012-02-21 07:08:09.124') values(1,3)")
            .run("show partitions ext_table2")
            .verifyResults(new String[]{
                    "load_time=2012-02-21 07%3A08%3A09.123",
                    "load_time=2012-02-21 07%3A08%3A09.124"})
            .dump(primaryDbName, clause);

    replica.load(replicatedDbName, primaryDbName, clause)
            .run("use " + replicatedDbName)
            .run("show tables")
            .verifyResults(new String[]{"acid_table", "table1", "ext_table1", "ext_table2"})
            .run("select * from ext_table1")
            .verifyResults(new String[]{"3", "4"})
            .run("select value from ext_table2")
            .verifyResults(new String[]{"2", "3"});
  }

  @Test
  public void testReplWithRetryDisabledIterators() throws Throwable {
    List<String> clause = new ArrayList<>();
    //NS replacement parameters has no effect when data is also copied to staging
    clause.add("'" + HiveConf.ConfVars.REPL_RUN_DATA_COPY_TASKS_ON_TARGET + "'='false'");
    clause.add("'" + HiveConf.ConfVars.REPL_COPY_FILE_LIST_ITERATOR_RETRY + "'='false'");
    WarehouseInstance.Tuple tuple = primary.run("use " + primaryDbName)
            .run("create table  acid_table (key int, value int) partitioned by (load_date date) " +
                    "clustered by(key) into 2 buckets stored as orc tblproperties ('transactional'='true')")
            .run("create table table1 (i String)")
            .run("insert into table1 values (1)")
            .run("insert into table1 values (2)")
            .dump(primaryDbName, clause);
    ReplicationTestUtils.assertFalseExternalFileList(primary, tuple.dumpLocation);
    replica.load(replicatedDbName, primaryDbName, clause)
            .run("use " + replicatedDbName)
            .run("show tables")
            .verifyResults(new String[] {"acid_table", "table1"})
            .run("select * from table1")
            .verifyResults(new String[] {"1", "2"});

    tuple = primary.run("use " + primaryDbName)
            .run("insert into table1 values (3)")
            .dump(primaryDbName, clause);
    ReplicationTestUtils.assertFalseExternalFileList(primary, tuple.dumpLocation);
    replica.load(replicatedDbName, primaryDbName, clause)
            .run("use " + replicatedDbName)
            .run("show tables")
            .verifyResults(new String[]{"acid_table", "table1"})
            .run("select * from table1")
            .verifyResults(new String[]{"1", "2", "3"});

    clause.add("'" + HiveConf.ConfVars.REPL_DUMP_METADATA_ONLY_FOR_EXTERNAL_TABLE.varname + "'='false'");
    tuple = primary.run("use " + primaryDbName)
            .run("create external table ext_table1 (id int)")
            .run("insert into ext_table1 values (3)")
            .run("insert into ext_table1 values (4)")
            .run("create external table  ext_table2 (key int, value int) partitioned by (load_time timestamp)")
            .run("insert into ext_table2 partition(load_time = '2012-02-21 07:08:09.123') values(1,2)")
            .run("insert into ext_table2 partition(load_time = '2012-02-21 07:08:09.124') values(1,3)")
            .run("show partitions ext_table2")
            .verifyResults(new String[]{
                    "load_time=2012-02-21 07%3A08%3A09.123",
                    "load_time=2012-02-21 07%3A08%3A09.124"})
            .dump(primaryDbName, clause);
    ReplicationTestUtils.assertExternalFileList(Arrays.asList("ext_table1", "ext_table2"), tuple.dumpLocation, primary);
    replica.load(replicatedDbName, primaryDbName, clause)
            .run("use " + replicatedDbName)
            .run("show tables")
            .verifyResults(new String[]{"acid_table", "table1", "ext_table1", "ext_table2"})
            .run("select * from ext_table1")
            .verifyResults(new String[]{"3", "4"})
            .run("select value from ext_table2")
            .verifyResults(new String[]{"2", "3"});
  }

  @Test
  public void testCreateFunctionWithHdfsNameservice() throws Throwable {
    Path identityUdfLocalPath = new Path("../../data/files/identity_udf.jar");
    Path identityUdf1HdfsPath = new Path(primary.functionsRoot, "idFunc1" + File.separator + "identity_udf1.jar");
    setupUDFJarOnHDFS(identityUdfLocalPath, identityUdf1HdfsPath);
    List<String> clause = getHdfsNameserviceClause();
    primary.run("CREATE FUNCTION " + primaryDbName
            + ".idFunc1 as 'IdentityStringUDF' "
            + "using jar  '" + identityUdf1HdfsPath.toString() + "'");
    WarehouseInstance.Tuple bootStrapDump = primary.dump(primaryDbName, clause);
    try{
      replica.load(replicatedDbName, primaryDbName, clause);
      Assert.fail("Expected the UnknownHostException to be thrown.");
    } catch (IllegalArgumentException ex) {
      assertTrue(ex.getMessage().contains("java.net.UnknownHostException: nsRemote"));
    }
  }

  @Test
  public void testRangerReplicationRetryExhausted() throws Throwable {
    List<String> clause = Arrays.asList("'" + HiveConf.ConfVars.REPL_INCLUDE_AUTHORIZATION_METADATA + "'='true'",
      "'" + HiveConf.ConfVars.REPL_RETRY_INTIAL_DELAY + "'='1s'", "'" + HiveConf.ConfVars.REPL_RETRY_TOTAL_DURATION
        + "'='30s'", "'" + HiveConf.ConfVars.HIVE_IN_TEST_REPL + "'='false'", "'" + HiveConf.ConfVars.HIVE_IN_TEST
        + "'='false'");
    List<String> testClause = Arrays.asList("'hive.repl.include.authorization.metadata'='true'",
      "'hive.in.test'='true'");
    try {
      primary.run("use " + primaryDbName)
        .run("create table  acid_table (key int, value int) partitioned by (load_date date) " +
          "clustered by(key) into 2 buckets stored as orc tblproperties ('transactional'='true')")
        .run("create table table1 (i String)")
        .run("insert into table1 values (1)")
        .run("insert into table1 values (2)")
        .dump(primaryDbName, clause);
      Assert.fail();
    } catch (Exception e) {
      Assert.assertEquals(ErrorMsg.REPL_RETRY_EXHAUSTED.getErrorCode(),
        ErrorMsg.getErrorMsg(e.getMessage()).getErrorCode());
    }
    //This is now non recoverable error
    try {
      primary.dump(primaryDbName, clause);
      Assert.fail();
    } catch (Exception e) {
      assertEquals(ErrorMsg.REPL_FAILED_WITH_NON_RECOVERABLE_ERROR.getErrorCode(),
        ErrorMsg.getErrorMsg(e.getMessage()).getErrorCode());
    }
    //Delete non recoverable marker to fix this
    Path baseDumpDir = new Path(primary.hiveConf.getVar(HiveConf.ConfVars.REPL_DIR));
    Path nonRecoverablePath = getNonRecoverablePath(baseDumpDir, primaryDbName);
    Assert.assertNotNull(nonRecoverablePath);
    baseDumpDir.getFileSystem(primary.hiveConf).delete(nonRecoverablePath, true);
    //This should pass as non recoverable marker removed and valid configs present.
    primary.dump(primaryDbName, testClause);
  }

  /*
  Can't test complete replication as mini ranger is not supported
  Testing just the configs and no impact on existing replication
 */
  @Test
  public void testFailureUnsupportedAuthorizerReplication() throws Throwable {
    List<String> clause = Arrays.asList("'hive.repl.include.authorization.metadata'='true'",
        "'hive.in.test'='true'", "'hive.repl.authorization.provider.service'='sentry'");
    primary.run("use " + primaryDbName)
        .run("create table  acid_table (key int, value int) partitioned by (load_date date) " +
            "clustered by(key) into 2 buckets stored as orc tblproperties ('transactional'='true')")
        .run("create table table1 (i String)")
        .run("insert into table1 values (1)")
        .run("insert into table1 values (2)");
    try {
      primary.dump(primaryDbName, clause);
      Assert.fail();
    } catch (SemanticException e) {
      assertEquals("Invalid config error : Authorizer sentry not supported for replication  " +
        "for ranger service.", e.getMessage());
      assertEquals(ErrorMsg.REPL_INVALID_CONFIG_FOR_SERVICE.getErrorCode(),
        ErrorMsg.getErrorMsg(e.getMessage()).getErrorCode());
    }
    //This is now non recoverable error
    try {
      primary.dump(primaryDbName, clause);
      Assert.fail();
    } catch (Exception e) {
      assertEquals(ErrorMsg.REPL_FAILED_WITH_NON_RECOVERABLE_ERROR.getErrorCode(),
        ErrorMsg.getErrorMsg(e.getMessage()).getErrorCode());
    }
    try {
      replica.load(replicatedDbName, primaryDbName, clause);
      Assert.fail();
    } catch (Exception e) {
      assertEquals(ErrorMsg.REPL_FAILED_WITH_NON_RECOVERABLE_ERROR.getErrorCode(),
        ErrorMsg.getErrorMsg(e.getMessage()).getErrorCode());
    }
    //Delete non recoverable marker to fix this
    Path baseDumpDir = new Path(primary.hiveConf.getVar(HiveConf.ConfVars.REPL_DIR));
    Path nonRecoverablePath = getNonRecoverablePath(baseDumpDir, primaryDbName);
    Assert.assertNotNull(nonRecoverablePath);
    baseDumpDir.getFileSystem(primary.hiveConf).delete(nonRecoverablePath, true);
    Assert.assertFalse(baseDumpDir.getFileSystem(primary.hiveConf).exists(nonRecoverablePath));
    //This should pass as non recoverable marker removed and valid configs present.
    WarehouseInstance.Tuple dump = primary.dump(primaryDbName);
    String stackTrace = null;
    try {
      replica.load(replicatedDbName, primaryDbName, clause);
    } catch (Exception e) {
      Assert.assertEquals(ErrorMsg.REPL_INVALID_CONFIG_FOR_SERVICE.getErrorCode(),
        ErrorMsg.getErrorMsg(e.getMessage()).getErrorCode());
      stackTrace = ExceptionUtils.getStackTrace(e);
    }
    //This is now non recoverable error
    try {
      replica.load(replicatedDbName, primaryDbName, clause);
      Assert.fail();
    } catch (Exception e) {
      assertEquals(ErrorMsg.REPL_FAILED_WITH_NON_RECOVERABLE_ERROR.getErrorCode(),
        ErrorMsg.getErrorMsg(e.getMessage()).getErrorCode());
    }
    try {
      replica.dump(primaryDbName, clause);
      Assert.fail();
    } catch (Exception e) {
      assertEquals(ErrorMsg.REPL_FAILED_WITH_NON_RECOVERABLE_ERROR.getErrorCode(),
        ErrorMsg.getErrorMsg(e.getMessage()).getErrorCode());
    }
    nonRecoverablePath = new Path(dump.dumpLocation, NON_RECOVERABLE_MARKER.toString());
    Assert.assertNotNull(nonRecoverablePath);
    //check non recoverable stack trace
    String actualStackTrace = readStackTrace(nonRecoverablePath, primary.hiveConf);
    Assert.assertEquals(stackTrace, actualStackTrace);
    //Delete non recoverable marker to fix this
    baseDumpDir.getFileSystem(primary.hiveConf).delete(nonRecoverablePath, true);
    //This should pass now
    replica.load(replicatedDbName, primaryDbName)
      .run("use " + replicatedDbName)
      .run("show tables")
      .verifyResults(new String[] {"acid_table", "table1"})
      .run("select * from table1")
      .verifyResults(new String[] {"1", "2"});
  }

  private String readStackTrace(Path nonRecoverablePath, HiveConf conf) {
    try {
      FileSystem fs = FileSystem.get(conf);
      FSDataInputStream in = fs.open(nonRecoverablePath);
      BufferedReader bufferedReader = new BufferedReader(
                new InputStreamReader(in, StandardCharsets.UTF_8));
      String line = null;
      StringBuilder builder = new StringBuilder();
      while ((line=bufferedReader.readLine())!=null){
        builder.append(line);
        builder.append("\n");
      }
      try {
        in.close();
      } catch (IOException e) {
        //Ignore
      }
      return builder.toString();
    } catch (IOException e) {
      return null;
    }
  }

  private Path getNonRecoverablePath(Path dumpDir, String dbName) throws IOException {
    Path dumpPath = new Path(dumpDir,
      Base64.getEncoder().encodeToString(dbName.toLowerCase()
        .getBytes(StandardCharsets.UTF_8.name())));
    FileSystem fs = dumpPath.getFileSystem(conf);
    if (fs.exists(dumpPath)) {
      FileStatus[] statuses = fs.listStatus(dumpPath);
      if (statuses.length > 0) {
        return new Path(statuses[0].getPath(), NON_RECOVERABLE_MARKER.toString());
      }
    }
    return null;
  }

  //Testing just the configs and no impact on existing replication
  @Test
  public void testAtlasReplication() throws Throwable {
    Map<String, String> confMap = defaultAtlasConfMap();
    primary.run("use " + primaryDbName)
            .run("create table  acid_table (key int, value int) partitioned by (load_date date) " +
                    "clustered by(key) into 2 buckets stored as orc tblproperties ('transactional'='true')")
            .run("create table table1 (i String)")
            .run("insert into table1 values (1)")
            .run("insert into table1 values (2)")
            .dump(primaryDbName, getAtlasClause(defaultAtlasConfMap()));
    verifyAtlasMetadataPresent();

    confMap.remove("hive.repl.atlas.replicatedto");
    replica.load(replicatedDbName, primaryDbName, getAtlasClause(confMap))
            .run("use " + replicatedDbName)
            .run("show tables")
            .verifyResults(new String[] {"acid_table", "table1"})
            .run("select * from table1")
            .verifyResults(new String[] {"1", "2"});
  }

  @Test
  public void testAtlasMissingConfigs() throws Throwable {
    primary.run("use " + primaryDbName)
            .run("create table  acid_table (key int, value int) partitioned by (load_date date) " +
                    "clustered by(key) into 2 buckets stored as orc tblproperties ('transactional'='true')")
            .run("create table table1 (i String)")
            .run("insert into table1 values (1)")
            .run("insert into table1 values (2)");
    Map<String, String> confMap = new HashMap<>();
    confMap.put(HiveConf.ConfVars.HIVE_IN_TEST.varname, "true");
    confMap.put(HiveConf.ConfVars.REPL_INCLUDE_ATLAS_METADATA.varname, "true");
    ensureFailedReplOperation(getAtlasClause(confMap), HiveConf.ConfVars.REPL_ATLAS_ENDPOINT.varname, true);
    ensureFailedAdminRepl(getAtlasClause(confMap), true);
    //Delete non recoverable marker to fix this
    Path baseDumpDir = new Path(primary.hiveConf.getVar(HiveConf.ConfVars.REPL_DIR));
    Path nonRecoverablePath = getNonRecoverablePath(baseDumpDir, primaryDbName);
    Assert.assertNotNull(nonRecoverablePath);
    baseDumpDir.getFileSystem(primary.hiveConf).delete(nonRecoverablePath, true);
    confMap.put(HiveConf.ConfVars.REPL_ATLAS_ENDPOINT.varname, "InvalidURL:atlas");
    ensureInvalidUrl(getAtlasClause(confMap), HiveConf.ConfVars.REPL_ATLAS_ENDPOINT.varname, true);
    confMap.put(HiveConf.ConfVars.REPL_ATLAS_ENDPOINT.varname, "http://localhost:21000/atlas");
    ensureFailedReplOperation(getAtlasClause(confMap), HiveConf.ConfVars.REPL_ATLAS_REPLICATED_TO_DB.varname, true);
    ensureFailedAdminRepl(getAtlasClause(confMap), true);
    //Delete non recoverable marker to fix this
    baseDumpDir = new Path(primary.hiveConf.getVar(HiveConf.ConfVars.REPL_DIR));
    nonRecoverablePath = getNonRecoverablePath(baseDumpDir, primaryDbName);
    Assert.assertNotNull(nonRecoverablePath);
    baseDumpDir.getFileSystem(primary.hiveConf).delete(nonRecoverablePath, true);
    confMap.put(HiveConf.ConfVars.REPL_ATLAS_REPLICATED_TO_DB.varname, replicatedDbName);
    ensureFailedReplOperation(getAtlasClause(confMap), HiveConf.ConfVars.REPL_SOURCE_CLUSTER_NAME.varname, true);
    ensureFailedAdminRepl(getAtlasClause(confMap), true);
    //Delete non recoverable marker to fix this
    baseDumpDir = new Path(primary.hiveConf.getVar(HiveConf.ConfVars.REPL_DIR));
    nonRecoverablePath = getNonRecoverablePath(baseDumpDir, primaryDbName);
    Assert.assertNotNull(nonRecoverablePath);
    baseDumpDir.getFileSystem(primary.hiveConf).delete(nonRecoverablePath, true);
    confMap.put(HiveConf.ConfVars.REPL_SOURCE_CLUSTER_NAME.varname, "cluster0");
    ensureFailedReplOperation(getAtlasClause(confMap), HiveConf.ConfVars.REPL_TARGET_CLUSTER_NAME.varname, true);
    ensureFailedAdminRepl(getAtlasClause(confMap), true);
    //Delete non recoverable marker to fix this
    baseDumpDir = new Path(primary.hiveConf.getVar(HiveConf.ConfVars.REPL_DIR));
    nonRecoverablePath = getNonRecoverablePath(baseDumpDir, primaryDbName);
    Assert.assertNotNull(nonRecoverablePath);
    baseDumpDir.getFileSystem(primary.hiveConf).delete(nonRecoverablePath, true);
    confMap.put(HiveConf.ConfVars.REPL_TARGET_CLUSTER_NAME.varname, "cluster1");
    primary.dump(primaryDbName, getAtlasClause(confMap));
    verifyAtlasMetadataPresent();

    confMap.clear();
    confMap.put(HiveConf.ConfVars.HIVE_IN_TEST.varname, "true");
    confMap.put(HiveConf.ConfVars.REPL_INCLUDE_ATLAS_METADATA.varname, "true");
    ensureFailedReplOperation(getAtlasClause(confMap), HiveConf.ConfVars.REPL_ATLAS_ENDPOINT.varname, false);
    ensureFailedAdminRepl(getAtlasClause(confMap), false);
    //Delete non recoverable marker to fix this
    baseDumpDir = new Path(primary.hiveConf.getVar(HiveConf.ConfVars.REPL_DIR));
    nonRecoverablePath = getNonRecoverablePath(baseDumpDir, primaryDbName);
    Assert.assertNotNull(nonRecoverablePath);
    baseDumpDir.getFileSystem(primary.hiveConf).delete(nonRecoverablePath, true);
    confMap.put(HiveConf.ConfVars.REPL_ATLAS_ENDPOINT.varname, "InvalidURL:atlas");
    ensureInvalidUrl(getAtlasClause(confMap), HiveConf.ConfVars.REPL_ATLAS_ENDPOINT.varname, false);
    confMap.put(HiveConf.ConfVars.REPL_ATLAS_ENDPOINT.varname, "http://localhost:21000/atlas");
    ensureFailedReplOperation(getAtlasClause(confMap), HiveConf.ConfVars.REPL_SOURCE_CLUSTER_NAME.varname, false);
    ensureFailedAdminRepl(getAtlasClause(confMap), false);
    //Delete non recoverable marker to fix this
    baseDumpDir = new Path(primary.hiveConf.getVar(HiveConf.ConfVars.REPL_DIR));
    nonRecoverablePath = getNonRecoverablePath(baseDumpDir, primaryDbName);
    Assert.assertNotNull(nonRecoverablePath);
    baseDumpDir.getFileSystem(primary.hiveConf).delete(nonRecoverablePath, true);
    confMap.put(HiveConf.ConfVars.REPL_SOURCE_CLUSTER_NAME.varname, "cluster0");
    ensureFailedReplOperation(getAtlasClause(confMap), HiveConf.ConfVars.REPL_TARGET_CLUSTER_NAME.varname, false);
    ensureFailedAdminRepl(getAtlasClause(confMap), false);
    //Delete non recoverable marker to fix this
    baseDumpDir = new Path(primary.hiveConf.getVar(HiveConf.ConfVars.REPL_DIR));
    nonRecoverablePath = getNonRecoverablePath(baseDumpDir, primaryDbName);
    Assert.assertNotNull(nonRecoverablePath);
    baseDumpDir.getFileSystem(primary.hiveConf).delete(nonRecoverablePath, true);
    confMap.put(HiveConf.ConfVars.REPL_TARGET_CLUSTER_NAME.varname, "cluster1");
    primary.load(replicatedDbName, primaryDbName, getAtlasClause(confMap));
  }

  private void ensureFailedAdminRepl(List<String> clause, boolean dump) throws Throwable {
    try {
      if (dump) {
        primary.dump(primaryDbName, clause);
      } else {
        primary.load(replicatedDbName, primaryDbName, clause);
      }
      Assert.fail();
    } catch (SemanticException e) {
      assertEquals(ErrorMsg.REPL_FAILED_WITH_NON_RECOVERABLE_ERROR.getErrorCode(),
        ErrorMsg.getErrorMsg(e.getMessage()).getErrorCode());
    }
  }

  @Test
  public void testReplicationUtils() throws Throwable {
    Path testPath = new Path("/tmp/testReplicationUtils" + System.currentTimeMillis());
    DistributedFileSystem fs = primary.miniDFSCluster.getFileSystem();
    fs.mkdirs(testPath);
    Path filePath1 = new Path(testPath, "file1");
    fs.setQuota(testPath, QUOTA_DONT_SET, 0);
    try {
      Utils.writeOutput("abc", filePath1, conf);
      fail("Expected exception due to quota violation");
    } catch (Exception e) {
      // Expected.
    }

    // Remove the quota & retry, this time it should be successful.
    fs.setQuota(testPath, QUOTA_DONT_SET, QUOTA_RESET);
    Utils.writeOutput("abc" + Utilities.newLineCode, filePath1, conf);

    // Check the contents of the file are written correctly.
    try (FSDataInputStream stream = fs.open(filePath1)) {
      assertEquals("abc" + Utilities.newLineCode + "\n", IOUtils.toString(stream, Charset.defaultCharset()));
    }

    // Check the Utils with writing a list of entries
    Path filePath2 = new Path(testPath, "file2");
    fs.setQuota(testPath, QUOTA_DONT_SET, 0);
    List<List<String>> data = Arrays.asList(Arrays.asList("a", "b"));
    try {
      Utils.writeOutput(data, filePath2, conf, true);
      fail("Expected exception due to quota violation");
    } catch (Exception e) {
      // Expected.
    }

    // Remove the quota & retry, this time it should be successful.
    fs.setQuota(testPath, QUOTA_DONT_SET, QUOTA_RESET);

    // Write the contents.
    Utils.writeOutput(data, filePath2, conf, true);

    // Check the contents of the file are written correctly.
    try (FSDataInputStream stream = fs.open(filePath2)) {
      assertEquals("a" + "\t" + "b" + "\n", IOUtils.toString(stream,
          Charset.defaultCharset()));
    }
  }

  private void ensureInvalidUrl(List<String> atlasClause, String endpoint, boolean dump) throws Throwable {
    try {
      if (dump) {
        primary.dump(primaryDbName, atlasClause);
      } else {
        primary.load(replicatedDbName, primaryDbName, atlasClause);
      }
    } catch (MalformedURLException e) {
      return;
    }
    Assert.fail("Atlas endpoint is invalid and but test didn't fail:" + endpoint);
  }

  private void verifyAtlasMetadataPresent() throws IOException {
    Path dbReplDir = new Path(primary.repldDir,
            Base64.getEncoder().encodeToString(primaryDbName.toLowerCase().getBytes(StandardCharsets.UTF_8.name())));
    FileSystem fs = FileSystem.get(dbReplDir.toUri(), primary.getConf());
    assertTrue(fs.exists(dbReplDir));
    FileStatus[] dumpRoots = fs.listStatus(dbReplDir);
    assert(dumpRoots.length == 1);
    Path dumpRoot = dumpRoots[0].getPath();
    assertTrue("Hive dump root doesn't exist", fs.exists(new Path(dumpRoot, ReplUtils.REPL_HIVE_BASE_DIR)));
    Path atlasDumpRoot = new Path(dumpRoot, ReplUtils.REPL_ATLAS_BASE_DIR);
    assertTrue("Atlas dump root doesn't exist", fs.exists(atlasDumpRoot));
    assertTrue("Atlas export file doesn't exist",
            fs.exists(new Path(atlasDumpRoot, ReplUtils.REPL_ATLAS_EXPORT_FILE_NAME)));
    assertTrue("Atlas dump metadata doesn't exist",
            fs.exists(new Path(atlasDumpRoot, EximUtil.METADATA_NAME)));
    BufferedReader br = null;
    try {
      br = new BufferedReader(new InputStreamReader(
              fs.open(new Path(atlasDumpRoot, EximUtil.METADATA_NAME)), Charset.defaultCharset()));
      String[] lineContents = br.readLine().split("\t", 5);
      assertEquals(primary.hiveConf.get("fs.defaultFS"), lineContents[0]);
      assertEquals(0, Long.parseLong(lineContents[1]));
    } finally {
      if (br != null) {
        br.close();
      }
    }
  }

  private void ensureFailedReplOperation(List<String> clause, String conf, boolean dump) throws Throwable {
    try {
      if (dump) {
        primary.dump(primaryDbName, clause);
      } else {
        primary.load(replicatedDbName, primaryDbName, clause);
      }
      Assert.fail(conf + " is mandatory config for Atlas metadata replication but it didn't fail.");
    } catch (SemanticException e) {
      assertEquals(e.getMessage(), ("Invalid config error : " + conf
        + " is mandatory config for Atlas metadata replication for atlas service."));
    }
  }

  private Map<String, String> defaultAtlasConfMap() {
    Map<String, String> confMap = new HashMap<>();
    confMap.put(HiveConf.ConfVars.HIVE_IN_TEST.varname, "true");
    confMap.put(HiveConf.ConfVars.REPL_INCLUDE_ATLAS_METADATA.varname, "true");
    confMap.put(HiveConf.ConfVars.REPL_ATLAS_ENDPOINT.varname, "http://localhost:21000/atlas");
    confMap.put(HiveConf.ConfVars.REPL_ATLAS_REPLICATED_TO_DB.varname, replicatedDbName);
    confMap.put(HiveConf.ConfVars.REPL_SOURCE_CLUSTER_NAME.varname, "cluster0");
    confMap.put(HiveConf.ConfVars.REPL_TARGET_CLUSTER_NAME.varname, "cluster1");
    return confMap;
  }

  private List<String> getAtlasClause(Map<String, String> confMap) {
    List confList = new ArrayList();
    for (Map.Entry<String, String> entry:confMap.entrySet()) {
      confList.add(quote(entry.getKey()) + "=" + quote(entry.getValue()));
    }
    return confList;
  }

  private String quote(String str) {
    return "'" + str + "'";
  }

  private void setupUDFJarOnHDFS(Path identityUdfLocalPath, Path identityUdfHdfsPath) throws IOException {
    FileSystem fs = primary.miniDFSCluster.getFileSystem();
    fs.copyFromLocalFile(identityUdfLocalPath, identityUdfHdfsPath);
  }

  private List<String> getHdfsNameserviceClause() {
    List<String> withClause = new ArrayList<>();
    withClause.add("'" + HiveConf.ConfVars.REPL_HA_DATAPATH_REPLACE_REMOTE_NAMESERVICE.varname + "'='true'");
    withClause.add("'" + HiveConf.ConfVars.REPL_HA_DATAPATH_REPLACE_REMOTE_NAMESERVICE_NAME.varname + "'='"
            + NS_REMOTE + "'");
    return withClause;
  }

  private void assertFunctionJarsOnTarget(String functionName, List<String> expectedJars) throws IOException {
    //correct location of jars on target is functionRoot/dbName/funcName/nanoTs/jarFile
    FileStatus[] fileStatuses = replica.miniDFSCluster.getFileSystem()
            .globStatus(new Path(replica.functionsRoot + "/" +
                    replicatedDbName.toLowerCase() + "/" + functionName.toLowerCase() + "/*/*")
            );
    assertEquals(fileStatuses.length, expectedJars.size());
    List<String> jars = new ArrayList<>();
    for (FileStatus fileStatus : fileStatuses) {
      jars.add(fileStatus.getPath().getName());
    }
    assertThat(jars, containsInAnyOrder(expectedJars.toArray()));

    //confirm no jars created as directories
    for (FileStatus jarStatus : fileStatuses) {
      assert(!jarStatus.isDirectory());
    }
  }
}
