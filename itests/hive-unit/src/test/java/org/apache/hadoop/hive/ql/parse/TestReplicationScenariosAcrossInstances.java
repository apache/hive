/*
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at
  <p>
  http://www.apache.org/licenses/LICENSE-2.0
  <p>
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
 */
package org.apache.hadoop.hive.ql.parse;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hive.ql.parse.repl.PathBuilder;
import org.apache.hadoop.hive.ql.util.DependencyResolver;
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
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

public class TestReplicationScenariosAcrossInstances {
  @Rule
  public final TestName testName = new TestName();

  @Rule
  public TestRule replV1BackwardCompat;

  protected static final Logger LOG = LoggerFactory.getLogger(TestReplicationScenarios.class);

  private static WarehouseInstance primary, replica;

  @BeforeClass
  public static void classLevelSetup() throws Exception {
    Configuration conf = new Configuration();
    conf.set("dfs.client.use.datanode.hostname", "true");
    MiniDFSCluster miniDFSCluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).build();
    primary = new WarehouseInstance(LOG, miniDFSCluster);
    replica = new WarehouseInstance(LOG, miniDFSCluster);
  }

  @AfterClass
  public static void classLevelTearDown() throws IOException {
    primary.close();
    replica.close();
  }

  private String primaryDbName, replicatedDbName;

  @Before
  public void setup() throws Throwable {
    replV1BackwardCompat = primary.getReplivationV1CompatRule(new ArrayList<>());
    primaryDbName = testName.getMethodName() + "_" + +System.currentTimeMillis();
    replicatedDbName = "replicated_" + primaryDbName;
    primary.run("create database " + primaryDbName);
  }

  @Test
  public void testCreateFunctionIncrementalReplication() throws Throwable {
    WarehouseInstance.Tuple bootStrapDump = primary.dump(primaryDbName, null);
    replica.load(replicatedDbName, bootStrapDump.dumpLocation)
        .run("REPL STATUS " + replicatedDbName)
        .verify(bootStrapDump.lastReplicationId);

    primary.run("CREATE FUNCTION " + primaryDbName
        + ".testFunction as 'hivemall.tools.string.StopwordUDF' "
        + "using jar  'ivy://io.github.myui:hivemall:0.4.0-2'");

    WarehouseInstance.Tuple incrementalDump =
        primary.dump(primaryDbName, bootStrapDump.lastReplicationId);
    replica.load(replicatedDbName, incrementalDump.dumpLocation)
        .run("REPL STATUS " + replicatedDbName)
        .verify(incrementalDump.lastReplicationId)
        .run("SHOW FUNCTIONS LIKE '" + replicatedDbName + "*'")
        .verify(replicatedDbName + ".testFunction");

    // Test the idempotent behavior of CREATE FUNCTION
    replica.load(replicatedDbName, incrementalDump.dumpLocation)
            .run("REPL STATUS " + replicatedDbName)
            .verify(incrementalDump.lastReplicationId)
            .run("SHOW FUNCTIONS LIKE '" + replicatedDbName + "*'")
            .verify(replicatedDbName + ".testFunction");
  }

  @Test
  public void testDropFunctionIncrementalReplication() throws Throwable {
    primary.run("CREATE FUNCTION " + primaryDbName
        + ".testFunction as 'hivemall.tools.string.StopwordUDF' "
        + "using jar  'ivy://io.github.myui:hivemall:0.4.0-2'");
    WarehouseInstance.Tuple bootStrapDump = primary.dump(primaryDbName, null);
    replica.load(replicatedDbName, bootStrapDump.dumpLocation)
        .run("REPL STATUS " + replicatedDbName)
        .verify(bootStrapDump.lastReplicationId);

    primary.run("Drop FUNCTION " + primaryDbName + ".testFunction ");

    WarehouseInstance.Tuple incrementalDump =
        primary.dump(primaryDbName, bootStrapDump.lastReplicationId);
    replica.load(replicatedDbName, incrementalDump.dumpLocation)
        .run("REPL STATUS " + replicatedDbName)
        .verify(incrementalDump.lastReplicationId)
        .run("SHOW FUNCTIONS LIKE '*testfunction*'")
        .verify(null);

    // Test the idempotent behavior of DROP FUNCTION
    replica.load(replicatedDbName, incrementalDump.dumpLocation)
            .run("REPL STATUS " + replicatedDbName)
            .verify(incrementalDump.lastReplicationId)
            .run("SHOW FUNCTIONS LIKE '*testfunction*'")
            .verify(null);
  }

  @Test
  public void testBootstrapFunctionReplication() throws Throwable {
    primary.run("CREATE FUNCTION " + primaryDbName
        + ".testFunction as 'hivemall.tools.string.StopwordUDF' "
        + "using jar  'ivy://io.github.myui:hivemall:0.4.0-2'");
    WarehouseInstance.Tuple bootStrapDump = primary.dump(primaryDbName, null);

    replica.load(replicatedDbName, bootStrapDump.dumpLocation)
        .run("SHOW FUNCTIONS LIKE '" + replicatedDbName + "*'")
        .verify(replicatedDbName + ".testFunction");
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
        .verify(replicatedDbName + ".anotherFunction");

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
}
