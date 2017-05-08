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
package org.apache.hadoop.hive.ql.parse;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestReplicationScenariosAcrossInstances {
  @Rule
  public final TestName testName = new TestName();
  protected static final Logger LOG = LoggerFactory.getLogger(TestReplicationScenarios.class);

  private static WarehouseInstance primary, replica;

  @BeforeClass
  public static void classLevelSetup() throws Exception {
    primary = new WarehouseInstance();
    replica = new WarehouseInstance();
  }

  private String primaryDbName, replicatedDbName;

  @Before
  public void setup() throws Throwable {
    primaryDbName = testName.getMethodName() + "_" + +System.currentTimeMillis();
    replicatedDbName = "replicated_" + primaryDbName;
    primary.run("create database " + primaryDbName);
  }

  @After
  public void tearDown() throws Throwable {
    primary.run(dropCommand(primaryDbName));
    replica.run(dropCommand(replicatedDbName));
  }

  private String dropCommand(String dbName) {
    return "drop database if exists " + dbName + " cascade ";
  }

  @Test
  public void testIncrementalFunctionReplication() throws Throwable {
    WarehouseInstance.Tuple bootStrapDump = primary.dump(primaryDbName, null);
    replica.load(replicatedDbName, bootStrapDump.dumpLocation)
        .run("REPL STATUS " + replicatedDbName)
        .verify(bootStrapDump.lastReplicationId);

    primary.run("CREATE FUNCTION " + primaryDbName
        + ".testFunction as 'com.yahoo.sketches.hive.theta.DataToSketchUDAF' "
        + "using jar  'ivy://com.yahoo.datasketches:sketches-hive:0.8.2'");

    WarehouseInstance.Tuple incrementalDump = primary.dump(primaryDbName, bootStrapDump.lastReplicationId);
    replica.load(replicatedDbName, incrementalDump.dumpLocation)
        .run("REPL STATUS " + replicatedDbName)
        .verify(incrementalDump.lastReplicationId)
        .run("SHOW FUNCTIONS LIKE '" + replicatedDbName + "*'")
        .verify(replicatedDbName + ".testFunction");
  }

  @Test
  public void testBootstrapFunctionReplication() throws Throwable {
    primary.run("CREATE FUNCTION " + primaryDbName
        + ".testFunction as 'com.yahoo.sketches.hive.theta.DataToSketchUDAF' "
        + "using jar  'ivy://com.yahoo.datasketches:sketches-hive:0.8.2'");
    WarehouseInstance.Tuple bootStrapDump = primary.dump(primaryDbName, null);

    replica.load(replicatedDbName, bootStrapDump.dumpLocation)
        .run("SHOW FUNCTIONS LIKE '" + replicatedDbName + "*'")
        .verify(replicatedDbName + ".testFunction");
  }
}
