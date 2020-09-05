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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.messaging.json.gzip.GzipJSONMessageEncoder;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hive.ql.exec.repl.ReplExternalTables.FILE_NAME;

/**
 * Test replication scenarios with staging on replica.
 */
public class TestReplicationScenariosExclusiveReplica extends BaseReplicationAcrossInstances {

  @BeforeClass
  public static void classLevelSetup() throws Exception {
    Map<String, String> overrides = new HashMap<>();
    overrides.put(MetastoreConf.ConfVars.EVENT_MESSAGE_FACTORY.getHiveName(),
        GzipJSONMessageEncoder.class.getCanonicalName());
    overrides.put(HiveConf.ConfVars.REPL_DUMP_METADATA_ONLY.varname, "false");
    overrides.put(HiveConf.ConfVars.REPL_INCLUDE_EXTERNAL_TABLES.varname, "true");
    overrides.put(HiveConf.ConfVars.HIVE_DISTCP_DOAS_USER.varname,
        UserGroupInformation.getCurrentUser().getUserName());
    internalBeforeClassSetupExclusiveReplica(overrides, overrides, TestReplicationScenarios.class);
  }

  @Before
  public void setup() throws Throwable {
    super.setup();
  }

  @After
  public void tearDown() throws Throwable {
    super.tearDown();
  }

  @Test
  public void externalTableReplicationWithRemoteStaging() throws Throwable {
    List<String> withClauseOptions = getStagingLocationConfig(replica.repldDir);
    WarehouseInstance.Tuple tuple = primary
        .run("use " + primaryDbName)
        .run("create external table t1 (id int)")
        .run("insert into table t1 values (100)")
        .run("create table t2 (id int)")
        .run("insert into table t2 values (200)")
        .dump(primaryDbName, withClauseOptions);

    // verify that the external table info is written correctly for bootstrap
    assertExternalFileInfo(Arrays.asList("t1"), tuple.dumpLocation, false, replica);

    replica.load(replicatedDbName, primaryDbName, withClauseOptions)
        .run("use " + replicatedDbName)
        .run("show tables like 't1'")
        .verifyResult("t1")
        .run("select id from t1")
        .verifyResult("100")
        .run("show tables like 't2'")
        .verifyResult("t2")
        .run("select id from t2")
        .verifyResult("200");

    tuple = primary.run("use " + primaryDbName)
        .run("create external table t3 (id int)")
        .run("insert into table t3 values (300)")
        .run("create table t4 (id int)")
        .run("insert into table t4 values (400)")
        .dump(primaryDbName, withClauseOptions);

    // verify that the external table info is written correctly for incremental
    assertExternalFileInfo(Arrays.asList("t1", "t3"), tuple.dumpLocation, true, replica);

    replica.load(replicatedDbName, primaryDbName, withClauseOptions)
        .run("use " + replicatedDbName)
        .run("show tables like 't1'")
        .verifyResult("t1")
        .run("show tables like 't2'")
        .verifyResult("t2")
        .run("show tables like 't3'")
        .verifyResult("t3")
        .run("show tables like 't4'")
        .verifyResult("t4")
        .run("select id from t1")
        .verifyResult("100")
        .run("select id from t2")
        .verifyResult("200")
        .run("select id from t3")
        .verifyResult("300")
        .run("select id from t4")
        .verifyResult("400");
  }

  @Test
  public void externalTableReplicationWithLocalStaging() throws Throwable {
    List<String> withClauseOptions = getStagingLocationConfig(primary.repldDir);
    WarehouseInstance.Tuple tuple = primary
            .run("use " + primaryDbName)
            .run("create external table t1 (id int)")
            .run("insert into table t1 values (500)")
            .run("create table t2 (id int)")
            .run("insert into table t2 values (600)")
            .dump(primaryDbName, withClauseOptions);

    // verify that the external table info is written correctly for bootstrap
    assertExternalFileInfo(Arrays.asList("t1"), tuple.dumpLocation, false, primary);

    replica.load(replicatedDbName, primaryDbName, withClauseOptions)
            .run("use " + replicatedDbName)
            .run("show tables like 't1'")
            .verifyResult("t1")
            .run("select id from t1")
            .verifyResult("500")
            .run("show tables like 't2'")
            .verifyResult("t2")
            .run("select id from t2")
            .verifyResult("600");

    tuple = primary.run("use " + primaryDbName)
            .run("create external table t3 (id int)")
            .run("insert into table t3 values (700)")
            .run("create table t4 (id int)")
            .run("insert into table t4 values (800)")
            .dump(primaryDbName, withClauseOptions);

    // verify that the external table info is written correctly for incremental
    assertExternalFileInfo(Arrays.asList("t1", "t3"), tuple.dumpLocation, true, primary);

    replica.load(replicatedDbName, primaryDbName, withClauseOptions)
            .run("use " + replicatedDbName)
            .run("show tables like 't1'")
            .verifyResult("t1")
            .run("show tables like 't2'")
            .verifyResult("t2")
            .run("show tables like 't3'")
            .verifyResult("t3")
            .run("show tables like 't4'")
            .verifyResult("t4")
            .run("select id from t1")
            .verifyResult("500")
            .run("select id from t2")
            .verifyResult("600")
            .run("select id from t3")
            .verifyResult("700")
            .run("select id from t4")
            .verifyResult("800");
  }

  private List<String> getStagingLocationConfig(String stagingLoc) {
    List<String> confList = new ArrayList<>();
    confList.add("'" + HiveConf.ConfVars.REPLDIR.varname + "'='" + stagingLoc + "'");
    return confList;
  }

  private void assertExternalFileInfo(List<String> expected, String dumplocation, boolean isIncremental,
                                      WarehouseInstance warehouseInstance)
      throws IOException {
    Path hivePath = new Path(dumplocation, ReplUtils.REPL_HIVE_BASE_DIR);
    Path metadataPath = new Path(hivePath, EximUtil.METADATA_PATH_NAME);
    Path externalTableInfoFile;
    if (isIncremental) {
      externalTableInfoFile = new Path(hivePath, FILE_NAME);
    } else {
      externalTableInfoFile = new Path(metadataPath, primaryDbName.toLowerCase() + File.separator + FILE_NAME);
    }
    ReplicationTestUtils.assertExternalFileInfo(warehouseInstance, expected, externalTableInfoFile);
  }
}
