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

import static org.apache.hadoop.hive.ql.exec.repl.ReplExternalTables.FILE_NAME;

public class TestReplicationScenariosExclusiveReplica extends BaseReplicationAcrossInstances {

  private static final String REPLICA_EXTERNAL_BASE = "/replica_external_base";

  @BeforeClass
  public static void classLevelSetup() throws Exception {
    HashMap<String, String> overrides = new HashMap<>();
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
    withClauseOptions.addAll(externalTableBasePathWithClause());
    WarehouseInstance.Tuple tuple = primary
        .run("use " + primaryDbName)
        .run("create external table t1 (id int)")
        .run("insert into table t1 values (100)")
        .dump(primaryDbName, withClauseOptions);

    // verify that the external table info is written correctly for bootstrap
    assertExternalFileInfo(Arrays.asList("t1"), tuple.dumpLocation, false, replica);

    replica.load(replicatedDbName, primaryDbName, withClauseOptions)
        .run("use " + replicatedDbName)
        .run("show tables like 't1'")
        .verifyResult("t1")
        .run("select id from t1")
        .verifyResult("100");

    tuple = primary.run("use " + primaryDbName)
        .run("create external table t2 (id int)")
        .run("insert into table t2 values (500)")
        .dump(primaryDbName, withClauseOptions);

    // verify that the external table info is written correctly for incremental
    assertExternalFileInfo(Arrays.asList("t1", "t2"), tuple.dumpLocation, true, replica);

    replica.load(replicatedDbName, primaryDbName, withClauseOptions)
        .run("use " + replicatedDbName)
        .run("show tables like 't2'")
        .verifyResult("t2")
        .run("select id from t1")
        .verifyResult("100")
        .run("select id from t2")
        .verifyResult("500");
  }

  private List<String> getStagingLocationConfig(String stagingLoc) {
    List<String> confList = new ArrayList<>();
    confList.add("'" + HiveConf.ConfVars.REPLDIR.varname + "'='" + stagingLoc + "'");
    return confList;
  }

  private List<String> externalTableBasePathWithClause() throws IOException, SemanticException {
    return ReplicationTestUtils.externalTableBasePathWithClause(REPLICA_EXTERNAL_BASE, replica);
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
