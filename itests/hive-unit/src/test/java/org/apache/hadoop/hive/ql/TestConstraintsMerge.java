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

package org.apache.hadoop.hive.ql;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.utils.TestTxnDbUtil;
import org.apache.hadoop.hive.ql.exec.errors.DataConstraintViolationError;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.File;

import static org.apache.hadoop.hive.ql.TestAcidOnTez.TEST_DATA_DIR;
import static org.apache.hadoop.hive.ql.TestAcidOnTez.TEST_WAREHOUSE_DIR;
import static org.apache.hadoop.hive.ql.TestAcidOnTez.runStatementOnDriver;
import static org.apache.hadoop.hive.ql.TestAcidOnTez.setupTez;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * This class resides in itests to facilitate running query using Tez engine, since the jars are
 * fully loaded here, which is not the case if it stays in ql.
 */
public class TestConstraintsMerge {
  //bucket count for test tables; set it to 1 for easier debugging
  private static int BUCKET_COUNT = 2;
  @Rule
  public TestName testName = new TestName();
  private HiveConf hiveConf;
  private IDriver d;

  private enum Table {
    TBL_SOURCE("table_source"),
    TBL_CHECK_MERGE("table_check_merge");

    private final String name;
    @Override
    public String toString() {
      return name;
    }
    Table(String name) {
      this.name = name;
    }
  }

  @Before
  public void setUp() throws Exception {
    hiveConf = new HiveConf(this.getClass());
    hiveConf.set(ConfVars.PRE_EXEC_HOOKS.varname, "");
    hiveConf.set(ConfVars.POST_EXEC_HOOKS.varname, "");
    hiveConf.set(ConfVars.METASTORE_WAREHOUSE.varname, TEST_WAREHOUSE_DIR);
    hiveConf.setBoolVar(ConfVars.HIVE_VECTORIZATION_ENABLED, false);
    hiveConf.setVar(ConfVars.HIVE_MAPRED_MODE, "nonstrict");
    hiveConf.setVar(ConfVars.HIVE_INPUT_FORMAT, HiveInputFormat.class.getName());
    hiveConf
        .setVar(ConfVars.HIVE_AUTHORIZATION_MANAGER,
            "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    TestTxnDbUtil.setConfValues(hiveConf);
    hiveConf.setInt(MRJobConfig.MAP_MEMORY_MB, 1024);
    hiveConf.setInt(MRJobConfig.REDUCE_MEMORY_MB, 1024);
    TestTxnDbUtil.prepDb(hiveConf);
    File f = new File(TEST_WAREHOUSE_DIR);
    if (f.exists()) {
      FileUtil.fullyDelete(f);
    }
    if (!(new File(TEST_WAREHOUSE_DIR).mkdirs())) {
      throw new RuntimeException("Could not create " + TEST_WAREHOUSE_DIR);
    }
    SessionState.start(new SessionState(hiveConf));
    d = DriverFactory.newDriver(hiveConf);
    dropTables();
    runStatementOnDriver("create table " + Table.TBL_SOURCE + "(name string, age int, gpa double) " +
            "clustered by (name) into " + BUCKET_COUNT + " buckets stored as orc " + getTblProperties(), hiveConf);
    runStatementOnDriver("create table " + Table.TBL_CHECK_MERGE
        + "(name string, age int, gpa double  CHECK (gpa BETWEEN 0.0 AND 4.0)) " +
            "clustered by (name) into " + BUCKET_COUNT + " buckets stored as orc " + getTblProperties(), hiveConf);
    runStatementOnDriver("insert into " + Table.TBL_SOURCE + "(name, age, gpa) values " +
            "('student1', 16, null)," +
            "(null, 20, 4.0)", hiveConf);
  }

  /**
   * this is to test differety types of Acid tables
   */
  String getTblProperties() {
    return "TBLPROPERTIES ('transactional'='true')";
  }

  private void dropTables() throws Exception {
    for(Table t : Table.values()) {
      runStatementOnDriver("drop table if exists " + t, hiveConf);
    }
  }

  @After
  public void tearDown() throws Exception {
    try {
      if (d != null) {
        dropTables();
        d.close();
        d.destroy();
        d = null;
      }
      TestTxnDbUtil.cleanDb(hiveConf);
    } finally {
      FileUtils.deleteDirectory(new File(TEST_DATA_DIR));
    }
  }

  @Test
  public void testUpdateInMergeViolatesCheckConstraint() throws Exception {
    HiveConf confForTez = new HiveConf(hiveConf);
    confForTez.setBoolVar(HiveConf.ConfVars.HIVE_EXPLAIN_USER, false);
    setupTez(confForTez);

    runStatementOnDriver("insert into " + Table.TBL_CHECK_MERGE + "(name, age, gpa) values " +
            "('student1', 16, 2.0)", confForTez);

    Throwable error = null;
    try {
      runStatementOnDriver("merge into " + Table.TBL_CHECK_MERGE +
              " using (select age from table_source) source\n" +
              "on source.age=table_check_merge.age\n" +
              "when matched then update set gpa=6", confForTez);
    } catch (Throwable t) {
      error = t;
    }

    assertTrue(error instanceof DataConstraintViolationError);
    assertTrue(error.getMessage().contains("Either CHECK or NOT NULL constraint violated!"));
  }
}
