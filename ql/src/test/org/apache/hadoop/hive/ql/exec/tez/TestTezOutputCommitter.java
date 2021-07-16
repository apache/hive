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

package org.apache.hadoop.hive.ql.exec.tez;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hive.testutils.HiveTestEnvSetup;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestTezOutputCommitter {

  @ClassRule
  public static HiveTestEnvSetup ENVIRONMENT = new HiveTestEnvSetup();

  private static final String ABORT_JOB_ERROR_MSG = "JobAbortingOutputCommitter error!!!";
  private static final String ABORT_TASK_ERROR_MSG = "TaskAbortingOutputCommitter error!!!";
  private static final int MAX_TASK_ATTEMPTS = 2;
  private static final String TEST_TABLE = "output_committer_test_table";

  private static int commitTaskCounter;
  private static int abortTaskCounter;
  private static int commitJobCounter;
  private static int abortJobCounter;

  private IDriver driver;

  @Before
  public void setUp() {
    commitTaskCounter = 0;
    abortTaskCounter = 0;
    commitJobCounter = 0;
    abortJobCounter = 0;
  }

  @Test
  public void testSuccessfulJob() throws Exception {
    driver = getDriverWithCommitter(CountingOutputCommitter.class.getName());

    driver.run(String.format("CREATE TABLE %s (a int)", TEST_TABLE));
    driver.run(String.format("INSERT INTO %s VALUES (4), (5)", TEST_TABLE));

    assertEquals(1, commitTaskCounter);
    assertEquals(0, abortTaskCounter);
    assertEquals(1, commitJobCounter);
    assertEquals(0, abortJobCounter);
  }

  @Test
  public void testAbortTask() throws Exception {
    driver = getDriverWithCommitter(TaskAbortingOutputCommitter.class.getName());

    try {
      driver.run(String.format("CREATE TABLE %s (a int)", TEST_TABLE));
      driver.run(String.format("INSERT INTO %s VALUES (4), (5)", TEST_TABLE));
      fail();
    } catch (Exception e) {
      assertTrue(e.getMessage().contains(ABORT_TASK_ERROR_MSG));
    }

    assertEquals(MAX_TASK_ATTEMPTS, commitTaskCounter);
    assertEquals(MAX_TASK_ATTEMPTS, abortTaskCounter);
    assertEquals(0, commitJobCounter);
    assertEquals(1, abortJobCounter);
  }

  @Test
  public void testAbortJob() throws Exception {
    driver = getDriverWithCommitter(JobAbortingOutputCommitter.class.getName());

    try {
      driver.run(String.format("CREATE TABLE %s (a int)", TEST_TABLE));
      driver.run(String.format("INSERT INTO %s VALUES (4), (5)", TEST_TABLE));
      fail();
    } catch (Exception e) {
      assertTrue(e.getMessage().contains(ABORT_JOB_ERROR_MSG));
    }

    assertEquals(1, commitTaskCounter);
    assertEquals(0, abortTaskCounter);
    assertEquals(1, commitJobCounter);
    assertEquals(1, abortJobCounter);
  }

  @After
  public void tearDown() throws Exception {
    driver.run(String.format("DROP TABLE %s", TEST_TABLE));
    driver.close();
  }

  private IDriver getDriverWithCommitter(String committerClass) {
    HiveConf conf = ENVIRONMENT.getTestCtx().hiveConf;
    conf.setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    conf.setBoolVar(HiveConf.ConfVars.HIVESTATSCOLAUTOGATHER, false);
    conf.setInt("tez.am.task.max.failed.attempts", MAX_TASK_ATTEMPTS);
    conf.set("mapred.output.committer.class", committerClass);

    SessionState.start(conf);
    return DriverFactory.newDriver(conf);
  }

  public static class TaskAbortingOutputCommitter extends CountingOutputCommitter {
    @Override
    public void commitTask(TaskAttemptContext taskAttemptContext) throws IOException {
      super.commitTask(taskAttemptContext);
      throw new RuntimeException(ABORT_TASK_ERROR_MSG);
    }
  }

  public static class JobAbortingOutputCommitter extends CountingOutputCommitter {
    @Override
    public void commitJob(JobContext jobContext) throws IOException {
      super.commitJob(jobContext);
      throw new RuntimeException(ABORT_JOB_ERROR_MSG);
    }
  }

  public static class CountingOutputCommitter extends OutputCommitter {

    @Override
    public void commitTask(TaskAttemptContext taskAttemptContext) throws IOException {
      commitTaskCounter++;
    }

    @Override
    public void abortTask(TaskAttemptContext taskAttemptContext) throws IOException {
      abortTaskCounter++;
    }

    @Override
    public void commitJob(JobContext jobContext) throws IOException {
      super.commitJob(jobContext);
      commitJobCounter++;
    }

    @Override
    public void abortJob(JobContext jobContext, int status) throws IOException {
      super.abortJob(jobContext, status);
      abortJobCounter++;
    }

    @Override
    public void setupJob(JobContext jobContext) throws IOException {

    }

    @Override
    public void setupTask(TaskAttemptContext taskAttemptContext) throws IOException {

    }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext taskAttemptContext) throws IOException {
      return true;
    }
  }
}
