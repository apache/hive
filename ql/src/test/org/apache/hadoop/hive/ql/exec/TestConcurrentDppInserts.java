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
package org.apache.hadoop.hive.ql.exec;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.testutils.HiveTestEnvSetup;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;

@Ignore
public class TestConcurrentDppInserts {

  static final private Logger LOG = LoggerFactory.getLogger(TestConcurrentDppInserts.class.getName());

  @ClassRule
  public static HiveTestEnvSetup env_setup = new HiveTestEnvSetup();

  @Rule
  public TestRule methodRule = env_setup.getMethodRule();

  @BeforeClass
  public static void beforeClass() throws Exception {
    IDriver driver = createDriver(false);
    dropTables(driver);
    String[] cmds = {
        // @formatter:off
        "create table tu(i int) partitioned by (k string, p string)",
        // @formatter:on
    };
    for (String cmd : cmds) {
      driver.run(cmd);
    }
  }

  @AfterClass
  public static void afterClass() throws Exception {
    IDriver driver = createDriver(false);
    dropTables(driver);
  }

  public static void dropTables(IDriver driver) throws Exception {
    String[] tables = new String[] { "tu" };
    for (String t : tables) {
      driver.run("drop table if exists " + t);
    }
  }

  int N = 3; // num parallel threads
  int M = 3; // num partitions a thread inserts into at a time
  int K = 3; // num tests to repeat

  CyclicBarrier barrier = new CyclicBarrier(N);
  Semaphore finished = new Semaphore(0);

  LinkedList<Exception> exceptions = new LinkedList<>();

  class InserterThread extends Thread {

    @Override
    public void run() {
      try {
        IDriver driver = createDriver(true);
        for (int i = 0; i < K; i++) {
          doTest(driver, i);
        }
      } catch (Throwable t) {
        System.out.println(t);
      } finally {
        finished.release();
      }

    }

    private void doTest(IDriver driver, int pIdx) {
      try {
        barrier.await(30, TimeUnit.SECONDS);
        List<String> parts = new ArrayList<>();
        for (int i = 0; i < M; i++) {
          parts.add(String.format("select %d as i,%d as p", M * pIdx + i, M * pIdx + i));
        }
        driver.run("insert into tu partition(k=1,p) (" + Joiner.on(" union all ").join(parts) + ")");

      } catch (Exception e) {
        LOG.info("Exception in InserterThread:", e);
        exceptions.add(e);
      }
    }

  }

  @Test(timeout = 600000)
  public void testConcurrentCreationOfSamePartition() throws Exception {
    List<Object> threads = new ArrayList<>();
    for (int i = 0; i < N; i++) {
      InserterThread e = new InserterThread();
      e.start();
      threads.add(e);
    }
    finished.acquire(N);

    IDriver driver = createDriver(true);
    driver.run("select p,count(i) as c from tu group by p");
    ArrayList<String> res = new ArrayList<>();
    assertEquals(0, exceptions.size(), " there were exceptions: " + getExceptionMessages());
    driver.getResults(res);
    assertEquals(K * M, res.size());
    for (String row : res) {
      String[] parts = row.split("\t");
      assertEquals(Integer.toString(N), parts[1], row);
    }
  }

  private String getExceptionMessages() {
    StringBuilder sb = new StringBuilder();
    for (Exception exception : exceptions) {
      if (sb.length() > 0) {
        sb.append(", ");
      }
      sb.append(exception.getClass().getName() + ":" + exception.getMessage());
    }
    return sb.toString();
  }

  private static IDriver createDriver(boolean custom) {
    HiveConf conf = new HiveConf(env_setup.getTestCtx().hiveConf);

    if (custom) {
      conf.setVar(ConfVars.HIVE_LOCK_FILE_MOVE_MODE, "all");
      conf.setBoolVar(ConfVars.HIVE_SUPPORT_CONCURRENCY, true);
      conf.setTimeVar(HiveConf.ConfVars.HIVE_LOCK_SLEEP_BETWEEN_RETRIES, 100, TimeUnit.MILLISECONDS);
    }

    SessionState.start(conf);

    IDriver driver = DriverFactory.newDriver(conf);
    return driver;
  }


}
