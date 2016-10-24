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

package org.apache.hive.service.cli.session;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hive.common.metrics.MetricsTestUtils;
import org.apache.hadoop.hive.common.metrics.common.MetricsConstant;
import org.apache.hadoop.hive.common.metrics.common.MetricsFactory;
import org.apache.hadoop.hive.common.metrics.metrics2.CodahaleMetrics;
import org.apache.hadoop.hive.common.metrics.metrics2.MetricsReporting;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.util.Time;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.OperationType;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.TableSchema;
import org.apache.hive.service.cli.operation.MetadataOperation;
import org.apache.hive.service.cli.operation.OperationManager;
import org.apache.hive.service.rpc.thrift.TProtocolVersion;
import org.apache.hive.service.server.HiveServer2;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;

/**
 * Test metrics from SessionManager.
 */
public class TestSessionManagerMetrics {

  private static SessionManager sm;
  private static CodahaleMetrics metrics;
  private static final int BARRIER_AWAIT_TIMEOUT = 30;
  private static final String FAIL_TO_START_MSG = "The tasks could not be started within "
      + BARRIER_AWAIT_TIMEOUT + " seconds before the %s metrics verification.";
  private static final String FAIL_TO_COMPLETE_MSG = "The tasks could not be completed within "
      + BARRIER_AWAIT_TIMEOUT + " seconds after the %s metrics verification.";

  @Before
  public void setup() throws Exception {
    HiveConf conf = new HiveConf();
    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_ASYNC_EXEC_THREADS, 2);
    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_ASYNC_EXEC_WAIT_QUEUE_SIZE, 10);
    conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_ASYNC_EXEC_KEEPALIVE_TIME, "1000000s");
    conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_IDLE_SESSION_TIMEOUT, "500ms");
    conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_SESSION_CHECK_INTERVAL, "3s");

    conf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_METRICS_ENABLED, true);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    conf.setVar(HiveConf.ConfVars.HIVE_METRICS_REPORTER, MetricsReporting.JSON_FILE.name() + "," + MetricsReporting.JMX.name());
    conf.setBoolVar(HiveConf.ConfVars.HIVEOPTIMIZEMETADATAQUERIES, false);
    MetricsFactory.init(conf);

    HiveServer2 hs2 = new HiveServer2();
    sm = new SessionManager(hs2);
    sm.init(conf);

    metrics = (CodahaleMetrics) MetricsFactory.getInstance();

    Hive doNothingHive = mock(Hive.class);
    Hive.set(doNothingHive);
  }

  class BarrierRunnable implements Runnable {

    private final CyclicBarrier ready;
    private final CyclicBarrier completed;

    BarrierRunnable(CyclicBarrier ready, CyclicBarrier completed) {
      this.ready = ready;
      this.completed = completed;
    }

    @Override
    public void run() {
      try {
        ready.await();
        completed.await();
      } catch (InterruptedException | BrokenBarrierException e) {
        throw new RuntimeException(e);
      }
    }
  }

  class BlockingOperation extends MetadataOperation {

    private final CyclicBarrier ready;
    private final CyclicBarrier completed;

    BlockingOperation(HiveSession parentSession, OperationType opType,
                         CyclicBarrier ready, CyclicBarrier completed) {
      super(parentSession, opType);
      this.ready = ready;
      this.completed = completed;
    }

    @Override
    protected void runInternal() throws HiveSQLException {
      try {
        ready.await();
        completed.await();
      } catch (InterruptedException | BrokenBarrierException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public TableSchema getResultSetSchema() throws HiveSQLException {
      return null;
    }

    @Override
    public RowSet getNextRowSet(FetchOrientation orientation, long maxRows) throws HiveSQLException {
      return null;
    }
  }

  /**
   * Tests metrics regarding async thread pool.
   *
   * The test does the following steps:
   * - Submit four tasks
   * - Wait with the metrics verification, until the first two tasks are running.
   * If, for some reason, the tasks are not started within a timeout period, make the test fail.
   * - Make the tasks wait until the metrics are checked.
   * - Verify the metrics. Both the EXEC_ASYNC_POOL_SIZE and EXEC_ASYNC_QUEUE_SIZE should be 2.
   * - Let the first two tasks complete, so the remaining two tasks can be removed from the queue and started.
   * - Wait until the remaining tasks are running.
   * Do the metrics check only if they are started to avoid the failures when the queue size was not 0.
   * If, for some reason, the tasks are not started within a timeout period, make the test fail.
   * - Verify the metrics.
   * The EXEC_ASYNC_POOL_SIZE should be 2 and the EXEC_ASYNC_QUEUE_SIZE should be 0.
   * - Let the remaining tasks complete.
   */
  @Test
  public void testThreadPoolMetrics() throws Exception {

    String errorMessage = null;
    CyclicBarrier ready = new CyclicBarrier(3);
    CyclicBarrier completed = new CyclicBarrier(3);
    try {
      sm.submitBackgroundOperation(new BarrierRunnable(ready, completed));
      sm.submitBackgroundOperation(new BarrierRunnable(ready, completed));
      sm.submitBackgroundOperation(new BarrierRunnable(ready, completed));
      sm.submitBackgroundOperation(new BarrierRunnable(ready, completed));

      errorMessage = String.format(FAIL_TO_START_MSG, "first");
      ready.await(BARRIER_AWAIT_TIMEOUT, TimeUnit.SECONDS);
      ready.reset();

      String json = metrics.dumpJson();
      MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.GAUGE, MetricsConstant.EXEC_ASYNC_POOL_SIZE, 2);
      MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.GAUGE, MetricsConstant.EXEC_ASYNC_QUEUE_SIZE, 2);

      errorMessage = String.format(FAIL_TO_COMPLETE_MSG, "first");
      completed.await(BARRIER_AWAIT_TIMEOUT, TimeUnit.SECONDS);
      completed.reset();

      errorMessage = String.format(FAIL_TO_START_MSG, "second");
      ready.await(BARRIER_AWAIT_TIMEOUT, TimeUnit.SECONDS);

      json = metrics.dumpJson();
      MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.GAUGE, MetricsConstant.EXEC_ASYNC_POOL_SIZE, 2);
      MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.GAUGE, MetricsConstant.EXEC_ASYNC_QUEUE_SIZE, 0);

      errorMessage = String.format(FAIL_TO_COMPLETE_MSG, "second");
      completed.await(BARRIER_AWAIT_TIMEOUT, TimeUnit.SECONDS);

    } catch (TimeoutException e) {
      Assert.fail(errorMessage);
    }
  }

  @Test
  public void testOpenSessionMetrics() throws Exception {

    String json = metrics.dumpJson();
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.GAUGE, MetricsConstant.HS2_OPEN_SESSIONS, 0);

    SessionHandle handle =
        sm.openSession(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V9, "user", "passw", "127.0.0.1",
            new HashMap<String, String>());

    json = metrics.dumpJson();
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.GAUGE, MetricsConstant.HS2_OPEN_SESSIONS, 1);

    sm.openSession(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V9, "user", "passw", "127.0.0.1",
        new HashMap<String, String>());

    json = metrics.dumpJson();
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.GAUGE, MetricsConstant.HS2_OPEN_SESSIONS, 2);

    sm.closeSession(handle);

    json = metrics.dumpJson();
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.GAUGE, MetricsConstant.HS2_OPEN_SESSIONS, 1);
  }

  @Test
  public void testOpenSessionTimeMetrics() throws Exception {

    String json = metrics.dumpJson();
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.GAUGE,
        MetricsConstant.HS2_AVG_OPEN_SESSION_TIME, "NaN");

    long firstSessionOpen = System.currentTimeMillis();
    SessionHandle handle =
        sm.openSession(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V9, "user", "passw", "127.0.0.1",
            new HashMap<String, String>());

    json = metrics.dumpJson();
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.GAUGE, MetricsConstant.HS2_AVG_OPEN_SESSION_TIME,
        (double)(System.currentTimeMillis() - firstSessionOpen), 100d);

    long secondSessionOpen = System.currentTimeMillis();
    sm.openSession(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V9, "user", "passw", "127.0.0.1",
        new HashMap<String, String>());

    json = metrics.dumpJson();
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.GAUGE, MetricsConstant.HS2_AVG_OPEN_SESSION_TIME,
        (double)(System.currentTimeMillis() - firstSessionOpen +
                 System.currentTimeMillis() - secondSessionOpen) / 2d, 100d);

    sm.closeSession(handle);

    json = metrics.dumpJson();
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.GAUGE, MetricsConstant.HS2_AVG_OPEN_SESSION_TIME,
        (double)(System.currentTimeMillis() - secondSessionOpen), 100d);

  }

  @Test
  public void testActiveSessionMetrics() throws Exception {

    final CyclicBarrier ready = new CyclicBarrier(2);
    CyclicBarrier completed = new CyclicBarrier(2);
    String json = metrics.dumpJson();
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.GAUGE, MetricsConstant.HS2_ACTIVE_SESSIONS, 0);

    SessionHandle handle =
        sm.openSession(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V9, "user", "passw", "127.0.0.1",
            new HashMap<String, String>());

    final HiveSession session = sm.getSession(handle);
    OperationManager operationManager = mock(OperationManager.class);
    when(operationManager.
        newGetTablesOperation(session, "catalog", "schema", "table", null))
          .thenReturn(new BlockingOperation(session, OperationType.GET_TABLES, ready, completed));
    session.setOperationManager(operationManager);

    new Thread(new Runnable() {

      @Override
      public void run() {
        try {
          OperationHandle handle = session.getTables("catalog", "schema", "table", null);
          session.closeOperation(handle);
        } catch (Exception e) {
          throw new RuntimeException(e);
        } finally {
          try {
            ready.await();
          } catch (InterruptedException | BrokenBarrierException e) {
            // ignore
          }
        }
      }
    }).start();

    ready.await(2, TimeUnit.SECONDS);
    ready.reset();

    json = metrics.dumpJson();
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.GAUGE, MetricsConstant.HS2_ACTIVE_SESSIONS, 1);

    completed.await(2, TimeUnit.SECONDS);
    ready.await(2, TimeUnit.SECONDS);

    json = metrics.dumpJson();
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.GAUGE, MetricsConstant.HS2_ACTIVE_SESSIONS, 0);
  }

  @Test
  public void testActiveSessionTimeMetrics() throws Exception {

    final CyclicBarrier ready = new CyclicBarrier(2);
    CyclicBarrier completed = new CyclicBarrier(2);

    String json = metrics.dumpJson();
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.GAUGE,
        MetricsConstant.HS2_AVG_ACTIVE_SESSION_TIME, "NaN");

    SessionHandle handle =
        sm.openSession(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V9, "user", "passw", "127.0.0.1",
            new HashMap<String, String>());

    final HiveSession session = sm.getSession(handle);
    OperationManager operationManager = mock(OperationManager.class);
    when(operationManager.
        newGetTablesOperation(session, "catalog", "schema", "table", null))
        .thenReturn(new BlockingOperation(session, OperationType.GET_TABLES, ready, completed));
    session.setOperationManager(operationManager);

    long sessionActivateTime = System.currentTimeMillis();
    new Thread(new Runnable() {

      @Override
      public void run() {
        try {
          OperationHandle handle = session.getTables("catalog", "schema", "table", null);
          session.closeOperation(handle);
        } catch (Exception e) {
          throw new RuntimeException(e);
        } finally {
          try {
            ready.await();
          } catch (InterruptedException | BrokenBarrierException e) {
            // ignore
          }
        }
      }
    }).start();

    ready.await(2, TimeUnit.SECONDS);
    ready.reset();

    json = metrics.dumpJson();
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.GAUGE, MetricsConstant.HS2_AVG_ACTIVE_SESSION_TIME,
        (double)System.currentTimeMillis() - sessionActivateTime, 100d);

    completed.await(2, TimeUnit.SECONDS);
    ready.await(2, TimeUnit.SECONDS);

    json = metrics.dumpJson();
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.GAUGE,
        MetricsConstant.HS2_AVG_ACTIVE_SESSION_TIME, "NaN");
  }


  @Test
  public void testAbandonedSessionMetrics() throws Exception {

    sm.start();
    String json = metrics.dumpJson();
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.COUNTER, MetricsConstant.HS2_ABANDONED_SESSIONS, "");

    sm.openSession(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V9, "user", "passw", "127.0.0.1",
                    new HashMap<String, String>());

    Thread.sleep(3200);

    json = metrics.dumpJson();
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.COUNTER, MetricsConstant.HS2_ABANDONED_SESSIONS, 1);
  }
}
