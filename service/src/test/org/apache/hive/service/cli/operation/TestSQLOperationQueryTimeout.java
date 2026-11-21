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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hive.service.cli.operation;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.QueryInfo;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.service.cli.HandleIdentifier;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationState;
import org.apache.hive.service.cli.OperationStatus;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.session.HiveSession;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class TestSQLOperationQueryTimeout {

  private HiveSession session;

  @Before
  public void setUp() {
    HiveConf conf = new HiveConf();
    session = mock(HiveSession.class);
    when(session.getHiveConf()).thenReturn(conf);
    when(session.getSessionState()).thenReturn(mock(SessionState.class));
    when(session.getUserName()).thenReturn("user");
    SessionHandle sessionHandle = mock(SessionHandle.class);
    when(sessionHandle.getHandleIdentifier()).thenReturn(new HandleIdentifier());
    when(session.getSessionHandle()).thenReturn(sessionHandle);
  }

  /**
   * The query timeout fires while the query is still compiling: the cancellation closes the driver,
   * which makes the compilation fail. The operation must end up TIMEDOUT with the timeout message,
   * not fail with an illegal TIMEDOUT to ERROR state transition.
   */
  @Test
  public void testQueryTimeoutWhileCompiling() throws Exception {
    CountDownLatch driverClosed = new CountDownLatch(1);
    IDriver driver = mock(IDriver.class);
    doAnswer(invocation -> {
      driverClosed.countDown();
      return null;
    }).when(driver).close();
    when(driver.compileAndRespond(anyString())).thenAnswer(invocation -> {
      assertTrue("The query timeout did not cancel the compilation",
          driverClosed.await(30, TimeUnit.SECONDS));
      throw new CommandProcessorException("Compilation was cancelled");
    });

    SQLOperation operation = new SQLOperation(session, "select 1", ImmutableMap.of(), false, 1L);
    try (MockedStatic<DriverFactory> driverFactory = mockStatic(DriverFactory.class)) {
      driverFactory.when(() -> DriverFactory.newDriver(any(QueryState.class), any(QueryInfo.class)))
          .thenReturn(driver);

      operation.runInternal();

      OperationStatus status = operation.getStatus();
      assertEquals(OperationState.TIMEDOUT, status.getState());
      HiveSQLException e = status.getOperationException();
      assertNotNull("The operation has no timeout exception", e);
      assertEquals("HYT00", e.getSQLState());
      assertTrue("Unexpected message: " + e.getMessage(),
          e.getMessage().startsWith("Query timed out after 1 seconds"));
    } finally {
      operation.close();
    }
  }
}
