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

package org.apache.hive.service.cli.operation;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.service.cli.HandleIdentifier;
import org.apache.hive.service.cli.OperationState;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.session.HiveSession;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

import java.lang.reflect.Field;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestSQLOperationDriverCleanup {

  private HiveSession session;
  private IDriver driver;

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
    driver = mock(IDriver.class);
  }

  @Test
  public void testSQLOperationCloseReleasesDriverInHPLSQLMode() throws Exception {
    SQLOperation operation = new SQLOperation(session, "insert into test values (1)",
        ImmutableMap.of(), false, 0L, true);
    setDriver(operation, driver);

    operation.close();

    InOrder order = inOrder(driver);
    order.verify(driver).close();
    order.verify(driver).destroy();
    assertNull(getDriver(operation));
  }

  @Test
  public void testSQLOperationCloseReleasesDriverInNonHPLSQLMode() throws Exception {
    SQLOperation operation = new SQLOperation(session, "insert into test values (1)",
        ImmutableMap.of(), false, 0L, false);
    setDriver(operation, driver);

    operation.close();

    InOrder order = inOrder(driver);
    order.verify(driver).close();
    order.verify(driver).destroy();
    assertEquals(OperationState.CLOSED, operation.getStatus().getState());
    assertNull(getDriver(operation));
  }

  private static void setDriver(SQLOperation operation, IDriver driver) throws Exception {
    Field driverField = SQLOperation.class.getDeclaredField("driver");
    driverField.setAccessible(true);
    driverField.set(operation, driver);
  }

  private static IDriver getDriver(SQLOperation operation) throws Exception {
    Field driverField = SQLOperation.class.getDeclaredField("driver");
    driverField.setAccessible(true);
    return (IDriver) driverField.get(operation);
  }
}
