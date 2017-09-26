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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.operation.ExecuteStatementOperation;
import org.apache.hive.service.cli.operation.OperationManager;
import org.apache.hive.service.rpc.thrift.TProtocolVersion;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import static org.mockito.Matchers.*;

import java.util.HashMap;
import java.util.Map;



public class TestHiveSessionImpl {
  /**
   * Verifying OperationManager.closeOperation(opHandle) is invoked when
   * get HiveSQLException during sync query
   * @throws HiveSQLException
   */
  @Test
  public void testLeakOperationHandle() throws HiveSQLException {
    //create HiveSessionImpl object
    TProtocolVersion protocol = TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V2;
    String username = "";
    String password = "";
    HiveConf serverhiveConf = new HiveConf();
    String ipAddress = null;
    HiveSessionImpl session = new HiveSessionImpl(null, protocol, username, password,
      serverhiveConf, ipAddress) {
      @Override
      protected synchronized void acquire(boolean userAccess, boolean isOperation) {
      }

      @Override
      protected synchronized void release(boolean userAccess, boolean isOperation) {
      }
    };

    //mock operationManager for session
    OperationManager operationManager = Mockito.mock(OperationManager.class);
    session.setOperationManager(operationManager);

    //mock operation and opHandle for operationManager
    ExecuteStatementOperation operation = Mockito.mock(ExecuteStatementOperation.class);
    OperationHandle opHandle = Mockito.mock(OperationHandle.class);
    Mockito.when(operation.getHandle()).thenReturn(opHandle);
    Map<String, String> confOverlay = new HashMap<String, String>();
    String hql = "drop table if exists table_not_exists";
    Mockito.when(operationManager.newExecuteStatementOperation(same(session), eq(hql),
        (Map<String, String>)Mockito.any(), eq(true), eq(0L))).thenReturn(operation);

    try {

      //Running a normal async query with no exceptions,then no need to close opHandle
      session.open(new HashMap<String, String>());
      session.executeStatementAsync(hql, confOverlay);
      Mockito.verify(operationManager, Mockito.times(0)).closeOperation(opHandle);

      // Throw an HiveSqlException when do async calls
      Mockito.doThrow(new HiveSQLException("Fail for clean up test")).when(operation).run();
      session.executeStatementAsync(hql, confOverlay);
      Assert.fail("HiveSqlException expected.");

    } catch (HiveSQLException e) {
      if (!"Fail for clean up test".equals(e.getMessage())) {
        Assert.fail("unexpected exception:" + e.getMessage());
      }
      //operationManager.closeOperation() is expected to be invoked once
      Mockito.verify(operationManager, Mockito.times(1)).closeOperation(opHandle);
    }
  }

}

