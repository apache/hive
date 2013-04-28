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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.OperationState;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.operation.OperationManager;


public class TestHiveSession {

  @Test
  public void checkOperationClosing() throws Exception {
    OperationManager opMgr = new OperationManager();
    HiveSession session = new HiveSessionImpl("user", "passw", null);

    session.setOperationManager(opMgr);

    OperationHandle opHandle1 = session.executeStatement("use default", null);
    assertNotNull(opHandle1);
    assertEquals(OperationState.FINISHED, opMgr.getOperationState(opHandle1));

    OperationHandle opHandle2 = session.executeStatement("show databases", null);
    assertNotNull(opHandle2);
    assertEquals(OperationState.FINISHED, opMgr.getOperationState(opHandle2));

    session.closeOperation(opHandle1); // Don't close directly from opMgr!

    try {
      opMgr.getOperationState(opHandle1);
      fail("Shouldn't work!");
    } catch (Exception e) {
      assertTrue(e instanceof HiveSQLException);
    }

    session.close(); // Close all remaining associated operations

    try {
      opMgr.getOperationState(opHandle2);
      fail("Shouldn't work this way now that it's fixed (HIVE-4398)!");
    } catch (Exception e) {
      assertTrue(e instanceof HiveSQLException);
    }
  }
}
