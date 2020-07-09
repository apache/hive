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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.util.ShutdownHookManagerInspector;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.OperationState;
import org.apache.hive.service.cli.OperationStatus;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.thrift.EmbeddedThriftBinaryCLIService;
import org.apache.hive.service.cli.thrift.ThriftCLIServiceClient;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class TestQueryShutdownHooks {

  private EmbeddedThriftBinaryCLIService service;
  private ThriftCLIServiceClient client;

  @Before
  public void setUp() throws Exception {

    service = new EmbeddedThriftBinaryCLIService();
    HiveConf hiveConf = new HiveConf();
    hiveConf.setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
            "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    hiveConf.setBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS, false);
    hiveConf.setVar(ConfVars.HIVE_LOCK_MANAGER, "org.apache.hadoop.hive.ql.lockmgr.EmbeddedLockManager");
    service.init(hiveConf);
    client = new ThriftCLIServiceClient(service);
    SessionHandle tempSession = client.openSession("anonymous", "anonymous", new HashMap<>());
    // any job causes creation of HadoopJobExecHelper's shutdown hook. It is once per JVM
    // We want it to be created before we count the hooks so it does not cause off by one error in our count
    client.executeStatement(tempSession, "select reflect(\"java.lang.System\", \"currentTimeMillis\")", new HashMap<>());
    client.closeSession(tempSession);
  }

  @Test
  public void testSync() throws Exception {
    Map<String, String> opConf = new HashMap<String, String>();

    SessionHandle sessHandle = client.openSession("anonymous",
            "anonymous", opConf);

    int shutdownHooksBeforeQueries = ShutdownHookManagerInspector.getShutdownHooksInOrder().size();

    String[] someQueries = {
            "CREATE TABLE sample_shutdown_hook (sample_id int, sample_value string)",
            "INSERT INTO sample_shutdown_hook VALUES (1, 'a')",
            "INSERT INTO sample_shutdown_hook VALUES (2, 'b')",
            "INSERT INTO sample_shutdown_hook VALUES (3, 'c')",
            "INSERT INTO sample_shutdown_hook VALUES (4, 'd')",
            "INSERT INTO sample_shutdown_hook VALUES (5, 'e')",
            "INSERT INTO sample_shutdown_hook VALUES (6, 'f')",
            "INSERT INTO sample_shutdown_hook VALUES (7, 'g')",
            "SELECT * FROM sample_shutdown_hook",
            "DROP TABLE sample_shutdown_hook",
    };
    for (String queryStr : someQueries) {
      OperationHandle opHandle = client.executeStatement(sessHandle, queryStr, opConf);
      assertNotNull(opHandle);
      OperationStatus opStatus = client.getOperationStatus(opHandle, false);
      assertNotNull(opStatus);
      OperationState state = opStatus.getState();
      assertEquals("Query should be finished", OperationState.FINISHED, state);
    }

    int shutdownHooksAfterFinished = ShutdownHookManagerInspector.getShutdownHooksInOrder().size();

    assertEquals(shutdownHooksBeforeQueries, shutdownHooksAfterFinished);

    client.closeSession(sessHandle);
  }

  @Test
  public void testAsync() throws Exception {
    Map<String, String> opConf = new HashMap<String, String>();

    SessionHandle sessHandle = client.openSession("anonymous", "anonymous", opConf);
    int shutdownHooksBeforeQueries = ShutdownHookManagerInspector.getShutdownHooksInOrder().size();

    String[] someQueries = {
            "select reflect(\"java.lang.Thread\", \"sleep\", bigint(1000))",
            "select reflect(\"java.lang.Thread\", \"sleep\", bigint(1000))",
            "select reflect(\"java.lang.Thread\", \"sleep\", bigint(1000))",
            "select reflect(\"java.lang.Thread\", \"sleep\", bigint(1000))"
    };

    List<OperationHandle> operationHandles = new ArrayList<>();
    for (String queryStr : someQueries) {
      OperationHandle opHandle = client.executeStatementAsync(sessHandle, queryStr, opConf);
      assertNotNull(opHandle);
      operationHandles.add(opHandle);
    }

    boolean allComplete = false;
    final long step = 200;
    final long timeout = System.currentTimeMillis() + 60000;

    while (!allComplete) {
      allComplete = true;
      for (OperationHandle opHandle : operationHandles) {
        OperationStatus operationStatus = client.getOperationStatus(opHandle, false);
        if (operationStatus.getState() != OperationState.FINISHED) {
          if (System.currentTimeMillis() > timeout) {
            fail("Queries did not complete timely");
          }
          allComplete = false;
          Thread.sleep(step);
          break;
        }
      }
    }

    int shutdownHooksAfterFinished = ShutdownHookManagerInspector.getShutdownHooksInOrder().size();

    assertEquals(shutdownHooksBeforeQueries, shutdownHooksAfterFinished);
    client.closeSession(sessHandle);
  }

  @Test
  public void testShutdownHookManagerIsRegistered() throws HiveSQLException, InterruptedException {
    Map<String, String> opConf = new HashMap<String, String>();

    SessionHandle sessHandle = client.openSession("anonymous", "anonymous", opConf);
    int shutdownHooksBeforeQuery = ShutdownHookManagerInspector.getShutdownHooksInOrder().size();

    String queryStr = "select reflect(\"java.lang.Thread\", \"sleep\", bigint(5000))";
    OperationHandle opHandle = client.executeStatementAsync(sessHandle, queryStr, opConf);
    assertNotNull(opHandle);

    assertEquals(shutdownHooksBeforeQuery + 1, ShutdownHookManagerInspector.getShutdownHooksInOrder().size());

    final long step = 200;
    final long timeout = System.currentTimeMillis() + 60000;

    while (true) {
      OperationStatus operationStatus = client.getOperationStatus(opHandle, false);
      if (operationStatus.getState() == OperationState.FINISHED) {
        break;
      }
      if (System.currentTimeMillis() > timeout) {
        fail("Query did not complete timely");
      }
      Thread.sleep(step);
    }

    int shutdownHooksAfterFinished = ShutdownHookManagerInspector.getShutdownHooksInOrder().size();

    assertEquals(shutdownHooksBeforeQuery, shutdownHooksAfterFinished);
    client.closeSession(sessHandle);
  }
}
