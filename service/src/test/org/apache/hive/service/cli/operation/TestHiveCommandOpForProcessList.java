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
import org.apache.hadoop.hive.common.io.SessionStream;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConfForTest;
import org.apache.hadoop.hive.ql.processors.ShowProcessListProcessor;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.session.HiveSession;
import org.apache.hive.service.cli.session.SessionManager;
import org.apache.hive.service.rpc.thrift.TProtocolVersion;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

public class TestHiveCommandOpForProcessList {

  private static HiveConf hiveConf;
  private ByteArrayOutputStream baos;
  private static SessionState state;
  private SessionManager sessionManager;
  private ShowProcessListProcessor processor;

  @Before
  public void setupTest() throws Exception {
    hiveConf = new HiveConfForTest(getClass());
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    hiveConf.setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    processor = new ShowProcessListProcessor();
    sessionManager = new SessionManager(null, true);
    sessionManager.init(hiveConf);
    sessionManager.start();
  }

  public void setCurrentSession() {
    SessionState.start(hiveConf);
    state = SessionState.get();
    baos = new ByteArrayOutputStream();
    state.out = new SessionStream(baos);
  }

  @Test
  public void testRunningQueryDisplay() throws HiveSQLException {

    HiveSession session1 = sessionManager
        .createSession(new SessionHandle(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8),
            TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8, "hive_test_user1", "", "10.128.00.78",
            new HashMap<String, String>(), false, "");

    HiveSession session2 = sessionManager
        .createSession(new SessionHandle(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8),
            TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8, "hive_test_user2", "", "10.128.00.78",
            new HashMap<String, String>(), false, "");

    CompletableFuture.runAsync(() -> {
          try {
            OperationHandle opHandle1 = session1.executeStatement("show databases",
                null);
            OperationHandle opHandle2 = session2.executeStatement("create table test_orc(key string,value string)",
                null);

            session1.closeOperation(opHandle1);
            session2.closeOperation(opHandle2);
          } catch (HiveSQLException e) {
            throw new RuntimeException(e);
          }
        });

    String query = "show processlist";
    setCurrentSession();
    ShowProcessListOperation sqlOperation = new ShowProcessListOperation(session2, query, processor, ImmutableMap.of());
    sqlOperation.run();
    state.out.flush();
     String output = baos.toString();

     //Show Pprocesslist output will have session ID for running query
     if(output !=null && !output.isEmpty()) {
       Assert.assertTrue(output.contains(session1.getSessionHandle().getHandleIdentifier().toString()) ||
           output.contains(session2.getSessionHandle().getHandleIdentifier().toString()));
     }
     session1.close();
     session2.close();
  }
}
