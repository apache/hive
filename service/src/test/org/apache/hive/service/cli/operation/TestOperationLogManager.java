/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.service.cli.operation;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.cli.CLIService;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.session.HiveSession;
import org.apache.hive.service.cli.session.HiveSessionImpl;
import org.apache.hive.service.cli.session.SessionManager;
import org.apache.hive.service.cli.thrift.EmbeddedThriftBinaryCLIService;
import org.apache.hive.service.cli.thrift.ThriftCLIServiceClient;
import org.apache.hive.service.rpc.thrift.TProtocolVersion;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestOperationLogManager {
  private static final AtomicInteger salt = new AtomicInteger(new Random().nextInt());
  private final String TEST_DATA_DIR = System.getProperty("java.io.tmpdir") + File.separator +
      TestOperationLogManager.class.getCanonicalName() + "-" + System.currentTimeMillis() + "_" + salt.getAndIncrement();
  private HiveConf hiveConf;

  @Before
  public void setUp() throws Exception {
    hiveConf = new HiveConf();
    HiveConf.setBoolVar(hiveConf, HiveConf.ConfVars.HIVE_SERVER2_HISTORIC_OPERATION_LOG_ENABLED, true);
    HiveConf.setIntVar(hiveConf, HiveConf.ConfVars.HIVE_SERVER2_WEBUI_MAX_HISTORIC_QUERIES, 1);
    HiveConf.setIntVar(hiveConf, HiveConf.ConfVars.HIVE_SERVER2_WEBUI_PORT, 8080);
    HiveConf.setBoolVar(hiveConf, HiveConf.ConfVars.HIVE_IN_TEST, true);
    HiveConf.setBoolVar(hiveConf, HiveConf.ConfVars.HIVE_TESTING_REMOVE_LOGS, false);
    HiveConf.setVar(hiveConf, HiveConf.ConfVars.HIVE_SERVER2_HISTORIC_OPERATION_LOG_FETCH_MAXBYTES, "128B");
    HiveConf.setBoolVar(hiveConf, HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    HiveConf.setVar(hiveConf, HiveConf.ConfVars.HIVE_SERVER2_LOGGING_OPERATION_LOG_LOCATION,
        TEST_DATA_DIR + File.separator + "operation_logs");
    HiveConf.setVar(hiveConf, HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
            "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
  }

  // Create subclass of EmbeddedThriftBinaryCLIService, set isEmbedded to false
  private class MyThriftBinaryCLIService extends EmbeddedThriftBinaryCLIService {
    public MyThriftBinaryCLIService() {
      super();
      isEmbedded = false;
    }
  }

  @Test
  public void testOperationLogManager() throws Exception {
    MyThriftBinaryCLIService service = new MyThriftBinaryCLIService();
    service.init(hiveConf);
    ThriftCLIServiceClient client = new ThriftCLIServiceClient(service);
    SessionManager sessionManager = ((CLIService)service.getService()).getSessionManager();

    SessionHandle session1 = client.openSession("user1", "foobar",
        Collections.<String, String>emptyMap());
    OperationHandle opHandle1 = client.executeStatement(session1, "select 1 + 1", null);
    Operation operation1 = sessionManager.getOperationManager().getOperation(opHandle1);

    String logLocation = operation1.getOperationLog().toString();

    assertEquals(logLocation, ((SQLOperation)operation1).getQueryInfo().getOperationLogLocation());

    File operationLogFile = new File(operation1.getOperationLog().toString());
    assertTrue(operationLogFile.exists());

    client.closeOperation(opHandle1);
    String op1HistoricLogLocation = ((SQLOperation)operation1).getQueryInfo().getOperationLogLocation();
    File op1HistoricLogFile = new File(op1HistoricLogLocation);
    assertTrue(op1HistoricLogFile.exists());

    // check that the log of operation1 exists even if the session1 has been closed
    client.closeSession(session1);
    assertTrue(op1HistoricLogFile.exists());

    SessionHandle session2 = client.openSession("user1", "foobar",
        Collections.<String, String>emptyMap());
    OperationHandle opHandle2 = client.executeStatement(session2, "select 2 + 2", null);
    Operation operation2 = sessionManager.getOperationManager().getOperation(opHandle2);
    client.closeOperation(opHandle2);

    // the operation1 becomes unreachable
    OperationManager operationManager = sessionManager.getOperationManager();
    assertTrue(operationManager.getAllCachedQueryIds().size() == 1
        && operationManager.getLiveQueryInfos().isEmpty());
    assertNull(operationManager.getQueryInfo(opHandle1.getHandleIdentifier().toString()));


    // OperationLogManager cleans up operation1's historical log, operation2's historical log remains.
    OperationLogManager logManager = sessionManager.getLogManager().get();
    logManager.deleteHistoricQueryLogs();
    assertFalse(op1HistoricLogFile.exists());

    // though session2 is closed, but there exists his operation(operation2) in cache and
    // log file under the historic session log dir, so the historic log dir of session2 would not be cleaned
    String op2LogLocation = ((SQLOperation)operation2).getQueryInfo().getOperationLogLocation();
    client.closeSession(session2);
    assertNotNull(operationManager.getQueryInfo(opHandle2.getHandleIdentifier().toString()));
    assertTrue(operationManager.getAllCachedQueryIds().size() == 1
        && operationManager.getLiveQueryInfos().isEmpty());

    logManager.deleteHistoricQueryLogs();
    assertTrue(new File(op2LogLocation).exists());
    FileUtils.deleteQuietly(new File(OperationLogManager.getHistoricLogDir()));
  }

  @Test
  public void testGetOperationLog() throws Exception {
    FakeHiveSession session = new FakeHiveSession(
        new SessionHandle(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V11), new HiveConf(hiveConf));
    session.setOperationLogSessionDir(new File(HiveConf.getVar(hiveConf,
        HiveConf.ConfVars.HIVE_SERVER2_LOGGING_OPERATION_LOG_LOCATION)));
    session.open(new HashMap<>());
    FakeSQLOperation operation = new FakeSQLOperation(session);
    operation.createOperationLog();
    String logLocation = operation.getOperationLog().toString();
    File logFile = new File(logLocation);
    int readLenght = (int) HiveConf.getSizeVar(hiveConf,
        HiveConf.ConfVars.HIVE_SERVER2_HISTORIC_OPERATION_LOG_FETCH_MAXBYTES);
    byte[] content = writeBytes(logFile, 2 * readLenght);
    operation.getQueryInfo().setOperationLogLocation(logLocation);
    String operationLog = OperationLogManager.getOperationLog(operation.getQueryInfo());
    assertEquals(logLocation, operation.getQueryInfo().getOperationLogLocation());
    assertEquals(new String(content, content.length - readLenght, readLenght), operationLog);
    FileUtils.deleteQuietly(new File(OperationLogManager.getHistoricLogDir()));
  }

  private byte[] writeBytes(File logFile, int maxBytes) throws Exception {
    byte[] samples = ("abcdefghigklmnopq" + System.lineSeparator()).getBytes();
    int written = 0;
    byte[] result;
    try (FileOutputStream fos = new FileOutputStream(logFile, true);
         ByteArrayOutputStream baos = new ByteArrayOutputStream(maxBytes)) {
      while (written < maxBytes) {
        fos.write(samples);
        baos.write(samples);
        written += samples.length;
      }
      result = baos.toByteArray();
    }
    return result;
  }

  private class FakeHiveSession extends HiveSessionImpl {
    public FakeHiveSession(SessionHandle sessionHandle, HiveConf serverConf) {
      super(sessionHandle, TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V11, "dummy", "",
          serverConf, "0.0.0.0", null);
    }
  }

  private class FakeSQLOperation extends SQLOperation {
    public FakeSQLOperation(HiveSession parentSession) {
      super(parentSession, "select 1", new HashMap<>(), true, 0);
    }
  }

}
