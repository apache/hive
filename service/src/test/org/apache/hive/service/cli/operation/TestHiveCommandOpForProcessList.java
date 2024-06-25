package org.apache.hive.service.cli.operation;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hive.common.io.SessionStream;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.processors.ShowProcesslistProcessor;
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
  private ShowProcesslistProcessor processor;

  @Before
  public void setupTest() throws Exception {
    hiveConf = new HiveConf();
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    hiveConf.setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    processor =new ShowProcesslistProcessor();
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
            OperationHandle opHandle = session1.executeStatement("show databases",
                null);
            OperationHandle opHandle1 = session2.executeStatement("create table test_orc(key string,value string)",
                null);
          } catch (HiveSQLException e) {
            throw new RuntimeException(e);
          }
        });

    String query = "show processlist";
    setCurrentSession();
    HiveCommandOperation sqlOperation = new HiveCommandOperation(session2, query, processor, ImmutableMap.of());
    sqlOperation.run();
     state.out.flush();
     String output = baos.toString();
     if(output !=null && !output.isEmpty()) {
       Assert.assertTrue(output.contains(session1.getSessionHandle().getHandleIdentifier().toString()) ||
           output.contains(session2.getSessionHandle().getHandleIdentifier().toString()));
     }
  }
}
