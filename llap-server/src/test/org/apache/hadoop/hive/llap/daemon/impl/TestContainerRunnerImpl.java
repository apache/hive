/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.llap.daemon.impl;

import com.google.common.primitives.Ints;
import com.google.protobuf.ByteString;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.DaemonId;
import org.apache.hadoop.hive.llap.configuration.LlapDaemonConfiguration;
import org.apache.hadoop.hive.llap.daemon.LlapDaemonTestUtils;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.RegisterDagRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SubmitWorkRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.QueryIdentifierProto;
import org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorMetrics;
import org.apache.hadoop.hive.llap.metrics.LlapMetricsSystem;
import org.apache.hadoop.hive.llap.metrics.MetricsUtils;
import org.apache.hadoop.hive.llap.security.LlapTokenIdentifier;
import org.apache.hadoop.hive.llap.security.LlapUgiFactoryFactory;
import org.apache.hadoop.hive.llap.shufflehandler.ShuffleHandler;
import org.apache.hadoop.hive.llap.tezplugins.LlapTezUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.tez.common.security.TokenCache;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.net.SocketFactory;
import java.io.File;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.mock;

/**
 * Test ContainerRunnerImpl.
 */
public class TestContainerRunnerImpl {
  ContainerRunnerImpl containerRunner;
  LlapDaemonConfiguration daemonConf = new LlapDaemonConfiguration();
  private final int numExecutors = 1;
  private final int waitQueueSize = HiveConf.getIntVar(
      daemonConf, HiveConf.ConfVars.LLAP_DAEMON_TASK_SCHEDULER_WAIT_QUEUE_SIZE);
  private final boolean enablePreemption = false;
  private final int numLocalDirs = 1;
  private final String[] localDirs = new String[numLocalDirs];
  private final File testWorkDir = new File("target", "container-runner-tests");
  private final AtomicReference<Integer> shufflePort = new AtomicReference<>();
  private final AtomicReference<InetSocketAddress> srvAddress = new AtomicReference<>();
  private final int executorMemoryPerInstance = 1024;
  private LlapDaemonExecutorMetrics metrics;
  private AMReporter amReporter;
  private final String testUser = "testUser";
  private final String appId = "application_1540489363818_0021";
  private final int dagId = 1234;
  private final int vId = 12345;
  private final String hostname = "test.cluster";
  private final DaemonId daemonId = new DaemonId(testUser,
      "ContainerTests", hostname,
      appId, System.currentTimeMillis());
  private final SocketFactory socketFactory = NetUtils.getDefaultSocketFactory(daemonConf);
  private QueryTracker queryTracker;
  private TaskExecutorService executorService;
  private InetSocketAddress serverSocket;


  @Before
  public void setup() throws Exception {

    String[] strIntervals = HiveConf.getTrimmedStringsVar(daemonConf,
        HiveConf.ConfVars.LLAP_DAEMON_TASK_PREEMPTION_METRICS_INTERVALS);
    List<Integer> intervalList = new ArrayList<>();
    if (strIntervals != null) {
      for (String strInterval : strIntervals) {
        intervalList.add(Integer.valueOf(strInterval));
      }
    }

    amReporter = mock(AMReporter.class);
    serverSocket = new InetSocketAddress("localhost", 0);
    srvAddress.set(serverSocket);

    this.metrics = LlapDaemonExecutorMetrics
        .create("ContinerRunerTests", MetricsUtils.getUUID(), numExecutors, waitQueueSize,
            Ints.toArray(intervalList), 0, 0L, 0);

    for (int i = 0; i < numLocalDirs; i++) {
      File f = new File(testWorkDir, "localDir");
      f.mkdirs();
      localDirs[i] = f.getAbsolutePath();
    }
    String waitQueueSchedulerClassName = HiveConf.getVar(
        daemonConf, HiveConf.ConfVars.LLAP_DAEMON_WAIT_QUEUE_COMPARATOR_CLASS_NAME);

    queryTracker = new QueryTracker(daemonConf, localDirs, daemonId.getClusterString());
    executorService = new TaskExecutorService(numExecutors, waitQueueSize,
        waitQueueSchedulerClassName, enablePreemption, Thread.currentThread().getContextClassLoader(), metrics, null);

    shufflePort.set(HiveConf.getIntVar(
        daemonConf, HiveConf.ConfVars.LLAP_DAEMON_RPC_PORT));
    containerRunner = new ContainerRunnerImpl(daemonConf, numExecutors,
        this.shufflePort, srvAddress, executorMemoryPerInstance, metrics,
        amReporter, queryTracker, executorService, daemonId, LlapUgiFactoryFactory
        .createFsUgiFactory(daemonConf), socketFactory);

    ShuffleHandler.initializeAndStart(daemonConf);

    executorService.init(daemonConf);
    executorService.start();
    queryTracker.init(daemonConf);
    queryTracker.start();
    containerRunner.init(daemonConf);
    containerRunner.start();
  }

  @After
  public void cleanup() throws Exception {
    for (Object key : ShuffleHandler.get().getRegisteredApps().keySet()) {
      String appId = (String) key;
      ShuffleHandler.get().unregisterDag(null, appId, dagId);
    }

    containerRunner.serviceStop();
    queryTracker.serviceStop();
    executorService.serviceStop();
    executorService.serviceStop();
    LlapMetricsSystem.shutdown();
  }

  @Test(timeout = 10000)
  public void testRegisterDag() throws Exception {
    Credentials credentials = new Credentials();
    Token<LlapTokenIdentifier> sessionToken = new Token<>(
        "identifier".getBytes(), "testPassword".getBytes(), new Text("kind"), new Text("service"));
    TokenCache.setSessionToken(sessionToken, credentials);

    RegisterDagRequestProto request = RegisterDagRequestProto.newBuilder()
        .setUser(testUser)
        .setCredentialsBinary(ByteString.copyFrom(LlapTezUtils.serializeCredentials(credentials)))
        .setQueryIdentifier(
            QueryIdentifierProto.newBuilder()
                .setApplicationIdString(appId)
                .setDagIndex(dagId)
                .build())
        .build();

    containerRunner.registerDag(request);
    Assert.assertEquals(ShuffleHandler.get().getRegisteredApps().size(), 1);
    Assert.assertEquals(ShuffleHandler.get().getRegisteredApps().get(appId), dagId);
    Assert.assertEquals(ShuffleHandler.get().getRegisteredDirectories().size(), 0);

    containerRunner.registerDag(request);
    Assert.assertEquals(ShuffleHandler.get().getRegisteredApps().size(), 1);
    Assert.assertEquals(ShuffleHandler.get().getRegisteredApps().get(appId), dagId);
    Assert.assertEquals(ShuffleHandler.get().getRegisteredDirectories().size(), 0);

    SubmitWorkRequestProto sRequest =
        LlapDaemonTestUtils.buildSubmitProtoRequest(1, appId,
            dagId, vId, "dagName", 0, 0,
            0, 0, 1,
            credentials);

    containerRunner.submitWork(sRequest);
    Assert.assertEquals(ShuffleHandler.get().getRegisteredApps().size(), 1);
    Assert.assertEquals(ShuffleHandler.get().getRegisteredApps().get(appId), dagId);
    if (ShuffleHandler.get().isDirWatcherEnabled()) {
      Assert.assertEquals(ShuffleHandler.get().getRegisteredDirectories().size(), 1);
      Assert.assertEquals(ShuffleHandler.get().getRegisteredDirectories().get(appId), dagId);
    }
  }

  @Test(timeout = 10000)
  public void testSubmitSameFragment() throws Exception {
    Credentials credentials = new Credentials();
    Token<LlapTokenIdentifier> sessionToken = new Token<>(
            "identifier".getBytes(), "testPassword".getBytes(), new Text("kind"), new Text("service"));
    TokenCache.setSessionToken(sessionToken, credentials);

    RegisterDagRequestProto request = RegisterDagRequestProto.newBuilder()
            .setUser(testUser)
            .setCredentialsBinary(ByteString.copyFrom(LlapTezUtils.serializeCredentials(credentials)))
            .setQueryIdentifier(
                    QueryIdentifierProto.newBuilder()
                            .setApplicationIdString(appId)
                            .setDagIndex(dagId)
                            .build())
            .build();
    containerRunner.registerDag(request);
    Assert.assertEquals(ShuffleHandler.get().getRegisteredApps().size(), 1);
    Assert.assertEquals(ShuffleHandler.get().getRegisteredApps().get(appId), dagId);
    Assert.assertEquals(ShuffleHandler.get().getRegisteredDirectories().size(), 0);

    int fragNum = 1;
    int attemptNum = 0;
    SubmitWorkRequestProto sRequest1 =
            LlapDaemonTestUtils.buildSubmitProtoRequest(fragNum, attemptNum, appId,
                    dagId, vId, "dagName", 0, 0,
                    0, 0, 1,
                    credentials);

    containerRunner.submitWork(sRequest1);
    Assert.assertEquals(ShuffleHandler.get().getRegisteredApps().size(), 1);
    Assert.assertEquals(ShuffleHandler.get().getRegisteredApps().get(appId), dagId);
    if (ShuffleHandler.get().isDirWatcherEnabled()) {
      Assert.assertEquals(ShuffleHandler.get().getRegisteredDirectories().size(), 1);
      Assert.assertEquals(ShuffleHandler.get().getRegisteredDirectories().get(appId), dagId);
    }

    // submitWork() was successful, should show up as an active fragment.
    Assert.assertEquals(1, containerRunner.getExecutorStatus().size());
    boolean caughtException = false;

    // Try exact same fragment ID + attempt number - should fail.
    try {
      SubmitWorkRequestProto sRequest2 =
              LlapDaemonTestUtils.buildSubmitProtoRequest(fragNum, attemptNum, appId,
                      dagId, vId, "dagName", 0, 0,
                      0, 0, 1,
                      credentials);

      containerRunner.submitWork(sRequest2);
    } catch (IllegalArgumentException err) {
      err.printStackTrace();
      caughtException = true;
    }
    Assert.assertTrue(caughtException);
    // request failed so should still only have the 1 fragment
    Assert.assertEquals(1, containerRunner.getExecutorStatus().size());

    // Try same fragment ID with different attempt number - should work.
    attemptNum = 1;
    SubmitWorkRequestProto sRequest3 =
            LlapDaemonTestUtils.buildSubmitProtoRequest(fragNum, attemptNum, appId,
                    dagId, vId, "dagName", 0, 0,
                    0, 0, 1,
                    credentials);

    containerRunner.submitWork(sRequest3);

    // Should now have 2 fragments registered.
    Assert.assertEquals(2, containerRunner.getExecutorStatus().size());
  }
}
