/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.exec.tez;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConfForTest;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.TezException;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestTezSessionState {
  private static final Logger LOG = LoggerFactory.getLogger(TestTezSessionState.class.getName());

  private static SessionState createSessionState() {
    HiveConf hiveConf = new HiveConfForTest(TestTezSessionState.class);
    hiveConf.set("hive.security.authorization.manager",
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdConfOnlyAuthorizerFactory");
    return SessionState.start(hiveConf);
  }

  @Test
  public void testSymlinkedLocalFilesAreLocalizedOnce() throws Exception {
    Path jarPath = Files.createTempFile("jar", "");
    Path symlinkPath = Paths.get(jarPath.toString() + ".symlink");
    Files.createSymbolicLink(symlinkPath, jarPath);

    // write some data into the fake jar, it's not a 0 length file in real life
    Files.write(jarPath, "testSymlinkedLocalFilesToBeLocalized".getBytes(), StandardOpenOption.APPEND);

    Assert.assertTrue(Files.isSymbolicLink(symlinkPath));

    HiveConf hiveConf = new HiveConfForTest(getClass());

    TezSessionState sessionState = new TezSessionState(DagUtils.getInstance(), hiveConf);

    LocalResource l1 = sessionState.createJarLocalResource(jarPath.toUri().toString());
    LocalResource l2 = sessionState.createJarLocalResource(symlinkPath.toUri().toString());

    // local resources point to the same original resource
    Assert.assertEquals(l1.getResource().toPath(), l2.getResource().toPath());
  }

  @Test
  public void testScratchDirDeletedInTheEventOfExceptionWhileOpeningSession() throws Exception {
    SessionState ss = createSessionState();
    HiveConf hiveConf = ss.getConf();

    final AtomicReference<String> scratchDirPath = new AtomicReference<>();

    TezSessionState sessionState = new TezSessionState(ss.getSessionId(), hiveConf) {
      @Override
      void openInternalUnsafe(boolean isAsync, SessionState.LogHelper console)
          throws TezException, IOException {
        super.openInternalUnsafe(isAsync, console);
        // save scratch dir here as it's nullified while calling the cleanup
        scratchDirPath.set(tezScratchDir.toUri().getPath());
        throw new RuntimeException("fake exception in openInternalUnsafe");
      }
    };

    TezSessionState.HiveResources resources =
        new TezSessionState.HiveResources(new org.apache.hadoop.fs.Path("/tmp"));

    try {
      sessionState.open(resources);
      Assert.fail("An exception should have been thrown while calling openInternal");
    } catch (Exception e) {
      Assert.assertEquals("fake exception in openInternalUnsafe", e.getMessage());
    }
    LOG.info("Checking if scratch dir exists: {}", scratchDirPath.get());
    Assert.assertFalse("Scratch dir is not supposed to exist after cleanup: " + scratchDirPath.get(),
        Files.exists(Paths.get(scratchDirPath.get())));
  }

  /**
   * Tests whether commonLocalResources is populated with app jar and localized resources when opening
   * a Tez session.
   */
  @Test
  public void testCommonLocalResourcesPopulatedOnSessionOpen() throws Exception {
    Path jarPath = Files.createTempFile("test-jar", ".jar");
    Files.write(jarPath, "testCommonLocalResourcesPopulated".getBytes(), StandardOpenOption.APPEND);

    SessionState ss = createSessionState();
    HiveConf hiveConf = ss.getConf();

    TezSessionState.HiveResources resources =
        new TezSessionState.HiveResources(new org.apache.hadoop.fs.Path("/tmp"));

    TezSessionState tempSession = new TezSessionState(ss.getSessionId(), hiveConf);

    LocalResource localizedLr = tempSession.createJarLocalResource(jarPath.toUri().toString());
    resources.localizedResources.add(localizedLr);

    final TezSessionState sessionStateForTest = new TezSessionState(ss.getSessionId(), hiveConf) {
      @Override
      void openInternalUnsafe(boolean isAsync, SessionState.LogHelper console) {
        Map<String, LocalResource> commonLocalResources = buildCommonLocalResources();
        Assert.assertEquals("commonLocalResources must contain exactly 2 jars (hive-exec app jar + localized test jar)",
            2, commonLocalResources.size());
        Assert.assertTrue("commonLocalResources must contain the hive-exec app jar",
            commonLocalResources.keySet().stream().anyMatch(k -> k.contains("hive-exec")));
        Assert.assertTrue("commonLocalResources must contain the added localized test jar",
            commonLocalResources.containsKey(DagUtils.getBaseName(localizedLr)));
      }
    };

    sessionStateForTest.open(resources);
  }

  /**
   * Tests that YarnClient is NOT initialized when queue metrics are disabled (default: interval=0).
   * This ensures zero overhead when the feature is disabled.
   */
  @Test
  public void testYarnClientNotInitializedWhenMetricsDisabled() {
    SessionState ss = createSessionState();
    HiveConf hiveConf = ss.getConf();
    
    // Default config: queue metrics disabled (interval = 0)
    Assert.assertEquals("Default interval should be 0 (disabled)",
        0, HiveConf.getTimeVar(hiveConf, HiveConf.ConfVars.HIVE_TEZ_QUEUE_METRICS_REFRESH_INTERVAL, TimeUnit.MILLISECONDS));

    TezSessionState sessionState = new TezSessionState(ss.getSessionId(), hiveConf);
    
    // Mock a TezClient and set it
    TezClient mockTezClient = Mockito.mock(TezClient.class);
    sessionState.setTezClient(mockTezClient);
    
    // getYarnClient() should return null when metrics disabled
    YarnClient yarnClient = sessionState.getYarnClient();
    Assert.assertNull("YarnClient should not be initialized when queue metrics are disabled", yarnClient);
  }

  /**
   * Tests that YarnClient IS lazily initialized when queue metrics are enabled.
   * This ensures the client is created only when needed.
   */
  @Test
  public void testYarnClientLazilyInitializedWhenMetricsEnabled() {
    SessionState ss = createSessionState();
    HiveConf hiveConf = ss.getConf();
    
    // Enable queue metrics with a positive interval
    hiveConf.setTimeVar(HiveConf.ConfVars.HIVE_TEZ_QUEUE_METRICS_REFRESH_INTERVAL, 10, TimeUnit.SECONDS);

    TezSessionState sessionState = new TezSessionState(ss.getSessionId(), hiveConf);
    
    // Mock a TezClient and set it
    TezClient mockTezClient = Mockito.mock(TezClient.class);
    sessionState.setTezClient(mockTezClient);
    
    // First call to getYarnClient() should initialize it
    YarnClient yarnClient = sessionState.getYarnClient();
    Assert.assertNotNull("YarnClient should be initialized when queue metrics are enabled", yarnClient);
    
    // Second call should return the same instance
    YarnClient yarnClient2 = sessionState.getYarnClient();
    Assert.assertSame("Should return the same YarnClient instance", yarnClient, yarnClient2);
  }

  /**
   * Tests that YarnClient is not initialized when TezClient is null,
   * even if queue metrics are enabled.
   */
  @Test
  public void testYarnClientNotInitializedWhenTezClientNull() {
    SessionState ss = createSessionState();
    HiveConf hiveConf = ss.getConf();
    
    // Enable queue metrics
    hiveConf.setTimeVar(HiveConf.ConfVars.HIVE_TEZ_QUEUE_METRICS_REFRESH_INTERVAL, 10, TimeUnit.SECONDS);

    TezSessionState sessionState = new TezSessionState(ss.getSessionId(), hiveConf);
    
    // Don't set TezClient (session is null)
    
    // getYarnClient() should return null when TezClient is not set
    YarnClient yarnClient = sessionState.getYarnClient();
    Assert.assertNull("YarnClient should not be initialized when TezClient is null", yarnClient);
  }

  /**
   * Tests the thread-safety of lazy YarnClient initialization with concurrent calls.
   */
  @Test
  public void testYarnClientLazyInitializationThreadSafety() throws InterruptedException {
    SessionState ss = createSessionState();
    HiveConf hiveConf = ss.getConf();
    
    // Enable queue metrics
    hiveConf.setTimeVar(HiveConf.ConfVars.HIVE_TEZ_QUEUE_METRICS_REFRESH_INTERVAL, 10, TimeUnit.SECONDS);

    TezSessionState sessionState = new TezSessionState(ss.getSessionId(), hiveConf);
    TezClient mockTezClient = Mockito.mock(TezClient.class);
    sessionState.setTezClient(mockTezClient);
    
    // Create multiple threads that call getYarnClient() concurrently
    final int threadCount = 10;
    Thread[] threads = new Thread[threadCount];
    YarnClient[] clients = new YarnClient[threadCount];
    
    for (int i = 0; i < threadCount; i++) {
      final int index = i;
      threads[i] = new Thread(() -> {
        clients[index] = sessionState.getYarnClient();
      });
    }
    
    // Start all threads
    for (Thread thread : threads) {
      thread.start();
    }
    
    // Wait for all threads to complete
    for (Thread thread : threads) {
      thread.join();
    }
    
    // All threads should get the same YarnClient instance
    YarnClient firstClient = clients[0];
    Assert.assertNotNull("YarnClient should be initialized", firstClient);
    
    for (int i = 1; i < threadCount; i++) {
      Assert.assertSame("All threads should get the same YarnClient instance", firstClient, clients[i]);
    }
  }
}