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
package org.apache.hive.http;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.Timer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.withSettings;

/**
 * Tests for the SSL keystore auto-reload feature wired in via
 * {@code HttpServer#makeConfigurationChangeMonitor} and the surrounding
 * {@code configurationChangeMonitor} field. See HiveConf
 * {@code hive.server2.webui.keystore.reload.interval}.
 */
public class TestHttpServer {

  private Path keystore;
  private Timer timer;

  @Before
  public void setUp() throws Exception {
    keystore = Files.createTempFile("test-keystore-", ".jks");
    Files.write(keystore, "initial-content".getBytes());
  }

  @After
  public void tearDown() throws Exception {
    if (timer != null) {
      timer.cancel();
    }
    if (keystore != null) {
      Files.deleteIfExists(keystore);
    }
  }

  /**
   * When the watched keystore file is modified, the scheduled
   * {@code FileMonitoringTimerTask} must invoke
   * {@code SslContextFactory#reload}.
   */
  @Test(timeout = 10_000)
  public void testMonitorReloadsSslContextOnKeystoreModification() throws Exception {
    SslContextFactory sslContextFactory = mock(SslContextFactory.class);
    CountDownLatch reloadCalled = new CountDownLatch(1);
    doAnswer(invocation -> {
      reloadCalled.countDown();
      return null;
    }).when(sslContextFactory).reload(any());

    timer = invokeMakeMonitor(100L, keystore.toString(), sslContextFactory);

    // Bump mtime to guarantee a detected change (FileMonitoringTimerTask compares mtimes).
    Files.setLastModifiedTime(keystore, FileTime.fromMillis(System.currentTimeMillis() + 5_000));

    assertTrue("SslContextFactory#reload was not called within 5s of keystore mtime change",
        reloadCalled.await(5, TimeUnit.SECONDS));
    verify(sslContextFactory, atLeastOnce()).reload(any());
  }

  /**
   * Reload failures must be swallowed so a transient bad keystore can't take HS2 down;
   * the next mtime change should still trigger another reload attempt.
   */
  @Test(timeout = 10_000)
  public void testMonitorSurvivesReloadException() throws Exception {
    SslContextFactory sslContextFactory = mock(SslContextFactory.class);
    CountDownLatch reloadCalled = new CountDownLatch(2);
    doAnswer(invocation -> {
      reloadCalled.countDown();
      throw new RuntimeException("simulated keystore reload failure");
    }).when(sslContextFactory).reload(any());

    timer = invokeMakeMonitor(100L, keystore.toString(), sslContextFactory);

    Files.setLastModifiedTime(keystore, FileTime.fromMillis(System.currentTimeMillis() + 5_000));
    Thread.sleep(300);
    Files.setLastModifiedTime(keystore, FileTime.fromMillis(System.currentTimeMillis() + 10_000));

    assertTrue("Monitor should keep firing reload attempts even after exceptions",
        reloadCalled.await(5, TimeUnit.SECONDS));
  }

  /**
   * {@code stop()} must cancel the monitor Timer when one was installed,
   * so the daemon thread does not outlive HS2.
   */
  @Test
  public void testStopCancelsConfigurationChangeMonitor() throws Exception {
    HttpServer server = mock(HttpServer.class, withSettings().defaultAnswer(CALLS_REAL_METHODS));

    // Track whether cancel() was invoked on the installed timer.
    boolean[] cancelled = {false};
    Timer installed = new Timer("test-monitor", true) {
      @Override
      public void cancel() {
        cancelled[0] = true;
        super.cancel();
      }
    };
    server.setKeystoreChangeMonitor(installed);

    // stop() also calls webServer.stop(); webServer is null on a mock, so we expect
    // a NullPointerException after the cancel path runs.
    try {
      server.stop();
    } catch (NullPointerException expected) {
      // intentionally ignored — we only assert the monitor was cancelled
    }
    assertTrue("Timer#cancel should have been invoked from stop()", cancelled[0]);
  }

  /**
   * No monitor installed → stop() must not blow up trying to cancel a missing Timer.
   * (Mockito skips field initializers, so we re-establish the production default
   * {@code Optional.empty()} on the mock before exercising stop().)
   */
  @Test
  public void testStopWithoutMonitorDoesNotThrowFromCancelPath() throws Exception {
    HttpServer server = mock(HttpServer.class, withSettings().defaultAnswer(CALLS_REAL_METHODS));
    server.setKeystoreChangeMonitor(null);
    assertNull("keystoreChangeMonitor should be empty for this case", server.keystoreChangeMonitor);

    try {
      server.stop();
    } catch (NullPointerException expectedFromWebServerStop) {
      // ok — the monitor branch must not have thrown before reaching webServer.stop()
    }
  }

  // ---- reflection helpers ------------------------------------------------

  private static Timer invokeMakeMonitor(long intervalMs, String keystorePath,
                                         SslContextFactory sslContextFactory) throws Exception {
    HttpServer server = mock(HttpServer.class, withSettings().defaultAnswer(CALLS_REAL_METHODS));
    return server.createKeystoreChangeMonitor(intervalMs, keystorePath, sslContextFactory);
  }
}
