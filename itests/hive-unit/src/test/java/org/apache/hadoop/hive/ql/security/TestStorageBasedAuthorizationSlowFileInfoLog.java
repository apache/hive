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

package org.apache.hadoop.hive.ql.security;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConfForTest;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.metadata.StringAppender;
import org.apache.hadoop.hive.ql.security.authorization.AuthorizationPreEventListener;
import org.apache.hadoop.hive.ql.security.authorization.StorageBasedAuthorizationProvider;
import org.apache.logging.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Integration tests for the slow-getFileInfo WARN logging in
 * StorageBasedAuthorizationProvider.checkPermissions().
 *
 * Exercises the real production code path end-to-end:
 *   msc.getDatabase() -> AuthorizationPreEventListener.authorizeReadDatabase()
 *   -> StorageBasedAuthorizationProvider.checkPermissions()
 *   -> FileUtils.getFileStatusOrNull() -> FileSystem.getFileStatus()
 *
 * Two scenarios are tested:
 * 1. threshold=-1 (disabled): no WARN emitted regardless of elapsed time
 * 2. threshold=10ms + SlowLocalFileSystem (sleeps 30ms): WARN IS emitted
 *    because elapsed(~30ms) > threshold(10ms) through the real code path
 *
 * Each test starts its own metastore instance so System.setProperty values
 * (threshold, fs.file.impl) are picked up by the metastore's HiveConf at startup.
 */
public class TestStorageBasedAuthorizationSlowFileInfoLog {

  private static final String THRESHOLD_KEY =
      HiveConf.ConfVars.METASTORE_AUTHORIZATION_FILEINFO_SLOW_WARN_THRESHOLD_MS.varname;
  private static final String LOGGER_NAME =
      StorageBasedAuthorizationProvider.class.getName();

  private HiveMetaStoreClient msc;
  private StringAppender appender;

  /**
   * LocalFileSystem wrapper that sleeps 30ms in getFileStatus to simulate
   * a slow HDFS NameNode RPC. Registered via fs.file.impl so it is used
   * by the metastore's StorageBasedAuthorizationProvider when it calls
   * FileUtils.getFileStatusOrNull(fs, path).
   */
  public static class SlowLocalFileSystem extends LocalFileSystem {
    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
      try {
        Thread.sleep(30); // 30ms > 10ms threshold used in the WARN test
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return super.getFileStatus(f);
    }
  }

  @Before
  public void setUpAppender() {
    appender = StringAppender.createStringAppender("%m");
    appender.addToLogger(LOGGER_NAME, Level.WARN);
    appender.start();
  }

  @After
  public void tearDown() throws Exception {
    if (appender != null) {
      appender.removeFromLogger(LOGGER_NAME);
    }
    if (msc != null) {
      msc.close();
    }
    InjectableDummyAuthenticator.injectMode(false);
    System.clearProperty(HiveConf.ConfVars.METASTORE_PRE_EVENT_LISTENERS.varname);
    System.clearProperty(HiveConf.ConfVars.HIVE_METASTORE_AUTHORIZATION_MANAGER.varname);
    System.clearProperty(HiveConf.ConfVars.HIVE_METASTORE_AUTHENTICATOR_MANAGER.varname);
    System.clearProperty(HiveConf.ConfVars.HIVE_METASTORE_AUTHORIZATION_AUTH_READS.varname);
    System.clearProperty(THRESHOLD_KEY);
    System.clearProperty("fs.file.impl");
    System.clearProperty("fs.file.impl.disable.cache");
  }

  /**
   * Starts a fresh metastore with all required System properties already set so
   * the metastore-side HiveConf picks them up at construction time.
   *
   * @param thresholdMs value for hive.metastore.authorization.fileinfo.slow.warn.threshold.ms
   * @param slowFs      if true, registers SlowLocalFileSystem as fs.file.impl
   */
  private void startMetaStore(long thresholdMs, boolean slowFs) throws Exception {
    System.setProperty(HiveConf.ConfVars.METASTORE_PRE_EVENT_LISTENERS.varname,
        AuthorizationPreEventListener.class.getName());
    System.setProperty(HiveConf.ConfVars.HIVE_METASTORE_AUTHORIZATION_MANAGER.varname,
        StorageBasedAuthorizationProvider.class.getName());
    System.setProperty(HiveConf.ConfVars.HIVE_METASTORE_AUTHENTICATOR_MANAGER.varname,
        InjectableDummyAuthenticator.class.getName());
    System.setProperty(HiveConf.ConfVars.HIVE_METASTORE_AUTHORIZATION_AUTH_READS.varname, "true");
    System.setProperty(THRESHOLD_KEY, String.valueOf(thresholdMs));
    if (slowFs) {
      System.setProperty("fs.file.impl", SlowLocalFileSystem.class.getName());
      System.setProperty("fs.file.impl.disable.cache", "true");
    }

    int port = MetaStoreTestUtils.startMetaStoreWithRetry();

    HiveConf clientConf = new HiveConfForTest(getClass());
    clientConf.setVar(HiveConf.ConfVars.METASTORE_URIS, "thrift://localhost:" + port);
    clientConf.setIntVar(HiveConf.ConfVars.METASTORE_THRIFT_CONNECTION_RETRIES, 3);
    clientConf.setBoolVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_ENABLED, false);

    msc = new HiveMetaStoreClient(clientConf);
    InjectableDummyAuthenticator.injectMode(false);
  }

  /**
   * threshold=-1 (disabled): no WARN emitted after a real getDatabase() auth check.
   * Fully deterministic — threshold=-1 suppresses the WARN unconditionally.
   */
  @Test
  public void testNoSlowWarnWhenThresholdDisabled() throws Exception {
    startMetaStore(-1, false);

    Database db = new Database("slow_log_disabled_db", null, null, null);
    msc.createDatabase(db);
    assertNotNull(msc.getDatabase("slow_log_disabled_db"));

    appender.reset();
    InjectableDummyAuthenticator.injectMode(true);
    try {
      msc.getDatabase("slow_log_disabled_db");
    } catch (Exception ignored) {
    } finally {
      InjectableDummyAuthenticator.injectMode(false);
    }

    String output = appender.getOutput();
    assertFalse(
        "No 'Slow getFileInfo' WARN expected when threshold=-1, got: " + output,
        output.contains("Slow getFileInfo"));

    msc.dropDatabase("slow_log_disabled_db");
  }

  /**
   * threshold=10ms + SlowLocalFileSystem (sleeps 30ms): WARN IS emitted.
   *
   * SlowLocalFileSystem is registered as fs.file.impl so the metastore's
   * StorageBasedAuthorizationProvider uses it when calling getFileStatusOrNull().
   * The 30ms sleep ensures elapsed(~30ms) > threshold(10ms), triggering the real
   * LOG.warn inside checkPermissions() in the production code.
   */
  @Test
  public void testSlowWarnEmittedWhenFileSystemIsSlow() throws Exception {
    startMetaStore(10, true); // 10ms threshold, SlowLocalFileSystem enabled

    Database db = new Database("slow_log_slow_db", null, null, null);
    msc.createDatabase(db);
    assertNotNull(msc.getDatabase("slow_log_slow_db"));

    appender.reset();
    InjectableDummyAuthenticator.injectMode(true);
    try {
      msc.getDatabase("slow_log_slow_db");
    } catch (Exception ignored) {
    } finally {
      InjectableDummyAuthenticator.injectMode(false);
    }

    String output = appender.getOutput();
    assertTrue(
        "Expected 'Slow getFileInfo' WARN when elapsed(~30ms) > threshold(10ms), got: " + output,
        output.contains("Slow getFileInfo"));

    msc.dropDatabase("slow_log_slow_db");
  }
}
