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
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConfForTest;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.tez.dag.api.TezException;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestTezSessionState {
  private static final Logger LOG = LoggerFactory.getLogger(TestTezSessionState.class.getName());

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
    HiveConf hiveConf = new HiveConfForTest(getClass());
    hiveConf.set("hive.security.authorization.manager",
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdConfOnlyAuthorizerFactory");
    SessionState.start(hiveConf);

    final AtomicReference<String> scratchDirPath = new AtomicReference<>();

    TezSessionState sessionState = new TezSessionState(SessionState.get().getSessionId(), hiveConf) {
      @Override
      void openInternalUnsafe(boolean isAsync, SessionState.LogHelper console) throws TezException, IOException {
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

    HiveConf hiveConf = new HiveConfForTest(getClass());
    hiveConf.set("hive.security.authorization.manager",
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdConfOnlyAuthorizerFactory");
    SessionState.start(hiveConf);

    TezSessionState.HiveResources resources =
        new TezSessionState.HiveResources(new org.apache.hadoop.fs.Path("/tmp"));

    TezSessionState tempSession = new TezSessionState(SessionState.get().getSessionId(), hiveConf);

    LocalResource localizedLr = tempSession.createJarLocalResource(jarPath.toUri().toString());
    resources.localizedResources.add(localizedLr);

    final TezSessionState sessionStateForTest = new TezSessionState(SessionState.get().getSessionId(), hiveConf) {
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
}