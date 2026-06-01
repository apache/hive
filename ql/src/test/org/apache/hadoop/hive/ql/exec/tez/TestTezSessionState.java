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

import java.net.URISyntaxException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;


import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConfForTest;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.tez.dag.api.TezException;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;


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
    java.nio.file.Path jarPath = Files.createTempFile("jar", "");
    java.nio.file.Path symlinkPath = Paths.get(jarPath.toString() + ".symlink");
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

  // --- createLocalResourceCredentialsExcludingDefaultFS ------------------------------------------

  private static final String DEFAULT_FS = "hdfs://defaultnn";

  private static LocalResource resourceAt(String pathStr) throws URISyntaxException {
    LocalResource lr = Mockito.mock(LocalResource.class);
    URL url = Mockito.mock(URL.class);
    Mockito.when(lr.getResource()).thenReturn(url);
    Mockito.when(url.toPath()).thenReturn(new Path(pathStr));
    return lr;
  }

  private static Map<String, LocalResource> resourceMapOf(String... pathStrs) throws URISyntaxException {
    // preserve insertion order so the assertions don't depend on HashMap ordering
    Map<String, LocalResource> m = new LinkedHashMap<>();
    for (int i = 0; i < pathStrs.length; i++) {
      m.put("r" + i, resourceAt(pathStrs[i]));
    }
    return m;
  }

  private static TezSessionState newSession() {
    HiveConf conf = new HiveConf();
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, DEFAULT_FS);
    return new TezSessionState(DagUtils.getInstance(), conf);
  }

  @Test
  public void testCredentialsEmptyResourceMapDoesNotRequestTokens() throws Exception {
    TezSessionState sessionState = newSession();
    try (MockedStatic<TokenCache> tokenCache = Mockito.mockStatic(TokenCache.class)) {
      Credentials credentials = sessionState.createLocalResourceCredentialsExcludingDefaultFS(new HashMap<>());

      Assert.assertNotNull(credentials);
      ArgumentCaptor<Path[]> paths = ArgumentCaptor.forClass(Path[].class);
      tokenCache.verify(() -> TokenCache.obtainTokensForNamenodes(
          Mockito.eq(credentials), paths.capture(), Mockito.any()));
      Assert.assertEquals(0, paths.getValue().length);
    }
  }

  @Test
  public void testCredentialsDefaultFsResourceIsSkipped() throws Exception {
    TezSessionState sessionState = newSession();
    Map<String, LocalResource> resources = resourceMapOf(DEFAULT_FS + "/user/x/hive-exec.jar", "/user/x/hadoop-auth.jar");

    try (MockedStatic<TokenCache> tokenCache = Mockito.mockStatic(TokenCache.class)) {
      Credentials credentials = sessionState.createLocalResourceCredentialsExcludingDefaultFS(resources);

      ArgumentCaptor<Path[]> paths = ArgumentCaptor.forClass(Path[].class);
      tokenCache.verify(() -> TokenCache.obtainTokensForNamenodes(
          Mockito.eq(credentials), paths.capture(), Mockito.any()));
      Assert.assertEquals(0, paths.getValue().length);
    }
  }

  @Test
  public void testCredentialsNonDefaultFsResourceCollectsPath() throws Exception {
    TezSessionState sessionState = newSession();
    String addedJar = "hdfs://othernn/user/u/add-jar.jar";
    Map<String, LocalResource> resources = resourceMapOf(addedJar);

    try (MockedStatic<TokenCache> tokenCache = Mockito.mockStatic(TokenCache.class)) {
      Credentials credentials = sessionState.createLocalResourceCredentialsExcludingDefaultFS(resources);

      ArgumentCaptor<Path[]> paths = ArgumentCaptor.forClass(Path[].class);
      tokenCache.verify(() -> TokenCache.obtainTokensForNamenodes(
          Mockito.eq(credentials), paths.capture(), Mockito.any()));
      Assert.assertArrayEquals(new Path[] { new Path(addedJar) }, paths.getValue());
    }
  }

  @Test
  public void testCredentialsMixedResourcesOnlyNonDefaultFsCollected() throws Exception {
    TezSessionState sessionState = newSession();
    String addedJarA = "hdfs://nn-a/user/u/jar-a.jar";
    String addedJarB = "hdfs://nn-b/user/u/jar-b.jar";
    Map<String, LocalResource> resources = resourceMapOf(
        DEFAULT_FS + "/tmp/hive/session/hive-exec.jar",   // defaultFS — skipped
        "/user/x/hadoop-auth.jar",                        // defaultFS — skipped
        addedJarA,                                        // collected
        addedJarB);                                       // collected

    try (MockedStatic<TokenCache> tokenCache = Mockito.mockStatic(TokenCache.class)) {
      Credentials credentials = sessionState.createLocalResourceCredentialsExcludingDefaultFS(resources);

      ArgumentCaptor<Path[]> paths = ArgumentCaptor.forClass(Path[].class);
      tokenCache.verify(() -> TokenCache.obtainTokensForNamenodes(
          Mockito.eq(credentials), paths.capture(), Mockito.any()));
      Assert.assertEquals(
          Arrays.asList(new Path(addedJarA), new Path(addedJarB)),
          Arrays.asList(paths.getValue()));
    }
  }
}
