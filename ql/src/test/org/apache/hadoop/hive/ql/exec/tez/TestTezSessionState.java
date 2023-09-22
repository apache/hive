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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestTezSessionState {

  private static final String TEST_DATA_DIR =
      new File(System.getProperty("java.io.tmpdir") + File.separator + TestTezSessionState.class.getCanonicalName()
          + "-" + System.currentTimeMillis() + "_" + new AtomicInteger(new Random().nextInt()).getAndIncrement())
              .getPath().replaceAll("\\\\", "/");

  @BeforeClass
  public static void beforeClass() throws IOException {
    Files.createDirectories(Paths.get(TEST_DATA_DIR));
  }

  private class TestTezSessionPoolManager extends TezSessionPoolManager {
    public TestTezSessionPoolManager() {
      super();
    }

    @Override
    public void setupPool(HiveConf conf) throws Exception {
      super.setupPool(conf);
    }

    @Override
    public TezSessionPoolSession createSession(String sessionId, HiveConf conf) {
      return new SampleTezSessionState(sessionId, this, conf);
    }
  }

  @Test
  public void testDuplicatedLocalizedResources() throws Exception {
    String jarFileContent = "asdflkjh";
    String jar = TEST_DATA_DIR + "/" + "hive-exec-fake.jar";
    Path jarPath = Paths.get(jar);
    Files.write(jarPath, jarFileContent.getBytes(), StandardOpenOption.CREATE_NEW);

    HiveConf hiveConf = new HiveConf();
    hiveConf.set(HiveConf.ConfVars.HIVE_JAR_DIRECTORY.varname, "/tmp");
    TezSessionPoolManager poolManager = new TestTezSessionPoolManager();
    String sha = DagUtils.getInstance().getSha(new org.apache.hadoop.fs.Path(jar), hiveConf);

    TezSessionState sessionState = poolManager.getSession(null, hiveConf, true, false);

    sessionState.createJarLocalResource(jar);
    Assert.assertEquals(1, sessionState.localizedJarSha.keySet().size());
    Assert.assertEquals(sha, sessionState.localizedJarSha.keySet().iterator().next());

    sessionState.createJarLocalResource(jar);
    Assert.assertEquals(1, sessionState.localizedJarSha.keySet().size());
    Assert.assertEquals(sha, sessionState.localizedJarSha.keySet().iterator().next());

    // if we localize the same jar with another name (due to a symlink for instance)
    // that should not be localized again
    String anotherJar = TEST_DATA_DIR + "/" + "another-jar-with-the-same-content.jar";
    Path anotherJarPath = Paths.get(anotherJar);
    Files.write(anotherJarPath, jarFileContent.getBytes(), StandardOpenOption.CREATE_NEW);

    sessionState.createJarLocalResource(anotherJar);
    Assert.assertEquals(1, sessionState.localizedJarSha.keySet().size());
    Assert.assertEquals(sha, sessionState.localizedJarSha.keySet().iterator().next());
  }
}
