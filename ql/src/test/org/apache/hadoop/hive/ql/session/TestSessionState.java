/**
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
package org.apache.hadoop.hive.ql.session;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hive.common.util.HiveTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.io.Files;

/**
 * Test SessionState
 */
@RunWith(value = Parameterized.class)
public class TestSessionState {
  private final boolean prewarm;
  private final static String clazzDistFileName = "RefreshedJarClass.jar.V1";
  private final static String clazzV2FileName = "RefreshedJarClass.jar.V2";
  private final static String reloadClazzFileName = "RefreshedJarClass.jar";
  private final static String versionMethodName = "version";
  private final static String RELOADED_CLAZZ_PREFIX_NAME = "RefreshedJarClass";
  private final static String V1 = "V1";
  private final static String V2 = "V2";
  private static String hiveReloadPath;
  private File reloadFolder;
  public static final Logger LOG = LoggerFactory.getLogger(TestSessionState.class);

  public TestSessionState(Boolean mode) {
    this.prewarm = mode.booleanValue();
  }

  @Parameters
  public static Collection<Boolean[]> data() {
    return Arrays.asList(new Boolean[][] { {false}, {true}});
  }

  @Before
  public void setUp() {
    HiveConf conf = new HiveConf();
    String tmp = System.getProperty("java.io.tmpdir");
    File tmpDir = new File(tmp);
    if (!tmpDir.exists()) {
      tmpDir.mkdir();
    }
    hiveReloadPath = Files.createTempDir().getAbsolutePath();
    // create the reloading folder to place jar files if not exist
    reloadFolder = new File(hiveReloadPath);
    if (!reloadFolder.exists()) {
      reloadFolder.mkdir();
    }

    try {
      generateRefreshJarFiles(V2);
      generateRefreshJarFiles(V1);
    } catch (Throwable e) {
      Assert.fail("fail to generate refresh jar file due to the error " + e);
    }

    if (prewarm) {
      HiveConf.setBoolVar(conf, ConfVars.HIVE_PREWARM_ENABLED, true);
      HiveConf.setIntVar(conf, ConfVars.HIVE_PREWARM_NUM_CONTAINERS, 1);
    }
    SessionState.start(conf);
  }

  @After
  public void tearDown(){
    FileUtils.deleteQuietly(reloadFolder);
  }

  /**
   * test set and get db
   */
  @Test
  public void testgetDbName() throws Exception {
    //check that we start with default db
    assertEquals(MetaStoreUtils.DEFAULT_DATABASE_NAME,
        SessionState.get().getCurrentDatabase());
    final String newdb = "DB_2";

    //set new db and verify get
    SessionState.get().setCurrentDatabase(newdb);
    assertEquals(newdb,
        SessionState.get().getCurrentDatabase());

    //verify that a new sessionstate has default db
    SessionState.start(new HiveConf());
    assertEquals(MetaStoreUtils.DEFAULT_DATABASE_NAME,
        SessionState.get().getCurrentDatabase());

  }

  @Test
  public void testClose() throws Exception {
    SessionState ss = SessionState.get();
    assertNull(ss.getTezSession());
    ss.close();
    assertNull(ss.getTezSession());
  }

  class RegisterJarRunnable implements Runnable {
    String jar;
    ClassLoader loader;
    SessionState ss;

    public RegisterJarRunnable(String jar, SessionState ss) {
      this.jar = jar;
      this.ss = ss;
    }

    public void run() {
      SessionState.start(ss);
      SessionState.registerJars(Arrays.asList(jar));
      loader = Thread.currentThread().getContextClassLoader();
    }
  }

  @Test
  public void testClassLoaderEquality() throws Exception {
    HiveConf conf = new HiveConf();
    final SessionState ss1 = new SessionState(conf);
    RegisterJarRunnable otherThread = new RegisterJarRunnable("./build/contrib/test/test-udfs.jar", ss1);
    Thread th1 = new Thread(otherThread);
    th1.start();
    th1.join();

    // set state in current thread
    SessionState.start(ss1);
    SessionState ss2 = SessionState.get();
    ClassLoader loader2 = ss2.getConf().getClassLoader();

    System.out.println("Loader1:(Set in other thread) " + otherThread.loader);
    System.out.println("Loader2:(Set in SessionState.conf) " + loader2);
    System.out.println("Loader3:(CurrentThread.getContextClassLoader()) " +
        Thread.currentThread().getContextClassLoader());
    assertEquals("Other thread loader and session state loader",
        otherThread.loader, loader2);
    assertEquals("Other thread loader and current thread loader",
        otherThread.loader, Thread.currentThread().getContextClassLoader());
  }

  private String getReloadedClazzVersion(ClassLoader cl) throws Exception {
    Class addedClazz = Class.forName(RELOADED_CLAZZ_PREFIX_NAME, true, cl);
    Method versionMethod = addedClazz.getMethod(versionMethodName);
    return (String) versionMethod.invoke(addedClazz.newInstance());
  }

  private void generateRefreshJarFiles(String version) throws IOException, InterruptedException {
    String u = HiveTestUtils
        .getFileFromClasspath(RELOADED_CLAZZ_PREFIX_NAME + version + HiveTestUtils.TXT_FILE_EXT);
    File jarFile = HiveTestUtils.genLocalJarForTest(u, RELOADED_CLAZZ_PREFIX_NAME);
    Files.move(jarFile, new File(jarFile.getAbsolutePath() + "." + version));
  }

  @Test
  public void testReloadAuxJars2() {
    HiveConf conf = new HiveConf();
    HiveConf.setVar(conf, ConfVars.HIVERELOADABLEJARS, hiveReloadPath);
    SessionState ss = new SessionState(conf);
    SessionState.start(ss);

    ss = SessionState.get();
    File dist = null;
    try {
      dist = new File(reloadFolder.getAbsolutePath() + File.separator + reloadClazzFileName);
      Files.copy(new File(HiveTestUtils.getFileFromClasspath(clazzDistFileName)), dist);
      ss.reloadAuxJars();
      Assert.assertEquals("version1", getReloadedClazzVersion(ss.getConf().getClassLoader()));
    } catch (Exception e) {
      LOG.error("Reload auxiliary jar test fail with message: ", e);
      Assert.fail(e.getMessage());
    } finally {
      FileUtils.deleteQuietly(dist);
      try {
        ss.close();
      } catch (IOException ioException) {
        Assert.fail(ioException.getMessage());
        LOG.error("Fail to close the created session: ", ioException);
      }
    }
  }

  @Test
  public void testReloadExistingAuxJars2() {
    HiveConf conf = new HiveConf();
    HiveConf.setVar(conf, ConfVars.HIVERELOADABLEJARS, hiveReloadPath);

    SessionState ss = new SessionState(conf);
    SessionState.start(ss);
    File dist = null;

    try {
      ss = SessionState.get();

      LOG.info("copy jar file 1");
      dist = new File(reloadFolder.getAbsolutePath() + File.separator + reloadClazzFileName);

      Files.copy(new File(HiveTestUtils.getFileFromClasspath(clazzDistFileName)), dist);
      ss.reloadAuxJars();

      Assert.assertEquals("version1", getReloadedClazzVersion(ss.getConf().getClassLoader()));

      LOG.info("copy jar file 2");
      FileUtils.deleteQuietly(dist);
      Files.copy(new File(HiveTestUtils.getFileFromClasspath(clazzV2FileName)), dist);

      ss.reloadAuxJars();
      Assert.assertEquals("version2", getReloadedClazzVersion(ss.getConf().getClassLoader()));

      FileUtils.deleteQuietly(dist);
      ss.reloadAuxJars();
    } catch (Exception e) {
      LOG.error("refresh existing jar file case failed with message: ", e);
      Assert.fail(e.getMessage());
    } finally {
      FileUtils.deleteQuietly(dist);
      try {
        ss.close();
      } catch (IOException ioException) {
        Assert.fail(ioException.getMessage());
        LOG.error("Fail to close the created session: ", ioException);
      }
    }
  }
}
