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

package org.apache.hive.testutils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import com.google.common.collect.Lists;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.lockmgr.zookeeper.CuratorFrameworkSingleton;
import org.apache.hadoop.hive.ql.lockmgr.zookeeper.ZooKeeperHiveLockManager;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hadoop.hive.shims.HadoopShims.MiniMrShim;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;

import com.google.common.collect.Sets;

/**
 * Helps in setting up environments to run high level hive tests
 *
 * For a sample testcase see {@link TestHiveTestEnvSetup}!

 * Because setting up such a complex environment is a bit more sophisticated than it should;
 * this class introduces some helper concepts beyond what juni4 has
 *
 * <ul>
 *  <li>parts are decomposed into smaller {@link IHiveTestRule}
 *  <li>{@link HiveTestEnvContext} is visible to every methodcall</li>
 *  <li>invocation order of before calls are "forward"
 *  <li>invocation order of before calls are "backward"
 *  </ul>
 *
 *
 * Later this should be migrated to junit5...when it will be possible; see HIVE-18495
 */
public class HiveTestEnvSetup extends ExternalResource {

  private static final String TEST_DATA_DIR = new File(System.getProperty("java.io.tmpdir") +
    File.separator + HiveTestEnvSetup.class.getCanonicalName()
    + "-" + System.currentTimeMillis()
  ).getPath().replaceAll("\\\\", "/");

  static interface IHiveTestRule {
    default void beforeClass(HiveTestEnvContext ctx) throws Exception {
    }

    default void afterClass(HiveTestEnvContext ctx) throws Exception {
    }

    default void beforeMethod(HiveTestEnvContext ctx) throws Exception {
    }

    default void afterMethod(HiveTestEnvContext ctx) throws Exception {
    }
  }

  public static class HiveTestEnvContext {

    public File tmpFolder;
    public HiveConf hiveConf;

  }

  static class TmpDirSetup implements IHiveTestRule {

    public TemporaryFolder tmpFolderRule = new TemporaryFolder(new File(HIVE_ROOT + "/target/tmp"));

    @Override
    public void beforeClass(HiveTestEnvContext ctx) throws Exception {
      tmpFolderRule.create();
      ctx.tmpFolder = tmpFolderRule.getRoot();
    }

    @Override
    public void afterClass(HiveTestEnvContext ctx) {
      tmpFolderRule.delete();
      ctx.tmpFolder = null;
    }
  }


  static class SetTestEnvs implements IHiveTestRule {
    @Override
    public void beforeClass(HiveTestEnvContext ctx) throws Exception {

      File tmpFolder = ctx.tmpFolder;
      String tmpFolderPath = tmpFolder.getAbsolutePath();

      // these are mostly copied from the root pom.xml
      System.setProperty("build.test.dir", tmpFolderPath);
      System.setProperty("derby.stream.error.file", tmpFolderPath + "/derby.log");
      System.setProperty("hadoop.bin.path", HIVE_ROOT + "/testutils/hadoop");
      System.setProperty("hadoop.log.dir", tmpFolderPath);
      System.setProperty("mapred.job.tracker", "local");
      System.setProperty("log4j.configurationFile", "file://" + tmpFolderPath + "/conf/hive-log4j2.properties");
      System.setProperty("log4j.debug", "true");
      System.setProperty("java.io.tmpdir", tmpFolderPath);
      System.setProperty("test.build.data", tmpFolderPath);
      System.setProperty("test.data.files", DATA_DIR + "/files");
      System.setProperty("test.data.dir", DATA_DIR + "/files");
      System.setProperty("test.tmp.dir", tmpFolderPath);
      System.setProperty("test.tmp.dir.uri", "file://" + tmpFolderPath);
      System.setProperty("test.dfs.mkdir", "-mkdir -p");
      System.setProperty("test.warehouse.dir", tmpFolderPath + "/warehouse"); // this is changed to be *under* tmp dir
      System.setProperty("java.net.preferIPv4Stack", "true"); // not sure if this will have any effect..
      System.setProperty("test.src.tables", "src");
      System.setProperty("hive.jar.directory", tmpFolderPath);
    }

  }

  static class SetupHiveConf implements IHiveTestRule {

    private HiveConf savedConf;

    @Override
    public void beforeClass(HiveTestEnvContext ctx) throws Exception {

      File confFolder = new File(ctx.tmpFolder, "conf");

      FileUtils.copyDirectory(new File(DATA_DIR + "/conf/"), confFolder);
      FileUtils.copyDirectory(new File(DATA_DIR + "/conf/tez"), confFolder);

      HiveConf.setHiveSiteLocation(new File(confFolder, "hive-site.xml").toURI().toURL());
      HiveConf.setHivemetastoreSiteUrl(new File(confFolder, "hivemetastore-site.xml").toURI().toURL());
      // FIXME: hiveServer2SiteUrl is not settable?

      ctx.hiveConf = new HiveConf(IDriver.class);
      ctx.hiveConf.setBoolVar(ConfVars.HIVE_IN_TEST_IDE, true);
    }

    @Override
    public void beforeMethod(HiveTestEnvContext ctx) throws Exception {
      if (savedConf == null) {
        savedConf = new HiveConf(ctx.hiveConf);
      }
      // service a fresh conf for every testMethod
      ctx.hiveConf = new HiveConf(savedConf);
    }

    @Override
    public void afterMethod(HiveTestEnvContext ctx) throws Exception {
      // create a fresh hiveconf; afterclass methods may get into trouble without this
      ctx.hiveConf = new HiveConf(savedConf);
    }

    @Override
    public void afterClass(HiveTestEnvContext ctx) throws Exception {
      savedConf = null;
      ctx.hiveConf = null;
    }
  }

  static class SetupZookeeper implements IHiveTestRule {

    private ZooKeeper zooKeeper;
    private MiniZooKeeperCluster zooKeeperCluster;
    private int zkPort;

    @Override
    public void beforeClass(HiveTestEnvContext ctx) throws Exception {
      File tmpDir = new File(ctx.tmpFolder, "zookeeper");
      zooKeeperCluster = new MiniZooKeeperCluster();
      zkPort = zooKeeperCluster.startup(tmpDir);
    }

    @Override
    public void beforeMethod(HiveTestEnvContext ctx) throws Exception {
      int sessionTimeout = (int) ctx.hiveConf.getTimeVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_SESSION_TIMEOUT, TimeUnit.MILLISECONDS);
      zooKeeper = new ZooKeeper("localhost:" + zkPort, sessionTimeout, new Watcher() {
        @Override
        public void process(WatchedEvent arg0) {
        }
      });

      String zkServer = "localhost";
      ctx.hiveConf.set("hive.zookeeper.quorum", zkServer);
      ctx.hiveConf.set("hive.zookeeper.client.port", "" + zkPort);
    }

    @Override
    public void afterMethod(HiveTestEnvContext ctx) throws Exception {
      zooKeeper.close();
      ZooKeeperHiveLockManager.releaseAllLocks(ctx.hiveConf);
    }

    @Override
    public void afterClass(HiveTestEnvContext ctx) throws Exception {
      CuratorFrameworkSingleton.closeAndReleaseInstance();

      if (zooKeeperCluster != null) {
        zooKeeperCluster.shutdown();
        zooKeeperCluster = null;
      }
    }

  }

  static class SetupTez implements IHiveTestRule {
    private MiniMrShim mr1;

    @Override
    public void beforeClass(HiveTestEnvContext ctx) throws Exception {
      HadoopShims shims = ShimLoader.getHadoopShims();
      mr1 = shims.getLocalMiniTezCluster(ctx.hiveConf, true);
      mr1.setupConfiguration(ctx.hiveConf);
      setupTez(ctx.hiveConf);
    }

    @Override
    public void afterClass(HiveTestEnvContext ctx) throws Exception {
      mr1.shutdown();
    }

    private void setupTez(HiveConf conf) {
      conf.setVar(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE, "tez");
      conf.setVar(HiveConf.ConfVars.HIVE_USER_INSTALL_DIR, TEST_DATA_DIR);
      conf.set("tez.am.resource.memory.mb", "128");
      conf.set("tez.am.dag.scheduler.class", "org.apache.tez.dag.app.dag.impl.DAGSchedulerNaturalOrderControlled");
      conf.setBoolean("tez.local.mode", true);
      conf.setBoolean("tez.local.mode.without.network", true);
      conf.set("fs.defaultFS", "file:///");
      conf.setBoolean("tez.runtime.optimize.local.fetch", true);
      conf.set("tez.staging-dir", TEST_DATA_DIR);
      conf.setBoolean("tez.ignore.lib.uris", true);
    }
  }

  public static final String HIVE_ROOT = getHiveRoot();
  public static final String DATA_DIR = HIVE_ROOT + "/data/";
  List<IHiveTestRule> parts = new ArrayList<>();

  public HiveTestEnvSetup() {
    parts.add(new TmpDirSetup());
    parts.add(new SetTestEnvs());
    parts.add(new SetupHiveConf());
    parts.add(new SetupZookeeper());
    parts.add(new SetupTez());
  }

  TemporaryFolder tmpFolderRule = new TemporaryFolder(new File(HIVE_ROOT + "/target/tmp"));
  private HiveTestEnvContext testEnvContext = new HiveTestEnvContext();

  @Override
  protected void before() throws Throwable {
    for (IHiveTestRule p : parts) {
      p.beforeClass(testEnvContext);
    }
  }

  @Override
  protected void after() {
    try {
      for (IHiveTestRule p : Lists.reverse(parts)) {
        p.afterClass(testEnvContext);
      }
    } catch (Exception e) {
      throw new RuntimeException("test-subsystem error", e);
    }
  }

  class MethodRuleProxy extends ExternalResource {

    @Override
    protected void before() throws Throwable {
      for (IHiveTestRule p : parts) {
        p.beforeMethod(testEnvContext);
      }
    }

    @Override
    protected void after() {
      try {
        for (IHiveTestRule p : Lists.reverse(parts)) {
          p.afterMethod(testEnvContext);
        }
      } catch (Exception e) {
        throw new RuntimeException("test-subsystem error", e);
      }
    }
  }

  private static String getHiveRoot() {
    List<String> candidateSiblings = new ArrayList<>();
    if (System.getProperty("hive.root") != null) {
      try {
        candidateSiblings.add(new File(System.getProperty("hive.root")).getCanonicalPath());
      } catch (IOException e) {
        throw new RuntimeException("error getting hive.root", e);
      }
    }
    candidateSiblings.add(new File(".").getAbsolutePath());

    for (String string : candidateSiblings) {
      File curr = new File(string);
      do {
        Set<String> lls = Sets.newHashSet(curr.list());
        if (lls.contains("itests") && lls.contains("ql") && lls.contains("metastore")) {
          System.out.println("detected hiveRoot: " + curr);
          return ensurePathEndsInSlash(curr.getAbsolutePath());
        }
        curr = curr.getParentFile();
      } while (curr != null);
    }
    throw new RuntimeException("unable to find hiveRoot");
  }

  public static String ensurePathEndsInSlash(String path) {
    if (path == null) {
      throw new NullPointerException("Path cannot be null");
    }
    if (path.endsWith(File.separator)) {
      return path;
    } else {
      return path + File.separator;
    }
  }

  public File getDir(String string) {
    try {
      return tmpFolderRule.newFolder(string);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public TestRule getMethodRule() {
    return new MethodRuleProxy();
  }

  public HiveTestEnvContext getTestCtx() {
    return testEnvContext;
  }

}