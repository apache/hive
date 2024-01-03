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
package org.apache.hadoop.hive.ql.session;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;

public class TestClearDanglingScratchDir {
  private static MiniDFSCluster m_dfs = null;
  private static HiveConf conf;
  private static Path scratchDir;
  private ByteArrayOutputStream stdout;
  private ByteArrayOutputStream stderr;
  private PrintStream origStdoutPs;
  private PrintStream origStderrPs;
  private static Path customScratchDir;
  private static Path customLocalTmpDir;

  @BeforeClass
  static public void oneTimeSetup() throws Exception {
    m_dfs = new MiniDFSCluster.Builder(new Configuration()).numDataNodes(1).format(true).build();
    conf = new HiveConf();
    conf.set(HiveConf.ConfVars.HIVE_SCRATCH_DIR_LOCK.toString(), "true");
    conf.set(HiveConf.ConfVars.METASTORE_AUTO_CREATE_ALL.toString(), "true");
    LoggerFactory.getLogger("SessionState");
    conf.setVar(HiveConf.ConfVars.METASTORE_WAREHOUSE,
        new Path(System.getProperty("test.tmp.dir"), "warehouse").toString());
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY,
        m_dfs.getFileSystem().getUri().toString());

    scratchDir = new Path(HiveConf.getVar(conf, HiveConf.ConfVars.SCRATCH_DIR));
    m_dfs.getFileSystem().mkdirs(scratchDir);
    m_dfs.getFileSystem().setPermission(scratchDir, new FsPermission("777"));
  }

  @AfterClass
  static public void shutdown() throws Exception {
    m_dfs.shutdown();

    // Need to make sure deleting in correct FS
    FileSystem fs = customScratchDir.getFileSystem(new Configuration());
    fs.delete(customScratchDir, true);
    fs.delete(customLocalTmpDir, true);
  }

  public void redirectStdOutErr() {
    stdout = new ByteArrayOutputStream();
    PrintStream psStdout = new PrintStream(stdout);
    origStdoutPs = System.out;
    System.setOut(psStdout);

    stderr = new ByteArrayOutputStream();
    PrintStream psStderr = new PrintStream(stderr);
    origStderrPs = System.err;
    System.setErr(psStderr);
  }

  public void rollbackStdOutErr() {
    System.setOut(origStdoutPs);
    System.setErr(origStderrPs);
  }

  @Test
  public void testClearDanglingScratchDir() throws Exception {

    // No scratch dir initially
    redirectStdOutErr();
    ClearDanglingScratchDir.main(new String[]{"-v", "-s",
        m_dfs.getFileSystem().getUri().toString() + scratchDir.toUri().toString()});
    rollbackStdOutErr();
    Assert.assertTrue(stderr.toString().contains("Cannot find any scratch directory to clear"));

    // Create scratch dir without lock files
    m_dfs.getFileSystem().mkdirs(new Path(new Path(scratchDir, "dummy"), UUID.randomUUID().toString()));
    redirectStdOutErr();
    ClearDanglingScratchDir.main(new String[]{"-v", "-s",
        m_dfs.getFileSystem().getUri().toString() + scratchDir.toUri().toString()});
    rollbackStdOutErr();
    Assert.assertEquals(StringUtils.countMatches(stderr.toString(),
        "since it does not contain " + SessionState.LOCK_FILE_NAME), 1);
    Assert.assertTrue(stderr.toString().contains("Cannot find any scratch directory to clear"));

    // One live session
    SessionState ss = SessionState.start(conf);
    redirectStdOutErr();
    ClearDanglingScratchDir.main(new String[]{"-v", "-s",
        m_dfs.getFileSystem().getUri().toString() + scratchDir.toUri().toString()});
    rollbackStdOutErr();
    Assert.assertEquals(StringUtils.countMatches(stderr.toString(), "is being used by live process"), 1);

    // One dead session with dry-run
    ss.releaseSessionLockFile();
    redirectStdOutErr();
    ClearDanglingScratchDir.main(new String[]{"-r", "-v", "-s",
        m_dfs.getFileSystem().getUri().toString() + scratchDir.toUri().toString()});
    rollbackStdOutErr();
    // Find one session dir to remove
    Assert.assertFalse(stdout.toString().isEmpty());

    // Remove the dead session dir
    redirectStdOutErr();
    ClearDanglingScratchDir.main(new String[]{"-v", "-s",
        m_dfs.getFileSystem().getUri().toString() + scratchDir.toUri().toString()});
    rollbackStdOutErr();
    Assert.assertTrue(stderr.toString().contains("Removing 1 scratch directories"));
    Assert.assertEquals(StringUtils.countMatches(stderr.toString(), "removed"), 1);
    ss.close();
  }

  /**
   * Testing behaviour of ClearDanglingScratchDir service over local tmp files/dirs
   * @throws Exception
   */
  @Test
  public void testLocalDanglingFilesCleaning() throws Exception {
    HiveConf conf = new HiveConf();
    conf.set("fs.default.name", "file:///");
    String tmpDir = System.getProperty("test.tmp.dir");
    conf.set("hive.exec.scratchdir", tmpDir + "/hive-27317-hdfsscratchdir");
    conf.set("hive.exec.local.scratchdir", tmpDir + "/hive-27317-localscratchdir");
    FileSystem fs = FileSystem.get(conf);

    // Constants
    String appId = "appId_" + System.currentTimeMillis();
    String userName = System.getProperty("user.name");
    String hdfs = "hdfs";
    String inuse = "inuse.lck";
    String l = File.separator;

    // Simulating hdfs dangling dir and its inuse.lck file
    // Note: Give scratch dirs all the write permissions
    FsPermission allPermissions = new FsPermission((short)00777);
    customScratchDir = new Path(HiveConf.getVar(conf, HiveConf.ConfVars.SCRATCH_DIR));
    Utilities.createDirsWithPermission(conf, customScratchDir, allPermissions, true);
    Path hdfsRootDir = new Path(customScratchDir + l + userName + l + hdfs);
    Path hdfsSessionDir = new Path(hdfsRootDir + l + userName + l + appId);
    Path hdfsSessionLock = new Path(hdfsSessionDir + l + inuse);
    fs.create(hdfsSessionLock);

    // Simulating local dangling files
    customLocalTmpDir = new Path (HiveConf.getVar(conf, HiveConf.ConfVars.LOCAL_SCRATCH_DIR));
    Path localSessionDir = new Path(customLocalTmpDir + l + appId);
    Path localPipeOutFileRemove = new Path(customLocalTmpDir + l
            + appId + "-started-with-session-name.pipeout");
    Path localPipeOutFileNotRemove = new Path(customLocalTmpDir + l
            + "not-started-with-session-name-" + appId + ".pipeout");
    Path localPipeOutFileFailRemove = new Path(customLocalTmpDir + l
            + appId + "-started-with-session-name-but-fail-delete.pipeout");

    // Create dirs/files
    Utilities.createDirsWithPermission(conf, localSessionDir, allPermissions, true);
    fs.create(localPipeOutFileRemove);
    fs.create(localPipeOutFileNotRemove);
    fs.create(localPipeOutFileFailRemove);

    // Set permission for localPipeOutFileFailRemove file as not writable
    // This will avoid file to be deleted as we check whether it is writable or not first
    fs.setPermission(localPipeOutFileFailRemove, FsPermission.valueOf("-r--r--r--"));

    // The main service will be identifying which session files/dirs are dangling
    ClearDanglingScratchDir clearDanglingScratchDirMain = new ClearDanglingScratchDir(false,
            false, true, hdfsRootDir.toString(), conf);
    clearDanglingScratchDirMain.run();

    // localSessionDir and localPipeOutFileRemove should be removed
    // localPipeOutFileNotRemove and localPipeOutFileFailRemove should not be removed
    Assert.assertFalse("Local session dir '" + localSessionDir
            + "' still exists, should have been removed!", fs.exists(localSessionDir));
    Assert.assertFalse("Local .pipeout file '" + localPipeOutFileRemove
            + "' still exists, should have been removed!", fs.exists(localPipeOutFileRemove));
    Assert.assertTrue("Local .pipeout file '" + localPipeOutFileNotRemove
            + "' does not exist, should have not been removed!", fs.exists(localPipeOutFileNotRemove));
    Assert.assertTrue("Local .pipeout file '" + localPipeOutFileFailRemove
            + "' does not exist, should have not been removed!", fs.exists(localPipeOutFileFailRemove));
  }
}
