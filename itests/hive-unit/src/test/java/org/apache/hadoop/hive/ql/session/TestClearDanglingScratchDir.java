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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.channels.FileChannel;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hive.conf.HiveConf;
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
  private PrintStream origStdoutPs;
  private static File logFile;

  @BeforeClass
  static public void oneTimeSetup() throws Exception {
    logFile = File.createTempFile("log", "");
    File log4jConfig = File.createTempFile("config", ".properties");
    log4jConfig.deleteOnExit();
    PrintWriter pw = new PrintWriter(log4jConfig);
    pw.println("appenders = console, file");
    pw.println("appender.console.type = Console");
    pw.println("appender.console.name = STDOUT");
    pw.println("appender.console.layout.type = PatternLayout");
    pw.println("appender.console.layout.pattern = %t %-5p %c{2} - %m%n");
    pw.println("appender.file.type = File");
    pw.println("appender.file.name = LOGFILE");
    pw.println("appender.file.fileName = " + logFile.getAbsolutePath());
    pw.println("appender.file.layout.type = PatternLayout");
    pw.println("appender.file.layout.pattern = %t %-5p %c{2} - %m%n");
    pw.println("rootLogger.level = debug");
    pw.println("rootLogger.appenderRefs = stdout");
    pw.println("rootLogger.appenderRef.stdout.ref = STDOUT");
    pw.println("loggers = file");
    pw.println("logger.file.name = SessionState");
    pw.println("logger.file.level = debug");
    pw.println("logger.file.appenderRefs = file");
    pw.println("logger.file.appenderRef.file.ref = LOGFILE");
    pw.close();
    System.setProperty("log4j.configurationFile", log4jConfig.getAbsolutePath());

    m_dfs = new MiniDFSCluster.Builder(new Configuration()).numDataNodes(1).format(true).build();
    conf = new HiveConf();
    conf.set(HiveConf.ConfVars.HIVE_SCRATCH_DIR_LOCK.toString(), "true");
    conf.set(HiveConf.ConfVars.METASTORE_AUTO_CREATE_ALL.toString(), "true");
    LoggerFactory.getLogger("SessionState");
    conf.setVar(HiveConf.ConfVars.METASTOREWAREHOUSE,
        new Path(System.getProperty("test.tmp.dir"), "warehouse").toString());
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY,
        m_dfs.getFileSystem().getUri().toString());

    scratchDir = new Path(HiveConf.getVar(conf, HiveConf.ConfVars.SCRATCHDIR));
    m_dfs.getFileSystem().mkdirs(scratchDir);
    m_dfs.getFileSystem().setPermission(scratchDir, new FsPermission("777"));
  }

  @AfterClass
  static public void shutdown() throws Exception {
    m_dfs.shutdown();
  }

  public void redirectOutput() throws IOException {
    stdout = new ByteArrayOutputStream();
    PrintStream psStdout = new PrintStream(stdout);
    origStdoutPs = System.out;
    System.setOut(psStdout);

    FileOutputStream fos = new FileOutputStream(logFile, true);
    FileChannel outChan = fos.getChannel();
    outChan.truncate(0);
    outChan.close();
    fos.close();
  }

  public void rollbackOutput() {
    System.setOut(origStdoutPs);
  }

  @Test
  public void testClearDanglingScratchDir() throws Exception {

    // No scratch dir initially
    redirectOutput();
    ClearDanglingScratchDir.main(new String[]{"-s",
        m_dfs.getFileSystem().getUri().toString() + scratchDir.toUri().toString()});
    rollbackOutput();
    Assert.assertTrue(FileUtils.readFileToString(logFile).contains("Cannot find any scratch directory to clear"));

    // Create scratch dir without lock files
    m_dfs.getFileSystem().mkdirs(new Path(new Path(scratchDir, "dummy"), UUID.randomUUID().toString()));
    redirectOutput();
    ClearDanglingScratchDir.main(new String[]{"-s",
        m_dfs.getFileSystem().getUri().toString() + scratchDir.toUri().toString()});
    rollbackOutput();
    Assert.assertEquals(StringUtils.countMatches(FileUtils.readFileToString(logFile),
        "since it does not contain " + SessionState.LOCK_FILE_NAME), 1);
    Assert.assertTrue(FileUtils.readFileToString(logFile).contains("Cannot find any scratch directory to clear"));

    // One live session
    SessionState ss = SessionState.start(conf);
    redirectOutput();
    ClearDanglingScratchDir.main(new String[]{"-s",
        m_dfs.getFileSystem().getUri().toString() + scratchDir.toUri().toString()});
    rollbackOutput();
    Assert.assertEquals(StringUtils.countMatches(FileUtils.readFileToString(logFile), "is being used by live process"), 1);

    // One dead session with dry-run
    ss.releaseSessionLockFile();
    redirectOutput();
    ClearDanglingScratchDir.main(new String[]{"-r", "-s",
        m_dfs.getFileSystem().getUri().toString() + scratchDir.toUri().toString()});
    rollbackOutput();
    // Find one session dir to remove
    Assert.assertFalse(stdout.toString().isEmpty());

    // Remove the dead session dir
    redirectOutput();
    ClearDanglingScratchDir.main(new String[]{"-s",
        m_dfs.getFileSystem().getUri().toString() + scratchDir.toUri().toString()});
    rollbackOutput();
    Assert.assertTrue(FileUtils.readFileToString(logFile).contains("Removing 1 scratch directories"));
    Assert.assertEquals(StringUtils.countMatches(FileUtils.readFileToString(logFile), "removed"), 1);
    ss.close();
  }
}
