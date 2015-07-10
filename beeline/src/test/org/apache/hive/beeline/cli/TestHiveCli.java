/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.beeline.cli;

import junit.framework.Assert;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;

public class TestHiveCli {
  private static final Log LOG = LogFactory.getLog(TestHiveCli.class.getName());
  private static final int ERRNO_OK = 0;
  private static final int ERRNO_ARGS = 1;
  private static final int ERRNO_OTHER = 2;

  private final static String SOURCE_CONTEXT =
      "create table if not exists test.testSrcTbl(a string, b string);";
  private final static String SOURCE_CONTEXT2 =
      "create table if not exists test.testSrcTbl2(a string);";
  private final static String SOURCE_CONTEXT3 =
      "create table if not exists test.testSrcTbl3(a string);";
  final static String CMD =
      "create database if not exists test;\ncreate table if not exists test.testTbl(a string, b "
          + "string);\n";
  private HiveCli cli;
  private OutputStream os;
  private PrintStream ps;
  private OutputStream errS;
  private PrintStream errPs;
  private File tmp = null;

  private void executeCMD(String[] args, String input, int retCode) {
    InputStream inputStream = null;
    int ret = 0;
    try {
      if (input != null) {
        inputStream = IOUtils.toInputStream(input);
      }
      ret = cli.runWithArgs(args, inputStream);
    } catch (Throwable e) {
      LOG.error("Failed to execute command due to the error: " + e);
    } finally {
      if (retCode != ret) {
        LOG.error("Failed due to the error:" + errS.toString());
        Assert.fail("Supported return code is " + retCode + " while the actual is " + ret);
      }
    }
  }

  private void verifyCMD(String CMD, String keywords, OutputStream os, String[] options,
      int retCode) {
    executeCMD(options, CMD, retCode);
    String output = os.toString();
    Assert.assertTrue("The expected keyword doesn't occur in the output: " + output,
        output.contains(keywords));
  }

  @Test
  public void testInValidCmd() {
    verifyCMD("!lss\n", "Unknown command: lss", errS, null, ERRNO_OK);
  }

  @Test
  public void testHelp() {
    verifyCMD(null, "usage: hive", os, new String[] { "-H" }, ERRNO_ARGS);
  }

  @Test
  public void testInvalidDatabaseOptions() {
    verifyCMD("\nshow tables;\nquit;\n", "Database does not exist: invalidDB", errS,
        new String[] { "--database", "invalidDB" }, ERRNO_OK);
  }

  @Test
  public void testDatabaseOptions() {
    verifyCMD("\nshow tables;\nquit;", "testTbl", os, new String[] { "--database", "test" },
        ERRNO_OK);
  }

  @Test
  public void testSourceCmd() {
    File f = generateTmpFile(SOURCE_CONTEXT);
    verifyCMD("source " + f.getPath() + ";" + "desc testSrcTbl;\nquit;\n", "col_name", os,
        new String[] { "--database", "test" }, ERRNO_OK);
    f.delete();
  }

  @Test
  public void testSourceCmd2() {
    File f = generateTmpFile(SOURCE_CONTEXT3);
    verifyCMD("source " + f.getPath() + ";" + "desc testSrcTbl3;\nquit;\n", "col_name", os,
        new String[] { "--database", "test" }, ERRNO_OK);
    f.delete();
  }

  @Test
  public void testSqlFromCmd() {
    verifyCMD(null, "", os, new String[] { "-e", "show databases;" }, ERRNO_OK);
  }

  @Test
  public void testSqlFromCmdWithDBName() {
    verifyCMD(null, "testTbl", os, new String[] { "-e", "show tables;", "--database", "test" },
        ERRNO_OK);
  }

  @Test
  public void testInvalidOptions() {
    verifyCMD(null, "The '-e' and '-f' options cannot be specified simultaneously", errS,
        new String[] { "-e", "show tables;", "-f", "path/to/file" }, ERRNO_ARGS);
  }

  @Test
  public void testInvalidOptions2() {
    verifyCMD(null, "Unrecognized option: -k", errS, new String[] { "-k" }, ERRNO_ARGS);
  }

  @Test
  public void testVariables() {
    verifyCMD("set system:xxx=5;\nset system:yyy=${system:xxx};\nset system:yyy;", "", os, null,
        ERRNO_OK);
  }

  @Test
  public void testVariablesForSource() {
    File f = generateTmpFile(SOURCE_CONTEXT2);
    verifyCMD(
        "set hiveconf:zzz=" + f.getAbsolutePath() + ";\nsource ${hiveconf:zzz};\ndesc testSrcTbl2;",
        "col_name", os, new String[] { "--database", "test" }, ERRNO_OK);
    f.delete();
  }

  private void redirectOutputStream() {
    // Setup output stream to redirect output to
    os = new ByteArrayOutputStream();
    ps = new PrintStream(os);
    errS = new ByteArrayOutputStream();
    errPs = new PrintStream(errS);
    System.setOut(ps);
    System.setErr(errPs);
  }

  private void initFromFile() {
    tmp = generateTmpFile(CMD);
    if (tmp == null) {
      Assert.fail("Fail to create the initial file");
    }
    executeCMD(new String[] { "-f", "\"" + tmp.getAbsolutePath() + "\"" }, null, 0);
  }

  private File generateTmpFile(String context) {
    File file = null;
    BufferedWriter bw = null;
    try {
      file = File.createTempFile("test", ".sql");
      bw = new BufferedWriter(new FileWriter(file));
      bw.write(context);
    } catch (IOException e) {
      LOG.error("Failed to write tmp file due to the exception: " + e);
    } finally {
      IOUtils.closeQuietly(bw);
    }
    return file;
  }

  @Before
  public void setup() {
    cli = new HiveCli();
    redirectOutputStream();
    initFromFile();
  }

  @After
  public void tearDown() {
    tmp.delete();
  }
}
