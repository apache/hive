/*
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

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.After;
import org.junit.Assert;
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
  private static final Logger LOG = LoggerFactory.getLogger(TestHiveCli.class.getName());
  private static final int ERRNO_OK = 0;
  private static final int ERRNO_ARGS = 1;
  private static final int ERRNO_OTHER = 2;

  private final static String SOURCE_CONTEXT =
      "create table if not exists test.testSrcTbl(sc1 string);";
  private final static String SOURCE_CONTEXT2 =
      "create table if not exists test.testSrcTbl2(sc2 string);";
  private final static String SOURCE_CONTEXT3 =
      "create table if not exists test.testSrcTbl3(sc3 string);";
  private final static String SOURCE_CONTEXT4 = "show tables;!ls;show tables;\nquit;";
  private final static String SOURCE_CONTEXT5 = "-- test;\n;show tables;\nquit;";
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

  /**
   * This method is used for verifying CMD to see whether the output contains the keywords provided.
   *
   * @param CMD
   * @param keywords
   * @param os
   * @param options
   * @param retCode
   * @param contains
   */
  private void verifyCMD(String CMD, String keywords, OutputStream os, String[] options, int retCode,
      boolean contains) {
    executeCMD(options, CMD, retCode);
    String output = os.toString();
    LOG.debug(output);
    if (contains) {
      Assert.assertTrue("The expected keyword \"" + keywords + "\" occur in the output: " + output,
          output.contains(keywords));
    } else {
      Assert.assertFalse(
          "The expected keyword \"" + keywords + "\" should be excluded occurred in the output: "
              + output, output.contains(keywords));
    }
  }

  @Test
  public void testInValidCmd() {
    verifyCMD("!lss\n", "Failed to execute lss", errS, null, ERRNO_OTHER, true);
  }

  @Test
  public void testCmd() {
    verifyCMD("show tables;!ls;show tables;\n", "src", os, null, ERRNO_OK, true);
  }

  @Test
  public void testCommentStripping() {
    // this should work as comments are stripped by HiveCli
    verifyCMD("!ls --abcdefghijklmnopqrstuvwxyz\n", "src", os, null, ERRNO_OK, true);
  }


  @Test
  public void testSetPromptValue() {
    verifyCMD("set hive.cli.prompt=MYCLI;SHOW\nTABLES;", "MYCLI> ", errS, null,
        ERRNO_OK, true);
  }

  @Test
  public void testSetHeaderValue() {
    verifyCMD(
        "create database if not exists test;\ncreate table if not exists test.testTbl(a string, b string);\nset hive.cli.print.header=true;\n select * from test.testTbl;\n",
        "testtbl.a testtbl.b", os, null, ERRNO_OK, true);
  }

  @Test
  public void testHelp() {
    verifyCMD(null, "usage: hive", os, new String[] { "-H" }, ERRNO_ARGS, true);
  }

  @Test
  public void testInvalidDatabaseOptions() {
    verifyCMD("\nshow tables;\nquit;\n", "Database does not exist: invalidDB",
        errS, new String[] { "--database", "invalidDB" }, ERRNO_OK, true);
  }

  @Test
  public void testDatabaseOptions() {
    verifyCMD("\nshow tables;\nquit;", "testtbl", os,
        new String[] { "--database", "test" }, ERRNO_OK, true);
  }

  @Test
  public void testSourceCmd() {
    File f = generateTmpFile(SOURCE_CONTEXT);
    verifyCMD("source " + f.getPath() + ";" + "desc testSrcTbl;\nquit;\n",
        "sc1", os, new String[] { "--database", "test" }, ERRNO_OK, true);
    f.delete();
  }

  @Test
  public void testSourceCmd2() {
    File f = generateTmpFile(SOURCE_CONTEXT3);
    verifyCMD("source " + f.getPath() + ";" + "desc testSrcTbl3;\nquit;\n",
        "sc3", os, new String[] { "--database", "test" }, ERRNO_OK, true);
    f.delete();
  }

  @Test
  public void testSourceCmd3() {
    File f = generateTmpFile(SOURCE_CONTEXT4);
    verifyCMD("source " + f.getPath() + ";" + "desc testSrcTbl4;\nquit;\n", "src", os,
        new String[] { "--database", "test" }, ERRNO_OTHER, true);
    f.delete();
  }

  @Test
  public void testSourceCmd4() {
    File f = generateTmpFile(SOURCE_CONTEXT5);
    verifyCMD("source " + f.getPath() + ";", "testtbl", os,
      new String[] { "--database", "test" }, ERRNO_OK, true);
    f.delete();
  }

  @Test
  public void testSqlFromCmd() {
    verifyCMD(null, "", os, new String[] { "-e", "show databases;" }, ERRNO_OK, true);
  }

  @Test
  public void testSqlFromCmdWithDBName() {
    verifyCMD(null, "testtbl", os,
        new String[] { "-e", "show tables;", "--database", "test" }, ERRNO_OK, true);
  }

  @Test
  public void testInvalidOptions() {
    verifyCMD(null,
        "The '-e' and '-f' options cannot be specified simultaneously", errS,
        new String[] { "-e", "show tables;", "-f", "path/to/file" }, ERRNO_ARGS, true);
  }

  @Test
  public void testInvalidOptions2() {
    verifyCMD(null, "Unrecognized option: -k", errS, new String[] { "-k" },
        ERRNO_ARGS, true);
  }

  @Test
  public void testVariables() {
    verifyCMD(
        "set system:xxx=5;\nset system:yyy=${system:xxx};\nset system:yyy;", "", os, null, ERRNO_OK, true);
  }

  @Test
  public void testVariablesForSource() {
    File f = generateTmpFile(SOURCE_CONTEXT2);
    verifyCMD(
        "set hiveconf:zzz=" + f.getAbsolutePath() + ";\nsource ${hiveconf:zzz};\ndesc testSrcTbl2;",
        "sc2", os, new String[] { "--database", "test" }, ERRNO_OK, true);
    f.delete();
  }

  @Test
  public void testErrOutput() {
    verifyCMD("show tables;set system:xxx=5;set system:yyy=${system:xxx};\nlss;",
        "cannot recognize input near 'lss' '<EOF>' '<EOF>'", errS, null, ERRNO_OTHER, true);
  }

  @Test
  public void testUseCurrentDB1() {
    verifyCMD(
        "create database if not exists testDB; set hive.cli.print.current.db=true;use testDB;\n"
            + "use default;drop if exists testDB;", "hive (testDB)>", errS, null, ERRNO_OTHER, true);
  }

  @Test
  public void testUseCurrentDB2() {
    verifyCMD(
        "create database if not exists testDB; set hive.cli.print.current.db=true;use\ntestDB;\nuse default;drop if exists testDB;",
        "hive (testDB)>", errS, null, ERRNO_OTHER, true);
  }

  @Test
  public void testUseCurrentDB3() {
    verifyCMD(
        "create database if not exists testDB; set hive.cli.print.current.db=true;use  testDB;\n"
            + "use default;drop if exists testDB;", "hive (testDB)>", errS, null, ERRNO_OTHER, true);
  }

  @Test
  public void testUseInvalidDB() {
    verifyCMD("set hive.cli.print.current.db=true;use invalidDB;",
        "hive (invalidDB)>", os, null, ERRNO_OTHER, false);
  }

  @Test
  public void testNoErrorDB() {
    verifyCMD(null, "Error: Method not supported (state=,code=0)", errS, new String[] { "-e", "show tables;" },
        ERRNO_OK, false);
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
    System.setProperty("datanucleus.schema.autoCreateAll", "true");
    cli = new HiveCli();
    redirectOutputStream();
    initFromFile();
  }

  @After
  public void tearDown() {
    tmp.delete();
  }
}
