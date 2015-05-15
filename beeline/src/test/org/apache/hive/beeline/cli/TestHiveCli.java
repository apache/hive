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

  final static String CMD = "create database if not exists test;\ncreate table if not exists test" +
      ".testTbl(a " +
      "" + "string, b string);\n";
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
    } catch (IOException e) {
      LOG.error("Failed to execute command due to the error: " + e);
    } finally {
      if (retCode != ret) {
        LOG.error("Failed due to the error:" + errS.toString());
        Assert.fail("Supported return code is " + retCode + " while the actual is " + ret);
      }
    }
  }

  private void verifyCMD(String CMD, String keywords, OutputStream os, String[] options, int
      retCode) {
    executeCMD(options, CMD, retCode);
    String output = os.toString();
    Assert.assertTrue(output.contains(keywords));
  }

  @Test
  public void testInValidCmd() {
    verifyCMD("!lss\n", "Failed to execute lss", errS, null, 0);
  }

  @Test
  public void testHelp() {
    verifyCMD(null, "usage: hive", os, new String[]{"-H"}, 1);
  }

  @Test
  public void testInvalidDatabaseOptions() {
    verifyCMD("\nshow tables\nquit\n", "Database does not exist: invalidDB", errS, new
        String[]{"--database", "invalidDB"}, 0);
  }

  @Test
  public void testDatabaseOptions() {
    verifyCMD("\nshow tables;\nquit;", "testTbl", os, new String[]{"--database", "test"}, 0);
  }

  @Test
  public void testSqlFromCmd() {
    verifyCMD(null, "", os, new String[]{"-e", "show databases;"}, 0);
  }

  @Test
  public void testSqlFromCmdWithDBName() {
    verifyCMD(null, "testTbl", os, new String[]{"-e", "show tables;", "--database", "test"}, 0);
  }

  @Test
  public void testInvalidOptions() {
    verifyCMD(null, "The '-e' and '-f' options cannot be specified simultaneously", errS, new
        String[]{"-e", "show tables;", "-f", "path/to/file"}, 1);
  }

  @Test
  public void testInvalidOptions2() {
    verifyCMD(null, "Unrecognized option: -k", errS, new String[]{"-k"}, 1);
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

  private void initFileFromFile() {
    BufferedWriter bw = null;
    try {
      // create a tmp file
      tmp = File.createTempFile("test", ".sql");
      bw = new BufferedWriter(new FileWriter(tmp));
      bw.write(CMD);
    } catch (IOException e) {
      LOG.error("Failed to write tmp file due to the exception: " + e);
    } finally {
      IOUtils.closeQuietly(bw);
    }
    executeCMD(new String[]{"-f", "\"" + tmp.getAbsolutePath() + "\""}, null, 0);
  }

  @Before
  public void setup() {
    cli = new HiveCli();
    redirectOutputStream();
    initFileFromFile();
  }

  @After
  public void tearDown() {
    tmp.delete();
  }
}
