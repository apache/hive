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

package org.apache.hive.beeline;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hive.common.util.HiveTestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Unit test for Beeline arg parser.
 */
@RunWith(Parameterized.class)
public class TestBeelineArgParsing {
  private static final Logger LOG = LoggerFactory.getLogger(TestBeelineArgParsing.class.getName());

  private static final String dummyDriverClazzName = "DummyDriver";

  private String connectionString;
  private String driverClazzName;
  private String driverJarFileName;
  private boolean defaultSupported;

  public TestBeelineArgParsing(String connectionString, String driverClazzName, String driverJarFileName,
                               boolean defaultSupported) {
    this.connectionString = connectionString;
    this.driverClazzName = driverClazzName;
    this.driverJarFileName = driverJarFileName;
    this.defaultSupported = defaultSupported;
  }

  public class TestBeeline extends BeeLine {

    String connectArgs = null;
    List<String> properties = new ArrayList<String>();
    List<String> queries = new ArrayList<String>();

    @Override
    boolean dispatch(String command) {
      String connectCommand = "!connect";
      String propertyCommand = "!properties";
      if (command.startsWith(connectCommand)) {
        this.connectArgs = command.substring(connectCommand.length() + 1, command.length());
      } else if (command.startsWith(propertyCommand)) {
        this.properties.add(command.substring(propertyCommand.length() + 1, command.length()));
      } else {
        this.queries.add(command);
      }
      return true;
    }

    public boolean addlocaldrivername(String driverName) {
      String line = "addlocaldrivername " + driverName;
      return getCommands().addlocaldrivername(line);
    }

    public boolean addLocalJar(String url){
      String line = "addlocaldriverjar " + url;
      return getCommands().addlocaldriverjar(line);
    }
  }

  @Parameters(name="{1}")
  public static Collection<Object[]> data() throws IOException, InterruptedException {
    // generate the dummy driver by using txt file
    String u = HiveTestUtils.getFileFromClasspath("DummyDriver.txt");
    Map<File, String> extraContent=new HashMap<>();
    extraContent.put(new File("META-INF/services/java.sql.Driver"), dummyDriverClazzName);
    File jarFile = HiveTestUtils.genLocalJarForTest(u, dummyDriverClazzName, extraContent);
    String pathToDummyDriver = jarFile.getAbsolutePath();
    String pathToPostgresJar = System.getProperty("maven.local.repository")
        + File.separator + "org"
        + File.separator + "postgresql"
        + File.separator + "postgresql"
        + File.separator + "42.7.3"
        + File.separator
        + "postgresql-42.7.3.jar";
    return Arrays.asList(new Object[][] {
        { "jdbc:postgresql://host:5432/testdb", "org.postgresql.Driver", pathToPostgresJar, true },
        { "jdbc:dummy://host:5432/testdb", dummyDriverClazzName, pathToDummyDriver, false } });
  }

  @Test
  public void testSimpleArgs() throws Exception {
    TestBeeline bl = new TestBeeline();
    String args[] = new String[] {"-u", "url", "-n", "name",
      "-p", "password", "-d", "driver", "-a", "authType"};
    org.junit.Assert.assertEquals(0, bl.initArgs(args));
    Assert.assertTrue(bl.connectArgs.equals("url name password driver"));
    Assert.assertTrue(bl.getOpts().getAuthType().equals("authType"));
  }

  @Test
  public void testEmptyHiveConfVariable() throws Exception {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    PrintStream ops = new PrintStream(os);
    TestBeeline bl = new TestBeeline();
    bl.setOutputStream(ops);
    BeeLineOpts opts = new BeeLineOpts(bl, System.getProperties());
    String[] args = { "--hiveconf", "hadoop.tmp.dir=/tmp" };

    File rcFile = new File(opts.saveDir(), "beeline.properties");
    rcFile.deleteOnExit();

    BufferedWriter writer = new BufferedWriter(new FileWriter(rcFile));
    writer.write("beeline.hiveconfvariables={}");
    writer.newLine();
    writer.write("beeline.hivevariables={}");
    writer.newLine();
    writer.flush();

    try (InputStream stream = new FileInputStream(rcFile)) {
      bl.getOpts().load(stream);
      bl.initArgs(args);
      bl.getOpts().getHiveVariables().get("test");
    }
  }


  @Test
  public void testPasswordFileArgs() throws Exception {
    TestBeeline bl = new TestBeeline();
    File passFile = new File("file.password");
    passFile.deleteOnExit();
    FileOutputStream passFileOut = new FileOutputStream(passFile);
    passFileOut.write("mypass\n".getBytes());
    passFileOut.close();
    String args[] = new String[] {"-u", "url", "-n", "name",
      "-w", "file.password", "-p", "not-taken-if-w-is-present",
      "-d", "driver", "-a", "authType"};
    bl.initArgs(args);
    System.out.println(bl.connectArgs);
    // Password file contents are trimmed of trailing whitespaces and newlines
    Assert.assertTrue(bl.connectArgs.equals("url name mypass driver"));
    Assert.assertTrue(bl.getOpts().getAuthType().equals("authType"));
    passFile.delete();
  }

  /**
   * The first flag is taken by the parser.
   */
  @Test
  public void testDuplicateArgs() throws Exception {
    TestBeeline bl = new TestBeeline();
    String args[] = new String[] {"-u", "url", "-u", "url2", "-n", "name",
      "-p", "password", "-d", "driver"};
    Assert.assertEquals(0, bl.initArgs(args));
    Assert.assertTrue(bl.connectArgs.equals("url name password driver"));
  }

  @Test
  public void testQueryScripts() throws Exception {
    TestBeeline bl = new TestBeeline();
    String args[] = new String[] {"-u", "url", "-n", "name",
      "-p", "password", "-d", "driver", "-e", "select1", "-e", "select2",
      "-e", "select \"hive\""};
    Assert.assertEquals(0, bl.initArgs(args));
    Assert.assertTrue(bl.connectArgs.equals("url name password driver"));
    Assert.assertTrue(bl.queries.contains("select1"));
    Assert.assertTrue(bl.queries.contains("select2"));
    Assert.assertTrue(bl.queries.contains("select \"hive\""));
  }

  /**
   * Test setting hive conf and hive vars with --hiveconf and --hivevar
   */
  @Test
  public void testHiveConfAndVars() throws Exception {
    TestBeeline bl = new TestBeeline();
    String args[] = new String[] {"-u", "url", "-n", "name",
      "-p", "password", "-d", "driver", "--hiveconf", "a=avalue", "--hiveconf", "b=bvalue",
      "--hivevar", "c=cvalue", "--hivevar", "d=dvalue"};
    Assert.assertEquals(0, bl.initArgs(args));
    Assert.assertTrue(bl.connectArgs.equals("url name password driver"));
    Assert.assertTrue(bl.getOpts().getHiveConfVariables().get("a").equals("avalue"));
    Assert.assertTrue(bl.getOpts().getHiveConfVariables().get("b").equals("bvalue"));
    Assert.assertTrue(bl.getOpts().getHiveVariables().get("c").equals("cvalue"));
    Assert.assertTrue(bl.getOpts().getHiveVariables().get("d").equals("dvalue"));
  }

  @Test
  public void testBeelineOpts() throws Exception {
    TestBeeline bl = new TestBeeline();
    String args[] =
        new String[] { "-u", "url", "-n", "name", "-p", "password", "-d", "driver",
            "--autoCommit=true", "--verbose", "--truncateTable" };
    Assert.assertEquals(0, bl.initArgs(args));
    Assert.assertTrue(bl.connectArgs.equals("url name password driver"));
    Assert.assertTrue(bl.getOpts().getAutoCommit());
    Assert.assertTrue(bl.getOpts().getVerbose());
    Assert.assertTrue(bl.getOpts().getTruncateTable());
  }

  @Test
  public void testBeelineAutoCommit() throws Exception {
    TestBeeline bl = new TestBeeline();
    String[] args = {};
    bl.initArgs(args);
    Assert.assertTrue(bl.getOpts().getAutoCommit());

    args = new String[] {"--autoCommit=false"};
    bl.initArgs(args);
    Assert.assertFalse(bl.getOpts().getAutoCommit());

    args = new String[] {"--autoCommit=true"};
    bl.initArgs(args);
    Assert.assertTrue(bl.getOpts().getAutoCommit());
    bl.close();
  }

  @Test
  public void testBeelineShowDbInPromptOptsDefault() throws Exception {
    TestBeeline bl = new TestBeeline();
    String args[] = new String[] { "-u", "url" };
    Assert.assertEquals(0, bl.initArgs(args));
    Assert.assertFalse(bl.getOpts().getShowDbInPrompt());
    Assert.assertEquals("", bl.getFormattedDb());
  }

  @Test
  public void testBeelineShowDbInPromptOptsTrue() throws Exception {
    TestBeeline bl = new TestBeeline();
    String args[] = new String[] { "-u", "url", "--showDbInPrompt=true" };
    Assert.assertEquals(0, bl.initArgs(args));
    Assert.assertTrue(bl.getOpts().getShowDbInPrompt());
    Assert.assertEquals(" (default)", bl.getFormattedDb());
  }


  /**
   * Test setting script file with -f option.
   */
  @Test
  public void testScriptFile() throws Exception {
    TestBeeline bl = new TestBeeline();
    String args[] = new String[] {"-u", "url", "-n", "name",
      "-p", "password", "-d", "driver", "-f", "myscript"};
    Assert.assertEquals(0, bl.initArgs(args));
    Assert.assertTrue(bl.connectArgs.equals("url name password driver"));
    Assert.assertTrue(bl.getOpts().getScriptFile().equals("myscript"));
  }

  /**
   * Test beeline with -f and -e simultaneously
   */
  @Test
  public void testCommandAndFileSimultaneously() throws Exception {
    TestBeeline bl = new TestBeeline();
    String args[] = new String[] {"-e", "myselect", "-f", "myscript"};
    Assert.assertEquals(1, bl.initArgs(args));
  }

  /**
   * Test beeline with multiple initfiles in -i.
   */
  @Test
  public void testMultipleInitFiles() {
    TestBeeline bl = new TestBeeline();
    String[] args = new String[] {"-i", "/url/to/file1", "-i", "/url/to/file2"};
    Assert.assertEquals(0, bl.initArgs(args));
    String[] files = bl.getOpts().getInitFiles();
    Assert.assertEquals("/url/to/file1", files[0]);
    Assert.assertEquals("/url/to/file2", files[1]);
  }

  /**
   * Displays the usage.
   */
  @Test
  public void testHelp() throws Exception {
    TestBeeline bl = new TestBeeline();
    String args[] = new String[] {"--help"};
    Assert.assertEquals(0, bl.initArgs(args));
    Assert.assertEquals(true, bl.getOpts().isHelpAsked());
  }

  /**
   * Displays the usage.
   */
  @Test
  public void testUnmatchedArgs() throws Exception {
    TestBeeline bl = new TestBeeline();
    String args[] = new String[] {"-u", "url", "-n"};
    Assert.assertEquals(-1, bl.initArgs(args));
  }

  @Test
  public void testAddLocalJar() throws Exception {
    TestBeeline bl = new TestBeeline();
    Assert.assertNull(bl.findLocalDriver(connectionString));

    LOG.info("Add " + driverJarFileName + " for the driver class " + driverClazzName);

    bl.addLocalJar(driverJarFileName);
    bl.addlocaldrivername(driverClazzName);
    Assert.assertEquals(bl.findLocalDriver(connectionString).getClass().getName(), driverClazzName);
  }

  @Test
  public void testAddLocalJarWithoutAddDriverClazz() throws Exception {
    TestBeeline bl = new TestBeeline();

    LOG.info("Add " + driverJarFileName + " for the driver class " + driverClazzName);
    assertTrue("expected to exists: "+driverJarFileName,new File(driverJarFileName).exists());
    bl.addLocalJar(driverJarFileName);
    if (!defaultSupported) {
      Assert.assertNull(bl.findLocalDriver(connectionString));
    } else {
      // no need to add for the default supported local jar driver
      Assert.assertNotNull(bl.findLocalDriver(connectionString));
      Assert.assertEquals(bl.findLocalDriver(connectionString).getClass().getName(), driverClazzName);
    }
  }

  @Test
  public void testBeelinePasswordMask() throws Exception {
    TestBeeline bl = new TestBeeline();
    File errFile = File.createTempFile("test", "tmp");
    bl.setErrorStream(new PrintStream(new FileOutputStream(errFile)));
    String args[] =
        new String[] { "-u", "url", "-n", "name", "-p", "password", "-d", "driver",
            "--autoCommit=true", "--verbose", "--truncateTable" };
    bl.initArgs(args);
    bl.close();
    String errContents = new String(Files.readAllBytes(Paths.get(errFile.toString())));
    Assert.assertTrue(errContents.contains(BeeLine.PASSWD_MASK));
  }

  /**
   * Test property file parameter option.
   */
  @Test
  public void testPropertyFile() throws Exception {
    TestBeeline bl = new TestBeeline();
    String args[] = new String[] {"--property-file", "props"};
    Assert.assertEquals(0, bl.initArgs(args));
    Assert.assertTrue(bl.properties.get(0).equals("props"));
    bl.close();
  }

  /**
   * Test maxHistoryRows parameter option.
   */
  @Test
  public void testMaxHistoryRows() throws Exception {
    TestBeeline bl = new TestBeeline();
    String args[] = new String[] {"--maxHistoryRows=100"};
    Assert.assertEquals(0, bl.initArgs(args));
    Assert.assertTrue(bl.getOpts().getMaxHistoryRows() == 100);
    bl.close();
  }

  /**
   * Test the file parameter option
   * @throws Exception
   */
  @Test
  public void testFileParam() throws Exception {
    TestBeeline bl = new TestBeeline();
    String args[] = new String[] {"-u", "url", "-n", "name",
        "-p", "password", "-d", "driver", "-f", "hdfs://myscript"};
    Assert.assertEquals(0, bl.initArgs(args));
    Assert.assertTrue(bl.connectArgs.equals("url name password driver"));
    Assert.assertTrue(bl.getOpts().getScriptFile().equals("hdfs://myscript"));
  }

  /**
   * Test the report parameter option.
   * @throws Exception
   */
  @Test
  public void testReport() throws Exception {
    TestBeeline bl = new TestBeeline();
    String args[] = new String[] {"--report=true"};
    Assert.assertEquals(0, bl.initArgs(args));
    Assert.assertTrue(bl.getOpts().isReport());
    bl.close();
  }

  /**
   * Test property file option with query.
   */
  @Test
  public void testPropertyFileWithQuery() throws Exception {
    TestBeeline bl = new TestBeeline();
    String args[] =
        new String[] {"--property-file", "props", "-e", "show tables"};
    Assert.assertEquals(0, bl.initArgs(args));
    Assert.assertEquals("props", bl.properties.get(0));
    Assert.assertEquals("show tables", bl.queries.get(0));
    bl.close();
  }
}
