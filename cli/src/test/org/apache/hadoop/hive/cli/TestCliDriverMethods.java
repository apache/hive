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
package org.apache.hadoop.hive.cli;


import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.security.Permission;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import jline.console.ConsoleReader;
import jline.console.completer.ArgumentCompleter;
import jline.console.completer.Completer;
import junit.framework.TestCase;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.junit.Test;


// Cannot call class TestCliDriver since that's the name of the generated
// code for the script-based testing
public class TestCliDriverMethods extends TestCase {

  SecurityManager securityManager;

  // Some of these tests require intercepting System.exit() using the SecurityManager.
  // It is safer to  register/unregister our SecurityManager during setup/teardown instead
  // of doing it within the individual test cases.
  @Override
  public void setUp() {
    securityManager = System.getSecurityManager();
    System.setSecurityManager(new NoExitSecurityManager(securityManager));
  }

  @Override
  public void tearDown() {
    System.setSecurityManager(securityManager);
  }

  // If the command has an associated schema, make sure it gets printed to use
  public void testThatCliDriverPrintsHeaderForCommandsWithSchema() {
    Schema mockSchema = mock(Schema.class);
    List<FieldSchema> fieldSchemas = new ArrayList<FieldSchema>();
    String fieldName = "FlightOfTheConchords";
    fieldSchemas.add(new FieldSchema(fieldName, "type", "comment"));

    when(mockSchema.getFieldSchemas()).thenReturn(fieldSchemas);

    PrintStream mockOut = headerPrintingTestDriver(mockSchema);
    // Should have printed out the header for the field schema
    verify(mockOut, times(1)).print(fieldName);
  }

  // If the command has no schema, make sure nothing is printed
  public void testThatCliDriverPrintsNoHeaderForCommandsWithNoSchema() {
    Schema mockSchema = mock(Schema.class);
    when(mockSchema.getFieldSchemas()).thenReturn(null);

    PrintStream mockOut = headerPrintingTestDriver(mockSchema);
    // Should not have tried to print any thing.
    verify(mockOut, never()).print(anyString());
  }

  // Test that CliDriver does not strip comments starting with '--'
  public void testThatCliDriverDoesNotStripComments() throws Exception {
    // We need to overwrite System.out and System.err as that is what is used in ShellCmdExecutor
    // So save old values...
    PrintStream oldOut = System.out;
    PrintStream oldErr = System.err;

    // Capture stdout and stderr
    ByteArrayOutputStream dataOut = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(dataOut);
    System.setOut(out);
    ByteArrayOutputStream dataErr = new ByteArrayOutputStream();
    PrintStream err = new PrintStream(dataErr);
    System.setErr(err);

    CliSessionState ss = new CliSessionState(new HiveConf());
    ss.out = out;
    ss.err = err;

    // Save output as yo cannot print it while System.out and System.err are weird
    String message;
    String errors;
    int ret;
    try {
      CliSessionState.start(ss);
      CliDriver cliDriver = new CliDriver();
      // issue a command with bad options
      ret = cliDriver.processCmd("!ls --abcdefghijklmnopqrstuvwxyz123456789");
    } finally {
      // restore System.out and System.err
      System.setOut(oldOut);
      System.setErr(oldErr);
    }
    message = dataOut.toString("UTF-8");
    errors = dataErr.toString("UTF-8");
    assertTrue("Comments with '--; should not have been stripped,"
        + " so command should fail", ret != 0);
    assertTrue("Comments with '--; should not have been stripped,"
        + " so we should have got an error in the output: '" + errors + "'.",
        errors.contains("option"));
    assertNotNull(message); // message kept around in for debugging
  }

  /**
   * Do the actual testing against a mocked CliDriver based on what type of schema
   *
   * @param mockSchema
   *          Schema to throw against test
   * @return Output that would have been sent to the user
   * @throws CommandNeedRetryException
   *           won't actually be thrown
   */
  private PrintStream headerPrintingTestDriver(Schema mockSchema) {
    CliDriver cliDriver = new CliDriver();

    // We want the driver to try to print the header...

    Configuration conf = mock(Configuration.class);
    when(conf.getBoolean(eq(ConfVars.HIVE_CLI_PRINT_HEADER.varname), anyBoolean()))
        .thenReturn(true);
    cliDriver.setConf(conf);

    IDriver proc = mock(IDriver.class);

    CommandProcessorResponse cpr = mock(CommandProcessorResponse.class);
    when(cpr.getResponseCode()).thenReturn(0);
    when(proc.run(anyString())).thenReturn(cpr);

    // and then see what happens based on the provided schema
    when(proc.getSchema()).thenReturn(mockSchema);

    CliSessionState mockSS = mock(CliSessionState.class);
    PrintStream mockOut = mock(PrintStream.class);

    mockSS.out = mockOut;

    cliDriver.processLocalCmd("use default;", proc, mockSS);
    return mockOut;
  }


  public void testGetCommandCompletor() {
    Completer[] completors = CliDriver.getCommandCompleter();
    assertEquals(2, completors.length);
    assertTrue(completors[0] instanceof ArgumentCompleter);
    assertTrue(completors[1] instanceof Completer);

    //comletor add space after last delimeter
   List<CharSequence>testList=new ArrayList<CharSequence>(Arrays.asList(new String[]{")"}));
    completors[1].complete("fdsdfsdf", 0, testList);
    assertEquals(") ", testList.get(0));
    testList=new ArrayList<CharSequence>();
    completors[1].complete("len", 0, testList);
    assertTrue(testList.get(0).toString().endsWith("length("));

    testList=new ArrayList<CharSequence>();
    completors[0].complete("set f", 0, testList);
    assertEquals("set", testList.get(0));

  }

  public void testRun() throws Exception {
    // clean history
    String historyDirectory = System.getProperty("user.home");
    if ((new File(historyDirectory)).exists()) {
      File historyFile = new File(historyDirectory + File.separator + ".hivehistory");
      historyFile.delete();
    }
    HiveConf configuration = new HiveConf();
    configuration.setBoolVar(ConfVars.HIVE_SESSION_HISTORY_ENABLED, true);
    PrintStream oldOut = System.out;
    ByteArrayOutputStream dataOut = new ByteArrayOutputStream();
    System.setOut(new PrintStream(dataOut));
    PrintStream oldErr = System.err;
    ByteArrayOutputStream dataErr = new ByteArrayOutputStream();
    System.setErr(new PrintStream(dataErr));
    CliSessionState ss = new CliSessionState(configuration);
    CliSessionState.start(ss);
    String[] args = {};

    try {
      new FakeCliDriver().run(args);
      assertTrue(dataOut.toString(), dataOut.toString().contains("test message"));
      assertTrue(dataErr.toString(), dataErr.toString().contains("Hive history file="));
      assertTrue(dataErr.toString(), dataErr.toString().contains("File: fakeFile is not a file."));
      dataOut.reset();
      dataErr.reset();

    } finally {
      System.setOut(oldOut);
      System.setErr(oldErr);

    }

  }

  /**
   * Test commands exit and quit
   */
  public void testQuit() throws Exception {

    CliSessionState ss = new CliSessionState(new HiveConf());
    ss.err = System.err;
    ss.out = System.out;

    try {
      CliSessionState.start(ss);
      CliDriver cliDriver = new CliDriver();
      cliDriver.processCmd("quit");
      fail("should be exit");
    } catch (ExitException e) {
      assertEquals(0, e.getStatus());

    } catch (Exception e) {
      throw e;
    }

    try {
      CliSessionState.start(ss);
      CliDriver cliDriver = new CliDriver();
      cliDriver.processCmd("exit");
      fail("should be exit");
    } catch (ExitException e) {
      assertEquals(0, e.getStatus());

    }

  }

  public void testProcessSelectDatabase() throws Exception {
    CliSessionState sessinState = new CliSessionState(new HiveConf());
    CliSessionState.start(sessinState);
    ByteArrayOutputStream data = new ByteArrayOutputStream();
    sessinState.err = new PrintStream(data);
    sessinState.database = "database";
    CliDriver driver = new CliDriver();

    try {
      driver.processSelectDatabase(sessinState);
      fail("shuld be exit");
    } catch (ExitException e) {
      e.printStackTrace();
      assertEquals(40000, e.getStatus());
    }

    assertTrue(data.toString().contains(
        "FAILED: ParseException line 1:4 cannot recognize input near 'database'"));
  }

  public void testprocessInitFiles() throws Exception {
    String oldHiveHome = System.getenv("HIVE_HOME");
    String oldHiveConfDir = System.getenv("HIVE_CONF_DIR");

    File homeFile = File.createTempFile("test", "hive");
    String tmpDir = homeFile.getParentFile().getAbsoluteFile() + File.separator
        + "TestCliDriverMethods";
    homeFile.delete();
    FileUtils.deleteDirectory(new File(tmpDir));
    homeFile = new File(tmpDir + File.separator + "bin" + File.separator + CliDriver.HIVERCFILE);
    homeFile.getParentFile().mkdirs();
    homeFile.createNewFile();
    FileUtils.write(homeFile, "-- init hive file for test ");
    setEnv("HIVE_HOME", homeFile.getParentFile().getParentFile().getAbsolutePath());
    setEnv("HIVE_CONF_DIR", homeFile.getParentFile().getAbsolutePath());
    CliSessionState sessionState = new CliSessionState(new HiveConf());

    ByteArrayOutputStream data = new ByteArrayOutputStream();

    sessionState.err = new PrintStream(data);
    sessionState.out = System.out;
    try {
      CliSessionState.start(sessionState);
      CliDriver cliDriver = new CliDriver();
      cliDriver.processInitFiles(sessionState);
      assertTrue(data.toString().contains(
          "Putting the global hiverc in $HIVE_HOME/bin/.hiverc is deprecated. " +
              "Please use $HIVE_CONF_DIR/.hiverc instead."));
      FileUtils.write(homeFile, "bla bla bla");
      // if init file contains incorrect row
      try {
        cliDriver.processInitFiles(sessionState);
        fail("should be exit");
      } catch (ExitException e) {
        assertEquals(40000, e.getStatus());
      }
      setEnv("HIVE_HOME", null);
      try {
        cliDriver.processInitFiles(sessionState);
        fail("should be exit");
      } catch (ExitException e) {
        assertEquals(40000, e.getStatus());
      }

    } finally {
      // restore data
      setEnv("HIVE_HOME", oldHiveHome);
      setEnv("HIVE_CONF_DIR", oldHiveConfDir);
      FileUtils.deleteDirectory(new File(tmpDir));
    }

    File f = File.createTempFile("hive", "test");
    FileUtils.write(f, "bla bla bla");
    try {
      sessionState.initFiles = Arrays.asList(new String[] {f.getAbsolutePath()});
      CliDriver cliDriver = new CliDriver();
      cliDriver.processInitFiles(sessionState);
      fail("should be exit");
    } catch (ExitException e) {
      assertEquals(40000, e.getStatus());
      assertTrue(data.toString().contains("cannot recognize input near 'bla' 'bla' 'bla'"));

    }
  }

  @Test
  public void testCommandSplits() {
    // Test double quote in the string
    String cmd1 = "insert into escape1 partition (ds='1', part='\"') values (\"!\")";
    assertEquals(cmd1, CliDriver.splitSemiColon(cmd1).get(0));
    assertEquals(cmd1, CliDriver.splitSemiColon(cmd1 + ";").get(0));

    // Test escape
    String cmd2 = "insert into escape1 partition (ds='1', part='\"\\'') values (\"!\")";
    assertEquals(cmd2, CliDriver.splitSemiColon(cmd2).get(0));
    assertEquals(cmd2, CliDriver.splitSemiColon(cmd2 + ";").get(0));

    // Test multiple commands
    List<String> results = CliDriver.splitSemiColon(cmd1 + ";" + cmd2);
    assertEquals(cmd1, results.get(0));
    assertEquals(cmd2, results.get(1));

    results = CliDriver.splitSemiColon(cmd1 + ";" + cmd2 + ";");
    assertEquals(cmd1, results.get(0));
    assertEquals(cmd2, results.get(1));
  }

  private static void setEnv(String key, String value) throws Exception {
    Class[] classes = Collections.class.getDeclaredClasses();
    Map<String, String> env = System.getenv();
    for (Class cl : classes) {
      if ("java.util.Collections$UnmodifiableMap".equals(cl.getName())) {
        Field field = cl.getDeclaredField("m");
        field.setAccessible(true);
        Object obj = field.get(env);
        Map<String, String> map = (Map<String, String>) obj;
        if (value == null) {
          map.remove(key);
        } else {
          map.put(key, value);
        }
      }
    }
  }

  private static class FakeCliDriver extends CliDriver {

    @Override
    protected void setupConsoleReader() throws IOException {
      reader = new FakeConsoleReader();
    }

  }

  private static class FakeConsoleReader extends ConsoleReader {
    private int counter = 0;
    File temp = null;

    public FakeConsoleReader() throws IOException {
      super();

    }

    @Override
    public String readLine(String prompt) throws IOException {
      FileWriter writer;
      switch (counter++) {
      case 0:
        return "!echo test message;";
      case 1:
        temp = File.createTempFile("hive", "test");
        temp.deleteOnExit();
        return "source  " + temp.getAbsolutePath() + ";";
      case 2:
        temp = File.createTempFile("hive", "test");
        temp.deleteOnExit();
        writer = new FileWriter(temp);
        writer.write("bla bla bla");
        writer.close();
        return "list file file://" + temp.getAbsolutePath() + ";";
      case 3:
        return "!echo ";
      case 4:
        return "test message;";
      case 5:
        return "source  fakeFile;";
      case 6:
        temp = File.createTempFile("hive", "test");
        temp.deleteOnExit();
        writer = new FileWriter(temp);
        writer.write("source  fakeFile;");
        writer.close();
        return "list file file://" + temp.getAbsolutePath() + ";";


        // drop table over10k;
      default:
        return null;
      }
    }
  }

  private static class NoExitSecurityManager extends SecurityManager {

    public SecurityManager parentSecurityManager;

    public NoExitSecurityManager(SecurityManager parent) {
      super();
      parentSecurityManager = parent;
      System.setSecurityManager(this);
    }

    @Override
    public void checkPermission(Permission perm, Object context) {
      if (parentSecurityManager != null) {
        parentSecurityManager.checkPermission(perm, context);
      }
    }

    @Override
    public void checkPermission(Permission perm) {
      if (parentSecurityManager != null) {
        parentSecurityManager.checkPermission(perm);
      }
    }

    @Override
    public void checkExit(int status) {
      throw new ExitException(status);
    }
  }

  private static class ExitException extends RuntimeException {
    int status;

    public ExitException(int status) {
      this.status = status;
    }

    public int getStatus() {
      return status;
    }
  }
}
