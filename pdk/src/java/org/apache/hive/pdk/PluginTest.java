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
package org.apache.hive.pdk;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.PrintStream;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.hadoop.hive.cli.CliDriver;

/**
 * PluginTest is a test harness for invoking all of the unit tests
 * annotated on the classes in a plugin.
 */
public class PluginTest extends TestCase {

  private HivePdkUnitTest unitTest;

  private PluginTest(HivePdkUnitTest unitTest) {
    super(unitTest.query());
    this.unitTest = unitTest;
  }

  public void runTest() throws Exception {
    String output = runHive(
      "-i",
      "metadata/add-jar.sql",
      "-e",
      unitTest.query());
    assertEquals(unitTest.result(), output);
  }

  static String runHive(String ... args) throws Exception {
    ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
    ByteArrayOutputStream errBytes = new ByteArrayOutputStream();
    PrintStream outSaved = System.out;
    PrintStream errSaved = System.err;
    System.setOut(new PrintStream(outBytes, true));
    System.setErr(new PrintStream(errBytes, true));
    try {
      CliDriver.run(args);
    } finally {
      System.setOut(outSaved);
      System.setErr(errSaved);
    }
    ByteArrayInputStream outBytesIn =
      new ByteArrayInputStream(outBytes.toByteArray());
    ByteArrayInputStream errBytesIn =
      new ByteArrayInputStream(errBytes.toByteArray());
    BufferedReader is =
      new BufferedReader(new InputStreamReader(outBytesIn));
    BufferedReader es =
      new BufferedReader(new InputStreamReader(errBytesIn));
    StringBuilder output = new StringBuilder();
    String line;
    while ((line = is.readLine()) != null) {
      if (output.length() > 0) {
        output.append("\n");
      }
      output.append(line);
    }
    if (output.length() == 0) {
      output = new StringBuilder();
      while ((line = es.readLine()) != null) {
        output.append("\n");
        output.append(line);
      }
    }
    return output.toString();
  }

  public static Test suite() throws Exception {
    String classList = System.getProperty("hive.plugin.class.list");
    String [] classNames = classList.split(" ");
    TestSuite suite = new TestSuite("Plugin Tests");
    for (String className : classNames) {
      Class<?> c = Class.forName(className);
      HivePdkUnitTests tests = c.getAnnotation(HivePdkUnitTests.class);
      if (tests == null) {
        continue;
      }
      TestSuite classSuite = new TestSuite(c.getName());
      for (HivePdkUnitTest unitTest : tests.cases()) {
        classSuite.addTest(new PluginTest(unitTest));
      }
      suite.addTest(new PluginTestSetup(classSuite, tests));
    }

    return new PluginGlobalSetup(suite);
  }

  public static void main(String [] args) throws Exception {
    junit.textui.TestRunner.run(suite());
  }

  public static class PluginTestSetup extends TestSetup {
    String name;
    HivePdkUnitTests unitTests;

    PluginTestSetup(TestSuite test, HivePdkUnitTests unitTests) {
      super(test);
      this.name = test.getName();
      this.unitTests = unitTests;
    }

    protected void setUp() throws Exception {
      String cleanup = unitTests.cleanup();
      String setup = unitTests.setup();
      if (cleanup == null) {
        cleanup = "";
      }
      if (setup == null) {
        setup = "";
      }
      if ((cleanup.length() > 0) || (setup.length() > 0)) {
        String result = runHive(
          "-e",
          cleanup + "\n" + setup);
        if (result.length() > 0) {
          System.err.println(name + " SETUP:  " + result);
        }
      }
    }

    protected void tearDown() throws Exception {
      String cleanup = unitTests.cleanup();
      if (cleanup != null) {
        String result = runHive(
          "-e",
          cleanup);
        if (result.length() > 0) {
          System.err.println(name + " TEARDOWN:  " + result);
        }
      }
    }
  }

  public static class PluginGlobalSetup extends TestSetup {
    private File testScriptDir;

    PluginGlobalSetup(Test test) {
      super(test);
      testScriptDir =
        new File(System.getProperty("hive.plugin.root.dir"), "test");
    }

    protected void setUp() throws Exception {
      String result = runHive(
        "-i",
        new File(testScriptDir, "cleanup.sql").toString(),
        "-i",
        "metadata/add-jar.sql",
        "-i",
        "metadata/class-registration.sql",
        "-f",
        new File(testScriptDir, "setup.sql").toString());
      if (result.length() > 0) {
        System.err.println("GLOBAL SETUP:  " + result);
      }
    }

    protected void tearDown() throws Exception {
      String result = runHive(
        "-f",
        new File(testScriptDir, "cleanup.sql").toString());
      if (result.length() > 0) {
        System.err.println("GLOBAL TEARDOWN:  " + result);
      }
    }
  }
}
