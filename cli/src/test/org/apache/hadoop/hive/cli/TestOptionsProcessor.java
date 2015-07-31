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

package org.apache.hadoop.hive.cli;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Test;

/**
 *  test class OptionsProcessor
 */
public class TestOptionsProcessor {

  /**
   * test pase parameters for Hive
   */
  @Test
  public void testOptionsProcessor() {
    OptionsProcessor processor = new OptionsProcessor();
    System.clearProperty("hiveconf");
    System.clearProperty("define");
    System.clearProperty("hivevar");
    assertNull(System.getProperty("_A"));
    String[] args = { "-hiveconf", "_A=B", "-define", "C=D", "-hivevar", "X=Y",
        "-S", "true", "-database", "testDb", "-e", "execString",  "-v", "true"};

    // stage 1
    assertTrue(processor.process_stage1(args));
    assertEquals("B", System.getProperty("_A"));
    assertEquals("D", processor.getHiveVariables().get("C"));
    assertEquals("Y", processor.getHiveVariables().get("X"));

    CliSessionState sessionState = new CliSessionState(new HiveConf());
    // stage 2
    processor.process_stage2(sessionState);
    assertEquals("testDb", sessionState.database);
    assertEquals("execString", sessionState.execString);
    assertEquals(0, sessionState.initFiles.size());
    assertTrue(sessionState.getIsVerbose());
    assertTrue(sessionState.getIsSilent());

  }
  /**
   * Test set fileName
   */
  @Test
  public void testFiles() {
    OptionsProcessor processor = new OptionsProcessor();

    String[] args = {"-i", "f1", "-i", "f2","-f", "fileName",};
    assertTrue(processor.process_stage1(args));

    CliSessionState sessionState = new CliSessionState(new HiveConf());
    processor.process_stage2(sessionState);
    assertEquals("fileName", sessionState.fileName);
    assertEquals(2, sessionState.initFiles.size());
    assertEquals("f1", sessionState.initFiles.get(0));
    assertEquals("f2", sessionState.initFiles.get(1));

  }
}
