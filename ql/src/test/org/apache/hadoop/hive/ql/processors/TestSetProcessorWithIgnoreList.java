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

package org.apache.hadoop.hive.ql.processors;

import org.apache.hadoop.hive.common.io.SessionStream;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.SystemVariables;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.*;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class TestSetProcessorWithIgnoreList {

  private ByteArrayOutputStream baos;
  private static SessionState state;
  private SetProcessor processor;

  @BeforeClass
  public static void before() throws Exception {
    HiveConf conf = new HiveConf();
    HiveConf.setVar(conf, HiveConf.ConfVars.HIVE_CONF_IGNORE_LIST, "hive.execution.engine");
    HiveConf conf2 = new HiveConf(conf, conf.getClass()); //workaround - this will populate the ignoreSet internally
    SessionState.start(conf2);
    state = SessionState.get();
  }

  @Before
  public void setupTest() throws Exception {
    baos = new ByteArrayOutputStream();
    state.out = new SessionStream(baos);
    processor = new SetProcessor();
  }

  @Test
  public void testIgnoredConfigOutputMessage() throws Exception {
    runSetProcessor(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE.varname+"=mr");
    String output = baos.toString();
    Assert.assertTrue(output.contains("ignored"));
  }

  @Test
  public void testIgnoredConfig() throws Exception {
    runSetProcessor(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE.varname);
    String output = baos.toString();

    baos.close();
    baos = new ByteArrayOutputStream();
    state.out = new SessionStream(baos);

    String defaultValue = HiveConf.ConfVars.HIVE_EXECUTION_ENGINE.defaultStrVal;
    String nonDefaultValue = "tez".equals(defaultValue) ? "mr" : "tez";
    runSetProcessor(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE.varname+"="+nonDefaultValue);

    baos.close();
    baos = new ByteArrayOutputStream();
    state.out = new SessionStream(baos);

    runSetProcessor(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE.varname);
    String outputAfterSetCommand = baos.toString();

    Assert.assertTrue(output.equals(outputAfterSetCommand));
  }

  @After
  public void tearDown() throws Exception {
    baos.close();
  }

  /*
   * Simulates the set <command>;
   */
  private void runSetProcessor(String command) throws CommandProcessorException {
    processor.run(command);
    state.out.flush();
  }

}
