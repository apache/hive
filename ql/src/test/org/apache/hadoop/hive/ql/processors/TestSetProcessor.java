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

package org.apache.hadoop.hive.ql.processors;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.SystemVariables;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestSetProcessor {

  private static final String TEST_SYSTEM_PROPERTY = "testSystemPropertyPassword";
  private static final String TEST_SYSTEM_PROPERTY_VALUE = "testSystemPropertyValue";
  private static final String TEST_ENV_VAR_PASSWORD_VALUE = "testEnvPasswordValue";
  private static final String TEST_ENV_VAR_PASSWORD = "testEnvPassword";
  private ByteArrayOutputStream baos;
  private static SessionState state;
  private SetProcessor processor;

  @BeforeClass
  public static void before() throws Exception {
    Map<String, String> env = new HashMap<>();
    env.put(TEST_ENV_VAR_PASSWORD, TEST_ENV_VAR_PASSWORD_VALUE);
    setEnv(env);
    System.setProperty(TEST_SYSTEM_PROPERTY, TEST_SYSTEM_PROPERTY_VALUE);
    HiveConf conf = new HiveConf();
    SessionState.start(conf);
    state = SessionState.get();
  }

  @Before
  public void setupTest() {
    baos = new ByteArrayOutputStream();
    state.out = new PrintStream(baos);
    processor = new SetProcessor();
  }

  @Test
  public void testHiddenConfig() throws Exception {
    runSetProcessor("");
    String output = baos.toString();
    Assert.assertFalse(output.contains(HiveConf.ConfVars.METASTOREPWD.varname + "="));
    Assert.assertFalse(output.contains(HiveConf.ConfVars.HIVE_SERVER2_SSL_KEYSTORE_PASSWORD.varname + "="));
  }

  @Test
  public void testHiddenConfigSetVarName() {
    runSetProcessor(HiveConf.ConfVars.METASTOREPWD.varname);
    String output = baos.toString();
    Assert.assertTrue(output.contains("hidden"));
  }

  @Test
  public void testEnvPasswordMask() throws Exception {
    runSetProcessor("");
    String output = baos.toString();
    Assert.assertFalse(output.contains(TEST_ENV_VAR_PASSWORD + "="));
  }

  @Test
  public void testEnvPasswordMaskIndividual() throws Exception {
    runSetProcessor(SystemVariables.ENV_PREFIX + TEST_ENV_VAR_PASSWORD);
    String output = baos.toString();
    Assert.assertFalse(output.contains(TEST_ENV_VAR_PASSWORD_VALUE));
    Assert.assertTrue(output.contains("hidden"));
  }

  @Test
  public void testSystemProperty() throws Exception {
    runSetProcessor("");
    String output = baos.toString();
    Assert.assertFalse(output.contains(TEST_SYSTEM_PROPERTY + "="));
  }

  @Test
  public void testSystemPropertyIndividual() throws Exception {
    runSetProcessor(SystemVariables.SYSTEM_PREFIX + TEST_SYSTEM_PROPERTY);
    String output = baos.toString();
    Assert.assertFalse(output.contains(TEST_SYSTEM_PROPERTY_VALUE));
    Assert.assertTrue(output.contains("hidden"));
  }

  /*
   * Simulates the set <command>;
   */
  private void runSetProcessor(String command) {
    processor.run(command);
    state.out.flush();
  }

  /*
   * Dirty hack to set the environment variables using reflection code. This method is for testing
   * purposes only and should not be used elsewhere
   */
  private final static void setEnv(Map<String, String> newenv) throws Exception {
    Class[] classes = Collections.class.getDeclaredClasses();
    Map<String, String> env = System.getenv();
    for (Class cl : classes) {
      if ("java.util.Collections$UnmodifiableMap".equals(cl.getName())) {
        Field field = cl.getDeclaredField("m");
        field.setAccessible(true);
        Object obj = field.get(env);
        Map<String, String> map = (Map<String, String>) obj;
        map.clear();
        map.putAll(newenv);
      }
    }
  }

}
