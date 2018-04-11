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

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;

import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class TestResetProcessor {

  @Test
  public void testResetClosesSparkSession() throws Exception {
    SessionState mockSessionState = createMockSparkSessionState();
    new ResetProcessor().run(mockSessionState, "");
    verify(mockSessionState).closeSparkSession();
  }

  @Test
  public void testResetExecutionEngineClosesSparkSession() throws Exception {
    SessionState mockSessionState = createMockSparkSessionState();
    new ResetProcessor().run(mockSessionState, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE.varname);
    verify(mockSessionState).closeSparkSession();
  }

  private static SessionState createMockSparkSessionState() {
    SessionState mockSessionState = mock(SessionState.class);
    Map<String, String> overriddenConfigurations = new HashMap<>();
    overriddenConfigurations.put(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE.varname, "spark");
    when(mockSessionState.getOverriddenConfigurations()).thenReturn(overriddenConfigurations);
    when(mockSessionState.getConf()).thenReturn(new HiveConf());
    return mockSessionState;
  }
}
