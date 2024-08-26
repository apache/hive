/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.hooks;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConfForTest;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentMatcher;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class TestQueryHooks {

  private static HiveConf conf;

  @BeforeClass
  public static void setUpBeforeClass() {
    conf = new HiveConfForTest(TestQueryHooks.class);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    conf.setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
            "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
  }

  @Test
  public void testAllQueryLifeTimeWithParseHooks() throws Exception {
    String query = "select 1";
    ArgumentMatcher<QueryLifeTimeHookContext> argMatcher = new QueryLifeTimeHookContextMatcher(query);
    QueryLifeTimeHookWithParseHooks mockHook = mock(QueryLifeTimeHookWithParseHooks.class);
    Driver driver = createDriver();
    driver.getHookRunner().addLifeTimeHook(mockHook);
    driver.run(query);

    verify(mockHook).beforeParse(argThat(argMatcher));
    verify(mockHook).afterParse(argThat(argMatcher), eq(false));
    verify(mockHook).beforeCompile(argThat(argMatcher));
    verify(mockHook).afterCompile(argThat(argMatcher), eq(false));
    verify(mockHook).beforeExecution(argThat(argMatcher));
    verify(mockHook).afterExecution(argThat(argMatcher), eq(false));
  }

  @Test
  public void testQueryLifeTimeWithParseHooksWithParseError() throws Exception {
    String query = "invalidquery";
    ArgumentMatcher<QueryLifeTimeHookContext> argMatcher = new QueryLifeTimeHookContextMatcher(query);
    QueryLifeTimeHookWithParseHooks mockHook = mock(QueryLifeTimeHookWithParseHooks.class);
    Driver driver = createDriver();
    driver.getHookRunner().addLifeTimeHook(mockHook);
    try {
      driver.run(query);
      Assert.fail("Expected parsing to fail");
    } catch (CommandProcessorException e) {
      // we expect to get here
    }


    verify(mockHook).beforeParse(argThat(argMatcher));
    verify(mockHook).afterParse(argThat(argMatcher), eq(true));
    verify(mockHook, never()).beforeCompile(any());
    verify(mockHook, never()).afterCompile(any(), anyBoolean());
    verify(mockHook, never()).beforeExecution(any());
    verify(mockHook, never()).afterExecution(any(), anyBoolean());
  }

  @Test
  public void testQueryLifeTimeWithParseHooksWithCompileError() throws Exception {
    String query = "select * from foo";
    ArgumentMatcher<QueryLifeTimeHookContext> argMatcher = new QueryLifeTimeHookContextMatcher(query);
    QueryLifeTimeHookWithParseHooks mockHook = mock(QueryLifeTimeHookWithParseHooks.class);
    Driver driver = createDriver();
    driver.getHookRunner().addLifeTimeHook(mockHook);
    try {
      driver.run(query);
      Assert.fail("Expected compilation to fail");
    } catch (CommandProcessorException e) {
      // we expect to get here
    }


    verify(mockHook).beforeParse(argThat(argMatcher));
    verify(mockHook).afterParse(argThat(argMatcher), eq(false));
    verify(mockHook).beforeCompile(argThat(argMatcher));
    verify(mockHook).afterCompile(argThat(argMatcher), eq(true));
    verify(mockHook, never()).beforeExecution(any());
    verify(mockHook, never()).afterExecution(any(), anyBoolean());
  }

  @Test
  public void testAllQueryLifeTimeHooks() throws Exception {
    String query = "select 1";
    ArgumentMatcher<QueryLifeTimeHookContext> argMatcher = new QueryLifeTimeHookContextMatcher(query);
    QueryLifeTimeHook mockHook = mock(QueryLifeTimeHook.class);
    Driver driver = createDriver();
    driver.getHookRunner().addLifeTimeHook(mockHook);
    driver.run(query);

    verify(mockHook).beforeCompile(argThat(argMatcher));
    verify(mockHook).afterCompile(argThat(argMatcher), eq(false));
    verify(mockHook).beforeExecution(argThat(argMatcher));
    verify(mockHook).afterExecution(argThat(argMatcher), eq(false));
  }

  @Test
  public void testQueryLifeTimeWithCompileError() throws Exception {
    String query = "select * from foo";
    ArgumentMatcher<QueryLifeTimeHookContext> argMatcher = new QueryLifeTimeHookContextMatcher(query);
    QueryLifeTimeHook mockHook = mock(QueryLifeTimeHook.class);
    Driver driver = createDriver();
    driver.getHookRunner().addLifeTimeHook(mockHook);
    try {
      driver.run(query);
      Assert.fail("Expected compilation to fail");
    } catch (CommandProcessorException e) {
      // we expect to get here
    }

    verify(mockHook).beforeCompile(argThat(argMatcher));
    verify(mockHook).afterCompile(argThat(argMatcher), eq(true));
    verify(mockHook, never()).beforeExecution(any());
    verify(mockHook, never()).afterExecution(any(), anyBoolean());
  }

  private Driver createDriver() throws IllegalAccessException, ClassNotFoundException, InstantiationException {
    SessionState.start(conf);
    Driver driver = new Driver(conf);
    return driver;
  }

  private static final class QueryLifeTimeHookContextMatcher implements ArgumentMatcher<QueryLifeTimeHookContext> {

    private final String command;

    private QueryLifeTimeHookContextMatcher(String command) {
      this.command = command;
    }

    @Override
    public boolean matches(QueryLifeTimeHookContext queryLifeTimeHookContext) {
      return queryLifeTimeHookContext.getCommand().equals(this.command);
    }
  }
}
