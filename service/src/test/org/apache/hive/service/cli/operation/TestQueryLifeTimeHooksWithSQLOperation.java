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

package org.apache.hive.service.cli.operation;

import com.google.common.collect.ImmutableMap;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConfForTest;
import org.apache.hadoop.hive.ql.hooks.QueryLifeTimeHookContext;
import org.apache.hadoop.hive.ql.hooks.QueryLifeTimeHookWithParseHooks;
import org.apache.hadoop.hive.ql.session.SessionState;

import org.apache.hive.service.cli.HandleIdentifier;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.session.HiveSession;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class TestQueryLifeTimeHooksWithSQLOperation {

  private static final String QUERY = "select 1";

  @Test
  public void testQueryInfoInHookContext() throws IllegalAccessException, ClassNotFoundException, InstantiationException, HiveSQLException {
    HiveConf conf = new HiveConfForTest(getClass());
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    conf.setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
            "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    conf.set(HiveConf.ConfVars.HIVE_QUERY_LIFETIME_HOOKS.varname, QueryInfoVerificationHook.class.getName());

    SessionState.start(conf);
    HiveSession mockHiveSession = mock(HiveSession.class);
    when(mockHiveSession.getHiveConf()).thenReturn(conf);
    when(mockHiveSession.getSessionState()).thenReturn(SessionState.get());
    SessionHandle sessionHandle = mock(SessionHandle.class);
    when(sessionHandle.getHandleIdentifier()).thenReturn(new HandleIdentifier());
    when(mockHiveSession.getSessionHandle()).thenReturn(sessionHandle);
    SQLOperation sqlOperation = new SQLOperation(mockHiveSession, QUERY, ImmutableMap.of(), false, 0);
    sqlOperation.run();
  }

  /**
   * Has to be public so that it can be created by the Driver
   */
  public final static class QueryInfoVerificationHook implements QueryLifeTimeHookWithParseHooks {

    @Override
    public void beforeParse(QueryLifeTimeHookContext ctx) {
      assertNotNull(ctx);
      assertEquals(ctx.getCommand().trim(), QUERY);
    }

    @Override
    public void afterParse(QueryLifeTimeHookContext ctx, boolean hasError) {
      assertNotNull(ctx);
      assertEquals(ctx.getCommand().trim(), QUERY);
      assertFalse(hasError);
    }

    @Override
    public void beforeCompile(QueryLifeTimeHookContext ctx) {
      assertNotNull(ctx);
      assertEquals(ctx.getCommand().trim(), QUERY);
    }

    @Override
    public void afterCompile(QueryLifeTimeHookContext ctx, boolean hasError) {
      assertNotNull(ctx);
      assertEquals(ctx.getCommand().trim(), QUERY);
      assertFalse(hasError);
    }

    @Override
    public void beforeExecution(QueryLifeTimeHookContext ctx) {
      assertNotNull(ctx);
      assertEquals(ctx.getCommand().trim(), QUERY);
      assertNotNull(ctx.getHookContext());
      assertNotNull(ctx.getHookContext().getQueryInfo());
      assertNotNull(ctx.getHookContext().getQueryInfo().getQueryDisplay());
    }

    @Override
    public void afterExecution(QueryLifeTimeHookContext ctx, boolean hasError) {
      assertNotNull(ctx);
      assertEquals(ctx.getCommand().trim(), QUERY);
      assertFalse(hasError);
      assertNotNull(ctx.getHookContext());
      assertNull(ctx.getHookContext().getErrorMessage());
      assertNull(ctx.getHookContext().getException());
      assertNotNull(ctx.getHookContext().getQueryInfo());
      assertNotNull(ctx.getHookContext().getQueryInfo().getQueryDisplay());
    }
  }
}
