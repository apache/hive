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
import org.apache.hadoop.hive.ql.hooks.QueryLifeTimeHookContext;
import org.apache.hadoop.hive.ql.hooks.QueryLifeTimeHookWithParseHooks;
import org.apache.hadoop.hive.ql.hooks.TestQueryHooks;
import org.apache.hadoop.hive.ql.session.SessionState;

import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.session.HiveSession;

import org.junit.Test;

import java.util.Calendar;
import java.util.GregorianCalendar;

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
    HiveConf conf = new HiveConf(TestQueryHooks.class);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    conf.setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
            "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    conf.set(HiveConf.ConfVars.HIVE_QUERY_LIFETIME_HOOKS.varname, QueryInfoVerificationHook.class.getName());

    SessionState.start(conf);
    HiveSession mockHiveSession = mock(HiveSession.class);
    when(mockHiveSession.getHiveConf()).thenReturn(conf);
    when(mockHiveSession.getSessionState()).thenReturn(SessionState.get());
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
      assertQueryId(ctx.getQueryId());
    }

    @Override
    public void afterParse(QueryLifeTimeHookContext ctx, boolean hasError) {
      assertNotNull(ctx);
      assertEquals(ctx.getCommand().trim(), QUERY);
      assertFalse(hasError);
      assertQueryId(ctx.getQueryId());
    }

    @Override
    public void beforeCompile(QueryLifeTimeHookContext ctx) {
      assertNotNull(ctx);
      assertEquals(ctx.getCommand().trim(), QUERY);
      assertQueryId(ctx.getQueryId());
    }

    @Override
    public void afterCompile(QueryLifeTimeHookContext ctx, boolean hasError) {
      assertNotNull(ctx);
      assertEquals(ctx.getCommand().trim(), QUERY);
      assertFalse(hasError);
      assertQueryId(ctx.getQueryId());
    }

    @Override
    public void beforeExecution(QueryLifeTimeHookContext ctx) {
      assertNotNull(ctx);
      assertEquals(ctx.getCommand().trim(), QUERY);
      assertNotNull(ctx.getHookContext());
      assertNotNull(ctx.getHookContext().getQueryInfo());
      assertNotNull(ctx.getHookContext().getQueryInfo().getQueryDisplay());
      assertQueryId(ctx.getQueryId());
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
      assertQueryId(ctx.getQueryId());
    }
  }

  /**
   * Asserts that the specified query id exists and has the expected prefix and size.
   *
   * <p>
   * A query id looks like below:
   * <pre>
   *   username_20220330093338_dab90f30-5e79-463d-8359-0d2fff57effa
   * </pre>
   * and we can accurately predict how the prefix should look like. T
   * </p>
   *
   * @param actualQueryId the query id to verify
   */
  private static void assertQueryId(String actualQueryId) {
    assertNotNull(actualQueryId);
    String expectedIdPrefix = makeQueryIdStablePrefix();
    String actualIdPrefix = actualQueryId.substring(0, expectedIdPrefix.length());
    assertEquals(expectedIdPrefix, actualIdPrefix);
    assertEquals(expectedIdPrefix.length() + 41, actualQueryId.length());
  }

  /**
   * Makes a query id prefix that is stable for an hour. The prefix changes every hour but this is enough to guarantee
   * that there will not be any flakiness when used in these tests.
   *
   * @return a query id prefix that remains stable for an hour.
   */
  private static String makeQueryIdStablePrefix() {
    GregorianCalendar gc = new GregorianCalendar();
    String userid = System.getProperty("user.name");

    return userid + "_" + String.format("%1$4d%2$02d%3$02d%4$02d",
        gc.get(Calendar.YEAR),
        gc.get(Calendar.MONTH) + 1,
        gc.get(Calendar.DAY_OF_MONTH),
        gc.get(Calendar.HOUR_OF_DAY));
  }
}
