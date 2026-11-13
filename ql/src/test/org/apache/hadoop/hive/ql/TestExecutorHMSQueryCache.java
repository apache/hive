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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.ql;

import static org.junit.Assert.assertNotNull;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConfForTest;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * The query-level HMS cache across the execution of a query.
 */
public class TestExecutorHMSQueryCache {
  private static final String TABLE = "query_cache_created_table";

  /** Looks up the created table by name, as a post-execution hook would. */
  public static class LookupCreatedTableHook implements ExecuteWithHookContext {
    static Table found;

    @Override
    public void run(HookContext hookContext) throws Exception {
      // the false variant returns null on a miss, so that assertNotNull is the actual check
      found = Hive.get().getTable("default", TABLE, false);
    }
  }

  private IDriver driver;

  @Before
  public void setUp() {
    HiveConf conf = new HiveConfForTest(getClass());
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    // as TestDriver does: the HiveConfForTest default authorizer is not on the ql test classpath
    conf.setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    conf.setBoolVar(HiveConf.ConfVars.HIVE_OPTIMIZE_HMS_QUERY_CACHE_ENABLED, true);
    conf.setVar(HiveConf.ConfVars.POST_EXEC_HOOKS, LookupCreatedTableHook.class.getName());
    SessionState.start(conf);
    driver = new Driver(conf);
    LookupCreatedTableHook.found = null;
  }

  @After
  public void tearDown() throws Exception {
    driver.run("drop table if exists " + TABLE);
    driver.close();
    SessionState.get().close();
  }

  @Test
  public void testPostExecHookFindsTableCreatedByQuery() throws Exception {
    // IF NOT EXISTS: the analyzer looks the table up at compile time and caches it as not found
    driver.run("create table if not exists " + TABLE + " (i int)");
    assertNotNull("a post-execution hook sees the table the query created", LookupCreatedTableHook.found);
  }
}
