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

package org.apache.hadoop.hive.ql.reexec;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext.HookType;
import org.junit.Test;

public class TestReExecutionOverlayPlugin {

  @Test
  public void testNormal() throws Exception {
    HiveConf conf = new HiveConf();
    Driver driver = new Driver(conf);
    driver.getConf().setBoolean("reexec.overlay.hive.auto.convert.join", false);
    assertTrue(driver.getConf().getBoolean("hive.auto.convert.join", false));

    ReExecutionOverlayPlugin plugin = new ReExecutionOverlayPlugin();
    plugin.initialize(driver);

    HookContext context = new HookContext(null, QueryState.getNewQueryState(new HiveConf(), null), null, null, null,
        null, null, null, null, false, null, null);
    context.setHookType(HookType.ON_FAILURE_HOOK);
    context.setException(new RuntimeException());

    driver.getHookRunner().runFailureHooks(context);
    plugin.prepareToReExecute();
    assertFalse(driver.getConf().getBoolean("hive.auto.convert.join", true));
  }

  @Test
  public void testNormalAndCustom() throws Exception {
    HiveConf conf = new HiveConf();
    Driver driver = new Driver(conf);
    driver.getConf().setBoolean("reexec.overlay.hive.auto.convert.join", false);
    driver.getConf().setBoolean("reexec.custom.overlay.OutOfMemoryError.hive.vectorized.execution.enabled", false);
    assertTrue(driver.getConf().getBoolean("hive.auto.convert.join", false));
    assertTrue(driver.getConf().getBoolean("hive.vectorized.execution.enabled", false));

    ReExecutionOverlayPlugin plugin = new ReExecutionOverlayPlugin();
    plugin.initialize(driver);

    HookContext context = new HookContext(null, QueryState.getNewQueryState(new HiveConf(), null), null, null, null,
        null, null, null, null, false, null, null);
    context.setHookType(HookType.ON_FAILURE_HOOK);
    context.setException(new OutOfMemoryError());

    driver.getHookRunner().runFailureHooks(context);
    plugin.prepareToReExecute();
    assertFalse(driver.getConf().getBoolean("hive.auto.convert.join", true));
    assertFalse(driver.getConf().getBoolean("hive.vectorized.execution.enabled", true));
  }

  @Test
  public void testNormalAndCustomNoMatch() throws Exception {
    HiveConf conf = new HiveConf();
    Driver driver = new Driver(conf);
    driver.getConf().setBoolean("reexec.overlay.hive.auto.convert.join", false);
    driver.getConf().setBoolean("reexec.custom.overlay.OutOfMemoryError.hive.vectorized.execution.enabled", false);
    assertTrue(driver.getConf().getBoolean("hive.auto.convert.join", false));
    assertTrue(driver.getConf().getBoolean("hive.vectorized.execution.enabled", false));

    ReExecutionOverlayPlugin plugin = new ReExecutionOverlayPlugin();
    plugin.initialize(driver);

    HookContext context = new HookContext(null, QueryState.getNewQueryState(new HiveConf(), null), null, null, null,
        null, null, null, null, false, null, null);
    context.setHookType(HookType.ON_FAILURE_HOOK);
    context.setException(new RuntimeException());

    driver.getHookRunner().runFailureHooks(context);
    plugin.prepareToReExecute();
    assertFalse(driver.getConf().getBoolean("hive.auto.convert.join", true));
    assertTrue(driver.getConf().getBoolean("hive.vectorized.execution.enabled", false));
  }
}
