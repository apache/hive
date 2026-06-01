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

package org.apache.hive.service.server;

import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hive.http.HttpServer;
import org.apache.hive.service.cli.CLIService;
import org.apache.hive.service.cli.session.SessionManager;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestHiveServer2 {

  @Test
  public void testMaybeStartCompactorThreadsOneCustomPool() {
    HiveServer2 hs2 = new HiveServer2();

    HiveConf conf = new HiveConf();
    MetastoreConf.setLongVar(conf, MetastoreConf.ConfVars.COMPACTOR_WORKER_THREADS, 1);
    conf.setInt("hive.compactor.worker.pool1.threads", 1);

    Map<String, Integer> startedWorkers = hs2.maybeStartCompactorThreads(conf);
    assertEquals(1, startedWorkers.size());
    assertEquals(Integer.valueOf(1), startedWorkers.get("pool1"));
  }

  @Test
  public void testMaybeStartCompactorThreadsZeroTotalWorkers() {
    HiveServer2 hs2 = new HiveServer2();

    HiveConf conf = new HiveConf();
    MetastoreConf.setLongVar(conf, MetastoreConf.ConfVars.COMPACTOR_WORKER_THREADS, 0);
    conf.setInt("hive.compactor.worker.pool1.threads", 5);

    Map<String, Integer> startedWorkers = hs2.maybeStartCompactorThreads(conf);
    assertEquals(0, startedWorkers.size());
  }

  @Test
  public void testMaybeStartCompactorThreadsZeroCustomWorkers() {
    HiveServer2 hs2 = new HiveServer2();

    HiveConf conf = new HiveConf();
    MetastoreConf.setLongVar(conf, MetastoreConf.ConfVars.COMPACTOR_WORKER_THREADS, 5);

    Map<String, Integer> startedWorkers = hs2.maybeStartCompactorThreads(conf);
    assertEquals(1, startedWorkers.size());
    assertEquals(Integer.valueOf(5), startedWorkers.get(Constants.COMPACTION_DEFAULT_POOL));
  }

  @Test
  public void testMaybeStartCompactorThreadsMultipleCustomPools() {
    HiveServer2 hs2 = new HiveServer2();

    HiveConf conf = new HiveConf();
    MetastoreConf.setLongVar(conf, MetastoreConf.ConfVars.COMPACTOR_WORKER_THREADS, 12);
    conf.setInt("hive.compactor.worker.pool1.threads", 3);
    conf.setInt("hive.compactor.worker.pool2.threads", 4);
    conf.setInt("hive.compactor.worker.pool3.threads", 5);

    Map<String, Integer> startedWorkers = hs2.maybeStartCompactorThreads(conf);
    assertEquals(3, startedWorkers.size());
    assertEquals(Integer.valueOf(3), startedWorkers.get("pool1"));
    assertEquals(Integer.valueOf(4), startedWorkers.get("pool2"));
    assertEquals(Integer.valueOf(5), startedWorkers.get("pool3"));
  }

  @Test
  public void testMaybeStartCompactorThreadsMultipleCustomPoolsAndDefaultPool() {
    HiveServer2 hs2 = new HiveServer2();

    HiveConf conf = new HiveConf();
    MetastoreConf.setLongVar(conf, MetastoreConf.ConfVars.COMPACTOR_WORKER_THREADS, 15);
    conf.setInt("hive.compactor.worker.pool1.threads", 3);
    conf.setInt("hive.compactor.worker.pool2.threads", 4);
    conf.setInt("hive.compactor.worker.pool3.threads", 5);

    Map<String, Integer> startedWorkers = hs2.maybeStartCompactorThreads(conf);
    assertEquals(4, startedWorkers.size());
    assertEquals(Integer.valueOf(3), startedWorkers.get("pool1"));
    assertEquals(Integer.valueOf(4), startedWorkers.get("pool2"));
    assertEquals(Integer.valueOf(5), startedWorkers.get("pool3"));
    assertEquals(Integer.valueOf(3), startedWorkers.get(Constants.COMPACTION_DEFAULT_POOL));
  }

  @Test
  public void testCreateHttpServerBuilderStampsStartcodeBeforeConfIsCopied() throws Exception {
    HiveConf conf = new HiveConf();

    CLIService cli = mock(CLIService.class);
    SessionManager sessionManager = mock(SessionManager.class);
    when(cli.getSessionManager()).thenReturn(sessionManager);

    HttpServer.Builder builder = HiveServer2.createHttpServerBuilder(
        "localhost", 0, "test", "/", conf, cli, null);

    // setConf stores a *copy* of the conf on the Builder. Read that copy back via
    // reflection — that's the same instance the servlet context exposes to the JSP.
    Field confField = HttpServer.Builder.class.getDeclaredField("conf");
    confField.setAccessible(true);
    HiveConf builderConf = (HiveConf) confField.get(builder);
    assertNotNull("Builder.conf must be set after createHttpServerBuilder", builderConf);
    assertNotNull("startcode must be exists", builderConf.get("startcode"));
  }

  // ---- WebUI custom auth filter wiring (createHttpServerBuilder) ----------

  /**
   * Default config: custom auth filter is off, builder must not carry any
   * filter wiring.
   */
  @Test
  public void testCustomAuthFilterDisabledByDefault() throws Exception {
    HiveConf conf = new HiveConf();
    HttpServer.Builder builder = invokeCreateHttpServerBuilder(conf);

    assertFalse("useCustomAuthFilter should default to false",
        builder.useCustomAuthFilter);
  }

  /**
   * When the ConfVars are set, createHttpServerBuilder must thread the filter
   * class name and every {@code ...custom.auth.filter.param.<name>} key into
   * the Builder (the param key is stored without the prefix).
   */
  @Test
  public void testCustomAuthFilterWiredFromConfig() throws Exception {
    HiveConf conf = new HiveConf();
    conf.setBoolVar(ConfVars.HIVE_SERVER2_WEBUI_USE_CUSTOM_AUTH_FILTER, true);
    conf.setVar(ConfVars.HIVE_SERVER2_WEBUI_CUSTOM_AUTH_FILTER, "com.example.MyAuthFilter");
    conf.set(ConfVars.HIVE_SERVER2_WEBUI_CUSTOM_AUTH_FILTER.varname + ".param.realm", "hive");
    conf.set(ConfVars.HIVE_SERVER2_WEBUI_CUSTOM_AUTH_FILTER.varname + ".param.ttl", "600");

    HttpServer.Builder builder = invokeCreateHttpServerBuilder(conf);

    assertTrue("useCustomAuthFilter should be true",
        builder.useCustomAuthFilter);
    assertEquals("com.example.MyAuthFilter",
        builder.customAuthFilter);

    @SuppressWarnings("unchecked")
    Map<String, String> params = builder.customAuthFilterParams;
    assertNotNull("customAuthFilterParams must be populated", params);
    assertEquals("hive", params.get("realm"));
    assertEquals("600", params.get("ttl"));
  }

  /**
   * Enabling the filter without providing a class name is a configuration
   * error and must fail fast at builder construction.
   */
  @Test
  public void testCustomAuthFilterRejectsEmptyClassName() throws Exception {
    HiveConf conf = new HiveConf();
    conf.setBoolVar(ConfVars.HIVE_SERVER2_WEBUI_USE_CUSTOM_AUTH_FILTER, true);
    conf.setVar(ConfVars.HIVE_SERVER2_WEBUI_CUSTOM_AUTH_FILTER, "");

    try {
      invokeCreateHttpServerBuilder(conf);
      fail("Expected IllegalArgumentException when custom auth filter class name is empty");
    } catch (IllegalArgumentException expected) {
      assertTrue("Exception message should reference the custom auth filter ConfVar",
          expected.getMessage().contains(ConfVars.HIVE_SERVER2_WEBUI_CUSTOM_AUTH_FILTER.varname));
    }
  }

  private static HttpServer.Builder invokeCreateHttpServerBuilder(HiveConf conf) throws Exception {
    CLIService cli = mock(CLIService.class);
    SessionManager sm = mock(SessionManager.class);
    when(cli.getSessionManager()).thenReturn(sm);
    return HiveServer2.createHttpServerBuilder("localhost", 0, "test", "/", conf, cli, null);
  }
}
