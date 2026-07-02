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

  /**
   * {@code hive.server2.webui.auth.method=CUSTOM} (case-insensitive) must
   * resolve to {@link HiveServer2.WebUIAuthMethod#CUSTOM}; unknown / null /
   * empty values fall back to NONE. This is the gate that lets the CUSTOM
   * branch in {@code init()} run.
   */
  @Test
  public void testGetWebUIAuthMethod() {
    assertEquals(HiveServer2.WebUIAuthMethod.NONE,
        HiveServer2.getWebUIAuthMethod(null));
    assertEquals(HiveServer2.WebUIAuthMethod.NONE,
        HiveServer2.getWebUIAuthMethod(""));
    assertEquals(HiveServer2.WebUIAuthMethod.NONE,
        HiveServer2.getWebUIAuthMethod("NONE"));
    assertEquals(HiveServer2.WebUIAuthMethod.LDAP,
        HiveServer2.getWebUIAuthMethod("LDAP"));
    assertEquals("lower-case input must resolve the same as upper-case",
        HiveServer2.WebUIAuthMethod.LDAP, HiveServer2.getWebUIAuthMethod("ldap"));
    assertEquals(HiveServer2.WebUIAuthMethod.CUSTOM,
        HiveServer2.getWebUIAuthMethod("CUSTOM"));
    assertEquals("lower-case input must resolve the same as upper-case",
        HiveServer2.WebUIAuthMethod.CUSTOM, HiveServer2.getWebUIAuthMethod("custom"));
    assertEquals("unknown values fall back to NONE",
        HiveServer2.WebUIAuthMethod.NONE, HiveServer2.getWebUIAuthMethod("bogus"));
  }

  /**
   * Sanity check that the conf carries the CUSTOM mode end-to-end: writing
   * the ConfVar with the string "CUSTOM" must round-trip through HiveConf
   * and through {@link HiveServer2#getWebUIAuthMethod} without coercion.
   */
  @Test
  public void testWebUIAuthMethodFromHiveConfRoundTrip() {
    HiveConf conf = new HiveConf();
    conf.setVar(ConfVars.HIVE_SERVER2_WEBUI_AUTH_METHOD, "CUSTOM");
    assertEquals(HiveServer2.WebUIAuthMethod.CUSTOM,
        HiveServer2.getWebUIAuthMethod(
            conf.getVar(ConfVars.HIVE_SERVER2_WEBUI_AUTH_METHOD)));
  }
}
