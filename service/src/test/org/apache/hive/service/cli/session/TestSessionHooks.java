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

package org.apache.hive.service.cli.session;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hive.service.auth.HiveAuthFactory;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.thrift.EmbeddedThriftBinaryCLIService;
import org.apache.hive.service.cli.thrift.ThriftCLIServiceClient;
import org.junit.Before;
import org.junit.Test;

public class TestSessionHooks extends TestCase {

  private static String sessionUserName = "user1";
  private EmbeddedThriftBinaryCLIService service;
  private ThriftCLIServiceClient client;

  public static class SessionHookTest implements HiveSessionHook {

   public static AtomicInteger runCount = new AtomicInteger(0);

    @Override
    public void run(HiveSessionHookContext sessionHookContext) throws HiveSQLException {
      Assert.assertEquals(sessionHookContext.getSessionUser(), sessionUserName);
      String sessionHook = sessionHookContext.getSessionConf().
          getVar(ConfVars.HIVE_SERVER2_SESSION_HOOK);
      Assert.assertTrue(sessionHook.contains(this.getClass().getName()));
      Assert.assertEquals(0, runCount.getAndIncrement());
    }
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    SessionHookTest.runCount.set(0);
    System.setProperty(ConfVars.HIVE_SERVER2_SESSION_HOOK.varname,
        TestSessionHooks.SessionHookTest.class.getName());
    service = new EmbeddedThriftBinaryCLIService();
    HiveConf hiveConf = new HiveConf();
    hiveConf
        .setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
            "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    service.init(hiveConf);
    client = new ThriftCLIServiceClient(service);
  }

  @Test
  public void testSessionHook () throws Exception {
    // create session, test if the hook got fired by checking the expected property
    SessionHandle sessionHandle = client.openSession(sessionUserName, "foobar",
          Collections.<String, String>emptyMap());
    Assert.assertEquals(1, SessionHookTest.runCount.get());
    client.closeSession(sessionHandle);
  }

  /***
   * Create session with proxy user property. Verify the effective session user
   * @throws Exception
   */
  @Test
  public void testProxyUser() throws Exception {
    String connectingUser = "user1";
    String proxyUser = System.getProperty("user.name");
    Map<String, String>sessConf = new HashMap<String,String>();
    sessConf.put(HiveAuthFactory.HS2_PROXY_USER, proxyUser);
    sessionUserName = proxyUser;
    SessionHandle sessionHandle = client.openSession(connectingUser, "foobar", sessConf);
    Assert.assertEquals(1, SessionHookTest.runCount.get());
    client.closeSession(sessionHandle);
  }
}
