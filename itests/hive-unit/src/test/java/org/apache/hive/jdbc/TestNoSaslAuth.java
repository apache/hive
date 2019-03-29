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

package org.apache.hive.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.session.HiveSessionHook;
import org.apache.hive.service.cli.session.HiveSessionHookContext;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestNoSaslAuth {
  private static MiniHS2 miniHS2 = null;
  private static String sessionUserName = "";

  public static class NoSaslSessionHook implements HiveSessionHook {
    public static boolean checkUser = false;

    @Override
    public void run(HiveSessionHookContext sessionHookContext)
        throws HiveSQLException {
      if (checkUser) {
        Assert.assertEquals(sessionHookContext.getSessionUser(), sessionUserName);
      }
    }
  }

  private Connection hs2Conn = null;

  @BeforeClass
  public static void beforeTest() throws Exception {
    Class.forName(MiniHS2.getJdbcDriverName());
    HiveConf conf = new HiveConf();
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    conf.setBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS, false);
    conf.setVar(ConfVars.HIVE_SERVER2_SESSION_HOOK,
        NoSaslSessionHook.class.getName());
    conf.setVar(ConfVars.HIVE_SERVER2_AUTHENTICATION, "NOSASL");
    miniHS2 = new MiniHS2(conf);
    Map<String, String> overlayProps = new HashMap<String, String>();
    miniHS2.start(overlayProps);
  }

  @Before
  public void setUp() throws Exception {
    // enable the hook check after the server startup,
    NoSaslSessionHook.checkUser = true;
  }

  @After
  public void tearDown() throws Exception {
    hs2Conn.close();
    NoSaslSessionHook.checkUser = false;
  }

  @AfterClass
  public static void afterTest() throws Exception {
    if (miniHS2.isStarted())
      miniHS2.stop();
  }

  /**
   * Initiate a non-sasl connection. The session hook will verfiy the user name
   * set correctly
   *
   * @throws Exception
   */
  @Test
  public void testConnection() throws Exception {
    sessionUserName = "user1";
    hs2Conn = DriverManager.getConnection(
        miniHS2.getJdbcURL() + ";auth=noSasl", sessionUserName, "foo");
  }
}
