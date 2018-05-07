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
package org.apache.hive.service.cli;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Assert;
import org.junit.Test;

public class TestCLIServiceRestore {

  CLIService service = getService();

  @Test
  public void testRestore() throws HiveSQLException {
    SessionHandle session = service.openSession("foo", "bar", null);
    service.stop();
    service = getService();
    try {
      service.getSessionManager().getSession(session);
      Assert.fail("session already exists before restore");
    } catch (HiveSQLException e) {
      Assert.assertTrue(e.getMessage().contains("Invalid SessionHandle"));
    }
    service.createSessionWithSessionHandle(session, "foo", "bar", null);
    Assert.assertNotNull(service.getSessionManager().getSession(session));
    service.stop();
  }

  public CLIService getService() {
    HiveConf conf = new HiveConf();
    conf.setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
      "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    CLIService service = new CLIService(null);
    service.init(conf);
    service.start();
    return service;
  }
}
