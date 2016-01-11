/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.service.cli.session;

import junit.framework.Assert;
import junit.framework.TestCase;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.cli.CLIService;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.rpc.thrift.TProtocolVersion;
import org.apache.hive.service.cli.thrift.ThriftBinaryCLIService;
import org.apache.hive.service.cli.thrift.ThriftCLIService;
import org.apache.hive.service.cli.thrift.ThriftCLIServiceClient;
import org.junit.Before;
import org.junit.Test;

public class TestPluggableHiveSessionImpl extends TestCase {

  private HiveConf hiveConf;
  private CLIService cliService;
  private ThriftCLIServiceClient client;
  private ThriftCLIService service;

  @Override
  @Before
  public void setUp() {
    hiveConf = new HiveConf();
    hiveConf.setVar(HiveConf.ConfVars.HIVE_SESSION_IMPL_CLASSNAME, TestHiveSessionImpl.class.getName());
    cliService = new CLIService(null);
    service = new ThriftBinaryCLIService(cliService, null);
    service.init(hiveConf);
    client = new ThriftCLIServiceClient(service);
  }


  @Test
  public void testSessionImpl() {
    SessionHandle sessionHandle = null;
    try {
      sessionHandle = client.openSession("tom", "password");
      Assert.assertEquals(TestHiveSessionImpl.class.getName(),
              service.getHiveConf().getVar(HiveConf.ConfVars.HIVE_SESSION_IMPL_CLASSNAME));
      Assert.assertTrue(cliService.getSessionManager().getSession(sessionHandle) instanceof TestHiveSessionImpl);
      client.closeSession(sessionHandle);
    } catch (HiveSQLException e) {
      e.printStackTrace();
    }
  }

  class TestHiveSessionImpl extends HiveSessionImpl {

    public TestHiveSessionImpl(TProtocolVersion protocol, String username, String password, HiveConf serverhiveConf, String ipAddress) {
      super(protocol, username, password, serverhiveConf, ipAddress);
    }
  }
}
