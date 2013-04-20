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
package org.apache.hive.service.cli.thrift;

import java.io.IOException;
import java.util.Collection;

import javax.security.auth.login.LoginException;

import junit.framework.TestCase;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hive.service.Service;
import org.apache.hive.service.auth.HiveAuthFactory;
import org.apache.hive.service.cli.CLIService;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.session.HiveSession;
import org.apache.hive.service.cli.session.SessionManager;

public class TestThriftCLIService extends TestCase{

  /**
   * Test setting {@link HiveConf.ConfVars}} config parameter
   *   HIVE_SERVER2_ENABLE_DOAS for kerberos secure mode
   * @throws IOException
   * @throws LoginException
   * @throws HiveSQLException
   */
  public void testDoAs() throws HiveSQLException, LoginException, IOException{
    HiveConf hconf = new HiveConf();
    assertTrue("default value of hive server2 doAs should be true",
        hconf.getBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS));

    hconf.setVar(ConfVars.HIVE_SERVER2_AUTHENTICATION,
        HiveAuthFactory.AuthTypes.KERBEROS.toString());

    CLIService cliService = new CLIService();
    cliService.init(hconf);
    ThriftCLIService tcliService = new ThriftCLIService(cliService);
    TOpenSessionReq req = new TOpenSessionReq();
    req.setUsername("testuser1");
    SessionHandle sHandle = tcliService.getSessionHandle(req );
    SessionManager sManager = getSessionManager(cliService.getServices());
    HiveSession session = sManager.getSession(sHandle);

    //Proxy class for doing doAs on all calls is used when doAs is enabled
    // and kerberos security is on
    assertTrue("check if session class is a proxy", session instanceof java.lang.reflect.Proxy);
  }

  private SessionManager getSessionManager(Collection<Service> services) {
    for(Service s : services){
      if(s instanceof SessionManager){
        return (SessionManager)s;
      }
    }
    return null;
  }
}
