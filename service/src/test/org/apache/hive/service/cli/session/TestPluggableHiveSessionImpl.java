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

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.cli.CLIService;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.thrift.ThriftBinaryCLIService;
import org.apache.hive.service.cli.thrift.ThriftCLIServiceClient;
import org.apache.hive.service.rpc.thrift.TProtocolVersion;
import org.junit.Test;

public class TestPluggableHiveSessionImpl {

  @Test
  public void testSessionImpl() throws Exception {

    HiveConf hiveConf = new HiveConf();
    hiveConf.setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
        HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER.getDefaultValue());
    hiveConf.setVar(HiveConf.ConfVars.HIVE_SESSION_IMPL_CLASSNAME,
        SampleHiveSessionImpl.class.getName());
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS, false);

    CLIService cliService = new CLIService(null, true);
    cliService.init(hiveConf);
    ThriftBinaryCLIService service = new ThriftBinaryCLIService(cliService);
    service.init(hiveConf);
    ThriftCLIServiceClient client = new ThriftCLIServiceClient(service);

    SessionHandle sessionHandle = null;
    sessionHandle = client.openSession("tom", "password");
    assertEquals(SampleHiveSessionImpl.class.getName(),
        service.getHiveConf().getVar(HiveConf.ConfVars.HIVE_SESSION_IMPL_CLASSNAME));
    HiveSession session = cliService.getSessionManager().getSession(sessionHandle);

    assertEquals(SampleHiveSessionImpl.MAGIC_RETURN_VALUE, session.getNoOperationTime());

    client.closeSession(sessionHandle);
  }

  @Test
  public void testSessionImplWithUGI() throws Exception {
    HiveConf hiveConf = new HiveConf();
    hiveConf.setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
        HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER.getDefaultValue());
    hiveConf.setVar(HiveConf.ConfVars.HIVE_SESSION_IMPL_WITH_UGI_CLASSNAME,
        SampleHiveSessionImplWithUGI.class.getName());
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS, true);

    CLIService cliService = new CLIService(null, true);
    cliService.init(hiveConf);
    ThriftBinaryCLIService service = new ThriftBinaryCLIService(cliService);
    service.init(hiveConf);
    ThriftCLIServiceClient client = new ThriftCLIServiceClient(service);

    SessionHandle sessionHandle = null;
    sessionHandle = client.openSession("tom", "password");
    assertEquals(SampleHiveSessionImplWithUGI.class.getName(),
        service.getHiveConf().getVar(HiveConf.ConfVars.HIVE_SESSION_IMPL_WITH_UGI_CLASSNAME));
    HiveSession session = cliService.getSessionManager().getSession(sessionHandle);

    assertEquals(SampleHiveSessionImplWithUGI.MAGIC_RETURN_VALUE, session.getNoOperationTime());

    client.closeSession(sessionHandle);
  }

  public static class SampleHiveSessionImpl extends HiveSessionImpl {
    public static final int MAGIC_RETURN_VALUE = 0xbeef0001;

    public SampleHiveSessionImpl(SessionHandle sessionHandle, TProtocolVersion protocol,
        String username, String password, HiveConf serverhiveConf, String ipAddress, List<String> forwardAddresses) {
      super(sessionHandle, protocol, username, password, serverhiveConf, ipAddress, forwardAddresses);
    }

    @Override
    public long getNoOperationTime() {
      return MAGIC_RETURN_VALUE;
    }
  }

  public static class SampleHiveSessionImplWithUGI extends HiveSessionImplwithUGI {

    public static final int MAGIC_RETURN_VALUE = 0xbeef0002;

    public SampleHiveSessionImplWithUGI(SessionHandle sessionHandle, TProtocolVersion protocol,
        String username, String password, HiveConf serverhiveConf, String ipAddress,
        String delegationToken, List<String> forwardedAddresses) throws HiveSQLException {
      super(sessionHandle, protocol, username, password, serverhiveConf, ipAddress,
          delegationToken, forwardedAddresses);
    }

    @Override
    public long getNoOperationTime() {
      return MAGIC_RETURN_VALUE;
    }
  }
}
