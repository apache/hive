package org.apache.hive.service.cli.session;

import junit.framework.Assert;
import junit.framework.TestCase;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.cli.CLIService;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.thrift.TProtocolVersion;
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
