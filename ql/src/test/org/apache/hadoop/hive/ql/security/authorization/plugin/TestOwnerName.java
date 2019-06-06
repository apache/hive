package org.apache.hadoop.hive.ql.security.authorization.plugin;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Assert;
import org.junit.Test;

public class TestOwnerName {
  private static final String authorizedUser = "sam";

  @Test public void testDBAndTableOwner() throws Exception {
    UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser(authorizedUser));
    try {
      HiveConf conf = new HiveConf(Driver.class);
      HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS, false);
      HiveConf.setVar(conf, HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER, DummyHiveAuthorizerFactory.class.getName());
      HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_AUTHORIZATION_ENABLED, true);

      SessionState.start(conf);
      Driver driver = new Driver(conf);
      int errorcode = driver.compile("create table default.t1(name string)");
      Assert.assertEquals("Owner Name not present", 0, errorcode);
    } catch (Exception e) {
      throw e;
    }
  }
}
