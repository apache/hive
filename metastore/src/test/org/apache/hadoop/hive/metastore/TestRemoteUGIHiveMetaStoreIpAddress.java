package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.hive.conf.HiveConf.ConfVars;

public class TestRemoteUGIHiveMetaStoreIpAddress extends TestRemoteHiveMetaStoreIpAddress {
  public TestRemoteUGIHiveMetaStoreIpAddress() {
    super();
    System.setProperty(ConfVars.METASTORE_EXECUTE_SET_UGI.varname, "true");
  }

}
