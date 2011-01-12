package org.apache.hadoop.hive.ql.security;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.HiveException;

public class DummyAuthenticator implements HiveAuthenticationProvider {
  
  private List<String> groupNames;
  private String userName;
  private Configuration conf;
  
  public DummyAuthenticator() {
    this.groupNames = new ArrayList<String>();
    groupNames.add("hive_test_group1");
    groupNames.add("hive_test_group2");
    userName = "hive_test_user";
  }

  @Override
  public void destroy() throws HiveException{
    return;
  }

  @Override
  public List<String> getGroupNames() {
    return groupNames;
  }

  @Override
  public String getUserName() {
    return userName;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }
  
  public Configuration getConf() {
    return this.conf;
  }

}
