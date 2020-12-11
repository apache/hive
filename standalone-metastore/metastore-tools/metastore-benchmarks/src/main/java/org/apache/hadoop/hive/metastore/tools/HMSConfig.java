package org.apache.hadoop.hive.metastore.tools;

import static org.apache.hadoop.hive.metastore.tools.Util.getServerUri;

public class HMSConfig {

  private static final HMSConfig instance = new HMSConfig();

  private String host;
  private Integer port;
  private String confDir;

  private HMSConfig(){}

  public static HMSConfig getInstance(){
    return instance;
  }

  public void init(String host, Integer port, String confDir){
    this.host = host;
    this.port = port;
    this.confDir = confDir;
  }

  public HMSClient newClient() throws Exception {
    return new HMSClient(getServerUri(host, String.valueOf(port)), confDir);
  }

}
