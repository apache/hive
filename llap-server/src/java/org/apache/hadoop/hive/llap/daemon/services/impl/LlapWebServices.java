package org.apache.hadoop.hive.llap.daemon.services.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.llap.configuration.LlapConfiguration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebApps;

public class LlapWebServices extends AbstractService {


  private int port;
  private boolean ssl;
  private Configuration conf;
  private WebApp webApp;
  private LlapWebApp webAppInstance;

  public LlapWebServices() {
    super("LlapWebServices");
  }

  @Override
  public void serviceInit(Configuration conf) {

    this.conf = new Configuration(conf);
    this.conf.addResource(YarnConfiguration.YARN_SITE_CONFIGURATION_FILE);

    this.port = conf.getInt(LlapConfiguration.LLAP_DAEMON_SERVICE_PORT,
            LlapConfiguration.LLAP_DAEMON_SERVICE_PORT_DEFAULT);
    this.ssl = conf.getBoolean(LlapConfiguration.LLAP_DAEMON_SERVICE_SSL,
            LlapConfiguration.LLAP_DAEMON_SERVICE_SSL_DEFAULT);

    this.webAppInstance = new LlapWebApp();
  }

  @Override
  public void serviceStart() throws Exception {
    String bindAddress = "0.0.0.0";
    this.webApp =
        WebApps.$for("llap").at(bindAddress).at(port).with(getConfig())
        /* TODO: security negotiation here */
            .start();
  }

  public void serviceStop() throws Exception {
    if (this.webApp != null) {
      this.webApp.stop();
    }
  }
}
