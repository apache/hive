package org.apache.hadoop.hive.llap.daemon.services.impl;

import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.YarnWebParams;

public class LlapWebApp extends WebApp {

  @Override
  public void setup() {
    // JMX / config are defaults
  }
}
