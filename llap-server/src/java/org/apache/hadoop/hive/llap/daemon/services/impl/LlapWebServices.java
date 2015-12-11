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
package org.apache.hadoop.hive.llap.daemon.services.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.http.HttpConfig.Policy;
import org.apache.hadoop.security.AuthenticationFilterInitializer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebApps;
import org.apache.hadoop.yarn.webapp.WebApps.Builder;

public class LlapWebServices extends AbstractService {


  private int port;
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

    this.port = HiveConf.getIntVar(conf, ConfVars.LLAP_DAEMON_WEB_PORT);

    this.webAppInstance = new LlapWebApp();
  }

  @Override
  public void serviceStart() throws Exception {
    String bindAddress = "0.0.0.0";
    Configuration conf = getConfig();
    if (UserGroupInformation.isSecurityEnabled()
        && HiveConf.getBoolVar(conf, ConfVars.LLAP_WEB_AUTO_AUTH)) {
      conf.set("hadoop.http.authentication.type", "kerberos");
      conf.set("hadoop.http.authentication.kerberos.principal",
          HiveConf.getVar(conf, ConfVars.LLAP_KERBEROS_PRINCIPAL));
      conf.set("hadoop.http.authentication.kerberos.keytab",
          HiveConf.getVar(conf, ConfVars.LLAP_KERBEROS_KEYTAB_FILE));
      String authFilterName = AuthenticationFilterInitializer.class.getName();
      String initializers = conf.getTrimmed("hadoop.http.filter.initializers");
      if (initializers == null || initializers.isEmpty()) {
        initializers = authFilterName;
      } else if (!initializers.contains(authFilterName)) {
        initializers = authFilterName + "," + initializers;
      }
      conf.set("hadoop.http.filter.initializers", initializers);
    }
    Builder<Object> webAppBuilder =
        WebApps.$for("llap").at(bindAddress).at(port).with(conf);
    if (UserGroupInformation.isSecurityEnabled()) {
      webAppBuilder.withHttpPolicy(conf, Policy.HTTPS_ONLY);
    }
    this.webApp = webAppBuilder.start();
  }

  public void serviceStop() throws Exception {
    if (this.webApp != null) {
      this.webApp.stop();
    }
  }
}
