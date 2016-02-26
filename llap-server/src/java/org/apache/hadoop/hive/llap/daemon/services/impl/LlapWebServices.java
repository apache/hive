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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.configuration.LlapDaemonConfiguration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.AbstractService;
import org.apache.hive.http.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LlapWebServices extends AbstractService {

  private static final Logger LOG = LoggerFactory.getLogger(LlapWebServices.class);

  private int port;
  private HttpServer http;
  private boolean useSSL = false;
  private boolean useSPNEGO = false;

  public LlapWebServices() {
    super("LlapWebServices");
  }

  @Override
  public void serviceInit(Configuration conf) {
    this.port = HiveConf.getIntVar(conf, ConfVars.LLAP_DAEMON_WEB_PORT);
    this.useSSL = HiveConf.getBoolVar(conf, ConfVars.LLAP_DAEMON_WEB_SSL);
    this.useSPNEGO = HiveConf.getBoolVar(conf, ConfVars.LLAP_WEB_AUTO_AUTH);
    String bindAddress = "0.0.0.0";
    HttpServer.Builder builder =
        new HttpServer.Builder().setName("llap").setPort(this.port).setHost(bindAddress);
    builder.setConf(new HiveConf(conf, HiveConf.class));
    if (UserGroupInformation.isSecurityEnabled()) {
      LOG.info("LLAP UI useSSL=" + this.useSSL + ", auto-auth/SPNEGO="
          + this.useSPNEGO + ", port=" + this.port);
      builder.setUseSSL(this.useSSL);
      if (this.useSPNEGO) {
        builder.setUseSPNEGO(true); // this setups auth filtering in build()
        builder.setSPNEGOPrincipal(HiveConf.getVar(conf, ConfVars.LLAP_KERBEROS_PRINCIPAL));
        builder.setSPNEGOKeytab(HiveConf.getVar(conf, ConfVars.LLAP_KERBEROS_KEYTAB_FILE));
      }
    }

    try {
      this.http = builder.build();
    } catch (IOException e) {
      LOG.warn("LLAP web service failed to come up", e);
    }
  }

  @Override
  public void serviceStart() throws Exception {
    if (this.http != null) {
      this.http.start();
    }
  }

  public void serviceStop() throws Exception {
    if (this.http != null) {
      this.http.stop();
    }
  }
}
