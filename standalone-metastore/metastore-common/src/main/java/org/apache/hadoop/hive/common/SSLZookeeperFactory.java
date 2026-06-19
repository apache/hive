/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.common;

import org.apache.commons.lang3.StringUtils;
import org.apache.curator.utils.ZookeeperFactory;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.client.ZKClientConfig;
import org.apache.zookeeper.common.ClientX509Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.KeyStore;

/**
 * Factory to create Zookeeper clients with the zookeeper.client.secure enabled,
 * allowing SSL communication with the Zookeeper server.
 */
public class SSLZookeeperFactory implements ZookeeperFactory {

  private static final Logger LOG = LoggerFactory.getLogger(SSLZookeeperFactory.class);

  private boolean sslEnabled;
  private String keyStoreLocation;
  private String keyStorePassword;
  private String keyStoreType;
  private String trustStoreLocation;
  private String trustStorePassword;
  private String trustStoreType;

  public SSLZookeeperFactory(boolean sslEnabled, String keyStoreLocation, String keyStorePassword,
      String keyStoreType, String trustStoreLocation, String trustStorePassword, String trustStoreType) {

    this.sslEnabled = sslEnabled;
    this.keyStoreLocation = keyStoreLocation;
    this.keyStorePassword = keyStorePassword;
    this.keyStoreType = (!StringUtils.isBlank(keyStoreType)) ? keyStoreType : KeyStore.getDefaultType();
    this.trustStoreLocation = trustStoreLocation;
    this.trustStorePassword = trustStorePassword;
    this.trustStoreType = (!StringUtils.isBlank(trustStoreType)) ? trustStoreType : KeyStore.getDefaultType();
    if (sslEnabled) {
      if (StringUtils.isBlank(keyStoreLocation)) {
        LOG.warn("Missing keystoreLocation parameter");
      }
      if (StringUtils.isBlank(trustStoreLocation)) {
        LOG.warn("Missing trustStoreLocation parameter");
      }
    }
  }

  @Override
  public ZooKeeper newZooKeeper(String connectString, int sessionTimeout, Watcher watcher,
      boolean canBeReadOnly) throws Exception {
    if (!this.sslEnabled) {
      return new ZooKeeper(connectString, sessionTimeout, watcher, canBeReadOnly);
    }
    ZKClientConfig clientConfig = new ZKClientConfig();
    clientConfig.setProperty(ZKClientConfig.SECURE_CLIENT, "true");
    clientConfig.setProperty(ZKClientConfig.ZOOKEEPER_CLIENT_CNXN_SOCKET, "org.apache.zookeeper.ClientCnxnSocketNetty");
    ClientX509Util x509Util = new ClientX509Util();
    clientConfig.setProperty(x509Util.getSslKeystoreLocationProperty(), this.keyStoreLocation);
    clientConfig.setProperty(x509Util.getSslKeystorePasswdProperty(), this.keyStorePassword);
    clientConfig.setProperty(x509Util.getSslKeystoreTypeProperty(), this.keyStoreType);
    clientConfig.setProperty(x509Util.getSslTruststoreLocationProperty(), this.trustStoreLocation);
    clientConfig.setProperty(x509Util.getSslTruststorePasswdProperty(), this.trustStorePassword);
    clientConfig.setProperty(x509Util.getSslTruststoreTypeProperty(), this.trustStoreType);
    return new ZooKeeper(connectString, sessionTimeout, watcher, canBeReadOnly, clientConfig);
  }
}
