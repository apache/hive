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
package org.apache.hive.beeline.hs2connection;

import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.ServerUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/*
 * Looks for a hive-site.xml from the classpath. If found this class parses the hive-site.xml
 * to return a set of connection properties which can be used to construct the connection url
 * for Beeline connection
 */
public class HiveSiteHS2ConnectionFileParser implements HS2ConnectionFileParser {
  private Configuration conf;
  private final URL hiveSiteURI;
  private final static String TRUSTSTORE_PASS_PROP = "javax.net.ssl.trustStorePassword";
  private final static String TRUSTSTORE_PROP = "javax.net.ssl.trustStore";
  private static final Logger log = LoggerFactory.getLogger(HiveSiteHS2ConnectionFileParser.class);

  public HiveSiteHS2ConnectionFileParser() {
    hiveSiteURI = HiveConf.getHiveSiteLocation();
    conf = new Configuration();
    if(hiveSiteURI == null) {
      log.debug("hive-site.xml not found for constructing the connection URL");
    } else {
      log.info("Using hive-site.xml at " + hiveSiteURI);
      conf.addResource(hiveSiteURI);
    }
  }

  @VisibleForTesting
  void setHiveConf(HiveConf hiveConf) {
    this.conf = hiveConf;
  }

  @Override
  public Properties getConnectionProperties() throws BeelineHS2ConnectionFileParseException {
    Properties props = new Properties();
    if(!configExists()) {
      return props;
    }
    props.setProperty(HS2ConnectionFileParser.URL_PREFIX_PROPERTY_KEY, "jdbc:hive2://");
    addHosts(props);
    addSSL(props);
    addKerberos(props);
    addHttp(props);
    return props;
  }

  private void addSSL(Properties props) {
    if (!HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_SERVER2_USE_SSL)) {
      return;
    } else {
      props.setProperty("ssl", "true");
    }
    String truststore = System.getenv(TRUSTSTORE_PROP);
    if (truststore != null && truststore.isEmpty()) {
      props.setProperty("sslTruststore", truststore);
    }
    String trustStorePassword = System.getenv(TRUSTSTORE_PASS_PROP);
    if (trustStorePassword != null && !trustStorePassword.isEmpty()) {
      props.setProperty("trustStorePassword", trustStorePassword);
    }
    String saslQop = HiveConf.getVar(conf, ConfVars.HIVE_SERVER2_THRIFT_SASL_QOP);
    if (!"auth".equalsIgnoreCase(saslQop)) {
      props.setProperty("sasl.qop", saslQop);
    }
  }

  private void addKerberos(Properties props) {
    if ("KERBEROS".equals(HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_SERVER2_AUTHENTICATION))) {
      props.setProperty("principal",
          HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_SERVER2_KERBEROS_PRINCIPAL));
    }
  }

  private void addHttp(Properties props) {
    if ("http".equalsIgnoreCase(HiveConf.getVar(conf, ConfVars.HIVE_SERVER2_TRANSPORT_MODE))) {
      props.setProperty("transportMode", "http");
    } else {
      return;
    }
    props.setProperty("httpPath", HiveConf.getVar(conf, ConfVars.HIVE_SERVER2_THRIFT_HTTP_PATH));
  }

  private void addHosts(Properties props) throws BeelineHS2ConnectionFileParseException {
    // if zk HA is enabled get hosts property
    if (HiveConf.getBoolVar(conf,
        HiveConf.ConfVars.HIVE_SERVER2_SUPPORT_DYNAMIC_SERVICE_DISCOVERY)) {
      addZKServiceDiscoveryHosts(props);
    } else {
      addDefaultHS2Hosts(props);
    }
  }

  private void addZKServiceDiscoveryHosts(Properties props)
      throws BeelineHS2ConnectionFileParseException {
    props.setProperty("serviceDiscoveryMode", "zooKeeper");
    props.setProperty("zooKeeperNamespace",
        HiveConf.getVar(conf, ConfVars.HIVE_SERVER2_ZOOKEEPER_NAMESPACE));
    props.setProperty("hosts", HiveConf.getVar(conf, ConfVars.HIVE_ZOOKEEPER_QUORUM));
  }

  private void addDefaultHS2Hosts(Properties props) throws BeelineHS2ConnectionFileParseException {
    String hiveHost = System.getenv("HIVE_SERVER2_THRIFT_BIND_HOST");
    if (hiveHost == null) {
      hiveHost = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST);
    }

    InetAddress serverIPAddress;
    try {
      serverIPAddress = ServerUtils.getHostAddress(hiveHost);
    } catch (UnknownHostException e) {
      throw new BeelineHS2ConnectionFileParseException(e.getMessage(), e);
    }
    int portNum = getPortNum(
        "http".equalsIgnoreCase(HiveConf.getVar(conf, ConfVars.HIVE_SERVER2_TRANSPORT_MODE)));
    props.setProperty("hosts", serverIPAddress.getHostName() + ":" + portNum);
  }

  private int getPortNum(boolean isHttp) {
    String portString;
    int portNum;
    if (isHttp) {
      portString = System.getenv("HIVE_SERVER2_THRIFT_HTTP_PORT");
      if (portString != null) {
        portNum = Integer.parseInt(portString);
      } else {
        portNum = HiveConf.getIntVar(conf, ConfVars.HIVE_SERVER2_THRIFT_HTTP_PORT);
      }
    } else {
      portString = System.getenv("HIVE_SERVER2_THRIFT_PORT");
      if (portString != null) {
        portNum = Integer.parseInt(portString);
      } else {
        portNum = HiveConf.getIntVar(conf, ConfVars.HIVE_SERVER2_THRIFT_PORT);
      }
    }
    return portNum;
  }

  @Override
  public boolean configExists() {
    return (hiveSiteURI != null);
  }
}
