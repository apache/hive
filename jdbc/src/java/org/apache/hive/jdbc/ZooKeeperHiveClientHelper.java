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

package org.apache.hive.jdbc;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.jdbc.Utils.JdbcConnectionParams;
import org.apache.hive.service.server.HS2ActivePassiveHARegistry;
import org.apache.hive.service.server.HS2ActivePassiveHARegistryClient;
import org.apache.hive.service.server.HiveServer2Instance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;

class ZooKeeperHiveClientHelper {
  static final Logger LOG = LoggerFactory.getLogger(ZooKeeperHiveClientHelper.class.getName());
  // Pattern for key1=value1;key2=value2
  private static final Pattern kvPattern = Pattern.compile("([^=;]*)=([^;]*)[;]?");

  private static String getZooKeeperNamespace(JdbcConnectionParams connParams) {
    String zooKeeperNamespace = connParams.getSessionVars().get(JdbcConnectionParams.ZOOKEEPER_NAMESPACE);
    if ((zooKeeperNamespace == null) || (zooKeeperNamespace.isEmpty())) {
      // if active passive HA enabled, use default HA namespace
      if (isZkHADynamicDiscoveryMode(connParams.getSessionVars())) {
        zooKeeperNamespace = JdbcConnectionParams.ZOOKEEPER_ACTIVE_PASSIVE_HA_DEFAULT_NAMESPACE;
      } else {
        zooKeeperNamespace = JdbcConnectionParams.ZOOKEEPER_DEFAULT_NAMESPACE;
      }
    }
    return zooKeeperNamespace;
  }

  /**
   * Returns true is only if HA service discovery mode is enabled
   *
   * @param sessionConf - session configuration
   * @return true if serviceDiscoveryMode=zooKeeperHA is specified in JDBC URI
   */
  public static boolean isZkHADynamicDiscoveryMode(Map<String, String> sessionConf) {
    final String discoveryMode = sessionConf.get(JdbcConnectionParams.SERVICE_DISCOVERY_MODE);
    return (discoveryMode != null) &&
      JdbcConnectionParams.SERVICE_DISCOVERY_MODE_ZOOKEEPER_HA.equalsIgnoreCase(discoveryMode);
  }

  /**
   * Returns true is any service discovery mode is enabled (HA or non-HA)
   *
   * @param sessionConf - session configuration
   * @return true if serviceDiscoveryMode is specified in JDBC URI
   */
  public static boolean isZkDynamicDiscoveryMode(Map<String, String> sessionConf) {
    final String discoveryMode = sessionConf.get(JdbcConnectionParams.SERVICE_DISCOVERY_MODE);
    return (discoveryMode != null)
      && (JdbcConnectionParams.SERVICE_DISCOVERY_MODE_ZOOKEEPER.equalsIgnoreCase(discoveryMode) ||
      JdbcConnectionParams.SERVICE_DISCOVERY_MODE_ZOOKEEPER_HA.equalsIgnoreCase(discoveryMode));
  }

  private static CuratorFramework getZkClient(JdbcConnectionParams connParams) throws Exception {
    String zooKeeperEnsemble = connParams.getZooKeeperEnsemble();
    CuratorFramework zooKeeperClient =
        CuratorFrameworkFactory.builder().connectString(zooKeeperEnsemble)
            .retryPolicy(new ExponentialBackoffRetry(1000, 3)).build();
    zooKeeperClient.start();
    return zooKeeperClient;
  }

  private static List<String> getServerHosts(JdbcConnectionParams connParams, CuratorFramework
      zooKeeperClient) throws Exception {
    List<String> serverHosts = zooKeeperClient.getChildren().forPath("/" + getZooKeeperNamespace(connParams));
    // Remove the znodes we've already tried from this list
    serverHosts.removeAll(connParams.getRejectedHostZnodePaths());
    if (serverHosts.isEmpty()) {
      throw new ZooKeeperHiveClientException(
          "Tried all existing HiveServer2 uris from ZooKeeper.");
    }
    return serverHosts;
  }

  private static void updateParamsWithZKServerNode(JdbcConnectionParams connParams,
      CuratorFramework zooKeeperClient, String serverNode) throws Exception {
    String zooKeeperNamespace = getZooKeeperNamespace(connParams);
    connParams.setCurrentHostZnodePath(serverNode);
    // Read data from the znode for this server node
    // This data could be either config string (new releases) or server end
    // point (old releases)
    String dataStr =
        new String(
            zooKeeperClient.getData().forPath("/" + zooKeeperNamespace + "/" + serverNode),
            Charset.forName("UTF-8"));
    // If dataStr is not null and dataStr is not a KV pattern,
    // it must be the server uri added by an older version HS2
    Matcher matcher = kvPattern.matcher(dataStr);
    if ((dataStr != null) && (!matcher.find())) {
      String[] split = dataStr.split(":");
      if (split.length != 2) {
        throw new ZooKeeperHiveClientException("Unable to read HiveServer2 uri from ZooKeeper: "
            + dataStr);
      }
      connParams.setHost(split[0]);
      connParams.setPort(Integer.parseInt(split[1]));
    } else {
      applyConfs(dataStr, connParams);
    }
  }

  static void configureConnParams(JdbcConnectionParams connParams) throws ZooKeeperHiveClientException {
    if (isZkHADynamicDiscoveryMode(connParams.getSessionVars())) {
      configureConnParamsHA(connParams);
    } else {
      CuratorFramework zooKeeperClient = null;
      try {
        zooKeeperClient = getZkClient(connParams);
        List<String> serverHosts = getServerHosts(connParams, zooKeeperClient);
        // Now pick a server node randomly
        String serverNode = serverHosts.get(new Random().nextInt(serverHosts.size()));
        updateParamsWithZKServerNode(connParams, zooKeeperClient, serverNode);
      } catch (Exception e) {
        throw new ZooKeeperHiveClientException("Unable to read HiveServer2 configs from ZooKeeper", e);
      } finally {
        // Close the client connection with ZooKeeper
        if (zooKeeperClient != null) {
          zooKeeperClient.close();
        }
      }
    }
  }

  private static void configureConnParamsHA(JdbcConnectionParams connParams) throws ZooKeeperHiveClientException {
    try {
      Configuration registryConf = new Configuration();
      registryConf.set(HiveConf.ConfVars.HIVE_ZOOKEEPER_QUORUM.varname, connParams.getZooKeeperEnsemble());
      registryConf.set(HiveConf.ConfVars.HIVE_SERVER2_ACTIVE_PASSIVE_HA_REGISTRY_NAMESPACE.varname,
        getZooKeeperNamespace(connParams));
      HS2ActivePassiveHARegistry haRegistryClient = HS2ActivePassiveHARegistryClient.getClient(registryConf);
      boolean foundLeader = false;
      String maxRetriesConf = connParams.getSessionVars().get(JdbcConnectionParams.RETRIES);
      final int maxRetries = StringUtils.isEmpty(maxRetriesConf) ? 5 : Integer.parseInt(maxRetriesConf);
      int retries = 0;
      int sleepMs = 1000;
      while (!foundLeader && retries < maxRetries) {
        for (HiveServer2Instance hiveServer2Instance : haRegistryClient.getAll()) {
          if (hiveServer2Instance.isLeader()) {
            foundLeader = true;
            connParams.setHost(hiveServer2Instance.getHost());
            connParams.setPort(hiveServer2Instance.getRpcPort());
            final String mode = hiveServer2Instance.getTransportMode().equals("http") ? "http:/" + hiveServer2Instance
              .getHttpEndpoint() : hiveServer2Instance.getTransportMode();
            LOG.info("Found HS2 Active Host: {} Port: {} Identity: {} Mode: {}", hiveServer2Instance.getHost(),
              hiveServer2Instance.getRpcPort(), hiveServer2Instance.getWorkerIdentity(), mode);
            // configurations are always published to ServiceRecord. Read/apply configs to JDBC connection params
            String serverConfStr = Joiner.on(';').withKeyValueSeparator("=").join(hiveServer2Instance.getProperties());
            if (LOG.isDebugEnabled()) {
              LOG.debug("Configurations applied to JDBC connection params. {}", hiveServer2Instance.getProperties());
            }
            applyConfs(serverConfStr, connParams);
            break;
          }
        }
        if (!foundLeader) {
          LOG.warn("Unable to connect to HS2 Active Host (No Leader Found!). Retrying after {} ms. retries: {}",
            sleepMs, retries);
          Thread.sleep(sleepMs);
          retries++;
        }
      }
      if (!foundLeader) {
        throw new ZooKeeperHiveClientException("Unable to connect to HiveServer2 Active host (No leader found!) after" +
          " " + maxRetries + " retries.");
      }
    } catch (Exception e) {
      throw new ZooKeeperHiveClientException("Unable to read HiveServer2 configs from ZooKeeper", e);
    }
  }

  static List<JdbcConnectionParams> getDirectParamsList(JdbcConnectionParams connParams)
      throws ZooKeeperHiveClientException {
    CuratorFramework zooKeeperClient = null;
    try {
      zooKeeperClient = getZkClient(connParams);
      List<String> serverHosts = getServerHosts(connParams, zooKeeperClient);
      final List<JdbcConnectionParams> directParamsList = new ArrayList<>();
      // For each node
      for (String serverNode : serverHosts) {
        JdbcConnectionParams directConnParams = new JdbcConnectionParams(connParams);
        directParamsList.add(directConnParams);
        updateParamsWithZKServerNode(directConnParams, zooKeeperClient, serverNode);
      }
      return directParamsList;
    } catch (Exception e) {
      throw new ZooKeeperHiveClientException("Unable to read HiveServer2 configs from ZooKeeper", e);
    } finally {
      // Close the client connection with ZooKeeper
      if (zooKeeperClient != null) {
        zooKeeperClient.close();
      }
    }
  }

  /**
   * Apply configs published by the server. Configs specified from client's JDBC URI override
   * configs published by the server.
   *
   * @param serverConfStr
   * @param connParams
   * @throws Exception
   */
  private static void applyConfs(String serverConfStr, JdbcConnectionParams connParams)
      throws Exception {
    Matcher matcher = kvPattern.matcher(serverConfStr);
    while (matcher.find()) {
      // Have to use this if-else since switch-case on String is supported Java 7 onwards
      if ((matcher.group(1) != null)) {
        if ((matcher.group(2) == null)) {
          throw new Exception("Null config value for: " + matcher.group(1)
              + " published by the server.");
        }
        // Set host
        if (matcher.group(1).equals("hive.server2.thrift.bind.host")) {
          connParams.setHost(matcher.group(2));
        }
        // Set transportMode
        if ((matcher.group(1).equals("hive.server2.transport.mode"))
            && !(connParams.getSessionVars().containsKey(JdbcConnectionParams.TRANSPORT_MODE))) {
          connParams.getSessionVars().put(JdbcConnectionParams.TRANSPORT_MODE, matcher.group(2));
        }
        // Set port
        if (matcher.group(1).equals("hive.server2.thrift.port")) {
          connParams.setPort(Integer.parseInt(matcher.group(2)));
        }
        if ((matcher.group(1).equals("hive.server2.thrift.http.port"))
            && !(connParams.getPort() > 0)) {
          connParams.setPort(Integer.parseInt(matcher.group(2)));
        }
        // Set sasl qop
        if ((matcher.group(1).equals("hive.server2.thrift.sasl.qop"))
            && !(connParams.getSessionVars().containsKey(JdbcConnectionParams.AUTH_QOP))) {
          connParams.getSessionVars().put(JdbcConnectionParams.AUTH_QOP, matcher.group(2));
        }
        // Set http path
        if ((matcher.group(1).equals("hive.server2.thrift.http.path"))
            && !(connParams.getSessionVars().containsKey(JdbcConnectionParams.HTTP_PATH))) {
          connParams.getSessionVars().put(JdbcConnectionParams.HTTP_PATH, matcher.group(2));
        }
        // Set SSL
        if ((matcher.group(1) != null) && (matcher.group(1).equals("hive.server2.use.SSL"))
            && !(connParams.getSessionVars().containsKey(JdbcConnectionParams.USE_SSL))) {
          connParams.getSessionVars().put(JdbcConnectionParams.USE_SSL, matcher.group(2));
        }
        /**
         * Note: this is pretty messy, but sticking to the current implementation.
         * Set authentication configs. Note that in JDBC driver, we have 3 auth modes: NOSASL,
         * Kerberos (including delegation token mechanism) and password based.
         * The use of JdbcConnectionParams.AUTH_TYPE==JdbcConnectionParams.AUTH_SIMPLE picks NOSASL.
         * The presence of JdbcConnectionParams.AUTH_PRINCIPAL==<principal> picks Kerberos.
         * If principal is absent, the presence of
         * JdbcConnectionParams.AUTH_TYPE==JdbcConnectionParams.AUTH_TOKEN uses delegation token.
         * Otherwise password based (which includes NONE, PAM, LDAP, CUSTOM)
         */
        if (matcher.group(1).equals("hive.server2.authentication")) {
          // NOSASL
          if (matcher.group(2).equalsIgnoreCase("NOSASL")
              && !(connParams.getSessionVars().containsKey(JdbcConnectionParams.AUTH_TYPE) && connParams
                  .getSessionVars().get(JdbcConnectionParams.AUTH_TYPE)
                  .equalsIgnoreCase(JdbcConnectionParams.AUTH_SIMPLE))) {
            connParams.getSessionVars().put(JdbcConnectionParams.AUTH_TYPE,
                JdbcConnectionParams.AUTH_SIMPLE);
          }
        }
        // KERBEROS
        // If delegation token is passed from the client side, do not set the principal
        if (matcher.group(1).equalsIgnoreCase("hive.server2.authentication.kerberos.principal")
            && !(connParams.getSessionVars().containsKey(JdbcConnectionParams.AUTH_TYPE) && connParams
                .getSessionVars().get(JdbcConnectionParams.AUTH_TYPE)
                .equalsIgnoreCase(JdbcConnectionParams.AUTH_TOKEN))
            && !(connParams.getSessionVars().containsKey(JdbcConnectionParams.AUTH_PRINCIPAL))) {
          connParams.getSessionVars().put(JdbcConnectionParams.AUTH_PRINCIPAL, matcher.group(2));
        }
      }
    }
  }
}
