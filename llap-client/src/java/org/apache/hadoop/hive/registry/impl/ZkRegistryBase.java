/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.hive.registry.impl;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.ZooKeeperHiveHelper;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.LlapUtil;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.utils.SecurityUtils;
import org.apache.hadoop.hive.registry.ServiceInstance;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hive-specific ZkRegistryBase that extends the base implementation from metastore-common.
 * This version uses HiveConf for ZooKeeper configuration to maintain compatibility with Hive components.
 * 
 * For HMS components, use the base class directly from metastore-common.
 */
public abstract class ZkRegistryBase<InstanceType extends ServiceInstance> 
    extends org.apache.hadoop.hive.metastore.registry.impl.ZkRegistryBase<InstanceType> {
  private static final Logger LOG = LoggerFactory.getLogger(ZkRegistryBase.class);

  /**
   * @param rootNs A single root namespace override. Not recommended.
   * @param nsPrefix The namespace prefix to use with default namespaces (appends 'sasl' for secure else 'unsecure'
   *                 to namespace prefix to get effective root namespace).
   * @param userScopePathPrefix The prefix to use for the user-specific part of the path.
   * @param workerPrefix The prefix to use for each worker znode.
   * @param workerGroup group name to use for all workers
   * @param zkSaslLoginContextName SASL login context name for ZK security; null if not needed.
   * @param zkPrincipal ZK security principal.
   * @param zkKeytab ZK security keytab.
   * @param aclsConfig A config setting to use to determine if ACLs should be verified.
   */
  public ZkRegistryBase(String instanceName, Configuration conf, String rootNs, String nsPrefix,
      String userScopePathPrefix, String workerPrefix, String workerGroup,
      String zkSaslLoginContextName, String zkPrincipal, String zkKeytab, ConfVars aclsConfig) {
    super(instanceName, conf, rootNs, nsPrefix, userScopePathPrefix, workerPrefix, workerGroup,
        zkSaslLoginContextName, zkPrincipal, zkKeytab, 
        aclsConfig != null ? MetastoreConf.ConfVars.THRIFT_ZOOKEEPER_USE_KERBEROS : null);
  }

  /**
   * Override to use HiveConf for ZooKeeper client configuration.
   */
  @Override
  protected CuratorFramework getZookeeperClient(Configuration conf, String namespace, ACLProvider zooKeeperAclProvider) {
    String keyStorePassword = "";
    String trustStorePassword = "";
    if (HiveConf.getBoolVar(conf, ConfVars.HIVE_ZOOKEEPER_SSL_ENABLE)) {
      try {
        keyStorePassword =
            ShimLoader.getHadoopShims().getPassword(conf, ConfVars.HIVE_ZOOKEEPER_SSL_KEYSTORE_PASSWORD.varname);
        trustStorePassword =
            ShimLoader.getHadoopShims().getPassword(conf, ConfVars.HIVE_ZOOKEEPER_SSL_TRUSTSTORE_PASSWORD.varname);
      } catch (Exception e) {
        throw new RuntimeException("Failed to read zookeeper conf passwords", e);
      }
    }
    return ZooKeeperHiveHelper.builder()
        .quorum(conf.get(ConfVars.HIVE_ZOOKEEPER_QUORUM.varname))
        .clientPort(conf.get(ConfVars.HIVE_ZOOKEEPER_CLIENT_PORT.varname,
            ConfVars.HIVE_ZOOKEEPER_CLIENT_PORT.getDefaultValue()))
        .connectionTimeout(
            (int) HiveConf.getTimeVar(conf, ConfVars.HIVE_ZOOKEEPER_CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS))
        .sessionTimeout(
            (int) HiveConf.getTimeVar(conf, ConfVars.HIVE_ZOOKEEPER_SESSION_TIMEOUT, TimeUnit.MILLISECONDS))
        .baseSleepTime(
            (int) HiveConf.getTimeVar(conf, ConfVars.HIVE_ZOOKEEPER_CONNECTION_BASESLEEPTIME, TimeUnit.MILLISECONDS))
        .maxRetries(HiveConf.getIntVar(conf, ConfVars.HIVE_ZOOKEEPER_CONNECTION_MAX_RETRIES))
        .sslEnabled(HiveConf.getBoolVar(conf, ConfVars.HIVE_ZOOKEEPER_SSL_ENABLE))
        .keyStoreLocation(HiveConf.getVar(conf, ConfVars.HIVE_ZOOKEEPER_SSL_KEYSTORE_LOCATION))
        .keyStorePassword(keyStorePassword)
        .keyStoreType(HiveConf.getVar(conf, ConfVars.HIVE_ZOOKEEPER_SSL_KEYSTORE_TYPE))
        .trustStoreLocation(HiveConf.getVar(conf, ConfVars.HIVE_ZOOKEEPER_SSL_TRUSTSTORE_LOCATION))
        .trustStorePassword(trustStorePassword)
        .trustStoreType(HiveConf.getVar(conf, ConfVars.HIVE_ZOOKEEPER_SSL_TRUSTSTORE_TYPE))
        .build().getNewZookeeperClient(zooKeeperAclProvider, namespace);
  }

  /**
   * Override to use HiveConf for checking ZooKeeper SASL enforcement.
   */
  @Override
  protected boolean isZkEnforceSASLClient(Configuration conf) {
    return UserGroupInformation.isSecurityEnabled() &&
        HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_ZOOKEEPER_USE_KERBEROS);
  }

  /**
   * Override to use LlapUtil for extracting username from principal.
   */
  @Override
  public void start() throws IOException {
    if (zooKeeperClient != null) {
      if (isZkEnforceSASLClient(conf)) {
        if (saslLoginContextName != null) {
          SecurityUtils.setZookeeperClientKerberosJaasConfig(zkPrincipal, zkKeytab, saslLoginContextName);
        }
        if (zkPrincipal != null) {
          // Use LlapUtil for Hive components
          userNameFromPrincipal = LlapUtil.getUserNameFromPrincipal(zkPrincipal);
        }
      }
      zooKeeperClient.start();
    }
  }
}
