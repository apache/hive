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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.security.auth.login.AppConfigurationEntry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.apache.zookeeper.client.ZooKeeperSaslClient;

public class ZookeeperUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ZookeeperUtils.class);

  public static String setupZookeeperAuth(Configuration conf, String saslLoginContextName,
      String zkPrincipal, String zkKeytab) throws IOException {
    // If the login context name is not set, we are in the client and don't need auth.
    if (UserGroupInformation.isSecurityEnabled() && saslLoginContextName != null) {
      LOG.info("UGI security is enabled. Setting up ZK auth.");

      if (zkPrincipal == null || zkPrincipal.isEmpty()) {
        throw new IOException("Kerberos principal is empty");
      }

      if (zkKeytab == null || zkKeytab.isEmpty()) {
        throw new IOException("Kerberos keytab is empty");
      }

      // Install the JAAS Configuration for the runtime
      return setZookeeperClientKerberosJaasConfig(saslLoginContextName, zkPrincipal, zkKeytab);
    } else {
      LOG.info("UGI security is not enabled, or no SASL context name. " +
          "Skipping setting up ZK auth.");
      return null;
    }
  }

  /**
   * Dynamically sets up the JAAS configuration that uses kerberos
   *
   * @param principal
   * @param keyTabFile
   * @throws IOException
   */
  private static String setZookeeperClientKerberosJaasConfig(
      String saslLoginContextName, String zkPrincipal, String zkKeytab) throws IOException {
    // ZooKeeper property name to pick the correct JAAS conf section
    System.setProperty(ZooKeeperSaslClient.LOGIN_CONTEXT_NAME_KEY, saslLoginContextName);

    String principal = SecurityUtil.getServerPrincipal(zkPrincipal, "0.0.0.0");
    JaasConfiguration jaasConf = new JaasConfiguration(
        saslLoginContextName, principal, zkKeytab);

    // Install the Configuration in the runtime.
    javax.security.auth.login.Configuration.setConfiguration(jaasConf);
    return principal;
  }

  /**
   * A JAAS configuration for ZooKeeper clients intended to use for SASL
   * Kerberos.
   */
  private static class JaasConfiguration extends javax.security.auth.login.Configuration {
    // Current installed Configuration
    private final javax.security.auth.login.Configuration baseConfig = javax.security.auth.login.Configuration
        .getConfiguration();
    private final String loginContextName;
    private final String principal;
    private final String keyTabFile;

    public JaasConfiguration(String loginContextName, String principal, String keyTabFile) {
      this.loginContextName = loginContextName;
      this.principal = principal;
      this.keyTabFile = keyTabFile;
    }

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String appName) {
      if (loginContextName.equals(appName)) {
        Map<String, String> krbOptions = new HashMap<String, String>();
        krbOptions.put("doNotPrompt", "true");
        krbOptions.put("storeKey", "true");
        krbOptions.put("useKeyTab", "true");
        krbOptions.put("principal", principal);
        krbOptions.put("keyTab", keyTabFile);
        krbOptions.put("refreshKrb5Config", "true");
        AppConfigurationEntry zooKeeperClientEntry = new AppConfigurationEntry(
            KerberosUtil.getKrb5LoginModuleName(),
            AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, krbOptions);
        return new AppConfigurationEntry[] { zooKeeperClientEntry };
      }
      // Try the base config
      if (baseConfig != null) {
        return baseConfig.getAppConfigurationEntry(appName);
      }
      return null;
    }
  }
}
