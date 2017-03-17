/**
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

package org.apache.hadoop.hive.shims;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.LoginException;
import javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag;

import org.apache.hadoop.hive.thrift.DelegationTokenIdentifier;
import org.apache.hadoop.hive.thrift.DelegationTokenSelector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenSelector;
import org.apache.zookeeper.client.ZooKeeperSaslClient;

public class Utils {

  private static final boolean IBM_JAVA = System.getProperty("java.vendor")
      .contains("IBM");

  public static UserGroupInformation getUGI() throws LoginException, IOException {
    String doAs = System.getenv("HADOOP_USER_NAME");
    if(doAs != null && doAs.length() > 0) {
     /*
      * this allows doAs (proxy user) to be passed along across process boundary where
      * delegation tokens are not supported.  For example, a DDL stmt via WebHCat with
      * a doAs parameter, forks to 'hcat' which needs to start a Session that
      * proxies the end user
      */
      return UserGroupInformation.createProxyUser(doAs, UserGroupInformation.getLoginUser());
    }
    return UserGroupInformation.getCurrentUser();
  }

  /**
   * Get the string form of the token given a token signature.
   * The signature is used as the value of the "service" field in the token for lookup.
   * Ref: AbstractDelegationTokenSelector in Hadoop. If there exists such a token
   * in the token cache (credential store) of the job, the lookup returns that.
   * This is relevant only when running against a "secure" hadoop release
   * The method gets hold of the tokens if they are set up by hadoop - this should
   * happen on the map/reduce tasks if the client added the tokens into hadoop's
   * credential store in the front end during job submission. The method will
   * select the hive delegation token among the set of tokens and return the string
   * form of it
   * @param tokenSignature
   * @return the string form of the token found
   * @throws IOException
   */
  public static String getTokenStrForm(String tokenSignature) throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    TokenSelector<? extends TokenIdentifier> tokenSelector = new DelegationTokenSelector();

    Token<? extends TokenIdentifier> token = tokenSelector.selectToken(
        tokenSignature == null ? new Text() : new Text(tokenSignature), ugi.getTokens());
    return token != null ? token.encodeToUrlString() : null;
  }

  /**
   * Create a delegation token object for the given token string and service.
   * Add the token to given UGI
   * @param ugi
   * @param tokenStr
   * @param tokenService
   * @throws IOException
   */
  public static void setTokenStr(UserGroupInformation ugi, String tokenStr, String tokenService)
      throws IOException {
    Token<DelegationTokenIdentifier> delegationToken = createToken(tokenStr, tokenService);
    ugi.addToken(delegationToken);
  }

  /**
   * Add a given service to delegation token string.
   * @param tokenStr
   * @param tokenService
   * @return
   * @throws IOException
   */
  public static String addServiceToToken(String tokenStr, String tokenService)
      throws IOException {
    Token<DelegationTokenIdentifier> delegationToken = createToken(tokenStr, tokenService);
    return delegationToken.encodeToUrlString();
  }

  /**
   * Create a new token using the given string and service
   * @param tokenStr
   * @param tokenService
   * @return
   * @throws IOException
   */
  private static Token<DelegationTokenIdentifier> createToken(String tokenStr, String tokenService)
      throws IOException {
    Token<DelegationTokenIdentifier> delegationToken = new Token<DelegationTokenIdentifier>();
    delegationToken.decodeFromUrlString(tokenStr);
    delegationToken.setService(new Text(tokenService));
    return delegationToken;
  }

  /**
   * Dynamically sets up the JAAS configuration that uses kerberos
   * @param principal
   * @param keyTabFile
   * @throws IOException
   */
  public static void setZookeeperClientKerberosJaasConfig(String principal, String keyTabFile) throws IOException {
    // ZooKeeper property name to pick the correct JAAS conf section
    final String SASL_LOGIN_CONTEXT_NAME = "HiveZooKeeperClient";
    System.setProperty(ZooKeeperSaslClient.LOGIN_CONTEXT_NAME_KEY, SASL_LOGIN_CONTEXT_NAME);

    principal = SecurityUtil.getServerPrincipal(principal, "0.0.0.0");
    JaasConfiguration jaasConf = new JaasConfiguration(SASL_LOGIN_CONTEXT_NAME, principal, keyTabFile);

    // Install the Configuration in the runtime.
    javax.security.auth.login.Configuration.setConfiguration(jaasConf);
  }

  /**
   * A JAAS configuration for ZooKeeper clients intended to use for SASL
   * Kerberos.
   */
  private static class JaasConfiguration extends javax.security.auth.login.Configuration {
    // Current installed Configuration
    private static final boolean IBM_JAVA = System.getProperty("java.vendor")
      .contains("IBM");
    private final javax.security.auth.login.Configuration baseConfig = javax.security.auth.login.Configuration
        .getConfiguration();
    private final String loginContextName;
    private final String principal;
    private final String keyTabFile;

    public JaasConfiguration(String hiveLoginContextName, String principal, String keyTabFile) {
      this.loginContextName = hiveLoginContextName;
      this.principal = principal;
      this.keyTabFile = keyTabFile;
    }

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String appName) {
      if (loginContextName.equals(appName)) {
        Map<String, String> krbOptions = new HashMap<String, String>();
        if (IBM_JAVA) {
        	krbOptions.put("credsType", "both");
        	krbOptions.put("useKeytab", keyTabFile);
      	} else {
        	krbOptions.put("doNotPrompt", "true");
        	krbOptions.put("storeKey", "true");
        	krbOptions.put("useKeyTab", "true");
        	krbOptions.put("keyTab", keyTabFile);
      	}
	krbOptions.put("principal", principal);
        krbOptions.put("refreshKrb5Config", "true");
        AppConfigurationEntry hiveZooKeeperClientEntry = new AppConfigurationEntry(
            KerberosUtil.getKrb5LoginModuleName(), LoginModuleControlFlag.REQUIRED, krbOptions);
        return new AppConfigurationEntry[] { hiveZooKeeperClientEntry };
      }
      // Try the base config
      if (baseConfig != null) {
        return baseConfig.getAppConfigurationEntry(appName);
      }
      return null;
    }
  }


}
