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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.LoginException;
import javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

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

  public static final String XSRF_CUSTOM_HEADER_PARAM = "custom-header";
  public static final String XSRF_CUSTOM_METHODS_TO_IGNORE_PARAM = "methods-to-ignore";
  private static final String XSRF_HEADER_DEFAULT = "X-XSRF-HEADER";
  private static final Set<String> XSRF_METHODS_TO_IGNORE_DEFAULT = new HashSet<String>(Arrays.asList("GET", "OPTIONS", "HEAD", "TRACE"));

  /*
   * Return Hadoop-native RestCsrfPreventionFilter if it is available.
   * Otherwise, construct our own copy of its logic.
   */
  public static Filter getXSRFFilter() {
    String filterClass = "org.apache.hadoop.security.http.RestCsrfPreventionFilter";
    try {
      Class<? extends Filter> klass = (Class<? extends Filter>) Class.forName(filterClass);
      Filter f = klass.newInstance();
      LOG.debug("Filter {} found, using as-is.", filterClass);
      return f;
    } catch (Exception e) {
      // ClassNotFoundException, InstantiationException, IllegalAccessException
      // Class could not be init-ed, use our local copy
      LOG.debug("Unable to use {}, got exception {}. Using internal shims impl of filter.",
          filterClass, e.getClass().getName());
    }
    return Utils.constructXSRFFilter();
  }

  private static Filter constructXSRFFilter() {
    // Note Hadoop 2.7.1 onwards includes a RestCsrfPreventionFilter class that is
    // usable as-is. However, since we have to work on a multitude of hadoop versions
    // including very old ones, we either duplicate their code here, or not support
    // an XSRFFilter on older versions of hadoop So, we duplicate to minimize evil(ugh).
    // See HADOOP-12691 for details of what this is doing.
    // This method should never be called if Hadoop 2.7+ is available.

    return new Filter(){

      private String  headerName = XSRF_HEADER_DEFAULT;
      private Set<String> methodsToIgnore = XSRF_METHODS_TO_IGNORE_DEFAULT;

      @Override
      public void init(FilterConfig filterConfig) throws ServletException {
        String customHeader = filterConfig.getInitParameter(XSRF_CUSTOM_HEADER_PARAM);
        if (customHeader != null) {
          headerName = customHeader;
        }
        String customMethodsToIgnore = filterConfig.getInitParameter(
            XSRF_CUSTOM_METHODS_TO_IGNORE_PARAM);
        if (customMethodsToIgnore != null) {
          parseMethodsToIgnore(customMethodsToIgnore);
        }
      }

      void parseMethodsToIgnore(String mti) {
        String[] methods = mti.split(",");
        methodsToIgnore = new HashSet<String>();
        for (int i = 0; i < methods.length; i++) {
          methodsToIgnore.add(methods[i]);
        }
      }

      @Override
      public void doFilter(
          ServletRequest request, ServletResponse response,
          FilterChain chain) throws IOException, ServletException {
          if (doXsrfFilter(request, response, methodsToIgnore, headerName)){
            chain.doFilter(request, response);
          }
      }

      @Override
      public void destroy() {
        // do nothing
      }
    };
  }

  // Method that provides similar filter functionality to filter-holder above, useful when
  // calling from code that does not use filters as-is.
  public static boolean doXsrfFilter(ServletRequest request, ServletResponse response,
      Set<String> methodsToIgnore, String headerName) throws IOException, ServletException {
    HttpServletRequest httpRequest = (HttpServletRequest)request;
    if (methodsToIgnore == null) { methodsToIgnore = XSRF_METHODS_TO_IGNORE_DEFAULT ; }
    if (headerName == null ) { headerName = XSRF_HEADER_DEFAULT; }
    if (methodsToIgnore.contains(httpRequest.getMethod()) ||
        httpRequest.getHeader(headerName) != null) {
      return true;
    } else {
      ((HttpServletResponse)response).sendError(
          HttpServletResponse.SC_BAD_REQUEST,
          "Missing Required Header for Vulnerability Protection");
      response.getWriter().println(
          "XSRF filter denial, requests must contain header : " + headerName);
      return false;
    }
  }


}
