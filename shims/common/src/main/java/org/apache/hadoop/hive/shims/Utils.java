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

package org.apache.hadoop.hive.shims;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag;
import javax.security.auth.login.LoginException;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.apache.zookeeper.client.ZooKeeperSaslClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {

  private static final Logger LOG = LoggerFactory.getLogger(Utils.class);
  public static final String RAW_RESERVED_VIRTUAL_PATH = "/.reserved/raw/";
  private static final boolean IBM_JAVA = System.getProperty("java.vendor")
      .contains("IBM");

  public static final String DISTCP_OPTIONS_PREFIX = "distcp.options.";


  private static Method getKeytab = null;
  static {
    Class<?> clz = UserGroupInformation.class;

    try {
      getKeytab = clz.getDeclaredMethod("getKeytab");
      getKeytab.setAccessible(true);
    } catch(NoSuchMethodException nme) {
      LOG.warn("Cannot find private method \"getKeytab\" in class:" +
          UserGroupInformation.class.getCanonicalName(), nme);
      getKeytab = null;
    }
  }

  public static UserGroupInformation getUGI() throws LoginException, IOException {
    if (UserGroupInformation.isSecurityEnabled()) {
      return UserGroupInformation.getCurrentUser();
    }

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
   * This is used for Jdbc client to authenticate with zk, where we don't know the exact principal.
   * @throws Exception
   */
  public static void setZookeeperClientKerberosJaasConfig() throws IOException {
    String principal = null, keyTabFile = null;
    UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
    try {
      if (getKeytab != null && UserGroupInformation.isSecurityEnabled() &&
          UserGroupInformation.isLoginKeytabBased()) {
        principal = loginUser.getUserName();
        keyTabFile = (String) getKeytab.invoke(loginUser);
      } else if (System.getProperty("java.security.auth.login.config") != null) {
        // The client provides the jaas configuration, use it
        return;
      }
    } catch (Exception e) {
      throw new IOException("Failed to get the principal or keyTab from UGI", e);
    }
    setZookeeperClientKerberosJaasConfig(principal, keyTabFile);
  }

  /**
   * Dynamically sets up the JAAS configuration that uses kerberos
   * @param principal
   * @param keyTabFile
   * @throws IOException
   */
  public static void setZookeeperClientKerberosJaasConfig(String principal, String keyTabFile,
      String... loginContext) throws IOException {
    // ZooKeeper property name to pick the correct JAAS conf section
    String loginContextName = "HiveZooKeeperClient";
    if (loginContext != null && loginContext.length > 0) {
      loginContextName = loginContext[0];
    }
    final String SASL_LOGIN_CONTEXT_NAME = loginContextName;
    if (System.getProperties().containsKey(SASL_LOGIN_CONTEXT_NAME)) {
      LOG.info("ZooKeeper JAAS context: {} has been set, ignore...", SASL_LOGIN_CONTEXT_NAME);
      return;
    }
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
          if (keyTabFile != null) {
            krbOptions.put("useKeytab", keyTabFile);
          } else {
            krbOptions.put("useDefaultCcache", "true");
          }
        } else {
          krbOptions.put("doNotPrompt", "true");
          if (keyTabFile != null) {
            krbOptions.put("useKeyTab", "true");
            krbOptions.put("keyTab", keyTabFile);
          } else {
            krbOptions.put("useTicketCache", "true");
          }
        }
        if (principal != null) {
          krbOptions.put("principal", principal);
        }
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

  public static boolean checkFileSystemXAttrSupport(FileSystem fs) throws IOException {
    return checkFileSystemXAttrSupport(fs, new Path(Path.SEPARATOR));
  }

  public static boolean checkFileSystemXAttrSupport(FileSystem fs, Path path) throws IOException {
    try {
      fs.getXAttrs(path);
      return true;
    } catch (UnsupportedOperationException e) {
      LOG.warn("XAttr won't be preserved since it is not supported for file system: " + fs.getUri());
      return false;
    }
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
