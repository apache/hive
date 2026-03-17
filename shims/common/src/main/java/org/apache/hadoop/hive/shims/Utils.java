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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

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
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {

  private static final Logger LOG = LoggerFactory.getLogger(Utils.class);
  public static final String RAW_RESERVED_VIRTUAL_PATH = "/.reserved/raw/";

  public static final String DISTCP_OPTIONS_PREFIX = "distcp.options.";

  public static UserGroupInformation getUGI() throws IOException {
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
