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

package org.apache.hive.service.auth.saml;

import com.google.common.base.Preconditions;
import java.net.URI;
import java.net.URISyntaxException;
import javax.servlet.http.HttpServletRequest;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hive.service.ServiceUtils;
import org.apache.hive.service.auth.HiveAuthConstants;

public class HiveSamlUtils {

  public static final String SSO_TOKEN_RESPONSE_PORT = "X-Hive-Token-Response-Port";
  public static final String SSO_CLIENT_IDENTIFIER = "X-Hive-Client-Identifier";
  public static final String TOKEN_KEY = "token";
  public static final String STATUS_KEY = "status";
  public static final String MESSAGE_KEY = "message";

  /**
   * Gets the configured callback url path for the SAML service provider. Also, makes sure
   * that the port number is same as the HTTP thrift port.
   * @param conf Hive server configuration.
   * @return the Callback URL http path.
   * @throws Exception In case the URL is invalid or if the port doesn't match http port.
   */
  public static String getCallBackPath(HiveConf conf) throws Exception {
    URI callbackURI = getCallBackUri(conf);
    return callbackURI.getPath();
  }

  public static URI getCallBackUri(HiveConf conf) throws Exception {
    String callbackUrl = conf.getVar(ConfVars.HIVE_SERVER2_SAML_CALLBACK_URL);
    try {
      URI uri = new URI(callbackUrl);
      int port = uri.getPort();
      int httpPort = conf.getIntVar(ConfVars.HIVE_SERVER2_THRIFT_HTTP_PORT);
      // we only support the callback url to be at the same port as the http
      // server. In case the port is not provided we default to using http port.
      Preconditions.checkArgument(port == -1 || port == httpPort,
          "Callback url " + callbackUrl + " must be at the same port " + httpPort
              + " defined by " + ConfVars.HIVE_SERVER2_THRIFT_HTTP_PORT.varname);
      return uri;
    } catch (URISyntaxException e) {
      throw new Exception("Invalid callback url configuration: "
          + ConfVars.HIVE_SERVER2_SAML_CALLBACK_URL.varname + " = " + callbackUrl);
    }
  }

  public static final String LOOP_BACK_INTERFACE = "127.0.0.1";
  public static String getLoopBackAddress(int port) {
    return String.format("http://%s:%s",LOOP_BACK_INTERFACE, port);
  }

  /**
   * Validates and returns the SAML response port number from the request.
   */
  public static int validateSamlResponsePort(HttpServletRequest request)
      throws HttpSamlAuthenticationException {
    String responsePort = request.getHeader(SSO_TOKEN_RESPONSE_PORT);
    if (responsePort == null || responsePort.isEmpty()) {
      throw new HttpSamlAuthenticationException("No response port specified");
    }
    try {
      return Integer.parseInt(responsePort);
    } catch (NumberFormatException e) {
      throw new HttpSamlAuthenticationException("Invalid response port received", e);
    }
  }
}