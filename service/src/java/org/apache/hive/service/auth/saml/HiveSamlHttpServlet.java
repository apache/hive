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
import java.io.IOException;
import java.io.PrintWriter;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveSamlHttpServlet extends HttpServlet {

  private static final Logger LOG = LoggerFactory
      .getLogger(HiveSamlHttpServlet.class);
  private final HiveConf conf;
  private final ISAMLAuthTokenGenerator tokenGenerator;

  public HiveSamlHttpServlet(HiveConf conf) {
    this.conf = Preconditions.checkNotNull(conf);
    tokenGenerator = HiveSamlAuthTokenGenerator.get(conf);
  }

  @Override
  protected void doPost(HttpServletRequest request, HttpServletResponse response) {
    String nameId;
    String relayState;
    int port;
    try {
      relayState = HiveSamlRelayStateStore.get().getRelayStateInfo(request, response);
      port = HiveSamlRelayStateStore.get().getRelayStateInfo(relayState).getPort();
    } catch (HttpSamlAuthenticationException e) {
      LOG.error("Invalid relay state" ,e);
      response.setStatus(HttpStatus.SC_UNAUTHORIZED);
      return;
    }
    try {
      LOG.info("RelayState = {}. Driver side port on loopback address is {}", relayState,
          port);
      nameId = HiveSaml2Client.get(conf).validate(request, response);
    } catch (HttpSamlAuthenticationException e) {
      if (e instanceof HttpSamlNoGroupsMatchedException) {
        LOG.error("Could not authenticate user since the groups didn't match", e);
      } else {
        LOG.error("SAML response could not be validated", e);
      }
      generateFormData(response, HiveSamlUtils.getLoopBackAddress(port), null, false,
          "SAML assertion could not be validated. Check server logs for more details.");
      return;
    }
    Preconditions.checkState(nameId != null);
    LOG.info(
        "Successfully validated saml response for user {}. Forwarding the token to port {}",
        nameId, port);
    generateFormData(response, HiveSamlUtils.getLoopBackAddress(port),
        tokenGenerator.get(nameId, relayState), true, "");
  }

  private void generateFormData(HttpServletResponse response, String url, String token,
      boolean success, String msg) {
    StringBuilder sb = new StringBuilder();
    sb.append("<html>");
    sb.append("<body onload='document.forms[\"form\"].submit()'>");
    sb.append(String.format("<form name='form' action='%s' method='POST'>", url));
    sb.append(String
        .format("<input type='hidden' name='%s' value='%s'>", HiveSamlUtils.TOKEN_KEY,
            token));
    sb.append(String.format("<input type='hidden' name='%s' value='%s'>",
        HiveSamlUtils.STATUS_KEY, success));
    sb.append(String
        .format("<input type='hidden' name='%s' value='%s'>", HiveSamlUtils.MESSAGE_KEY,
            msg));
    sb.append("</form>");
    sb.append("</body>");
    sb.append("</html>");
    response.setContentType("text/html");
    try {
      response.getWriter().write(sb.toString());
      response.getWriter().flush();
    } catch (IOException e) {
      LOG.error("Could not generate the form data for sending a response to url " + url,
          e);
      // if there is an error set a response code to internal error.
      response.setStatus(HttpStatus.SC_INTERNAL_SERVER_ERROR);
    }
  }
}
