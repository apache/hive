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

package org.apache.hive.service.cli.thrift;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServlet;

public class ThriftHttpServlet extends TServlet {

  private static final long serialVersionUID = 1L;
  public static final Log LOG = LogFactory.getLog(ThriftHttpServlet.class.getName());

  public ThriftHttpServlet(TProcessor processor, TProtocolFactory protocolFactory) {
    super(processor, protocolFactory);
  }

  @Override
  protected void doPost(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    logRequestHeader(request);
    super.doPost(request, response);
  }

  protected void logRequestHeader(HttpServletRequest request) {
    String authHeaderBase64 = request.getHeader("Authorization");
    if(authHeaderBase64 == null) {
      LOG.warn("ThriftHttpServlet: no HTTP Authorization header");
    }
    else {
      if(!authHeaderBase64.startsWith("Basic")) {
        LOG.warn("ThriftHttpServlet: HTTP Authorization header exists but is not Basic.");
      }
      else if(LOG.isDebugEnabled()) {
        String authHeaderBase64_Payload = authHeaderBase64.substring("Basic ".length());
        String authHeaderString = StringUtils.newStringUtf8(
            Base64.decodeBase64(authHeaderBase64_Payload.getBytes()));
        String[] creds = authHeaderString.split(":");
        String username = null;
        String password = null;

        if(creds.length >= 1) {
          username = creds[0];
        }
        if(creds.length >= 2) {
          password = creds[1];
        }
        if(password == null || password.equals("null") || password.equals("")) {
          password = "<no password>";
        }
        else {
          // don't log the actual password.
          password = "******";
        }
        LOG.debug("HttpServlet:  HTTP Authorization header:: username=" + username +
            " password=" + password);
      }
    }
  }

}

