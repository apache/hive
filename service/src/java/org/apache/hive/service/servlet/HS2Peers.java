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
package org.apache.hive.service.servlet;

import java.io.IOException;
import java.util.Collection;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.http.HttpConstants;
import org.apache.hive.http.HttpServer;
import org.apache.hive.service.server.HS2ActivePassiveHARegistry;
import org.apache.hive.service.server.HS2ActivePassiveHARegistryClient;
import org.apache.hive.service.server.HiveServer2Instance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

/**
 * Returns all HS2 instances in Active-Passive standy modes.
 */
public class HS2Peers extends HttpServlet {
  private static final Logger LOG = LoggerFactory.getLogger(HS2Peers.class);

  public static class HS2Instances {
    private Collection<HiveServer2Instance> hiveServer2Instances;

    // empty c'tor to make jackson happy
    public HS2Instances() {
    }

    public HS2Instances(final Collection<HiveServer2Instance> hiveServer2Instances) {
      this.hiveServer2Instances = hiveServer2Instances;
    }

    public Collection<HiveServer2Instance> getHiveServer2Instances() {
      return hiveServer2Instances;
    }

    public void setHiveServer2Instances(final Collection<HiveServer2Instance> hiveServer2Instances) {
      this.hiveServer2Instances = hiveServer2Instances;
    }

    @JsonIgnore
    public String toJson() throws IOException {
      ObjectMapper mapper = new ObjectMapper();
      mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
      return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(this);
    }
  }

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
    // admin check -
    // allows when hadoop.security.instrumentation.requires.admin is set to false
    // when hadoop.security.instrumentation.requires.admin is set to true, checks if hadoop.security.authorization
    // is true and if the logged in user (via PAM or SPNEGO + kerberos) is in hive.users.in.admin.role list
    final ServletContext context = getServletContext();
    if (!HttpServer.isInstrumentationAccessAllowed(context, request, response)) {
      LOG.warn("Unauthorized to perform GET action. remoteUser: {}", request.getRemoteUser());
      return;
    }

    response.setContentType(HttpConstants.CONTENT_TYPE_JSON);
    response.setHeader(HttpConstants.ACCESS_CONTROL_ALLOW_METHODS, HttpConstants.METHOD_GET);
    response.setHeader(HttpConstants.ACCESS_CONTROL_ALLOW_ORIGIN, HttpConstants.WILDCARD);

    ServletContext ctx = getServletContext();
    HiveConf hiveConf = (HiveConf) ctx.getAttribute("hiveconf");
    HS2ActivePassiveHARegistry hs2Registry = HS2ActivePassiveHARegistryClient.getClient(hiveConf);
    HS2Instances instances = new HS2Instances(hs2Registry.getAll());
    response.getWriter().write(instances.toJson());
    response.setStatus(HttpServletResponse.SC_OK);
    response.flushBuffer();
  }
}
