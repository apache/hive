/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.llap.daemon.services.impl;

import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.registry.LlapServiceInstance;
import org.apache.hadoop.hive.llap.registry.LlapServiceInstanceSet;
import org.apache.hadoop.hive.llap.registry.impl.LlapRegistryService;
import org.apache.hadoop.hive.registry.ServiceInstanceSet;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hive.common.util.HiveVersionInfo;
import org.apache.hive.http.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

public class LlapWebServices extends AbstractService {

  private static final Logger LOG = LoggerFactory.getLogger(LlapWebServices.class);

  // this is what allows the UI to do cross-domain reads of the contents
  // only apply to idempotent GET ops (all others need crumbs)
  static final String ACCESS_CONTROL_ALLOW_METHODS = "Access-Control-Allow-Methods";
  static final String ACCESS_CONTROL_ALLOW_ORIGIN = "Access-Control-Allow-Origin";

  static final String REGISTRY_ATTRIBUTE = "llap.registry";
  static final String PARENT_ATTRIBUTE = "llap.parent";

  private int port;
  private HttpServer http;
  private boolean useSSL = false;
  private boolean useSPNEGO = false;
  private final CompositeService parent;
  private final LlapRegistryService registry;

  public LlapWebServices(int port, CompositeService parent, LlapRegistryService registry) {
    super("LlapWebServices");
    this.port = port;
    this.registry = registry;
    this.parent = parent;
  }

  @Override
  public void serviceInit(Configuration conf) {

    this.useSSL = HiveConf.getBoolVar(conf, ConfVars.LLAP_DAEMON_WEB_SSL);
    this.useSPNEGO = HiveConf.getBoolVar(conf, ConfVars.LLAP_WEB_AUTO_AUTH);
    String bindAddress = "0.0.0.0";
    HttpServer.Builder builder =
        new HttpServer.Builder("llap").setPort(this.port).setHost(bindAddress);
    builder.setConf(new HiveConf(conf, HiveConf.class));
    builder.setDisableDirListing(true);
    if (conf.getBoolean(ConfVars.LLAP_DAEMON_WEB_XFRAME_ENABLED.varname,
        ConfVars.LLAP_DAEMON_WEB_XFRAME_ENABLED.defaultBoolVal)) {
      builder.configureXFrame(true).setXFrameOption(
          conf.get(ConfVars.LLAP_DAEMON_WEB_XFRAME_VALUE.varname,
              ConfVars.LLAP_DAEMON_WEB_XFRAME_VALUE.defaultStrVal));
    }
    if (UserGroupInformation.isSecurityEnabled()) {
      LOG.info("LLAP UI useSSL=" + this.useSSL + ", auto-auth/SPNEGO="
          + this.useSPNEGO + ", port=" + this.port);
      builder.setUseSSL(this.useSSL);
      if (this.useSPNEGO) {
        builder.setUseSPNEGO(true); // this setups auth filtering in build()
        builder.setSPNEGOPrincipal(HiveConf.getVar(conf, ConfVars.LLAP_WEBUI_SPNEGO_PRINCIPAL,
            HiveConf.getVar(conf, ConfVars.LLAP_KERBEROS_PRINCIPAL)));
        builder.setSPNEGOKeytab(HiveConf.getVar(conf, ConfVars.LLAP_WEBUI_SPNEGO_KEYTAB_FILE,
            HiveConf.getVar(conf, ConfVars.LLAP_KERBEROS_KEYTAB_FILE)));
      }
    }

    builder.setContextAttribute(REGISTRY_ATTRIBUTE, registry);
    builder.setContextAttribute(PARENT_ATTRIBUTE, parent);

    // make conf available to the locking stats servle
    LlapLockingServlet.setConf(conf);

    try {
      this.http = builder.build();
      this.http.addServlet("status", "/status", LlapStatusServlet.class);
      this.http.addServlet("peers", "/peers", LlapPeerRegistryServlet.class);
      this.http.addServlet("iomem", "/iomem", LlapIoMemoryServlet.class);
      this.http.addServlet("system", "/system", SystemConfigurationServlet.class);
      this.http.addServlet("locking", "/locking", LlapLockingServlet.class);
    } catch (IOException e) {
      LOG.warn("LLAP web service failed to come up", e);
    }
  }

  @InterfaceAudience.Private
  public int getPort() {
    return this.http.getPort();
  }

  @Override
  public void serviceStart() throws Exception {
    if (this.http != null) {
      this.http.start();
    }
  }

  @Override
  public void serviceStop() throws Exception {
    if (this.http != null) {
      this.http.stop();
    }
  }

  public static class LlapStatusServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;

    /**
     * Possible values: UNKNOWN, NOTINITED, INITED, STARTED, STOPPED
     */
    private static final String STATUS_ATTRIBUTE = "status";
    private static final String UPTIME_ATTRIBUTE = "uptime";
    private static final String BUILD_ATTRIBUTE = "build";

    private static final String UNKNOWN_STATE = "UNKNOWN";

    protected transient JsonFactory jsonFactory;

    protected RuntimeMXBean runtimeBean;

    @Override
    public void init() throws ServletException {
      jsonFactory = new JsonFactory();
      runtimeBean = ManagementFactory.getRuntimeMXBean();
    }

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) {
      JsonGenerator jg = null;
      PrintWriter writer = null;
      final ServletContext context = getServletContext();
      final Object parent = context.getAttribute(PARENT_ATTRIBUTE);

      final long uptime = runtimeBean.getUptime();

      try {
        try {
          writer = response.getWriter();

          response.setContentType("application/json; charset=utf8");
          response.setHeader(ACCESS_CONTROL_ALLOW_METHODS, "GET");
          response.setHeader(ACCESS_CONTROL_ALLOW_ORIGIN, "*");
          jg = jsonFactory.createJsonGenerator(writer);
          jg.disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
          jg.useDefaultPrettyPrinter();
          jg.writeStartObject();
          if (parent != null && parent instanceof CompositeService) {
            jg.writeStringField(STATUS_ATTRIBUTE, ((CompositeService) parent).getServiceState()
                .toString());
          } else {
            jg.writeStringField(STATUS_ATTRIBUTE, UNKNOWN_STATE);
          }
          jg.writeNumberField(UPTIME_ATTRIBUTE, uptime);
          jg.writeStringField(BUILD_ATTRIBUTE, HiveVersionInfo.getBuildVersion());
          jg.writeEndObject();
        } finally {
          if (jg != null) {
            jg.close();
          }
          if (writer != null) {
            writer.close();
          }
        }
      } catch (IOException e) {
        LOG.error("Caught an exception while processing /status request", e);
        response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      }
    }
  }

  public static class LlapPeerRegistryServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;
    protected transient JsonFactory jsonFactory;

    @Override
    public void init() throws ServletException {
      jsonFactory = new JsonFactory();
    }

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) {
      JsonGenerator jg = null;
      PrintWriter writer = null;
      final ServletContext context = getServletContext();
      final LlapRegistryService registry = (LlapRegistryService)context.getAttribute(REGISTRY_ATTRIBUTE);

      try {
        // admin check
        if (!HttpServer.isInstrumentationAccessAllowed(context, request, response)) {
          return;
        }
        try {
          writer = response.getWriter();

          response.setContentType("application/json; charset=utf8");
          response.setHeader(ACCESS_CONTROL_ALLOW_METHODS, "GET");
          response.setHeader(ACCESS_CONTROL_ALLOW_ORIGIN, "*");
          jg = jsonFactory.createJsonGenerator(writer);
          jg.disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
          jg.useDefaultPrettyPrinter();
          jg.writeStartObject();
          if (registry.isDynamic()) {
            jg.writeBooleanField("dynamic", true);
          }
          jg.writeStringField("identity", registry.getWorkerIdentity());
          jg.writeArrayFieldStart("peers");
          ServiceInstanceSet<LlapServiceInstance> instanceSet = registry.getInstances();
          for (LlapServiceInstance s : ((LlapServiceInstanceSet) instanceSet).getAllInstancesOrdered(false)) {
            jg.writeStartObject();
            jg.writeStringField("identity", s.getWorkerIdentity());
            jg.writeStringField("host", s.getHost());
            jg.writeNumberField("management-port", s.getManagementPort());
            jg.writeNumberField("rpc-port", s.getRpcPort());
            jg.writeNumberField("shuffle-port", s.getShufflePort());
            Resource r = s.getResource();
            if (r != null) {
              jg.writeObjectFieldStart("resource");
              jg.writeNumberField("vcores", r.getVirtualCores());
              jg.writeNumberField("memory", r.getMemory());
              jg.writeEndObject();
            }
            jg.writeStringField("host", s.getHost());
            jg.writeEndObject();
          }
          jg.writeEndArray();
          jg.writeEndObject();
        } finally {
          if (jg != null) {
            jg.close();
          }
          if (writer != null) {
            writer.close();
          }
        }
      } catch (IOException e) {
        LOG.error("Caught an exception while processing /status request", e);
        response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      }
    }
  }

}
