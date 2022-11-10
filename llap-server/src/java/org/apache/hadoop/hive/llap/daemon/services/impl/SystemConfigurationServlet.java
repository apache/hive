/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.llap.daemon.services.impl;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.atomic.AtomicReference;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.hive.llap.LlapDaemonInfo;
import org.apache.hadoop.util.Shell;
import org.apache.hive.http.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import io.netty.util.NetUtil;

/**
 * A servlet to print system specific configurations that are not exposed via JMX.
 * Currently it exposes
 * - kernel configs
 * - network configs
 * - memory configs
 *
 * /system?refresh=true will run sysctl command again
 */
public class SystemConfigurationServlet extends HttpServlet {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(SystemConfigurationServlet.class);
  private static final String ACCESS_CONTROL_ALLOW_METHODS = "Access-Control-Allow-Methods";
  private static final String ALLOWED_METHODS = "GET";
  private static final String ACCESS_CONTROL_ALLOW_ORIGIN = "Access-Control-Allow-Origin";
  private static final String CONTENT_TYPE_JSON_UTF8 = "application/json; charset=utf8";
  protected transient JsonFactory jsonFactory;
  private static final String SYSCTL_KV_SEPARATOR = Shell.MAC ? ":" : "=";
  private AtomicReference<String> sysctlOutRef;
  private String FAILED = "failed";

  @Override
  public void init() throws ServletException {
    this.jsonFactory = new JsonFactory();
    this.sysctlOutRef = new AtomicReference<>(null);
  }

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, IOException {
    JsonGenerator jg = null;
    PrintWriter writer = null;
    if (!HttpServer.isInstrumentationAccessAllowed(getServletContext(),
      request, response)) {
      response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
      return;
    }
    setResponseHeader(response);
    boolean refresh = Boolean.parseBoolean(request.getParameter("refresh"));
    try {
      writer = response.getWriter();
      jg = jsonFactory.createJsonGenerator(writer);
      jg.disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
      jg.useDefaultPrettyPrinter();
      jg.writeStartObject();

      jg.writeObjectField("pid", LlapDaemonInfo.INSTANCE.getPID());
      jg.writeObjectField("os.name", System.getProperty("os.name"));
      if (Shell.WINDOWS) {
        jg.writeObjectField("net.core.somaxconn", NetUtil.SOMAXCONN);
      } else {
        String sysctlCmd = "sysctl -a";
        try {
          if (sysctlOutRef.get() == null || refresh) {
            LOG.info("Reading kernel configs via sysctl..");
            String sysctlOutput = Shell.execCommand(sysctlCmd.split("\\s+"));
            sysctlOutRef.set(sysctlOutput);
          }
        } catch (IOException e) {
          LOG.warn("Unable to execute '{}' command", sysctlCmd, e);
          sysctlOutRef.set(FAILED); // failures will not be retried (to avoid fork + exec running sysctl command)
          jg.writeObjectField("sysctl", FAILED);
          jg.writeObjectField("sysctl-failure-reason", e.getMessage());
        }

        if (sysctlOutRef.get() != null && !sysctlOutRef.get().equals(FAILED)) {
          String[] lines = sysctlOutRef.get().split("\\r?\\n");
          for (String line : lines) {
            int sepIdx = line.indexOf(SYSCTL_KV_SEPARATOR);
            String key = sepIdx == -1 ? line.trim() : line.substring(0, sepIdx).trim();
            String value = sepIdx == -1 ? null : line.substring(sepIdx + 1).trim().replaceAll("\t", "  ");
            if (!key.isEmpty()) {
              jg.writeObjectField(key, value);
            }
          }
        }

        if (!Shell.MAC) {
          // Red Hat: /sys/kernel/mm/redhat_transparent_hugepage/enabled
          //          /sys/kernel/mm/redhat_transparent_hugepage/defrag
          // CentOS/Ubuntu/Debian, OEL, SLES: /sys/kernel/mm/transparent_hugepage/enabled
          //                                  /sys/kernel/mm/transparent_hugepage/defrag
          String thpFileName = "/sys/kernel/mm/transparent_hugepage/enabled";
          String thpFileStr = PrivilegedFileReader.read(thpFileName);
          if (thpFileStr == null) {
            LOG.warn("Unable to read contents of {}", thpFileName);
            thpFileName = "/sys/kernel/mm/redhat_transparent_hugepage/enabled";
            thpFileStr = PrivilegedFileReader.read(thpFileName);
          }

          if (thpFileStr != null) {
            // Format: "always madvise [never]"
            int strIdx = thpFileStr.indexOf('[');
            int endIdx = thpFileStr.indexOf(']');
            jg.writeObjectField(thpFileName, thpFileStr.substring(strIdx + 1, endIdx));
          } else {
            LOG.warn("Unable to read contents of {}", thpFileName);
          }

          String thpDefragFileName = "/sys/kernel/mm/transparent_hugepage/defrag";
          String thpDefragFileStr = PrivilegedFileReader.read(thpDefragFileName);
          if (thpDefragFileStr == null) {
            LOG.warn("Unable to read contents of {}", thpDefragFileName);
            thpDefragFileName = "/sys/kernel/mm/redhat_transparent_hugepage/defrag";
            thpDefragFileStr = PrivilegedFileReader.read(thpDefragFileName);
          }

          if (thpDefragFileStr != null) {
            // Format: "always madvise [never]"
            int strIdx = thpDefragFileStr.indexOf('[');
            int endIdx = thpDefragFileStr.indexOf(']');
            jg.writeObjectField(thpDefragFileName, thpDefragFileStr.substring(strIdx + 1, endIdx));
          } else {
            LOG.warn("Unable to read contents of {}", thpDefragFileName);
          }
        }
      }

      jg.writeEndObject();
      response.setStatus(HttpServletResponse.SC_OK);
    } catch (Exception e) {
      LOG.error("Caught exception while processing llap /system web service request", e);
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    } finally {
      if (jg != null) {
        jg.close();
      }
      if (writer != null) {
        writer.close();
      }
    }
  }

  private void setResponseHeader(final HttpServletResponse response) {
    response.setContentType(CONTENT_TYPE_JSON_UTF8);
    response.setHeader(ACCESS_CONTROL_ALLOW_METHODS, ALLOWED_METHODS);
    response.setHeader(ACCESS_CONTROL_ALLOW_ORIGIN, "*");
  }

  private static class PrivilegedFileReader {
    public static String read(String filename) {
      String result = AccessController.doPrivileged(new PrivilegedAction<String>() {
        @Override
        public String run() {
          String fileString = null;
          try {
            fileString = new String(Files.readAllBytes(Paths.get(filename)), StandardCharsets.UTF_8);
          } catch (Exception e) {
            LOG.warn("Unable to read file: {}", filename, e);
          }
          return fileString;
        }
      });
      return result;
    }
  }
}
