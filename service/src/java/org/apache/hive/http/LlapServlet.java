/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.http;

import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.cli.LlapStatusOptionsProcessor;
import org.apache.hadoop.hive.llap.cli.LlapStatusServiceDriver;

@SuppressWarnings("serial")
public class LlapServlet extends HttpServlet {

  private static final Log LOG = LogFactory.getLog(LlapServlet.class);

  /**
   * Initialize this servlet.
   */
  @Override
  public void init() throws ServletException {
  }

  /**
   * Return the Configuration of the daemon hosting this servlet.
   * This is populated when the HttpServer starts.
   */
  private Configuration getConfFromContext() {
    Configuration conf = (Configuration)getServletContext().getAttribute(
        HttpServer.CONF_CONTEXT_ATTRIBUTE);
    assert conf != null;
    return conf;
  }

  /**
   * Process a GET request for the specified resource.
   *
   * @param request
   *          The servlet request we are processing
   * @param response
   *          The servlet response we are creating
   */
  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response) {
    try {
      if (!HttpServer.isInstrumentationAccessAllowed(getServletContext(), request, response)) {
        return;
      }
      PrintWriter writer = null;
      String clusterName =
          HiveConf.getVar(getConfFromContext(), HiveConf.ConfVars.LLAP_DAEMON_SERVICE_HOSTS);

      try {
        response.setContentType(HttpConstants.CONTENT_TYPE_JSON);
        response.setHeader(HttpConstants.ACCESS_CONTROL_ALLOW_METHODS, HttpConstants.METHOD_GET);
        response.setHeader(HttpConstants.ACCESS_CONTROL_ALLOW_ORIGIN, HttpConstants.WILDCARD);
        response.setHeader(HttpConstants.CACHE_CONTROL, "no-transform,public,max-age=60,s-maxage=60");

        writer = response.getWriter();

        if (clusterName != null) {
          clusterName = clusterName.trim();
        }

        if (clusterName == null || clusterName.isEmpty()) {
          writer.print("{\"LLAP\": \"No llap daemons configured. ");
          writer.print("Check hive.llap.daemon.service.hosts.\"}");
          return;
        }

        if (clusterName.startsWith("@")) {
          clusterName = clusterName.substring(1);
        }

        LOG.info("Retrieving info for cluster: " + clusterName);
        LlapStatusServiceDriver driver = new LlapStatusServiceDriver();
        int ret = driver.run(new LlapStatusOptionsProcessor.LlapStatusOptions(clusterName), 0);
        if (ret == LlapStatusServiceDriver.ExitCode.SUCCESS.getInt()) {
          driver.outputJson(writer);
        }

      } finally {
        if (writer != null) {
          writer.close();
        }
      }
    } catch (Exception e) {
      LOG.error("Caught exception while processing llap status request", e);
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    }
  }
}
