/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License a
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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.PrintWriter;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.metrics.ReadWriteLockMetrics;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hive.http.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Servlet to produce the JSON output for the locking endpoint.
 * The servlet produces and writes a JSON document, that lists all the locking statistics,
 * available through the <code>ReadWriteLockMetrics</code> instrumentation.
 */
public class LlapLockingServlet extends HttpServlet {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(LlapLockingServlet.class);
  private static final String ACCESS_CONTROL_ALLOW_METHODS = "Access-Control-Allow-Methods";
  private static final String ACCESS_CONTROL_ALLOW_ORIGIN = "Access-Control-Allow-Origin";
  private static Configuration conf = null;

  /**
   * Configuration setter, used to figure out the lock statistics collection setting.
   *
   * @param c The configuration to use
   */
  public static void setConf(Configuration c) {
    conf = c;
  }

  @Override
  public void init() throws ServletException {
    LOG.info("LlapLockingServlet initialized");
  }

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response) {
    try {
      if (!HttpServer.isInstrumentationAccessAllowed(getServletContext(),
                                                     request, response)) {
        response.setStatus(HttpServletResponse.SC_FORBIDDEN);
      } else {
        String  collString   = "\"disabled\"";
        boolean statsEnabled = false;

        // populate header
        response.setContentType("application/json; charset=utf8");
        response.setHeader(ACCESS_CONTROL_ALLOW_METHODS, "GET");
        response.setHeader(ACCESS_CONTROL_ALLOW_ORIGIN, "*");
        response.setHeader("Cache-Control",
                           "no-transform,public,max-age=60,s-maxage=60");

        if (null != conf && HiveConf.getBoolVar(conf,
                            HiveConf.ConfVars.LLAP_COLLECT_LOCK_METRICS)) {
          collString = "\"enabled\"";
          statsEnabled = true;
        }

        StringBuffer result = new StringBuffer();
        List<MetricsSource> sourceList
            = ReadWriteLockMetrics.getAllMetricsSources();
        if (null == sourceList) {
          // should actually never happen
          result.append("{\"error\":\"R/W statistics not found\"}");
        } else {
          sourceList.sort(new ReadWriteLockMetrics.MetricsComparator());
          boolean first = true;

          result.append("{\"statsCollection\":");
          result.append(collString);
          result.append(",\"lockStats\":[");

          // dump an object per lock label
          if (statsEnabled) {
            for (MetricsSource ms : sourceList) {
              if (!first) {
                result.append(",");
              }

              first = false;
              result.append(ms);
            }
          }

          result.append("]}");
        }

        // send string through JSON parser/builder to pretty print it.
        JsonParser parser = new JsonParser();
        JsonObject json   = parser.parse(result.toString()).getAsJsonObject();
        Gson       gson   = new GsonBuilder().setPrettyPrinting().create();
        try (PrintWriter w = response.getWriter()) {
          w.println(gson.toJson(json));
        }
      }
    }
    catch (Exception e) {
      LOG.error("Exception while processing locking stats request", e);
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    }
  }
}