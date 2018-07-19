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

package org.apache.hadoop.hive.llap.daemon.services.impl;

import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.hive.llap.io.api.LlapIo;
import org.apache.hadoop.hive.llap.io.api.LlapProxy;
import org.apache.hive.http.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")
public class LlapIoMemoryServlet extends HttpServlet {

  private static final Logger LOG = LoggerFactory.getLogger(LlapIoMemoryServlet.class);
  static final String ACCESS_CONTROL_ALLOW_METHODS = "Access-Control-Allow-Methods";
  static final String ACCESS_CONTROL_ALLOW_ORIGIN = "Access-Control-Allow-Origin";

  /**
   * Initialize this servlet.
   */
  @Override
  public void init() throws ServletException {
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

      try {
        response.setContentType("text/plain; charset=utf8");
        response.setHeader(ACCESS_CONTROL_ALLOW_METHODS, "GET");
        response.setHeader(ACCESS_CONTROL_ALLOW_ORIGIN, "*");
        response.setHeader("Cache-Control", "no-transform,public,max-age=60,s-maxage=60");

        writer = response.getWriter();

        LlapIo<?> llapIo = LlapProxy.getIo();
        if (llapIo == null) {
          writer.write("LLAP IO not found");
        } else {
          writer.write(llapIo.getMemoryInfo());
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
