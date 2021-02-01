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

import org.apache.hadoop.hive.ql.QueryInfo;
import org.apache.hive.service.cli.operation.OperationManager;
import org.apache.hive.service.cli.session.HiveSession;
import org.apache.hive.service.cli.session.SessionManager;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.Version;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializerProvider;
import org.codehaus.jackson.map.module.SimpleModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;

/**
 * QueriesRESTfulAPIServlet.
 *
 */
public class QueriesRESTfulAPIServlet extends HttpServlet {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(QueriesRESTfulAPIServlet.class);

  private static final String API_V1 = "v1";
  private static final String REQ_QUERIES = "queries";
  private static final String REQ_SESSIONS = "sessions";
  private static final String REQ_ACTIVE = "active";
  private static final String REQ_HISTORICAL = "historical";


  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
        /*
            Available endpoints are:
             - /v1/queries/active
             - /v1/queries/historical
             - /v1/sessions
        */

    String pathInfo = request.getPathInfo();
    if (pathInfo == null || "/".equals(pathInfo)) {
      sendError(response, HttpServletResponse.SC_BAD_REQUEST, "Path to the endpoint is missing");
      return;
    }


    String[] splits = pathInfo.split("/");
    if (splits.length < 3) { //expecting at least 2 parts in the path
      sendError(response, HttpServletResponse.SC_BAD_REQUEST, "Expecting at least 2 parts in the path");
      return;
    }

    ServletContext ctx = getServletContext();
    SessionManager sessionManager =
        (SessionManager) ctx.getAttribute("hive.sm");
    OperationManager operationManager = sessionManager.getOperationManager();

    String apiVersion = splits[1];
    if (apiVersion.equals(API_V1)) {
      String reqType = splits[2];
      if (reqType.equals(REQ_QUERIES)) {
        if (splits.length != 4) {
          sendError(response, HttpServletResponse.SC_NOT_FOUND,
              "Expecting 3 parts in the path: /v1/queries/active or /v1/queries/historical");
          return;
        }
        String queriesType = splits[3];
        if (queriesType.equals(REQ_ACTIVE)) {
          Collection<QueryInfo> operations = operationManager.getLiveQueryInfos();
          LOG.info("Returning active SQL operations via the RESTful API");
          sendAsJson(response, operations);
        } else if (queriesType.equals(REQ_HISTORICAL)) {
          Collection<QueryInfo> operations = operationManager.getHistoricalQueryInfos();
          LOG.info("Returning historical SQL operations via the RESTful API");
          sendAsJson(response, operations);
        } else {
          sendError(response, HttpServletResponse.SC_BAD_REQUEST, "Unknown query type: " + queriesType);
          return;
        }
      } else if (reqType.equals(REQ_SESSIONS)) {
        Collection<HiveSession> hiveSessions = sessionManager.getSessions();
        LOG.info("Returning active sessions via the RESTful API");
        sendAsJson(response, hiveSessions);
      } else { // unrecognized request
        sendError(response, HttpServletResponse.SC_NOT_FOUND, "Unknown request type: " + reqType);
        return;
      }
    } else { // unrecognized API version
      sendError(response, HttpServletResponse.SC_BAD_REQUEST, "This server only handles API v1");
      return;
    }
  }

  private void sendError(HttpServletResponse response,
                         Integer errorCode,
                         String message) {
    response.setStatus(errorCode);
    response.setContentType("application/json");
    response.setCharacterEncoding("UTF-8");
    try {
      response.getWriter().write("{\"message\" : " + message + "}");
    } catch (IOException e) {
      LOG.error("Caught an exception while writing an HTTP error status", e);
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    }
  }

  private void sendAsJson(
      HttpServletResponse response,
      Object obj) {
    response.setContentType("application/json");
    response.setStatus(HttpServletResponse.SC_OK);
    ObjectMapper mapper = new ObjectMapper();
    SimpleModule module = new SimpleModule("CustomSessionModule", new Version(1, 0, 0, null));
    module.addSerializer(HiveSession.class, new HiveSessionSerializer());
    mapper.registerModule(module);

    try {
      PrintWriter out = response.getWriter();
      String objectAsJson = mapper.writeValueAsString(obj);
      out.print(objectAsJson);
      out.flush();
      out.close();
    } catch (IOException e) {
      LOG.error("Caught an exception while writing an HTTP response", e);
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    }
  }

  private static class HiveSessionSerializer extends JsonSerializer<HiveSession> {
    @Override
    public void serialize(
        HiveSession hiveSession,
        JsonGenerator jgen,
        SerializerProvider serializerProvider)
        throws IOException, JsonProcessingException {
      long currentTime = System.currentTimeMillis();

      jgen.writeStartObject();
      jgen.writeStringField("sessionId", hiveSession.getSessionHandle().getSessionId().toString());
      jgen.writeStringField("username", hiveSession.getUserName());
      jgen.writeStringField("ipAddress", hiveSession.getIpAddress());
      jgen.writeNumberField("operationCount", hiveSession.getOpenOperationCount());
      jgen.writeNumberField("activeTime", (currentTime - hiveSession.getCreationTime()) / 1000);
      jgen.writeNumberField("idleTime", (currentTime - hiveSession.getLastAccessTime()) / 1000);
      jgen.writeEndObject();
    }
  }
}
