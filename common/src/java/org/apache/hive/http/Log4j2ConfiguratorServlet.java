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
package org.apache.hive.http;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A servlet to configure log4j2.
 * <br>
 * HTTP GET returns all loggers and it's log level in JSON formatted response.
 * <br>
 * HTTP POST is used for configuring the loggers. POST data should be in the same format as GET's response.
 * To configure (add/update existing loggers), use HTTP POST with logger names and level in the following JSON format.
 *
 * <br>
 * <pre>
 * <code>{
 *  "loggers": [ {
 *    "logger" : "",
 *    "level" : "INFO"
 *  }, {
 *    "logger" : "LlapIoOrc",
 *    "level" : "WARN"
 *  }, {
 *    "logger" : "org.apache.zookeeper.server.NIOServerCnxn",
 *    "level" : "WARN"
 *  }]
 * }</code>
 * </pre>
 *
 * <br>
 * Example usage:
 * <ul>
 * <li>
 *    Returns all loggers with levels in JSON format:
 *    <pre>
 *      curl http://hostame:port/conflog
 *    </pre>
 * </li>
 * <li>
 *    Set root logger to INFO:
 *    <pre>
 *      curl -v -H "Content-Type: application/json" -X POST -d '{ "loggers" : [ { "logger" : "", "level" : "INFO" } ] }'
 *      http://hostame:port/conflog
 *    </pre>
 * </li>
 * <li>
 *    Set logger with level:
 *    <pre>
 *      curl -v -H "Content-Type: application/json" -X POST -d '{ "loggers" : [
 *      { "logger" : "LlapIoOrc", "level" : "INFO" } ] }' http://hostame:port/conflog
 *    </pre>
 * </li>
 * <li>
 *    Set log level for all classes under a package:
 *    <pre>
 *      curl -v -H "Content-Type: application/json" -X POST -d '{ "loggers" : [
 *      { "logger" : "org.apache.orc", "level" : "INFO" } ] }' http://hostame:port/conflog
 *    </pre>
 * </li>
 * <li>
 *    Set log levels for multiple loggers:
 *    <pre>
 *      curl -v -H "Content-Type: application/json" -X POST -d '{ "loggers" : [ { "logger" : "", "level" : "INFO" },
 *      { "logger" : "LlapIoOrc", "level" : "WARN" },
 *      { "logger" : "org.apache.hadoop.hive.llap.daemon.impl.LlapDaemon", "level" : "INFO" },
 *      { "logger" : "org.apache.orc", "level" : "INFO" } ] }' http://hostame:port/conflog
 *    </pre>
 * </li>
 * </ul>
 * <br>
 * Response Status Codes:
 * <br>
 * <ul>
 * <li>200 - OK : If the POST data is valid and if the request succeeds or if GET request succeeds.</li>
 * <li>401 - UNAUTHORIZED : If the user does not have privileges to access instrumentation servlets.
 *                      Refer <code>hadoop.security.instrumentation.requires.admin</code> config for more info.</li>
 * <li>400 - BAD_REQUEST : If the POST data is not a valid JSON.</li>
 * <li>500 - INTERNAL_SERVER_ERROR : If GET requests throws any IOException during JSON output generation.</li>
 * </ul>
 */
public class Log4j2ConfiguratorServlet extends HttpServlet {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(Log4j2ConfiguratorServlet.class);
  private static final String ACCESS_CONTROL_ALLOW_METHODS = "Access-Control-Allow-Methods";
  private static final String ALLOWED_METHODS = "POST, GET";
  private static final String ACCESS_CONTROL_ALLOW_ORIGIN = "Access-Control-Allow-Origin";
  private static final String CONTENT_TYPE_JSON_UTF8 = "application/json; charset=utf8";

  private transient LoggerContext context;
  private transient Configuration conf;

  private static class ConfLoggers {
    private List<ConfLogger> loggers;

    public ConfLoggers() {
      this.loggers = new ArrayList<>();
    }

    public List<ConfLogger> getLoggers() {
      return loggers;
    }

    public void setLoggers(final List<ConfLogger> loggers) {
      this.loggers = loggers;
    }
  }

  private static class ConfLogger {
    private String logger;
    private String level;

    // no-arg ctor required for JSON deserialization
    public ConfLogger() {
      this(null, null);
    }

    public ConfLogger(String logger, String level) {
      this.logger = logger;
      this.level = level;
    }

    public String getLogger() {
      return logger == null ? logger : logger.trim();
    }

    public void setLogger(final String logger) {
      this.logger = logger;
    }

    public String getLevel() {
      return level == null ? level : level.trim().toUpperCase();
    }

    public void setLevel(final String level) {
      this.level = level;
    }
  }

  /**
   * Initialize this servlet.
   */
  @Override
  public void init() throws ServletException {
    context = (LoggerContext) LogManager.getContext(false);
    conf = context.getConfiguration();
  }

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, IOException {
    if (!HttpServer.isInstrumentationAccessAllowed(getServletContext(),
      request, response)) {
      response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
      return;
    }

    setResponseHeader(response);

    // list the loggers and their levels
    listLoggers(response);
  }

  private void setResponseHeader(final HttpServletResponse response) {
    response.setContentType(CONTENT_TYPE_JSON_UTF8);
    response.setHeader(ACCESS_CONTROL_ALLOW_METHODS, ALLOWED_METHODS);
    response.setHeader(ACCESS_CONTROL_ALLOW_ORIGIN, "*");
  }

  @Override
  protected void doPost(final HttpServletRequest request, final HttpServletResponse response)
    throws ServletException, IOException {
    if (!HttpServer.isInstrumentationAccessAllowed(getServletContext(),
      request, response)) {
      response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
      return;
    }
    setResponseHeader(response);

    String dataJson = request.getReader().lines().collect(Collectors.joining());
    ObjectMapper objectMapper = new ObjectMapper();
    try {
      ConfLoggers confLoggers = objectMapper.readValue(dataJson, ConfLoggers.class);
      configureLogger(confLoggers);
    } catch (IOException e) {
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      LOG.error("Error configuring log4j2 via /conflog endpoint.", e);
      return;
    }

    response.setStatus(HttpServletResponse.SC_OK);
  }

  private void configureLogger(final ConfLoggers confLoggers) {
    if (confLoggers != null) {
      for (ConfLogger logger : confLoggers.getLoggers()) {
        String loggerName = logger.getLogger();
        Level logLevel = Level.getLevel(logger.getLevel());
        if (logLevel == null) {
          LOG.warn("Invalid log level: {} for logger: {}. Ignoring reconfiguration.", loggerName, logger.getLevel());
          continue;
        }

        LoggerConfig loggerConfig = conf.getLoggerConfig(loggerName);
        // if the logger name is not found, root logger is returned. We don't want to change root logger level
        // since user either requested a new logger or specified invalid input. In which, we will add the logger
        // that user requested.
        if (!loggerName.equals(LogManager.ROOT_LOGGER_NAME) &&
          loggerConfig.getName().equals(LogManager.ROOT_LOGGER_NAME)) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Requested logger ({}) not found. Adding as new logger with {} level", loggerName, logLevel);
          }
          // requested logger not found. Add the new logger with the requested level
          conf.addLogger(loggerName, new LoggerConfig(loggerName, logLevel, true));
        } else {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Updating logger ({}) to {} level", loggerName, logLevel);
          }
          // update the log level for the specified logger
          loggerConfig.setLevel(logLevel);
        }
      }
      context.updateLoggers(conf);
    }
  }

  private void listLoggers(final HttpServletResponse response) throws IOException {
    PrintWriter writer = null;
    try {
      writer = response.getWriter();
      ConfLoggers confLoggers = new ConfLoggers();
      Collection<LoggerConfig> loggerConfigs = conf.getLoggers().values();
      loggerConfigs.forEach(lc -> confLoggers.getLoggers().add(new ConfLogger(lc.getName(), lc.getLevel().toString())));
      ObjectMapper objectMapper = new ObjectMapper();
      objectMapper.writerWithDefaultPrettyPrinter().writeValue(writer, confLoggers);
    } catch (IOException e) {
      LOG.error("Caught an exception while processing Log4j2 configuration request", e);
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      return;
    } finally {
      if (writer != null) {
        writer.close();
      }
    }
    response.setStatus(HttpServletResponse.SC_OK);
  }
}
