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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.http;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@code /conflog} endpoint ({@link Log4j2ConfiguratorServlet#doPost}), the logic behind
 * the HiveServer2 WebUI "Configure logging" page.
 */
public class TestLog4j2ConfiguratorServlet {

  private static final String PARENT_LOGGER = "org.apache.hive.test.conflog";
  private static final String CHILD_LOGGER = "org.apache.hive.test.conflog.child";

  // The levels offered by the WebUI "Configure logging" page dropdown.
  private static final Level[] SUPPORTED_LEVELS = new Level[] {
      Level.TRACE, Level.DEBUG, Level.INFO, Level.WARN, Level.ERROR, Level.FATAL
  };

  private Log4j2ConfiguratorServlet servlet;
  private LoggerContext loggerContext;
  private Configuration configuration;
  private Level originalRootLevel;

  @Before
  public void setUp() throws Exception {
    // doPost checks instrumentation access, which reads the Hadoop conf from the servlet context.
    ServletContext servletContext = mock(ServletContext.class);
    when(servletContext.getAttribute(HttpServer.CONF_CONTEXT_ATTRIBUTE))
        .thenReturn(new org.apache.hadoop.conf.Configuration(false));
    ServletConfig servletConfig = mock(ServletConfig.class);
    when(servletConfig.getServletContext()).thenReturn(servletContext);

    servlet = new Log4j2ConfiguratorServlet();
    servlet.init(servletConfig);
    loggerContext = (LoggerContext) LogManager.getContext(false);
    configuration = loggerContext.getConfiguration();
    originalRootLevel = configuration.getRootLogger().getLevel();
  }

  @After
  public void tearDown() {
    // Restore the root level so this test cannot leak into other tests in the same JVM.
    configuration.getRootLogger().setLevel(originalRootLevel);
  }

  /**
   * Sets the level of a single logger the way the WebUI does: by POSTing JSON to the servlet.
   */
  private void postLevel(final String loggerName, final Level level) {
    String body = "{\"loggers\":[{\"logger\":\"" + loggerName + "\",\"level\":\"" + level + "\"}]}";
    HttpServletRequest request = mock(HttpServletRequest.class);
    HttpServletResponse response = mock(HttpServletResponse.class);
    try {
      when(request.getReader()).thenReturn(new BufferedReader(new StringReader(body)));
      servlet.doPost(request, response);
    } catch (IOException | ServletException e) {
      throw new AssertionError("POST to /conflog failed", e);
    }
    verify(response).setStatus(HttpServletResponse.SC_OK);
  }

  /**
   * Setting a level for a not-yet-configured child logger must create a dedicated logger for it
   * and must not change the level of an existing ancestor logger.
   */
  @Test
  public void testSetLevelOnNewLoggerDoesNotAffectAncestor() {
    postLevel(PARENT_LOGGER, Level.INFO);
    postLevel(CHILD_LOGGER, Level.DEBUG);

    LoggerConfig childConfig = configuration.getLoggerConfig(CHILD_LOGGER);
    assertEquals("Child logger should have its own configuration", CHILD_LOGGER, childConfig.getName());
    assertEquals("Child logger level should be the requested one", Level.DEBUG, childConfig.getLevel());

    LoggerConfig parentConfig = configuration.getLoggerConfig(PARENT_LOGGER);
    assertEquals("Ancestor logger level must not change when a child is configured",
        Level.INFO, parentConfig.getLevel());
  }

  /**
   * Setting a level for an already-configured logger must update that logger in place.
   */
  @Test
  public void testSetLevelUpdatesExistingLogger() {
    postLevel(PARENT_LOGGER, Level.INFO);
    postLevel(PARENT_LOGGER, Level.WARN);

    LoggerConfig parentConfig = configuration.getLoggerConfig(PARENT_LOGGER);
    assertEquals("Existing logger should be updated in place", PARENT_LOGGER, parentConfig.getName());
    assertEquals("Existing logger level should reflect the last update", Level.WARN, parentConfig.getLevel());
  }

  /**
   * The empty logger name is the Log4j2 root logger and must update the root config directly.
   */
  @Test
  public void testSetLevelUpdatesRootLogger() {
    postLevel(LogManager.ROOT_LOGGER_NAME, Level.ERROR);

    LoggerConfig rootConfig = configuration.getLoggerConfig(LogManager.ROOT_LOGGER_NAME);
    assertEquals("Root logger name should stay empty", LogManager.ROOT_LOGGER_NAME, rootConfig.getName());
    assertEquals("Root logger level should be the requested one", Level.ERROR, rootConfig.getLevel());
  }

  /**
   * Every level offered by the WebUI must be applied to and reflected back by a normal logger.
   */
  @Test
  public void testEveryLevelIsAppliedToLogger() {
    for (Level level : SUPPORTED_LEVELS) {
      postLevel(PARENT_LOGGER, level);
      assertEquals("Logger level should reflect the requested level " + level,
          level, configuration.getLoggerConfig(PARENT_LOGGER).getLevel());
    }
  }

  /**
   * Every level offered by the WebUI must be applied to and reflected back by the root logger.
   */
  @Test
  public void testEveryLevelIsAppliedToRootLogger() {
    for (Level level : SUPPORTED_LEVELS) {
      postLevel(LogManager.ROOT_LOGGER_NAME, level);
      assertEquals("Root logger level should reflect the requested level " + level,
          level, configuration.getLoggerConfig(LogManager.ROOT_LOGGER_NAME).getLevel());
    }
  }

  /**
   * End-to-end proof that the configured level actually controls what reaches the log: for each
   * level the servlet is asked to set, a message is emitted at every level and only the messages
   * at or above the configured threshold must be delivered to the appender.
   */
  @Test
  public void testConfiguredLevelControlsEmittedLogs() {
    final String loggerName = "org.apache.hive.test.conflog.emit";
    CapturingAppender appender = new CapturingAppender("capture-conflog");
    appender.start();

    Logger logger = LogManager.getLogger(loggerName);
    try {
      for (Level configured : SUPPORTED_LEVELS) {
        postLevel(loggerName, configured);
        LoggerConfig loggerConfig = configuration.getLoggerConfig(loggerName);
        if (!loggerConfig.getAppenders().containsKey(appender.getName())) {
          loggerConfig.addAppender(appender, null, null);
        }
        // Do not also route to the root appenders, so we only measure this logger's output.
        loggerConfig.setAdditive(false);
        loggerContext.updateLoggers();

        appender.drainLevels();
        for (Level emitted : SUPPORTED_LEVELS) {
          logger.log(emitted, "conflog test message");
        }

        assertEquals("Logger set to " + configured + " must emit exactly the levels at or above it",
            atOrAbove(configured), appender.drainLevels());
      }
    } finally {
      configuration.getLoggerConfig(loggerName).removeAppender(appender.getName());
      appender.stop();
      loggerContext.updateLoggers();
    }
  }

  /**
   * The levels from {@link #SUPPORTED_LEVELS} that are at least as severe as {@code threshold},
   * i.e. the ones a logger configured at {@code threshold} is expected to emit.
   */
  private static List<Level> atOrAbove(final Level threshold) {
    List<Level> expected = new ArrayList<>();
    for (Level level : SUPPORTED_LEVELS) {
      if (level.isMoreSpecificThan(threshold)) {
        expected.add(level);
      }
    }
    return expected;
  }

  /**
   * A minimal Log4j2 appender that records the level of every event it receives, used to assert
   * which messages a logger actually emits at a given configured level.
   */
  private static final class CapturingAppender extends AbstractAppender {

    private final List<Level> levels = Collections.synchronizedList(new ArrayList<>());

    CapturingAppender(final String name) {
      super(name, null, null, true, Property.EMPTY_ARRAY);
    }

    @Override
    public void append(final LogEvent event) {
      levels.add(event.getLevel());
    }

    List<Level> drainLevels() {
      List<Level> snapshot = new ArrayList<>(levels);
      levels.clear();
      return snapshot;
    }
  }
}
