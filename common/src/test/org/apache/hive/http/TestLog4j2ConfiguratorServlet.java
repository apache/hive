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
package org.apache.hive.http;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link Log4j2ConfiguratorServlet#setLogLevel(String, Level)}, the logic behind
 * the HiveServer2 WebUI "Configure logging" page and the {@code /conflog} endpoint.
 */
public class TestLog4j2ConfiguratorServlet {

  private static final String PARENT_LOGGER = "org.apache.hive.test.conflog";
  private static final String CHILD_LOGGER = "org.apache.hive.test.conflog.child";

  // The levels offered by the WebUI "Configure logging" page dropdown.
  private static final Level[] SUPPORTED_LEVELS =
      { Level.TRACE, Level.DEBUG, Level.INFO, Level.WARN, Level.ERROR, Level.FATAL };

  private Log4j2ConfiguratorServlet servlet;
  private Configuration configuration;
  private Level originalRootLevel;

  @Before
  public void setUp() throws Exception {
    servlet = new Log4j2ConfiguratorServlet();
    servlet.init();
    configuration = ((LoggerContext) LogManager.getContext(false)).getConfiguration();
    originalRootLevel = configuration.getRootLogger().getLevel();
  }

  @After
  public void tearDown() {
    // Restore the root level so this test cannot leak into other tests in the same JVM.
    configuration.getRootLogger().setLevel(originalRootLevel);
  }

  /**
   * Setting a level for a not-yet-configured child logger must create a dedicated logger for it
   * and must not change the level of an existing ancestor logger.
   */
  @Test
  public void testSetLevelOnNewLoggerDoesNotAffectAncestor() {
    servlet.setLogLevel(PARENT_LOGGER, Level.INFO);
    servlet.setLogLevel(CHILD_LOGGER, Level.DEBUG);

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
    servlet.setLogLevel(PARENT_LOGGER, Level.INFO);
    servlet.setLogLevel(PARENT_LOGGER, Level.WARN);

    LoggerConfig parentConfig = configuration.getLoggerConfig(PARENT_LOGGER);
    assertEquals("Existing logger should be updated in place", PARENT_LOGGER, parentConfig.getName());
    assertEquals("Existing logger level should reflect the last update", Level.WARN, parentConfig.getLevel());
  }

  /**
   * The empty logger name is the Log4j2 root logger and must update the root config directly.
   */
  @Test
  public void testSetLevelUpdatesRootLogger() {
    servlet.setLogLevel(LogManager.ROOT_LOGGER_NAME, Level.ERROR);

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
      servlet.setLogLevel(PARENT_LOGGER, level);
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
      servlet.setLogLevel(LogManager.ROOT_LOGGER_NAME, level);
      assertEquals("Root logger level should reflect the requested level " + level,
          level, configuration.getLoggerConfig(LogManager.ROOT_LOGGER_NAME).getLevel());
    }
  }
}
