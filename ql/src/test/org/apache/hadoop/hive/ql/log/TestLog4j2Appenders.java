/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.log;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hive.ql.metadata.StringAppender;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class TestLog4j2Appenders {

  @Before
  public void setup() {
    // programmatically set root logger level to INFO. By default if log4j2-test.properties is not
    // available root logger will use ERROR log level
    LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
    Configuration config = ctx.getConfiguration();
    LoggerConfig loggerConfig = config.getLoggerConfig(LogManager.ROOT_LOGGER_NAME);
    loggerConfig.setLevel(Level.INFO);
    ctx.updateLoggers();
  }

  @Test
  public void testStringAppender() throws Exception {
    // Get the RootLogger which, if you don't have log4j2-test.properties defined, will only log ERRORs
    Logger logger = LogManager.getRootLogger();
    // Create a String Appender to capture log output
    StringAppender appender = StringAppender.createStringAppender("%m");
    appender.addToLogger(logger.getName(), Level.INFO);
    appender.start();

    // Logger to the string appender
    logger.info("Hello!");
    logger.info(" World");

    assertEquals("Hello! World", appender.getOutput());
    appender.removeFromLogger(LogManager.getRootLogger().getName());
  }

  @Test
  public void testHiveEventCounterAppender() throws Exception {
    Logger logger = LogManager.getRootLogger();
    HiveEventCounter appender = HiveEventCounter.createInstance("EventCounter", true, null, null);
    appender.addToLogger(logger.getName(), Level.INFO);
    appender.start();

    logger.info("Test");
    logger.info("Test");
    logger.info("Test");
    logger.info("Test");

    logger.error("Test");
    logger.error("Test");
    logger.error("Test");

    logger.warn("Test");
    logger.warn("Test");

    logger.fatal("Test");

    assertEquals(4, appender.getInfo());
    assertEquals(3, appender.getError());
    assertEquals(2, appender.getWarn());
    assertEquals(1, appender.getFatal());
    appender.removeFromLogger(LogManager.getRootLogger().getName());
  }
}
