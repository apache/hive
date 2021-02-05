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
package org.apache.hive.testutils.junit.extensions;

import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Tests for {@link Log4jConfigExtension}.
 */
public class TestLog4jConfigExtension {

  @Test
  @Log4jConfig("test0-log4j2.properties")
  void testUseExplicitConfig0() {
    LoggerContext ctx = LoggerContext.getContext(false);
    Configuration c = ctx.getConfiguration();
    assertEquals("TestConfig0Log4j2", c.getName());
    assertNotNull(c.getAppender("appender_zero"));
    assertEquals(1, c.getAppenders().size());
    assertEquals(2, c.getLoggers().size());
    assertNotNull(c.getLoggerConfig("logger_zero"));
    assertNotNull(c.getLoggerConfig("root"));
  }

  @Test
  void testNoExplicitConfig() {
    LoggerContext ctx = LoggerContext.getContext(false);
    Configuration c = ctx.getConfiguration();
    assertEquals("HiveLog4j2Test", c.getName());
  }

  @Test
  @Log4jConfig("test1-log4j2.properties")
  void testUseExplicitConfig1() {
    LoggerContext ctx = LoggerContext.getContext(false);
    Configuration c = ctx.getConfiguration();
    assertEquals("TestConfig1Log4j2", c.getName());
    assertNotNull(c.getAppender("appender_one"));
    assertEquals(1, c.getAppenders().size());
    assertEquals(2, c.getLoggers().size());
    assertNotNull(c.getLoggerConfig("logger_one"));
    assertNotNull(c.getLoggerConfig("root"));
  }

}
