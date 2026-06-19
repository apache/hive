/*
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

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.hooks.LineageLogger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test configuration and use of SlidingFilenameRolloverStrategy
 * @see SlidingFilenameRolloverStrategy
 */
public class TestSlidingFilenameRolloverStrategy {
  // properties file used to configure log4j2
  private static final String PROPERTIES_FILE =
      "log4j2_test_sliding_rollover.properties";

  //  file pattern that is set in PROPERTIES_FILE
  private static final String FILE_PATTERN = "./target/tmp/log/slidingTest.log";

  @BeforeClass
  public static void setUp() throws Exception {
    System.setProperty("log4j.configurationFile", PROPERTIES_FILE);
  }

  @AfterClass
  public static void tearDown() {
    System.clearProperty("log4j.configurationFile");
    LogManager.shutdown();
  }

  @Test
  public void testSlidingLogFiles() throws Exception {
    assertEquals("bad props file", PROPERTIES_FILE,
        System.getProperty("log4j.configurationFile"));

    // Where the log files wll be written
    Path logTemplate = FileSystems.getDefault().getPath(FILE_PATTERN);
    String fileName = logTemplate.getFileName().toString();
    Path parent = logTemplate.getParent();
    try {
      Files.createDirectory(parent);
    } catch (FileAlreadyExistsException e) {
      // OK, fall through.
    }

    // Delete any stale log files left around from previous failed tests
    deleteLogFiles(parent, fileName);

    Logger logger = LogManager.getLogger(LineageLogger.class);

    // Does the logger config look correct?
    org.apache.logging.log4j.core.Logger coreLogger =
        (org.apache.logging.log4j.core.Logger) logger;
    LoggerConfig loggerConfig = coreLogger.get();
    Map<String, Appender> appenders = loggerConfig.getAppenders();

    assertNotNull("sliding appender is missing", appenders.get("sliding"));

    // Do some logging and force log rollover
    int NUM_LOGS = 7;
    logger.debug("Debug Message Logged !!!");
    logger.info("Info Message Logged !!!");

    String errorString = "Error Message Logged ";
    for (int i = 0; i < NUM_LOGS; i++) {
      TimeUnit.MILLISECONDS.sleep(100);
      // log an exception - this produces enough text to force a new logfile
      // (as appender.sliding.policies.size.size=1KB)
      logger.error(errorString + i,
          new RuntimeException("part of a test"));
    }

    // Check log files look OK
    DirectoryStream<Path> stream =
        Files.newDirectoryStream(parent, fileName + ".*");
    int count = 0;
    for (Path path : stream) {
      count++;
      String contents = new String(Files.readAllBytes(path), "UTF-8");
      // There should be one exception message per file
      assertTrue("File " + path + " did not have expected content",
          contents.contains(errorString));
      String suffix = StringUtils.substringAfterLast(path.toString(), ".");
      // suffix should be a timestamp
      try {
        long timestamp = Long.parseLong(suffix);
      } catch (NumberFormatException e) {
        fail("Suffix " + suffix + " is not a long");
      }
    }
    assertEquals("bad count of log files", NUM_LOGS, count);

    // Check there is no log file without the suffix
    assertFalse("file should not exist:" + logTemplate,
        Files.exists(logTemplate));

    // Clean up
    deleteLogFiles(parent, fileName);
  }

  private void deleteLogFiles(Path parent, String fileName) throws IOException {
    DirectoryStream<Path> stream =
        Files.newDirectoryStream(parent, fileName + ".*");
    for (Path path : stream) {
      Files.delete(path);
    }
  }
}
