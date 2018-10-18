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

import org.apache.logging.log4j.core.appender.rolling.DirectFileRolloverStrategy;
import org.apache.logging.log4j.core.appender.rolling.RollingFileManager;
import org.apache.logging.log4j.core.appender.rolling.RolloverDescription;
import org.apache.logging.log4j.core.appender.rolling.RolloverDescriptionImpl;
import org.apache.logging.log4j.core.appender.rolling.RolloverStrategy;
import org.apache.logging.log4j.core.appender.rolling.action.AbstractAction;
import org.apache.logging.log4j.core.appender.rolling.action.Action;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginConfiguration;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;

/**
 * A RolloverStrategy that does not rename files and
 * uses file names that are based on a millisecond timestamp.
 */
@Plugin(name = "SlidingFilenameRolloverStrategy",
    category = "Core",
    printObject = true)
public class SlidingFilenameRolloverStrategy
    implements RolloverStrategy, DirectFileRolloverStrategy {

  @PluginFactory
  public static SlidingFilenameRolloverStrategy createStrategy(
      @PluginConfiguration Configuration config) {
    return new SlidingFilenameRolloverStrategy();
  }

  /**
   * Do rollover with no renaming.
   */
  @Override
  public RolloverDescription rollover(RollingFileManager manager)
      throws SecurityException {
    Action shiftToNextActiveFile = new AbstractAction() {
      @Override
      public boolean execute() throws IOException {
        return true;
      }
    };
    return new RolloverDescriptionImpl("ignored", false, shiftToNextActiveFile,
        null);
  }

  /**
   * Get the new log file name.
   */
  @Override
  public String getCurrentFileName(RollingFileManager rollingFileManager) {
    String pattern = rollingFileManager.getPatternProcessor().getPattern();
    return getLogFileName(pattern);
  }

  /**
   * @return Mangled file name formed by appending the current timestamp
   */
  private static String getLogFileName(String oldFileName) {
    return oldFileName + "." + Long.toString(System.currentTimeMillis());
  }
}
