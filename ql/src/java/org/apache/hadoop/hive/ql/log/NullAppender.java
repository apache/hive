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

import java.io.Serializable;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.layout.PatternLayout;

/**
 * A NullAppender merely exists, it never outputs a message to any device.
 */
@Plugin(name = "NullAppender", category = "Core", elementType = "appender", printObject = false)
public class NullAppender extends AbstractAppender {

  private static LoggerContext context = (LoggerContext) LogManager.getContext(false);
  private static Configuration configuration = context.getConfiguration();

  protected NullAppender(String name, Filter filter,
      Layout<? extends Serializable> layout, boolean ignoreExceptions) {
    super(name, filter, layout, ignoreExceptions);
  }

  @PluginFactory
  public static NullAppender createNullAppender() {
    return new NullAppender("NullAppender", null, PatternLayout.createDefaultLayout(), true);
  }

  public void addToLogger(String loggerName, Level level) {
    LoggerConfig loggerConfig = configuration.getLoggerConfig(loggerName);
    loggerConfig.addAppender(this, level, null);
    context.updateLoggers();
  }

  public void append(LogEvent event) {
    // no-op
  }
}
