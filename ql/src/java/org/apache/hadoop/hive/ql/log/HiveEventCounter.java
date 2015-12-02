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
import java.util.concurrent.atomic.AtomicLongArray;

import org.apache.hadoop.hive.common.classification.InterfaceAudience;
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
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.layout.PatternLayout;

import com.google.common.annotations.VisibleForTesting;

/**
 * A log4J2 Appender that simply counts logging events in four levels:
 * fatal, error, warn and info. The class name can be used in log4j2.properties
 */
@Plugin(name = "HiveEventCounter", category = "Core", elementType = "appender", printObject = true)
public class HiveEventCounter extends AbstractAppender {
  private static LoggerContext context = (LoggerContext) LogManager.getContext(false);
  private static Configuration configuration = context.getConfiguration();
  private static final String APPENDER_NAME = "HiveEventCounter";
  private static final int FATAL = 0;
  private static final int ERROR = 1;
  private static final int WARN = 2;
  private static final int INFO = 3;

  private static class EventCounts {
    private final AtomicLongArray counts = new AtomicLongArray(4);

    private void incr(int i) {
      counts.incrementAndGet(i);
    }

    private long get(int i) {
      return counts.get(i);
    }
  }

  private static EventCounts counts = new EventCounts();

  protected HiveEventCounter(String name, Filter filter,
      Layout<? extends Serializable> layout, boolean ignoreExceptions) {
    super(name, filter, layout, ignoreExceptions);
  }

  @PluginFactory
  public static HiveEventCounter createInstance(@PluginAttribute("name") String name,
      @PluginAttribute("ignoreExceptions") boolean ignoreExceptions,
      @PluginElement("Layout") Layout layout,
      @PluginElement("Filters") Filter filter) {
    if (name == null) {
      name = APPENDER_NAME;
    }

    if (layout == null) {
      layout = PatternLayout.createDefaultLayout();
    }
    return new HiveEventCounter(name, filter, layout, ignoreExceptions);
  }

  @InterfaceAudience.Private
  public static long getFatal() {
    return counts.get(FATAL);
  }

  @InterfaceAudience.Private
  public static long getError() {
    return counts.get(ERROR);
  }

  @InterfaceAudience.Private
  public static long getWarn() {
    return counts.get(WARN);
  }

  @InterfaceAudience.Private
  public static long getInfo() {
    return counts.get(INFO);
  }

  @VisibleForTesting
  public void addToLogger(String loggerName, Level level) {
    LoggerConfig loggerConfig = configuration.getLoggerConfig(loggerName);
    loggerConfig.addAppender(this, level, null);
    context.updateLoggers();
  }

  @VisibleForTesting
  public void removeFromLogger(String loggerName) {
    LoggerConfig loggerConfig = configuration.getLoggerConfig(loggerName);
    loggerConfig.removeAppender(APPENDER_NAME);
    context.updateLoggers();
  }

  public void append(LogEvent event) {
    Level level = event.getLevel();
    if (level.equals(Level.INFO)) {
      counts.incr(INFO);
    } else if (level.equals(Level.WARN)) {
      counts.incr(WARN);
    } else if (level.equals(Level.ERROR)) {
      counts.incr(ERROR);
    } else if (level.equals(Level.FATAL)) {
      counts.incr(FATAL);
    }
  }
}
