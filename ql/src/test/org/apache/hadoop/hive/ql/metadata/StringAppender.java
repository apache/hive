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
package org.apache.hadoop.hive.ql.metadata;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Serializable;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractOutputStreamAppender;
import org.apache.logging.log4j.core.appender.OutputStreamManager;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.layout.PatternLayout;

import com.google.common.annotations.VisibleForTesting;

/**
 * Log4j2 appender that writers to in-memory string object.
 */
@Plugin(name = "StringAppender", category = "Core", elementType = "appender", printObject = true)
public class StringAppender
    extends AbstractOutputStreamAppender<StringAppender.StringOutputStreamManager> {

  private static final String APPENDER_NAME = "StringAppender";
  private static LoggerContext context = (LoggerContext) LogManager.getContext(false);
  private static Configuration configuration = context.getConfiguration();
  private StringOutputStreamManager manager;

  /**
   * Instantiate a WriterAppender and set the output destination to a
   * new {@link OutputStreamWriter} initialized with <code>os</code>
   * as its {@link OutputStream}.
   *
   * @param name             The name of the Appender.
   * @param layout           The layout to format the message.
   * @param filter
   * @param ignoreExceptions
   * @param immediateFlush
   * @param manager          The OutputStreamManager.
   */
  protected StringAppender(String name,
      Layout<? extends Serializable> layout, Filter filter,
      boolean ignoreExceptions, boolean immediateFlush,
      StringOutputStreamManager manager) {
    super(name, layout, filter, ignoreExceptions, immediateFlush, manager);
    this.manager = manager;
  }

  @PluginFactory
  public static StringAppender createStringAppender(
      @PluginAttribute("name") String nullablePatternString) {
    PatternLayout layout;
    if (nullablePatternString == null) {
      layout = PatternLayout.createDefaultLayout();
    } else {
      layout = PatternLayout.createLayout(nullablePatternString, null, configuration,
          null, null, true, false, null, null);
    }

    return new StringAppender(APPENDER_NAME, layout, null, false, true,
        new StringOutputStreamManager(new ByteArrayOutputStream(), "StringStream", layout));
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

  public String getOutput() {
    manager.flush();
    return new String(manager.getStream().toByteArray());
  }

  public void reset() {
    manager.reset();
  }

  protected static class StringOutputStreamManager extends OutputStreamManager {
    ByteArrayOutputStream stream;

    protected StringOutputStreamManager(ByteArrayOutputStream os, String streamName,
        Layout<?> layout) {
      super(os, streamName, layout, true);
      stream = os;
    }

    public ByteArrayOutputStream getStream() {
      return stream;
    }

    public void reset() {
      stream.reset();
    }
  }
}
