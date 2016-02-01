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
package org.apache.hive.service.cli.operation;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.session.OperationLog;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractOutputStreamAppender;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.appender.OutputStreamManager;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.filter.AbstractFilter;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;

/**
 * Divert appender to redirect operation logs to separate files.
 */
public class LogDivertAppender
    extends AbstractOutputStreamAppender<LogDivertAppender.StringOutputStreamManager> {
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(LogDivertAppender.class.getName());
  private static LoggerContext context = (LoggerContext) LogManager.getContext(false);
  private static Configuration configuration = context.getConfiguration();
  public static final Layout<? extends Serializable> verboseLayout = PatternLayout.createLayout(
      "%d{yy/MM/dd HH:mm:ss} %p %c{2}: %m%n", null, configuration, null, null, true, false, null, null);
  public static final Layout<? extends Serializable> nonVerboseLayout = PatternLayout.createLayout(
      "%-5p : %m%n", null, configuration, null, null, true, false, null, null);

  private final OperationManager operationManager;
  private final StringOutputStreamManager manager;
  private boolean isVerbose;
  private final Layout<? extends Serializable> layout;

  /**
   * Instantiate a WriterAppender and set the output destination to a
   * new {@link OutputStreamWriter} initialized with <code>os</code>
   * as its {@link OutputStream}.
   *
   * @param name             The name of the Appender.
   * @param filter           Filter
   * @param manager          The OutputStreamManager.
   * @param operationManager Operation manager
   */
  protected LogDivertAppender(String name, Filter filter,
      StringOutputStreamManager manager, OperationManager operationManager,
      OperationLog.LoggingLevel loggingMode) {
    super(name, null, filter, false, true, manager);
    this.operationManager = operationManager;
    this.manager = manager;
    this.isVerbose = (loggingMode == OperationLog.LoggingLevel.VERBOSE);
    this.layout = getDefaultLayout();
  }

  public Layout<? extends Serializable> getDefaultLayout() {
    // There should be a ConsoleAppender. Copy its Layout.
    Logger root = LogManager.getRootLogger();
    Layout layout = null;

    for (Appender ap : ((org.apache.logging.log4j.core.Logger) root).getAppenders().values()) {
      if (ap.getClass().equals(ConsoleAppender.class)) {
        layout = ap.getLayout();
        break;
      }
    }

    return layout;
  }

  /**
   * A log filter that filters messages coming from the logger with the given names.
   * It be used as a white list filter or a black list filter.
   * We apply black list filter on the Loggers used by the log diversion stuff, so that
   * they don't generate more logs for themselves when they process logs.
   * White list filter is used for less verbose log collection
   */
  private static class NameFilter extends AbstractFilter {
    private Pattern namePattern;
    private OperationLog.LoggingLevel loggingMode;
    private final OperationManager operationManager;

    /* Patterns that are excluded in verbose logging level.
     * Filter out messages coming from log processing classes, or we'll run an infinite loop.
     */
    private static final Pattern verboseExcludeNamePattern = Pattern.compile(Joiner.on("|").
        join(new String[]{LOG.getName(), OperationLog.class.getName(),
            OperationManager.class.getName()}));

    /* Patterns that are included in execution logging level.
     * In execution mode, show only select logger messages.
     */
    private static final Pattern executionIncludeNamePattern = Pattern.compile(Joiner.on("|").
        join(new String[]{"org.apache.hadoop.mapreduce.JobSubmitter",
            "org.apache.hadoop.mapreduce.Job", "SessionState", Task.class.getName(),
            Driver.class.getName(), "org.apache.hadoop.hive.ql.exec.spark.status.SparkJobMonitor"}));

    /* Patterns that are included in performance logging level.
     * In performance mode, show execution and performance logger messages.
     */
    private static final Pattern performanceIncludeNamePattern = Pattern.compile(
        executionIncludeNamePattern.pattern() + "|" + PerfLogger.class.getName());

    private void setCurrentNamePattern(OperationLog.LoggingLevel mode) {
      if (mode == OperationLog.LoggingLevel.VERBOSE) {
        this.namePattern = verboseExcludeNamePattern;
      } else if (mode == OperationLog.LoggingLevel.EXECUTION) {
        this.namePattern = executionIncludeNamePattern;
      } else if (mode == OperationLog.LoggingLevel.PERFORMANCE) {
        this.namePattern = performanceIncludeNamePattern;
      }
    }

    public NameFilter(OperationLog.LoggingLevel loggingMode, OperationManager op) {
      this.operationManager = op;
      this.loggingMode = loggingMode;
      setCurrentNamePattern(loggingMode);
    }

    @Override
    public Result filter(LogEvent event) {
      OperationLog log = operationManager.getOperationLogByThread();
      boolean excludeMatches = (loggingMode == OperationLog.LoggingLevel.VERBOSE);

      if (log == null) {
        return Result.DENY;
      }

      OperationLog.LoggingLevel currentLoggingMode = log.getOpLoggingLevel();
      // If logging is disabled, deny everything.
      if (currentLoggingMode == OperationLog.LoggingLevel.NONE) {
        return Result.DENY;
      }
      // Look at the current session's setting
      // and set the pattern and excludeMatches accordingly.
      if (currentLoggingMode != loggingMode) {
        loggingMode = currentLoggingMode;
        excludeMatches = (loggingMode == OperationLog.LoggingLevel.VERBOSE);
        setCurrentNamePattern(loggingMode);
      }

      boolean isMatch = namePattern.matcher(event.getLoggerName()).matches();

      if (excludeMatches == isMatch) {
        // Deny if this is black-list filter (excludeMatches = true) and it
        // matched or if this is whitelist filter and it didn't match
        return Result.DENY;
      }
      return Result.NEUTRAL;
    }
  }

  public static LogDivertAppender createInstance(OperationManager operationManager,
      OperationLog.LoggingLevel loggingMode) {
    return new LogDivertAppender("LogDivertAppender", new NameFilter(loggingMode, operationManager),
        new StringOutputStreamManager(new ByteArrayOutputStream(), "StringStream", null),
        operationManager, loggingMode);
  }

  public String getOutput() {
    return new String(manager.getStream().toByteArray());
  }

  @Override
  public void start() {
    super.start();
  }

  @Override
  public Layout<? extends Serializable> getLayout() {

    // If there is a logging level change from verbose->non-verbose or vice-versa since
    // the last subAppend call, change the layout to preserve consistency.
    OperationLog log = operationManager.getOperationLogByThread();
    if (log != null) {
      isVerbose = (log.getOpLoggingLevel() == OperationLog.LoggingLevel.VERBOSE);
    }

    // layout is immutable in log4j2, so we cheat here and return a different layout when
    // verbosity changes
    if (isVerbose) {
      return verboseLayout;
    } else {
      return layout == null ? nonVerboseLayout : layout;
    }
  }

  @Override
  public void append(LogEvent event) {
    super.append(event);

    String logOutput = getOutput();
    manager.reset();

    OperationLog log = operationManager.getOperationLogByThread();
    if (log == null) {
      LOG.debug(" ---+++=== Dropped log event from thread " + event.getThreadName());
      return;
    }
    log.writeOperationLog(logOutput);
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
