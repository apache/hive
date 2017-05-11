/**
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
package org.apache.hive.service.cli.operation;

import org.apache.hadoop.hive.ql.session.OperationLog;
import org.apache.hive.service.cli.CLIServiceUtils;
import org.apache.log4j.Appender;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.WriterAppender;
import org.apache.log4j.spi.Filter;
import org.apache.log4j.spi.LoggingEvent;

import java.io.CharArrayWriter;
import java.util.Enumeration;


/**
 * Divert appender to redirect and filter test operation logs to match the output of the original
 * CLI qtest results.
 */
public class LogDivertAppenderForTest extends WriterAppender {
  private static final Logger LOG = Logger.getLogger(LogDivertAppenderForTest.class.getName());
  private final OperationManager operationManager;
  /** This is where the log message will go to */
  private final CharArrayWriter writer = new CharArrayWriter();

  public LogDivertAppenderForTest(OperationManager operationManager) {
    setWriter(writer);
    setName("LogDivertAppenderForTest");
    this.operationManager = operationManager;
    addFilter(new LogDivertAppenderForTest.TestFilter());
    initLayout(false);
  }

  private void setLayout (boolean isVerbose, Layout lo) {
    if (isVerbose) {
      if (lo == null) {
        lo = CLIServiceUtils.verboseLayout;
        LOG.info("Cannot find a Layout from a ConsoleAppender. Using default Layout pattern.");
      }
    } else {
      lo = CLIServiceUtils.nonVerboseLayout;
    }
    setLayout(lo);
  }

  private void initLayout(boolean isVerbose) {
    // There should be a ConsoleAppender. Copy its Layout.
    Logger root = Logger.getRootLogger();
    Layout layout = null;

    Enumeration<?> appenders = root.getAllAppenders();
    while (appenders.hasMoreElements()) {
      Appender ap = (Appender) appenders.nextElement();
      if (ap.getClass().equals(ConsoleAppender.class)) {
        layout = ap.getLayout();
        break;
      }
    }
    setLayout(isVerbose, layout);
  }

  /**
   * A log filter that filters test messages coming from the logger.
   */
  private static class TestFilter extends Filter {
    @Override
    public int decide(LoggingEvent event) {
      if (event.getLevel().equals(Level.INFO) && "SessionState".equals(event.getLoggerName())) {
        if (event.getRenderedMessage().startsWith("PREHOOK:")
            || event.getRenderedMessage().startsWith("POSTHOOK:")
       	    || event.getRenderedMessage().startsWith("unix_timestamp(void)")
          	|| event.getRenderedMessage().startsWith("Warning: ")
            ) {
          return Filter.ACCEPT;
        }
      }
      return Filter.DENY;
    }
  }

  /**
   * Overrides WriterAppender.subAppend(), which does the real logging. No need
   * to worry about concurrency since log4j calls this synchronously.
   */
  @Override
  protected void subAppend(LoggingEvent event) {
    super.subAppend(event);
    // That should've gone into our writer. Notify the LogContext.
    String logOutput = writer.toString();
    writer.reset();

    OperationLog log = operationManager.getOperationLogByThread();
    if (log == null) {
      LOG.debug(" ---+++=== Dropped log event from thread " + event.getThreadName());
      return;
    }
    log.writeOperationLogForTest(logOutput);
  }
}
