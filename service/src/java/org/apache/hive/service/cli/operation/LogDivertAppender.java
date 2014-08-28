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
import java.io.CharArrayWriter;

import org.apache.log4j.Layout;
import org.apache.log4j.Logger;
import org.apache.log4j.WriterAppender;
import org.apache.log4j.spi.Filter;
import org.apache.log4j.spi.LoggingEvent;

/**
 * An Appender to divert logs from individual threads to the LogObject they belong to.
 */
public class LogDivertAppender extends WriterAppender {
  private static final Logger LOG = Logger.getLogger(LogDivertAppender.class.getName());
  private final OperationManager operationManager;

  /**
   * A log filter that exclude messages coming from the logger with the given name.
   * We apply this filter on the Loggers used by the log diversion stuff, so that
   * they don't generate more logs for themselves when they process logs.
   */
  private static class NameExclusionFilter extends Filter {
    private String excludeLoggerName = null;

    public NameExclusionFilter(String excludeLoggerName) {
      this.excludeLoggerName = excludeLoggerName;
    }

    @Override
    public int decide(LoggingEvent ev) {
      if (ev.getLoggerName().equals(excludeLoggerName)) {
        return Filter.DENY;
      }
      return Filter.NEUTRAL;
    }
  }

  /** This is where the log message will go to */
  private final CharArrayWriter writer = new CharArrayWriter();

  public LogDivertAppender(Layout layout, OperationManager operationManager) {
    setLayout(layout);
    setWriter(writer);
    setName("LogDivertAppender");
    this.operationManager = operationManager;

    // Filter out messages coming from log processing classes, or we'll run an infinite loop.
    addFilter(new NameExclusionFilter(LOG.getName()));
    addFilter(new NameExclusionFilter(OperationLog.class.getName()));
    addFilter(new NameExclusionFilter(OperationManager.class.getName()));
  }

  /**
   * Overrides WriterAppender.subAppend(), which does the real logging.
   * No need to worry about concurrency since log4j calls this synchronously.
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
    log.writeOperationLog(logOutput);
  }
}
