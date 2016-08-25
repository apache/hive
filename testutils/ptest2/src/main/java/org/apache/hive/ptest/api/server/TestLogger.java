/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.ptest.api.server;

import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.slf4j.helpers.FormattingTuple;
import org.slf4j.helpers.MarkerIgnoringBase;
import org.slf4j.helpers.MessageFormatter;

/**
 * Simple logger which allows each test to have it's own log file.
 */
public class TestLogger extends MarkerIgnoringBase {

  private static final long serialVersionUID = -1711679924980202258L;
  private final LEVEL mLevel;
  private final PrintStream mLog;
  private SimpleDateFormat mDateFormatter;

  public TestLogger(PrintStream logFile, LEVEL level) {
    mLog = logFile;
    mLevel = level;
    mDateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
  }

  public static enum LEVEL {
    TRACE(1),
    DEBUG(2),
    INFO(3),
    WARN(4),
    ERROR(5);
    private int index;
    private LEVEL(int index) {
      this.index = index;
    }
  }
  @Override
  public boolean isTraceEnabled() {
    return mLevel.index >= LEVEL.TRACE.index;
  }

  @Override
  public void trace(String msg) {
    log(LEVEL.TRACE, msg, null);
  }

  @Override
  public void trace(String format, Object arg) {
    FormattingTuple ft = MessageFormatter.format(format, arg);
    log(LEVEL.TRACE, ft.getMessage(), ft.getThrowable());
  }

  @Override
  public void trace(String format, Object arg1, Object arg2) {
    FormattingTuple ft = MessageFormatter.format(format, arg1, arg2);
    log(LEVEL.TRACE, ft.getMessage(), ft.getThrowable());
  }

  @Override
  public void trace(String format, Object[] argArray) {
    FormattingTuple ft = MessageFormatter.arrayFormat(format, argArray);
    log(LEVEL.TRACE, ft.getMessage(), ft.getThrowable());
  }

  @Override
  public void trace(String msg, Throwable t) {
    log(LEVEL.TRACE, msg, t);
  }

  @Override
  public boolean isDebugEnabled() {
    return mLevel.index >= LEVEL.DEBUG.index;
  }

  @Override
  public void debug(String msg) {
    log(LEVEL.DEBUG, msg, null);
  }

  @Override
  public void debug(String format, Object arg) {
    FormattingTuple ft = MessageFormatter.format(format, arg);
    log(LEVEL.DEBUG, ft.getMessage(), ft.getThrowable());
  }

  @Override
  public void debug(String format, Object arg1, Object arg2) {
    FormattingTuple ft = MessageFormatter.format(format, arg1, arg2);
    log(LEVEL.DEBUG, ft.getMessage(), ft.getThrowable());
  }

  @Override
  public void debug(String format, Object[] argArray) {
    FormattingTuple ft = MessageFormatter.arrayFormat(format, argArray);
    log(LEVEL.DEBUG, ft.getMessage(), ft.getThrowable());
  }

  @Override
  public void debug(String msg, Throwable t) {
    log(LEVEL.DEBUG, msg, t);
  }

  @Override
  public boolean isInfoEnabled() {
    return mLevel.index >= LEVEL.INFO.index;
  }

  @Override
  public void info(String msg) {
    log(LEVEL.INFO, msg, null);
  }

  @Override
  public void info(String format, Object arg) {
    FormattingTuple ft = MessageFormatter.format(format, arg);
    log(LEVEL.INFO, ft.getMessage(), ft.getThrowable());
  }

  @Override
  public void info(String format, Object arg1, Object arg2) {
    FormattingTuple ft = MessageFormatter.format(format, arg1, arg2);
    log(LEVEL.INFO, ft.getMessage(), ft.getThrowable());
  }

  @Override
  public void info(String format, Object[] argArray) {
    FormattingTuple ft = MessageFormatter.arrayFormat(format, argArray);
    log(LEVEL.INFO, ft.getMessage(), ft.getThrowable());
  }

  @Override
  public void info(String msg, Throwable t) {
    log(LEVEL.INFO, msg, t);
  }

  @Override
  public boolean isWarnEnabled() {
    return mLevel.index >= LEVEL.WARN.index;
  }
  @Override
  public void warn(String msg) {
    log(LEVEL.WARN, msg, null);
  }

  @Override
  public void warn(String format, Object arg) {
    FormattingTuple ft = MessageFormatter.format(format, arg);
    log(LEVEL.WARN, ft.getMessage(), ft.getThrowable());
  }

  @Override
  public void warn(String format, Object arg1, Object arg2) {
    FormattingTuple ft = MessageFormatter.format(format, arg1, arg2);
    log(LEVEL.WARN, ft.getMessage(), ft.getThrowable());
  }

  @Override
  public void warn(String format, Object[] argArray) {
    FormattingTuple ft = MessageFormatter.arrayFormat(format, argArray);
    log(LEVEL.WARN, ft.getMessage(), ft.getThrowable());
  }

  @Override
  public void warn(String msg, Throwable t) {
    log(LEVEL.WARN, msg, t);
  }

  @Override
  public boolean isErrorEnabled() {
    return mLevel.index >= LEVEL.ERROR.index;
  }

  @Override
  public void error(String msg) {
    log(LEVEL.ERROR, msg, null);
  }

  @Override
  public void error(String format, Object arg) {
    FormattingTuple ft = MessageFormatter.format(format, arg);
    log(LEVEL.ERROR, ft.getMessage(), ft.getThrowable());
  }

  @Override
  public void error(String format, Object arg1, Object arg2) {
    FormattingTuple ft = MessageFormatter.format(format, arg1, arg2);
    log(LEVEL.ERROR, ft.getMessage(), ft.getThrowable());
  }

  @Override
  public void error(String format, Object[] argArray) {
    FormattingTuple ft = MessageFormatter.arrayFormat(format, argArray);
    log(LEVEL.ERROR, ft.getMessage(), ft.getThrowable());
  }

  @Override
  public void error(String msg, Throwable t) {
    log(LEVEL.ERROR, msg, t);
  }

  private String getCaller() {
    StackTraceElement[] stack = new Exception().getStackTrace();
    if(stack.length > 3) {
      return getCallerShortName(stack[3]);
    }
    return "<unknown>";
  }

  private String getThreadName() {
    return Thread.currentThread().getName();
  }

  private String getCallerShortName(StackTraceElement frame) {
    String className = frame.getClassName();
    String methodName = frame.getMethodName();
    int lineNumber = frame.getLineNumber();
    int pos = className.lastIndexOf(".");
    if(pos > 0) {
      className = className.substring(pos + 1);
    }
    return String.format("%s.%s:%d", className, methodName, lineNumber);
  }

  private synchronized void log(LEVEL level, String msg, Throwable t) {
    if(level.index >= mLevel.index) {
      mLog.print(mDateFormatter.format(new Date()));
      mLog.print(" ");
      mLog.print(String.format("%5s", level.name()));
      mLog.print(" ");
      mLog.print("[");
      mLog.print(getThreadName());
      mLog.print("]");
      mLog.print(" ");
      mLog.print(getCaller());
      mLog.print(" ");
      mLog.print(msg);
      if(t != null) {
        mLog.print(" ");
        t.printStackTrace(mLog);
      }
      mLog.print("\n");
      mLog.flush();
    }
  }
}
