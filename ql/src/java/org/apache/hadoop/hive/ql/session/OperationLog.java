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
package org.apache.hadoop.hive.ql.session;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * OperationLog wraps the actual operation log file, and provides interface
 * for accessing, reading, writing, and removing the file.
 */
public class OperationLog {
  private static final Logger LOG = LoggerFactory.getLogger(OperationLog.class.getName());

  private final String operationName;
  private final LogFile logFile;
  private LoggingLevel opLoggingLevel = LoggingLevel.UNKNOWN;

  public PrintStream getPrintStream() {
    return logFile.getPrintStream();
  }

  public enum LoggingLevel {
    NONE, EXECUTION, PERFORMANCE, VERBOSE, UNKNOWN
  }

  public OperationLog(String name, File file, HiveConf hiveConf) throws FileNotFoundException {
    operationName = name;
    logFile = new LogFile(file);

    if (hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_LOGGING_OPERATION_ENABLED)) {
      String logLevel = hiveConf.getVar(HiveConf.ConfVars.HIVE_SERVER2_LOGGING_OPERATION_LEVEL);
      opLoggingLevel = getLoggingLevel(logLevel);
    }
  }

  public static LoggingLevel getLoggingLevel (String mode) {
    if (mode.equalsIgnoreCase("none")) {
      return LoggingLevel.NONE;
    } else if (mode.equalsIgnoreCase("execution")) {
      return LoggingLevel.EXECUTION;
    } else if (mode.equalsIgnoreCase("verbose")) {
      return LoggingLevel.VERBOSE;
    } else if (mode.equalsIgnoreCase("performance")) {
      return LoggingLevel.PERFORMANCE;
    } else {
      return LoggingLevel.UNKNOWN;
    }
  }

  public LoggingLevel getOpLoggingLevel() {
    return opLoggingLevel;
  }

  /**
   * Singleton OperationLog object per thread.
   */
  private static final ThreadLocal<OperationLog> THREAD_LOCAL_OPERATION_LOG = new
      ThreadLocal<OperationLog>() {
    @Override
    protected OperationLog initialValue() {
      return null;
    }
  };

  public static void setCurrentOperationLog(OperationLog operationLog) {
    THREAD_LOCAL_OPERATION_LOG.set(operationLog);
  }

  public static OperationLog getCurrentOperationLog() {
    return THREAD_LOCAL_OPERATION_LOG.get();
  }

  public static void removeCurrentOperationLog() {
    THREAD_LOCAL_OPERATION_LOG.remove();
  }

  /**
   * Write operation execution logs into log file
   * @param operationLogMessage one line of log emitted from log4j
   */
  public void writeOperationLog(String operationLogMessage) {
    logFile.write(operationLogMessage);
  }

  /**
   * Write operation execution logs into log file
   * @param operationLogMessage one line of log emitted from log4j
   */
  public void writeOperationLog(LoggingLevel level, String operationLogMessage) {
    if (opLoggingLevel.compareTo(level) < 0) return;
    logFile.write(operationLogMessage);
  }


  /**
   * Read operation execution logs from log file
   * @param isFetchFirst true if the Enum FetchOrientation value is Fetch_First
   * @param maxRows the max number of fetched lines from log
   * @return
   * @throws java.sql.SQLException
   */
  public List<String> readOperationLog(boolean isFetchFirst, long maxRows)
      throws SQLException{
    return logFile.read(isFetchFirst, maxRows);
  }

  /**
   * Close this OperationLog when operation is closed. The log file will be removed.
   */
  public void close() {
    logFile.remove();
  }

  /**
   * Wrapper for read/write the operation log file
   */
  private class LogFile {
    private final File file;
    private BufferedReader in;
    private final PrintStream out;
    private volatile boolean isRemoved;

    LogFile(File file) throws FileNotFoundException {
      this.file = file;
      in = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
      out = new PrintStream(new FileOutputStream(file));
      isRemoved = false;
    }

    synchronized void write(String msg) {
      // write log to the file
      out.print(msg);
    }

    synchronized List<String> read(boolean isFetchFirst, long maxRows)
        throws SQLException{
      // reset the BufferReader, if fetching from the beginning of the file
      if (isFetchFirst) {
        resetIn();
      }

      return readResults(maxRows);
    }

    synchronized void remove() {
      try {
        if (in != null) {
          in.close();
        }
        if (out != null) {
          out.close();
        }
        if (!isRemoved) {
          FileUtils.forceDelete(file);
          isRemoved = true;
        }
      } catch (Exception e) {
        LOG.error("Failed to remove corresponding log file of operation: " + operationName, e);
      }
    }

    private void resetIn() {
      if (in != null) {
        IOUtils.closeStream(in);
        in = null;
      }
    }

    private List<String> readResults(long nLines) throws SQLException {
      if (in == null) {
        try {
          in = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
        } catch (FileNotFoundException e) {
          if (isRemoved) {
            throw new SQLException("The operation has been closed and its log file " +
                file.getAbsolutePath() + " has been removed.", e);
          } else {
            throw new SQLException("Operation Log file " + file.getAbsolutePath() +
                " is not found.", e);
          }
        }
      }

      List<String> logs = new ArrayList<String>();
      String line = "";
      // if nLines <= 0, read all lines in log file.
      for (int i = 0; i < nLines || nLines <= 0; i++) {
        try {
          line = in.readLine();
          if (line == null) {
            break;
          } else {
            logs.add(line);
          }
        } catch (IOException e) {
          if (isRemoved) {
            throw new SQLException("The operation has been closed and its log file " +
                file.getAbsolutePath() + " has been removed.", e);
          } else {
            throw new SQLException("Reading operation log file encountered an exception: ", e);
          }
        }
      }
      return logs;
    }

    public PrintStream getPrintStream() {
      return out;
    }
  }
}
