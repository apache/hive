/*
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
  // If in test mode then the LogDivertAppenderForTest created an extra log file containing only
  // the output needed for the qfile results.
  private final LogFile testLogFile;
  // True if we are running test and the extra test file should be used when the logs are
  // requested.
  private final boolean isShortLogs;
  // True if the logs should be removed after the operation. Should be used only in test mode
  private final boolean isRemoveLogs;
  private LoggingLevel opLoggingLevel = LoggingLevel.UNKNOWN;

  public enum LoggingLevel {
    NONE, EXECUTION, PERFORMANCE, VERBOSE, UNKNOWN
  }

  public OperationLog(String name, File file, HiveConf hiveConf) {
    operationName = name;
    logFile = new LogFile(file);

    if (hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_LOGGING_OPERATION_ENABLED)) {
      String logLevel = hiveConf.getVar(HiveConf.ConfVars.HIVE_SERVER2_LOGGING_OPERATION_LEVEL);
      opLoggingLevel = getLoggingLevel(logLevel);
    }

    // If in test mod create a test log file which will contain only logs which are supposed to
    // be written to the qtest output
    if (hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_IN_TEST)) {
      isRemoveLogs = hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_TESTING_REMOVE_LOGS);
      if (hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_TESTING_SHORT_LOGS)) {
        testLogFile = new LogFile(new File(file.getAbsolutePath() + ".test"));
        isShortLogs = true;
      } else {
        testLogFile = null;
        isShortLogs = false;
      }
    } else {
      testLogFile = null;
      isShortLogs = false;
      isRemoveLogs = true;
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
   * Read operation execution logs from log file
   * @param isFetchFirst true if the Enum FetchOrientation value is Fetch_First
   * @param maxRows the max number of fetched lines from log
   * @return
   * @throws java.sql.SQLException
   */
  public List<String> readOperationLog(boolean isFetchFirst, long maxRows)
      throws SQLException {
    if (isShortLogs) {
      return testLogFile.read(isFetchFirst, maxRows);
    } else {
      return logFile.read(isFetchFirst, maxRows);
    }
  }

  /**
   * Close this OperationLog when operation is closed. The log file will be removed.
   */
  public void close() {
    if (isShortLogs) {
      // In case of test, do just close the log files, do not remove them.
      logFile.close(isRemoveLogs);
      testLogFile.close(isRemoveLogs);
    } else {
      logFile.close(true);
    }
  }

  /**
   * Wrapper for read the operation log file
   */
  private class LogFile {
    private final File file;
    private BufferedReader in;
    private volatile boolean isRemoved;

    LogFile(File file) {
      this.file = file;
      isRemoved = false;
    }

    synchronized List<String> read(boolean isFetchFirst, long maxRows)
        throws SQLException{
      // reset the BufferReader, if fetching from the beginning of the file
      if (isFetchFirst) {
        resetIn();
      }

      return readResults(maxRows);
    }

    /**
     * Close the logs, and remove them if specified.
     * @param removeLog If true, remove the log file
     */
    synchronized void close(boolean removeLog) {
      try {
        if (in != null) {
          in.close();
        }
        if (!isRemoved && removeLog && file.exists()) {
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
      List<String> logs = new ArrayList<String>();
      if (in == null) {
        try {
          in = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
          // Adding name of the log file in an extra log line, so it is easier to find
          // the original if there is a test error
          if (isShortLogs) {
            logs.add("Reading log file: " + file);
            nLines--;
          }
        } catch (FileNotFoundException e) {
          return logs;
        }
      }

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
  }
}
