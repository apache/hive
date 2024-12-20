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

package org.apache.hadoop.hive.ql;

import java.io.IOException;

import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility functions for the Driver.
 */
public final class DriverUtils {
  private static final String CLASS_NAME = Driver.class.getName();
  private static final Logger LOG = LoggerFactory.getLogger(CLASS_NAME);
  private static final LogHelper CONSOLE = new LogHelper(LOG);

  private DriverUtils() {
    throw new UnsupportedOperationException("DriverUtils should not be instantiated!");
  }

  @FunctionalInterface
  private interface DriverCreator {
    Driver createDriver(QueryState qs);
  }

  /**
   * For Query Based compaction to run the query to generate the compacted data.
   */
  public static void runOnDriver(HiveConf conf, SessionState sessionState, String query) throws HiveException {
    runOnDriverInternal(query, conf, sessionState, Driver::new);
  }

  private static void runOnDriverInternal(String query, HiveConf conf, SessionState sessionState, DriverCreator creator) throws HiveException {
    SessionState.setCurrentSessionState(sessionState);
    boolean isOk = false;
    try {
      Driver driver = creator.createDriver(
          new QueryState.Builder()
              .withHiveConf(conf)
              .withGenerateNewQueryId(true)
              .nonIsolated()
              .build());
      try {
        try {
          driver.run(query);
        } catch (CommandProcessorException e) {
          LOG.error("Failed to run " + query, e);
          throw new HiveException("Failed to run " + query, e);
        }
      } finally {
        driver.close();
        driver.destroy();
      }
      isOk = true;
    } finally {
      if (!isOk) {
        try {
          sessionState.close(); // This also resets SessionState.get.
        } catch (Throwable th) {
          LOG.warn("Failed to close a bad session", th);
          SessionState.detachSession();
        }
      }
    }
  }

  public static SessionState setUpSessionState(HiveConf conf, String user, boolean doStart) {
    SessionState sessionState = SessionState.get();
    if (sessionState == null) {
      // Note: we assume that workers run on the same threads repeatedly, so we can set up
      //       the session here and it will be reused without explicitly storing in the worker.
      sessionState = new SessionState(conf, user);
      if (doStart) {
        // TODO: Required due to SessionState.getHDFSSessionPath. Why wasn't it required before?
        sessionState.setIsHiveServerQuery(true);
        SessionState.start(sessionState);
      }
      SessionState.setCurrentSessionState(sessionState);
    }
    else {
      sessionState.setConf(conf);
    }
    return sessionState;
  }

  public static void checkInterrupted(DriverState driverState, DriverContext driverContext, String msg,
      HookContext hookContext, PerfLogger perfLogger) throws CommandProcessorException {
    if (driverState.isAborted()) {
      String errorMessage = "FAILED: command has been interrupted: " + msg;
      CONSOLE.printError(errorMessage);
      if (hookContext != null) {
        try {
          invokeFailureHooks(driverContext, perfLogger, hookContext, errorMessage, null);
        } catch (Exception e) {
          LOG.warn("Caught exception attempting to invoke Failure Hooks", e);
        }
      }
      throw createProcessorException(driverContext, 1000, errorMessage, "HY008", null);
    }
  }

  public static void invokeFailureHooks(DriverContext driverContext, PerfLogger perfLogger, HookContext hookContext,
      String errorMessage, Throwable exception) throws Exception {
    hookContext.setHookType(HookContext.HookType.ON_FAILURE_HOOK);
    hookContext.setErrorMessage(errorMessage);
    hookContext.setException(exception);
    // Get all the failure execution hooks and execute them.
    driverContext.getHookRunner().runFailureHooks(hookContext);
  }

  public static void handleHiveException(DriverContext driverContext, HiveException e, int ret, String rootMsg)
      throws CommandProcessorException {
    String errorMessage = "FAILED: Hive Internal Error: " + Utilities.getNameMessage(e);
    if (rootMsg != null) {
      errorMessage += "\n" + rootMsg;
    }
    String sqlState = e.getCanonicalErrorMsg() != null ?
        e.getCanonicalErrorMsg().getSQLState() : ErrorMsg.findSQLState(e.getMessage());
    CONSOLE.printError(errorMessage + "\n" + StringUtils.stringifyException(e));
    throw DriverUtils.createProcessorException(driverContext, ret, errorMessage, sqlState, e);
  }

  public static CommandProcessorException createProcessorException(DriverContext driverContext, int ret,
      String errorMessage, String sqlState, Throwable downstreamError) {
    SessionState.getPerfLogger().cleanupPerfLogMetrics();
    driverContext.getQueryDisplay().setErrorMessage(errorMessage);
    if (downstreamError != null && downstreamError instanceof HiveException) {
      ErrorMsg em = ((HiveException)downstreamError).getCanonicalErrorMsg();
      if (em != null) {
        return new CommandProcessorException(ret, em.getErrorCode(), errorMessage, sqlState, downstreamError);
      }
    }
    return new CommandProcessorException(ret, -1, errorMessage, sqlState, downstreamError);
  }

  public static boolean checkConcurrency(DriverContext driverContext) {
    return driverContext.getConf().getBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY);
  }

  public static String getUserFromUGI(DriverContext driverContext) throws CommandProcessorException {
    // Don't use the userName member, as it may or may not have been set.  Get the value from
    // conf, which calls into getUGI to figure out who the process is running as.
    try {
      return driverContext.getConf().getUser();
    } catch (IOException e) {
      String errorMessage = "FAILED: Error in determining user while acquiring locks: " + e.getMessage();
      CONSOLE.printError(errorMessage, "\n" + StringUtils.stringifyException(e));
      throw createProcessorException(driverContext, 10, errorMessage, ErrorMsg.findSQLState(e.getMessage()), e);
    }
  }
}
