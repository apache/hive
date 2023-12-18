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
package org.apache.hadoop.hive.metastore.txn.retry;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.DatabaseProduct;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.txn.MetaWrapperException;
import org.apache.hadoop.hive.metastore.utils.StackThreadLocal;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.UncategorizedSQLException;

import java.sql.SQLException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

/**
 * Supports building resilient APIs by automatically retrying failed function calls.
 */
public class SqlRetryHandler {

  private static final Logger LOG = LoggerFactory.getLogger(SqlRetryHandler.class);

  private static final int ALLOWED_REPEATED_DEADLOCKS = 10;
  static final String MANUAL_RETRY = "ManualRetry";

  private final StackThreadLocal<Object> threadLocal = new StackThreadLocal<>();

  /**
   * Derby specific concurrency control
   */
  private static final ReentrantLock derbyLock = new ReentrantLock(true);

  private final DatabaseProduct databaseProduct;
  private final long deadlockRetryInterval;
  private final long retryInterval;
  private final int retryLimit;
  
  private final Configuration conf;
  
  public static String getMessage(Exception ex) {
    StringBuilder sb = new StringBuilder();
    sb.append(ex.getMessage());
    if (ex instanceof SQLException) {
      SQLException sqlEx = (SQLException)ex; 
      sb.append(" (SQLState=").append(sqlEx.getSQLState()).append(", ErrorCode=").append(sqlEx.getErrorCode()).append(")");
    }
    return sb.toString();
  }
  
  public SqlRetryHandler(Configuration conf, DatabaseProduct databaseProduct){
    this.conf = conf;
    this.databaseProduct = databaseProduct;

    retryInterval = MetastoreConf.getTimeVar(conf, MetastoreConf.ConfVars.HMS_HANDLER_INTERVAL,
        TimeUnit.MILLISECONDS);
    retryLimit = MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.HMS_HANDLER_ATTEMPTS);
    deadlockRetryInterval = retryInterval / 10;
  }

  /**
   * Executes the passed {@link SqlRetryFunction} and automatically retries the execution in case of failure. If there is
   * a retry-call upper in the call hierarchy, {@link SqlRetryCallProperties#withRetryPropagation } can be used to 
   * control wether to join it or create a nested retry-call.
   * @param properties {@link SqlRetryCallProperties} instance used to fine-tune/alter the execution-mechanism.
   * @param function The {@link SqlRetryFunction} to execute.
   * @return Returns with the result of the execution of the passed {@link SqlRetryFunction}.
   * @param <Result> Type of the result
   * @throws MetaException Thrown in case of execution error.
   */
  public <Result> Result executeWithRetry(SqlRetryCallProperties properties, SqlRetryFunction<Result> function) throws TException {
    Objects.requireNonNull(function, "RetryFunction<Result> cannot be null!");
    Objects.requireNonNull(properties, "RetryCallProperties cannot be null!");

    if (threadLocal.isSet() && properties.getRetryPropagation().canJoinContext()) {
      /*
        If there is a context in the ThreadLocal and we are allowed to join it, we can skip establishing a nested retry-call.
      */
      LOG.info("Already established retry-call detected, current call will join it. The passed SqlRetryCallProperties " +
          "instance will be ignored, using the original one.");
      try {
        return function.execute();
      } catch (SQLException e) {
        //Since we are in a nested call, This will be caught in the below exception handler.
        throw new UncategorizedSQLException(null, null, e);
      }
    }

    if (!properties.getRetryPropagation().canCreateContext()) {
      throw new MetaException("The current RetryPropagation mode (" + properties.getRetryPropagation().name() +
          ") allows only to join an existing retry-call, but there is no retry-call upper in the callstack!");
    }

    if (StringUtils.isEmpty(properties.getCaller())) {
      properties.withCallerId(function.toString());
    }
    properties
        .setRetryCount(retryLimit)
        .setDeadlockCount(ALLOWED_REPEATED_DEADLOCKS);
    
    try {
      if (properties.isLockInternally()) {
        lockInternal();
      }
      threadLocal.set(new Object());
      return executeWithRetryInternal(properties, function);
    } finally {
      threadLocal.unset();
      if (properties.isLockInternally()) {
        unlockInternal();
      }
    }
  }

  private <Result> Result executeWithRetryInternal(SqlRetryCallProperties properties, SqlRetryFunction<Result> function) 
      throws TException {
    LOG.debug("Running retry function:" + properties);

    try {
      return function.execute();
    } catch (DataAccessException | SQLException e) {
      SQLException sqlEx = null;
      if (e.getCause() instanceof SQLException) {
        sqlEx = (SQLException) e.getCause();
      } else if (e instanceof SQLException) {
        sqlEx = (SQLException) e;
      }      
      if (sqlEx != null) {
        if (checkDeadlock(sqlEx, properties)) {
          properties.setDeadlockCount(properties.getDeadlockCount() - 1); 
          return executeWithRetryInternal(properties, function);
        }
        if (checkRetryable(sqlEx, properties)) {
          properties.setRetryCount(properties.getRetryCount() - 1);
          return executeWithRetryInternal(properties, function);
        }
      }
      LOG.error("Execution failed for caller {}", properties, e);
      throw new MetaException("Failed to execute function: " + properties.getCaller() + ", details:" + e.getMessage());
    } catch (MetaWrapperException e) {
      //unwrap and re-throw
      LOG.error("Execution failed for caller {}", properties, e.getCause());
      throw (MetaException) e.getCause();
    }
  }
  
  private boolean checkDeadlock(SQLException e, SqlRetryCallProperties properties) {
    if (databaseProduct.isDeadlock(e)) {
      if (properties.getDeadlockCount() > 0) {
        long waitInterval = deadlockRetryInterval * (ALLOWED_REPEATED_DEADLOCKS - properties.getDeadlockCount());
        LOG.warn("Deadlock detected in {}. Will wait {} ms try again up to {} times.", 
            properties.getCaller(), waitInterval, properties.getDeadlockCount());
        // Pause for a just a bit for retrying to avoid immediately jumping back into the deadlock.
        try {
          Thread.sleep(waitInterval);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          // NOP
        }
        return true;
      } else {
        LOG.error("Too many repeated deadlocks in {}, giving up.", properties.getCaller());
      }
    }
    return false;
  }
  
  /**
   * Determine if an exception was such that it makes sense to retry.  Unfortunately there is no standard way to do
   * this, so we have to inspect the error messages and catch the telltale signs for each
   * different database.
   * if the error is retry-able.
   * @param e exception that was thrown.
   * @param properties {@link SqlRetryCallProperties} instance containing the rety call settings
   * @return True if retry is possible, false otherwise.
   */
  private boolean checkRetryable(SQLException e, SqlRetryCallProperties properties) {

    LOG.error("Execution error, checking if retry is possible", e);
    
    // If you change this function, remove the @Ignore from TestTxnHandler.deadlockIsDetected()
    // to test these changes.
    // MySQL and MSSQL use 40001 as the state code for rollback.  Postgres uses 40001 and 40P01.
    // Oracle seems to return different SQLStates and messages each time,
    // so I've tried to capture the different error messages (there appear to be fewer different
    // error messages than SQL states).
    // Derby and newer MySQL driver use the new SQLTransactionRollbackException
    boolean retry = false;
    if (isRetryable(conf, e)) {
      //in MSSQL this means Communication Link Failure
      retry = waitForRetry(properties.getCaller(), e.getMessage(), properties.getRetryCount());
    } else if (properties.isRetryOnDuplicateKey() && databaseProduct.isDuplicateKeyError(e)) {
      retry = waitForRetry(properties.getCaller(), e.getMessage(), properties.getRetryCount());
    } else {
      //make sure we know we saw an error that we don't recognize
      LOG.info("Non-retryable error in {} : {}", properties.getCaller(), getMessage(e));
    }
    return retry;
  }

  private boolean waitForRetry(String caller, String errMsg, int retryCount) {
    if (retryCount > 0) {
      LOG.warn("Retryable error detected in {}. Will wait {} ms and retry up to {} times. Error: {}", caller,
          retryInterval, retryCount, errMsg);
      try {
        Thread.sleep(retryInterval);
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }
      return true;
    } else {
      LOG.error("Fatal error in {}. Retry limit ({}) reached. Last error: {}", caller, retryLimit, errMsg);
    }
    return false;
  }

  /**
   * Returns true if {@code ex} should be retried
   */
  static boolean isRetryable(Configuration conf, Exception ex) {
    if(ex instanceof SQLException) {
      SQLException sqlException = (SQLException)ex;
      if (MANUAL_RETRY.equalsIgnoreCase(sqlException.getSQLState())) {
        // Manual retry exception was thrown
        return true;
      }
      if ("08S01".equalsIgnoreCase(sqlException.getSQLState())) {
        //in MSSQL this means Communication Link Failure
        return true;
      }
      if ("ORA-08176".equalsIgnoreCase(sqlException.getSQLState()) ||
          sqlException.getMessage().contains("consistent read failure; rollback data not available")) {
        return true;
      }

      String regex = MetastoreConf.getVar(conf, MetastoreConf.ConfVars.TXN_RETRYABLE_SQLEX_REGEX);
      if (regex != null && !regex.isEmpty()) {
        String[] patterns = regex.split(",(?=\\S)");
        String message = getMessage(ex);
        for (String p : patterns) {
          if (Pattern.matches(p, message)) {
            return true;
          }
        }
      }
      //see also https://issues.apache.org/jira/browse/HIVE-9938
    }
    return false;
  }

  /**
   * lockInternal() and {@link #unlockInternal()} are used to serialize those operations that require
   * Select ... For Update to sequence operations properly.  In practice that means when running
   * with Derby database.  See more notes at class level.
   */
  private void lockInternal() {
    if(databaseProduct.isDERBY()) {
      derbyLock.lock();
    }
  }
  private void unlockInternal() {
    if(databaseProduct.isDERBY()) {
      derbyLock.unlock();
    }
  }

}
