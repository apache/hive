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
package org.apache.hadoop.hive.metastore.txn.retryhandling;

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.datanucleus.transaction.TransactionIsolation;

import java.util.function.Function;

/**
 * Contains all the properties which can alter the behavior of a retry-call in 
 * {@link org.apache.hadoop.hive.metastore.txn.retryhandling.RetryHandler}.
 */
public class RetryCallProperties {

  private String dataSource;
  private String caller = null;
  private boolean retryOnDuplicateKey = false;
  private int transactionIsolationLevel = TransactionIsolation.READ_COMMITTED;
  private boolean lockInternally = false;
  private boolean rollbackOnError = true;
  private Function<Exception, MetaException> exceptionSupplier =
      (e -> new MetaException("Failed to execute function: " + caller + ", details:" + e.getMessage()));

  /**
   * @param dataSource Sets the name of the datasource to use within the {@link DataSourceWrapper.RetryContext}
   * @return itself
   */
  public RetryCallProperties withDataSource(String dataSource) {
    this.dataSource = dataSource;
    return this;
  }

  /**
   * @param caller Sets the caller id to use during the retry-call
   * @return itself
   */
  public RetryCallProperties withCallerId(String caller) {
    this.caller = caller;
    return this;
  }

  /**
   * @param retryOnDuplicateKey Sets if the call should be retried on duplicate key error, false otherwise. 
   *                            The default value is false.
   * @return itself
   */
  public RetryCallProperties withRetryOnDuplicateKey(boolean retryOnDuplicateKey) {
    this.retryOnDuplicateKey = retryOnDuplicateKey;
    return this;
  }

  /**
   * @param transactionIsolationLevel Sets the transaction isolation level to use during the retry-call. The default value
   *                                  is {@link TransactionIsolation#READ_COMMITTED}.
   * @return itself
   */
  public RetryCallProperties withTransactionIsolationLevel(int transactionIsolationLevel) {
    this.transactionIsolationLevel = transactionIsolationLevel;
    return this;
  }

  /**
   * @param lockInternally Sets if the internal (derby-related) lock mechanism has to be used during the retry-call.
   *                       The default value is false.
   * @return itself
   */
  public RetryCallProperties withLockInternally(boolean lockInternally) {
    this.lockInternally = lockInternally;
    return this;
  }

  /**
   * @param rollbackOnError Sets if the transaction needs to be rolled back in case of exception during the retry-call.
   *                        The default value is true.
   * @return itself
   */
  public RetryCallProperties withRollbackOnError(boolean rollbackOnError) {
    this.rollbackOnError = rollbackOnError;
    return this;
  }

  /**
   * @param exceptionSupplier Sets a function responsuble for creating a {@link MetaException} from any oher 
   *                          {@link Exception}. The default supplier will create a {@link MetaException} with the 
   *                          following message: Failed to execute function: <b>caller</b>, details: <b>exception message</b>
   * @return itself
   */
  public RetryCallProperties withExceptionSupplier(Function<Exception, MetaException> exceptionSupplier) {
    this.exceptionSupplier = exceptionSupplier;
    return this;
  }

  String getDataSource() {
    return dataSource;
  }

  String getCaller() {
    return caller;
  }

  boolean isRetryOnDuplicateKey() {
    return retryOnDuplicateKey;
  }

  int getTransactionIsolationLevel() {
    return transactionIsolationLevel;
  }

  boolean isLockInternally() {
    return lockInternally;
  }

  boolean isRollbackOnError() {
    return rollbackOnError;
  }

  Function<Exception, MetaException> getExceptionSupplier() {
    return exceptionSupplier;
  }

  @Override
  public String toString() {
    return "[Id:" + caller
        + ", retryOnDuplicateKey:" + retryOnDuplicateKey
        + ", transactionIsolationLevel:" + transactionIsolationLevel 
        + ", rollbackOnError:" + rollbackOnError
        + "]";
  }

}
