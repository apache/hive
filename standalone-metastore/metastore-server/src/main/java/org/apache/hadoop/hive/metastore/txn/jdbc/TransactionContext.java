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
package org.apache.hadoop.hive.metastore.txn.jdbc;

import org.springframework.lang.NonNull;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionStatus;

/**
 * Wraps the {@link TransactionStatus} object into an {@link AutoCloseable} object to allow using it in
 * try-with-resources block. If the {@link TransactionStatus#isCompleted()} is false upon exiting the try block,
 * the transaction will rolled back, otherwise nothing happens.
 * <b>In other words:</b> This wrapper automatically rolls back uncommitted transactions, but the commit
 * needs to be done manually using {@link TransactionContextManager#commit(TransactionContext)} method.
 */
public class TransactionContext implements TransactionStatus, AutoCloseable {

  private final TransactionStatus transactionStatus;
  private final TransactionContextManager transactionManager;

  TransactionContext(TransactionStatus transactionStatus, TransactionContextManager transactionManager) {
    this.transactionStatus = transactionStatus;
    this.transactionManager = transactionManager;
  }

  @Override
  public boolean hasSavepoint() {
    return transactionStatus.hasSavepoint();
  }

  @Override
  public void flush() {
    transactionStatus.flush();
  }

  @NonNull
  @Override
  public Object createSavepoint() throws TransactionException {
    return transactionStatus.createSavepoint();
  }

  @Override
  public void rollbackToSavepoint(@NonNull Object savepoint) throws TransactionException {
    transactionStatus.rollbackToSavepoint(savepoint);
  }

  @Override
  public void releaseSavepoint(@NonNull Object savepoint) throws TransactionException {
    transactionStatus.releaseSavepoint(savepoint);
  }

  @Override
  public boolean isNewTransaction() {
    return transactionStatus.isNewTransaction();
  }

  @Override
  public void setRollbackOnly() {
    transactionStatus.setRollbackOnly();
  }

  @Override
  public boolean isRollbackOnly() {
    return transactionStatus.isRollbackOnly();
  }

  @Override
  public boolean isCompleted() {
    return transactionStatus.isCompleted();
  }

  /**
   * @return Returns the {@link TransactionStatus} instance wrapped by this object.
   */
  TransactionStatus getTransactionStatus() {
    return transactionStatus;
  }

  /**
   * @see TransactionContext class level javadoc.
   */
  @Override
  public void close() {
    transactionManager.rollbackIfNotCommitted(this);
  }
}
