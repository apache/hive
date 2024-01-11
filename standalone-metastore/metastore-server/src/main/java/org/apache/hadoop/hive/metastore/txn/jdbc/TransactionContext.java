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

import org.springframework.transaction.TransactionStatus;

/**
 * Wraps the {@link TransactionStatus} object into an {@link AutoCloseable} object to allow using it in
 * try-with-resources block. If the {@link TransactionStatus#isCompleted()} is false upon exiting the try block,
 * the transaction will rolled back, otherwise nothing happens.
 * <b>In other words:</b> This wrapper automatically rolls back uncommitted transactions, but the commit
 * needs to be done manually using {@link TransactionContextManager#commit(TransactionContext)} method.
 */
public class TransactionContext implements AutoCloseable {

  private final TransactionStatus transactionStatus;
  private final TransactionContextManager transactionManager;

  TransactionContext(TransactionStatus transactionStatus, TransactionContextManager transactionManager) {
    this.transactionStatus = transactionStatus;
    this.transactionManager = transactionManager;
  }

  /**
   * @return Returns the {@link TransactionStatus} instance wrapped by this object.
   */
  public TransactionStatus getTransactionStatus() {
    return transactionStatus;
  }

  /**
   * @see TransactionContext TransactionWrapper class level javadoc.
   */
  @Override
  public void close() {
    transactionManager.rollbackIfNotCommitted(this);
  }
}
