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

import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;

/**
 * A special TransactionManager which wraps a {@link PlatformTransactionManager} instance, to be able to wrap the
 * returned {@link TransactionStatus} instances into {@link TransactionContext}. {@link TransactionContext} implements
 * {@link AutoCloseable}, so it can be used in try-with-resources blocks. For more information see {@link TransactionContext}. 
 * @see TransactionContext 
 */
public class TransactionContextManager {

  private final PlatformTransactionManager realTransactionManager;

  TransactionContextManager(PlatformTransactionManager realTransactionManager) {
    this.realTransactionManager = realTransactionManager;
  }

  /**
   * Begins a new transaction or returns an existing, depending on the passed Transaction Propagation.
   * The created transaction is wrapped into a {@link TransactionContext} which is {@link AutoCloseable} and allows using
   * the wrapper inside a try-with-resources block. 
   * @param propagation The transaction propagation to use.
   */
  public TransactionContext getTransaction(int propagation) {
      return new TransactionContext(realTransactionManager.getTransaction(new DefaultTransactionDefinition(propagation)), this);
  }

  public void commit(TransactionContext context) {
      realTransactionManager.commit(context.getTransactionStatus());
  }

  public void rollback(TransactionContext context) {
      realTransactionManager.rollback(context.getTransactionStatus());
  }

  void rollbackIfNotCommitted(TransactionContext context) {
    TransactionStatus status = context.getTransactionStatus();
      if (!status.isCompleted()) {
        realTransactionManager.rollback(status);
      }
  }

}
