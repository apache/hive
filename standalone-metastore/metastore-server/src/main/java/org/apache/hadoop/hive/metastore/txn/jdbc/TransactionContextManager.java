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

import org.apache.hadoop.hive.metastore.utils.StackThreadLocal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

  private static final Logger LOG = LoggerFactory.getLogger(TransactionContextManager.class);
  
  private final PlatformTransactionManager realTransactionManager;

  /**
   * We must keep track of the requested transactions, to be able to return the current transaction in {@link #getActiveTransaction()}.
   * In Spring JDBCTemplate users have to choose programmatic transaction management to access the {@link TransactionStatus}
   * object which can be used for savepoint management. With this enhancement, it is possible to identify and return
   * the active transaction, which allows combining the use of {@link org.springframework.transaction.annotation.Transactional} 
   * annotation with programmatic savepoint management.
   */
  private final StackThreadLocal<TransactionContext> contexts = new StackThreadLocal<>();

  TransactionContextManager(PlatformTransactionManager realTransactionManager) {
    this.realTransactionManager = realTransactionManager;
  }

  /**
   * Begins a new transaction or returns an existing, depending on the passed Transaction Propagation.
   * The created transaction is wrapped into a {@link TransactionContext} which is {@link AutoCloseable} and allows using
   * the wrapper inside a try-with-resources block.
   *
   * @param propagation The transaction propagation to use.
   */
  public TransactionContext getNewTransaction(int propagation) {
    TransactionContext context = new TransactionContext(realTransactionManager.getTransaction(
        new DefaultTransactionDefinition(propagation)), this);
    contexts.set(context);
    return context;
  }
  
  public TransactionContext getActiveTransaction() {
    return contexts.get();
  }
  
  public void commit(TransactionContext context) {
    TransactionContext storedContext = contexts.get();
    if (!storedContext.equals(context)) {
      throw new IllegalStateException();
    }
    try {
      realTransactionManager.commit(context.getTransactionStatus());      
    } finally {
      contexts.unset();
    }
  }

  public void rollback(TransactionContext context) {
    TransactionContext storedContext = contexts.get();
    if (!storedContext.equals(context)) {
      throw new IllegalStateException();
    }
    try {
      realTransactionManager.rollback(context.getTransactionStatus());
    } finally {
      contexts.unset();
    }
  }

  void rollbackIfNotCommitted(TransactionContext context) {
    if (!context.isCompleted()) {
      LOG.debug("The transaction is not committed and we are leaving the try-with-resources block. Going to rollback: {}", context);
      rollback(context);
    }
  }

}