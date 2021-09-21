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

package org.apache.hadoop.hive.metastore.utils;

import java.util.Objects;

import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;
import javax.jdo.Transaction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class HiveTransactionWrapper {

  private static final Logger LOG = LoggerFactory.getLogger(HiveTransactionWrapper.class);

  private final PersistenceManager pm;
  private int txCount = 0;
  private boolean isReadOnly = false;
  private Transaction tx = null;

  public HiveTransactionWrapper(PersistenceManager pm) {
    this.pm = Objects.requireNonNull(pm);
  }

  /**
   * Open a read-only transaction. Once a transaction is opened read-only it will
   * not support additional open transaction calls to the same PersistenceManager.
   * For every call to this method, caller must also call commitTransaction.
   *
   * @throws JDOUserException if the transaction has been closed outside the scope
   *                          of this wrapper class or if a read-only transaction
   *                          is already present.
   */
  public void openTransaction() {
    this.tx = this.pm.currentTransaction();
    if (txCount == 0) {
      this.tx.begin();
      this.isReadOnly = false;
    } else if (!tx.isActive()) {
      throw new JDOUserException(
          "openTransaction called in an interior transaction scope, but currentTransaction is not active.");
    } else if (this.isReadOnly) {
      throw new JDOUserException("openTransaction called on top of a read-only scope, this is not supported");
    }
    this.txCount++;
  }

  /**
   * Open a read-only transaction. Once a transaction is opened read-only it will
   * not support additional open transaction calls to the same PersistenceManager.
   *
   * @throws JDOUserException if the transaction is already active.
   */
  public void openReadOnlyTransaction() {
    this.tx = this.pm.currentTransaction();
    this.tx.begin();
    this.tx.setRollbackOnly();
    this.isReadOnly = true;
    this.txCount = 1;
  }

  /**
   * Commit the transaction.
   *
   * @throws JDOUserException if the transaction is not active
   */
  public void commitTransaction() {
    try {
      if (this.txCount <= 0) {
        LOG.warn("A transaction was committed more times than it was opened: {}", this.txCount);
        if (LOG.isDebugEnabled()) {
          LOG.debug("TRACE", new Exception("Debug Dump Stack Trace (Not an Exception)"));
        }
      }
      if (this.isReadOnly) {
        this.tx.rollback();
      } else {
        this.txCount--;
        if (this.txCount == 0) {
          this.tx.commit();
        }
      }
    } finally {
      this.tx = null;
      this.txCount = 0;
      this.isReadOnly = false;
    }
  }

  /**
   * Roll back the transaction.
   */
  public void rollback() {
    final boolean localReadOnly = this.isReadOnly;
    try {
      if (this.tx.isActive()) {
        this.tx.rollback();
      }
    } finally {
      this.tx = null;
      this.txCount = 0;
      this.isReadOnly = false;
      if (!localReadOnly) {
        pm.evictAll();
      }
    }
  }

  public boolean isTransactionActive() {
    return this.tx != null && this.tx.isActive();
  }

}
