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

import org.datanucleus.transaction.TransactionIsolation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Can be put on methods to tell {@link org.apache.hadoop.hive.metastore.txn.RetryingTxnHandler} that the method
 * must be executed within a {@link DataSourceWrapper.RetryContext}, and in case of failure, the method will be re-executed.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Retry {

  /**
   * @return The name of the datasource to use for creating the {@link DataSourceWrapper.RetryContext} 
   */
  String datasource();

  /**
   * @return The trabsaction isolation level of the transaction. {@link TransactionIsolation#READ_COMMITTED} by default.
   */
  int transactionIsolation() default TransactionIsolation.READ_COMMITTED;

  /**
   * @return True if the internal (DERBY) lock is necessary. False by defaul.
   */
  boolean lockInternally() default false;

  /**
   * @return True if the method execution should be retried in case of duplicate key error. False by default.
   */
  boolean retryOnDuplicateKey() default false;

  /**
   * @return True if the transaction has to be rolled back on error. True by default.
   */
  boolean rollbackOnError() default true;

}