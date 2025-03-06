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

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchLockException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;

/**
 * A functional interface representing a function call (typically a query or statement) which has a result and done within
 * a database transaction.
 * @param <Result> The type of the function call result
 */
@FunctionalInterface
public interface TransactionalFunction<Result> {
  
  /**
   * Implementations typically should execute transsactional database calls inside.
   * @param jdbcResource A {@link MultiDataSourceJdbcResource} instance responsible for providing all the necessary resources 
   *                          to be able to perform transactional database calls.
   * @return Returns with the result of the function call. 
   * @throws org.springframework.dao.DataAccessException Thrown if any of the JDBC calls fail
   * @throws MetaException Thrown in case of application error within the function
   */
  Result execute(MultiDataSourceJdbcResource jdbcResource) throws MetaException, NoSuchTxnException, TxnAbortedException, 
      NoSuchLockException;

}