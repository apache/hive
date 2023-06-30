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

import java.sql.SQLException;

/**
 * A functional interface representing a function call (typically a statement) which is done within a database transaction.
 */
@FunctionalInterface
public interface TransactionalVoidFunction {

  /**
   * Implementations typically should execute transsactional database calls inside.
   * @param dataSourceWrapper A {@link DataSourceWrapper} instance responsible for providing all the necessary resources 
   *                          to be able to perform transactional database calls.
   * @throws SQLException Thrown if any of the JDBC calls fail
   * @throws MetaException Thrown in case of application error within the function
   */
  void call(DataSourceWrapper dataSourceWrapper) throws SQLException, MetaException;
  
}
