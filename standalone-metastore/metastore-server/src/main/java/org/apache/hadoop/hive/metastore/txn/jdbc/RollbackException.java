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

import java.lang.reflect.Method;

/**
 * This exception can be used to trigger rollback in 
 * {@link org.apache.hadoop.hive.metastore.txn.TransactionalRetryProxy#invoke(Object, Method, Object[])}
 * for the current transaction, without propagating the exception to the caller. The proxy will catch this exception,
 * rollback the transaction (if not yet completed already) and return the value supplied in the constructor to te caller.
 */
public class RollbackException extends RuntimeException {  
  
  private final Object result;
  
  public RollbackException(Object result) {
    this.result = result;
  }

  public Object getResult() {
    return result;
  }
  
}