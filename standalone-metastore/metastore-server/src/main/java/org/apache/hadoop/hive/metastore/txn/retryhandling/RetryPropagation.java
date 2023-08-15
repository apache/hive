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

/**
 * Specifies how {@link DataSourceWrapper.RetryContext} propagation is done in various situations.
 */
public enum RetryPropagation {

  /**
   * If there is no existing {@link DataSourceWrapper.RetryContext}, a new one is created. If there is a context 
   * (a method upper in the call hierarchy is also annotated with the {@link Retry} annotation), this method will join it. 
   * Joining a {@link DataSourceWrapper.RetryContext} means that if this method fails, the fail will be propagated to 
   * the caller, and the caller method will be retried (and this method will be retried as a part of the parent retry).
   */
  CREATE_OR_JOIN(3),
  /**
   * A new {@link DataSourceWrapper.RetryContext} will be created regardless if there's an existing one or not. If there 
   * is a context (a method upper in the call hierarchy is also annotated with the {@link Retry} annotation), a new 
   * nested context is created. Creating a nested context means that if this method fails, the error will be caught and 
   * the method will be retried. The caller method won't know about this failure, unless all the retry-attemps are used up.
   */
  CREATE_OR_NESTED(1),
  /**
   * An existing {@link DataSourceWrapper.RetryContext} is required to join it.
   */
  JOIN_EXISTING(2);

  private static final int CREATE_FLAG = 1;
  private static final int JOIN_FLAG = 2;

  private int flag;

  RetryPropagation(int flag) {
    this.flag = flag;
  }

  boolean canCreateContext() {
    return (CREATE_FLAG & flag) == CREATE_FLAG;
  }

  boolean canJoinContext() {
    return (JOIN_FLAG & flag) == JOIN_FLAG;
  }

}
