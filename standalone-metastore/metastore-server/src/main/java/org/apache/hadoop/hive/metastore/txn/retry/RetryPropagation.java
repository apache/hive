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
package org.apache.hadoop.hive.metastore.txn.retry;

/**
 * Specifies how the Retry context propagation is done in various situations.
 */
public enum RetryPropagation {

  /**
   * If there is no retry-call upper in the callstack, the {@link SqlRetry} annotated method will be executed as a 
   * retry-call, meaning that in case of SQL related errors, the method will be re-executed. If there is an existing 
   * retry-call method upper in the callstack (a method also annotated with the{@link SqlRetry} annotation), this 
   * method will join it. Joining the retry-call means that if this method fails, the fail will be propagated to the method
   * upper in the callstack and that method will be retried (and this method will be retried as a part of that retry). 
   * <b>In other words:</b> regardless if the inner or outer method fails, the outer method will be re-executed.
   */
  CREATE_OR_JOIN(3),
  /**
   * Regardless if there is a retry-call upper in the callstack, the  {@link SqlRetry} annotated method will be executed
   * as a retry-call, meaning that in case of SQL related errors, the method will be re-executed. 
   * <b>In other words:</b> If the method fails, the error will be caught, and the method will be re-executed.
   * If there is another retry-call upper in the callstack, this error remains unkown for it, unless all retry attempt
   * are used up.
   */
  CREATE_OR_NESTED(1),
  /**
   * An existing retry-call is required to join it.
   * <b>In other words:</b> While this method does not being executed as a retry-call, it can be called only when 
   * there is a {@link SqlRetry} annotated method upper in the callstack.
   */
  JOIN_EXISTING(2);

  private static final int CREATE_FLAG = 1;
  private static final int JOIN_FLAG = 2;

  private final int flag;

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
