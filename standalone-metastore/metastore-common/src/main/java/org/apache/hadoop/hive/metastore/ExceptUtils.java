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

package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.hive.metastore.api.MetaException;

public class ExceptUtils {

  // We even wrap a MetaException with itself to indicate where the exception was processed by the client.
  public static MetaException wrapMetastoreClientException(Exception excForRightCallStack, String methodName,
                                                           Throwable cause) {
    Throwable rootCause = cause;
    while (true) {
      Throwable nextCause = rootCause.getCause();
      if (nextCause == null) {
        break;
      }
      rootCause = nextCause;
    }
    String rootMsg = rootCause.getMessage();

    StackTraceElement[] rightCallStack = excForRightCallStack.getStackTrace();
    String msg = methodName + " error: " + rootCause.getClass().getName() + (rootMsg.isEmpty() ? "" : " " + rootMsg);
    MetaException me = new MetaException(msg);
    // Remove wrapMetastoreClientException method from the stack trace. And, also set an Exception cause since
    // generated MetaException does not have a constructor with Exception parameter.
    me.setStackTrace(rightCallStack);
    return me;
  }
}
