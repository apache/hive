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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExceptUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ExceptUtils.class.getName());

  // We even wrap a MetaException with itself to indicate where the exception was processed by the client.
  public static MetaException wrapMetastoreClientException(String methodName,
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
    String msg = methodName + " error: " + rootCause.getClass().getName() + (rootMsg.isEmpty() ? "" : " " + rootMsg);
    MetaException me = new MetaException(msg);
    // Get rid of the call to wrapMetastoreClientException.
    ExceptUtils.removeFirstStackTraceEle(me);
    return me;
  }

  /*
   * Technically, you are not suppose to catch java.lang.Error (OOM, etc.) but it is unavoidable with wrap
   * exceptions like UndeclaredThrowableException.
   *
   * Attempt to create wrapper Error to show we intercepted it here, log, and throw. Otherwise, rethrow.
   */
  public static void handleFatalError(String operationName, Error error) {
    boolean isRethrow = true;
    Error wrappedError = null;
    try {
      wrappedError = new Error(operationName + " fatal error: ", error);
      // Get rid of the call to handleFatalError.
      ExceptUtils.removeFirstStackTraceEle(wrappedError);
      LOG.error("Fatal error for " + operationName + ": ", wrappedError);
      isRethrow = false;
    } catch (Throwable t) {
      // Suppress..
    }
    if (!isRethrow) {
      throw wrappedError;
    } else {
      throw error;
    }
  }

  public static void removeFirstStackTraceEle(Throwable t) {
    StackTraceElement[] stackTrace = t.getStackTrace();
    final int len = stackTrace.length;
    StackTraceElement[] newStackTrace = new StackTraceElement[len - 1];
    for (int i = 1; i < len; i++) {
      newStackTrace[i - 1] = stackTrace[i];
    }
    t.setStackTrace(newStackTrace);
  }
}
