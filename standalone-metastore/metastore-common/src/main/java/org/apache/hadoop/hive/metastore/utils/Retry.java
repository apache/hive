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

/**
 * Class to implement any retry logic in case of exceptions.
 */
public abstract class Retry<T> {

  public static final int MAX_RETRIES = 4;
  public static final int DELAY = 30 * 1000;
  private int tries = 0;
  private Class retryExceptionType;

  public Retry(Class exceptionClassType) {
    this.retryExceptionType = exceptionClassType;
  }

  public abstract T execute() throws Exception;

  public T run() throws Exception {
    try {
      return execute();
    } catch(Exception e) {
      if (e.getClass().equals(retryExceptionType)){
        tries++;
        if (MAX_RETRIES == tries) {
          throw e;
        } else {
          return run();
        }
      } else {
        throw e;
      }
    }
  }

  public T runWithDelay() throws Exception {
    try {
      return execute();
    } catch(Exception e) {
      if (e.getClass().equals(retryExceptionType)){
        tries++;
        if (MAX_RETRIES == tries) {
          throw e;
        } else {
          Thread.sleep((long) DELAY * tries);
          return runWithDelay();
        }
      } else {
        throw e;
      }
    }
  }
}
