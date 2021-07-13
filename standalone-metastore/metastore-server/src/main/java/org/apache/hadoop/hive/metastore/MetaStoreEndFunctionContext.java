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

/**
 * Base class which provides context to implementations of MetaStoreEndFunctionListener
 */

public class MetaStoreEndFunctionContext {

  /**
   * whether method was successful or not.
   */
  private final boolean success;
  private final Exception e;
  private final String inputTableName;
  /**
   * the input args of this method
   */
  private final Object[] fArgs;

  public MetaStoreEndFunctionContext(boolean success, Exception e, String inputTableName) {
    this.success = success;
    this.e = e;
    this.inputTableName = inputTableName;
    this.fArgs = null;
  }

  public MetaStoreEndFunctionContext(Throwable e, Object[] args) {
    if (e instanceof Exception) {
      this.e = (Exception) e;
    } else {
      this.e = new RuntimeException(e);
    }
    this.fArgs = args;
    this.success = (e == null);
    this.inputTableName = null;
  }

  /**
   * @return whether or not the method succeeded.
   */
  public boolean isSuccess() {
    return success;
  }

  public Exception getException() {
    return e;
  }

  /**
   * Use the method getfArgs to get the inputTableName
   */
  @Deprecated
  public String getInputTableName() {
    return inputTableName;
  }

  public Object[] getfArgs() {
    return fArgs;
  }

}
