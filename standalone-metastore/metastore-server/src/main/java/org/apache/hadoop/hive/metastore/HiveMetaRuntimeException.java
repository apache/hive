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

import com.google.errorprone.annotations.FormatMethod;

/**
 * This is the root of all Hive Runtime Exceptions. If a client can reasonably
 * be expected to recover from an exception, make it a checked exception. If a
 * client cannot do anything to recover from the exception, make it an unchecked
 * exception.
 */
public class HiveMetaRuntimeException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public HiveMetaRuntimeException(String message) {
    super(message);
  }

  public HiveMetaRuntimeException(String message, Throwable cause) {
    super(message, cause);
  }

  @FormatMethod
  public HiveMetaRuntimeException(Throwable throwable, String message, Object... args) {
    super(String.format(message, args), throwable);
  }

}
