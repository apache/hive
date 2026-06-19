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
package org.apache.hadoop.hive.common.io;

/**
 * Callers will use this interface to notify the class that processes the stream
 * about the content that will be processed. mark the beginning/end
 * of the output of an executed command, and whether
 * the command has been a query.
 */
public interface FetchCallback {
  /**
   * Marks the beginning of the output of an executed command.
   */
  void fetchStarted();

  /**
   * Marks the beginning of the output of an executed command.
   */
  void fetchFinished();

  /**
   * Whether the executed command is a query.
   * The method has to be called before processing the output of the command.
   */
  void foundQuery(boolean foundQuery);
}
