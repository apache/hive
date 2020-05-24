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
package org.apache.hadoop.hive.ql.qoption;

import org.apache.hadoop.hive.ql.QTestUtil;

/**
 * Qtest options might be usefull to prepare the test environment or do some extra checks/cleanup.
 */
public interface QTestOptionHandler {

  /**
   * For a matching option; the arguments are supplied to the handler by this method. 
   */
  void processArguments(String arguments);

  /**
   * Invoked before the actual test is executed.
   * 
   * At the time of this call all the options for the actual test is already processed.
   */
  void beforeTest(QTestUtil qt) throws Exception;

  /**
   * Invoked right after the test is executed.
   * 
   * Can be used to cleanup things and/or clear internal state of the handler.
   */
  void afterTest(QTestUtil qt) throws Exception;

}
