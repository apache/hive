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
import org.junit.Assume;

import com.google.common.base.Strings;

/**
 * QTest disabled directive handler
 *
 * Example:
 * --! qt:disabled:reason
 *
 */
public class QTestDisabledHandler implements QTestOptionHandler {

  private String message;

  @Override
  public void processArguments(String arguments) {
    message = arguments;
    if (Strings.isNullOrEmpty(message)) {
      throw new RuntimeException("you have to give a reason why it was ignored");
    }
  }

  @Override
  public void beforeTest(QTestUtil qt) throws Exception {
    Assume.assumeTrue(message, (message == null));
  }

  @Override
  public void afterTest(QTestUtil qt) throws Exception {
    message = null;
  }

}
