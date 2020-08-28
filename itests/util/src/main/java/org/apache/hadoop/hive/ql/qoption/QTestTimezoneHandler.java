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

import com.google.common.base.Strings;
import java.util.TimeZone;
import org.apache.hadoop.hive.ql.QTestUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * QTest custom timezone handler
 *
 * Enables a custom timezone for the test.
 *
 * Example:
 * --! qt:timezone:Asia/Singapore
 *
 */
public class QTestTimezoneHandler implements QTestOptionHandler {

  private static final Logger LOG = LoggerFactory.getLogger(QTestTimezoneHandler.class);
  private boolean enabled = false;
  private TimeZone originalTimeZone;
  private TimeZone newTimeZone;

  @Override
  public void processArguments(String arguments) {
    if (Strings.isNullOrEmpty(arguments)) {
      throw new RuntimeException("illegal timezone arg: " + arguments);
    }
    originalTimeZone = TimeZone.getDefault();
    newTimeZone = TimeZone.getTimeZone(arguments);
    LOG.info("Enabling timezone change: {} => {}", originalTimeZone, newTimeZone);
    enabled = true;
  }

  @Override
  public void beforeTest(QTestUtil qt) throws Exception {
    if (enabled) {
      TimeZone.setDefault(newTimeZone);
    }
  }

  @Override
  public void afterTest(QTestUtil qt) throws Exception {
    if (enabled) {
      TimeZone.setDefault(originalTimeZone);
      enabled = false;
    }
  }

}
