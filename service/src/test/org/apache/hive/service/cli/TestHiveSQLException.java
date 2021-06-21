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

package org.apache.hive.service.cli;

import org.apache.hive.service.rpc.thrift.TStatus;
import org.apache.hive.service.rpc.thrift.TStatusCode;
import org.junit.Assert;
import org.junit.Test;

public class TestHiveSQLException {

  /**
   * Tests the conversion from a regular exception to the TStatus object
   */
  @Test
  public void testExceptionToTStatus() {
    Exception ex1 = createException();
    ex1.initCause(createSimpleCause());
    TStatus status = HiveSQLException.toTStatus(ex1);

    Assert.assertEquals(TStatusCode.ERROR_STATUS, status.getStatusCode());
    Assert.assertEquals(ex1.getMessage(), status.getErrorMessage());
    Assert.assertEquals(HiveSQLException.DEFAULT_INFO, status.getInfoMessages());
  }

  /**
   * Tests the conversion from a HiveSQLException exception to the TStatus object
   */
  @Test
  public void testHiveSQLExceptionToTStatus() {
    String expectedMessage = "reason";
    String expectedSqlState = "sqlState";
    int expectedVendorCode = 10;

    Exception ex1 = new HiveSQLException(expectedMessage, expectedSqlState, expectedVendorCode, createSimpleCause());
    TStatus status = HiveSQLException.toTStatus(ex1);

    Assert.assertEquals(TStatusCode.ERROR_STATUS, status.getStatusCode());
    Assert.assertEquals(expectedSqlState, status.getSqlState());
    Assert.assertEquals(expectedMessage, status.getErrorMessage());
    Assert.assertEquals(HiveSQLException.DEFAULT_INFO, status.getInfoMessages());
  }

  interface Dummy {
    void testExceptionConversion();
  }

  private static Exception createException() {
    return new RuntimeException("exception1");
  }

  private static Exception createSimpleCause() {
    return new UnsupportedOperationException("exception2");
  }

}
