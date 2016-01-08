/**
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

import java.util.List;

import junit.framework.Assert;

import org.apache.commons.lang.StringUtils;
import org.apache.hive.service.cli.thrift.TStatus;
import org.apache.hive.service.cli.thrift.TStatusCode;
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
    Assert.assertEquals(HiveSQLException.toString(ex1), status.getInfoMessages());
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
    Assert.assertEquals(HiveSQLException.toString(ex1), status.getInfoMessages());
  }

  /**
   * Tests the conversion between the exception text with the simple cause and the
   * Throwable object
   */
  @Test
  public void testExceptionMarshalling() throws Exception {
    Exception ex1 = createException();
    ex1.initCause(createSimpleCause());
    Throwable ex = HiveSQLException.toCause(HiveSQLException.toString(ex1));

    Assert.assertSame(RuntimeException.class, ex.getClass());
    Assert.assertEquals("exception1", ex.getMessage());
    Assert.assertSame(UnsupportedOperationException.class, ex.getCause().getClass());
    Assert.assertEquals("exception2", ex.getCause().getMessage());
  }

  /**
   * Tests the conversion between the exception text with nested cause and
   * the Throwable object
   */
  @Test
  public void testNestedException() {
    Exception ex1 = createException();
    ex1.initCause(createNestedCause());
    Throwable ex = HiveSQLException.toCause(HiveSQLException.toString(ex1));

    Assert.assertSame(RuntimeException.class, ex.getClass());
    Assert.assertEquals("exception1", ex.getMessage());

    Assert.assertSame(UnsupportedOperationException.class, ex.getCause().getClass());
    Assert.assertEquals("exception2", ex.getCause().getMessage());

    Assert.assertSame(Exception.class, ex.getCause().getCause().getClass());
    Assert.assertEquals("exception3", ex.getCause().getCause().getMessage());
  }

  /**
   * Tests the conversion of the exception with unknown source
   */
  @Test
  public void testExceptionWithUnknownSource() {
    Exception ex1 = createException();
    ex1.initCause(createSimpleCause());
    List<String> details = HiveSQLException.toString(ex1);

    // Simulate the unknown source
    String[] tokens = details.get(1).split(":");
    tokens[2] = null;
    tokens[3] = "-1";
    details.set(1, StringUtils.join(tokens, ":"));

    Throwable ex = HiveSQLException.toCause(details);

    Assert.assertSame(RuntimeException.class, ex.getClass());
    Assert.assertEquals("exception1", ex.getMessage());
    Assert.assertSame(UnsupportedOperationException.class, ex.getCause().getClass());
    Assert.assertEquals("exception2", ex.getCause().getMessage());
  }

  /**
   * Tests the conversion of the exception that the class type of one of the causes
   * doesn't exist. The stack trace text is generated on the server and passed to JDBC
   * client. It's possible that some cause types don't exist on the client and HiveSQLException
   * can't convert them and use RunTimeException instead.
   */
  @Test
  public void testExceptionWithMissingTypeOnClient() {
    Exception ex1 = new UnsupportedOperationException();
    ex1.initCause(createSimpleCause());
    List<String> details = HiveSQLException.toString(ex1);

    // Simulate an unknown type
    String[] tokens = details.get(0).split(":");
    tokens[0] = "*DummyException";
    details.set(0, StringUtils.join(tokens, ":"));

    Throwable ex = HiveSQLException.toCause(details);
    Assert.assertEquals(RuntimeException.class, ex.getClass());
  }

  /**
   * Tests the conversion of the exception from anonymous class
   */
  @Test
  public void testExceptionFromAnonymousClass() {
    Dummy d = new Dummy() {

      public void testExceptionConversion() {
        Exception ex1 = createException();
        ex1.initCause(createSimpleCause());
        Throwable ex = HiveSQLException.toCause(HiveSQLException.toString(ex1));

        Assert.assertSame(RuntimeException.class, ex.getClass());
        Assert.assertEquals("exception1", ex.getMessage());
        Assert.assertSame(UnsupportedOperationException.class, ex.getCause().getClass());
        Assert.assertEquals("exception2", ex.getCause().getMessage());
      }
    };

    d.testExceptionConversion();
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

  private static Exception createNestedCause() {
    return new UnsupportedOperationException("exception2", new Exception("exception3"));
  }
}
