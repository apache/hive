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
package org.apache.hive.service.auth;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.lang.reflect.Field;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.security.MetastoreDelegationTokenManager;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.rpc.thrift.TStatus;
import org.junit.Test;
import org.apache.thrift.transport.TTransportException;

public class TestDelegationTokenLeakPrevention {
  private static final String LIVE_TOKEN = "LIVE_DELEGATION_TOKEN_ABC123";

  @Test
  public void testCancelDelegationTokenExceptionDoesNotContainToken() throws Exception {
    MetastoreDelegationTokenManager tokenManager = mock(MetastoreDelegationTokenManager.class);
    doThrow(new IOException("boom")).when(tokenManager).cancelDelegationToken(LIVE_TOKEN);

    HiveAuthFactory authFactory = createAuthFactoryWithTokenManager(tokenManager);
    assertTokenIsNotExposed(() -> authFactory.cancelDelegationToken(LIVE_TOKEN),
        "Error canceling delegation token");
  }

  @Test
  public void testRenewDelegationTokenExceptionDoesNotContainToken() throws Exception {
    MetastoreDelegationTokenManager tokenManager = mock(MetastoreDelegationTokenManager.class);
    doThrow(new IOException("boom")).when(tokenManager).renewDelegationToken(LIVE_TOKEN);

    HiveAuthFactory authFactory = createAuthFactoryWithTokenManager(tokenManager);
    assertTokenIsNotExposed(() -> authFactory.renewDelegationToken(LIVE_TOKEN),
        "Error renewing delegation token");
  }

  @Test
  public void testVerifyDelegationTokenExceptionDoesNotContainToken() throws Exception {
    MetastoreDelegationTokenManager tokenManager = mock(MetastoreDelegationTokenManager.class);
    doThrow(new IOException("boom")).when(tokenManager).verifyDelegationToken(LIVE_TOKEN);

    HiveAuthFactory authFactory = createAuthFactoryWithTokenManager(tokenManager);
    assertTokenIsNotExposed(() -> authFactory.verifyDelegationToken(LIVE_TOKEN),
        "Error verifying delegation token");
  }

  @Test
  public void testGetUserFromTokenExceptionDoesNotContainToken() throws Exception {
    MetastoreDelegationTokenManager tokenManager = mock(MetastoreDelegationTokenManager.class);
    doThrow(new IOException("boom")).when(tokenManager).getUserFromToken(LIVE_TOKEN);

    HiveAuthFactory authFactory = createAuthFactoryWithTokenManager(tokenManager);
    assertTokenIsNotExposed(() -> authFactory.getUserFromToken(LIVE_TOKEN),
        "Error extracting user from delegation token");
  }

  private HiveAuthFactory createAuthFactoryWithTokenManager(MetastoreDelegationTokenManager tokenManager)
      throws NoSuchFieldException, IllegalAccessException, TTransportException {
    HiveAuthFactory authFactory = new HiveAuthFactory(new HiveConf(), false);
    Field delegationTokenManagerField = HiveAuthFactory.class.getDeclaredField("delegationTokenManager");
    delegationTokenManagerField.setAccessible(true);
    delegationTokenManagerField.set(authFactory, tokenManager);
    return authFactory;
  }

  private void assertTokenIsNotExposed(ThrowingRunnable callback, String expectedMessagePrefix)
      throws Exception {
    try {
      callback.run();
      fail("Expected HiveSQLException to be thrown");
    } catch (HiveSQLException e) {
      assertMessageIsSanitized(e.getMessage(), expectedMessagePrefix);
      TStatus status = HiveSQLException.toTStatus(e);
      assertMessageIsSanitized(status.getErrorMessage(), expectedMessagePrefix);
    }
  }

  private void assertMessageIsSanitized(String message, String expectedMessagePrefix) {
    assertTrue(message.contains(expectedMessagePrefix));
    assertFalse(message.contains(LIVE_TOKEN));
  }

  @FunctionalInterface
  private interface ThrowingRunnable {
    void run() throws Exception;
  }
}
