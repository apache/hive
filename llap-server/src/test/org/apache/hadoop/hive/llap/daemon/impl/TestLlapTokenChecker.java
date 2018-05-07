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

package org.apache.hadoop.hive.llap.daemon.impl;

import static org.junit.Assert.*;

import org.apache.hadoop.io.Text;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hive.llap.security.LlapTokenIdentifier;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

public class TestLlapTokenChecker {

  @Test
  public void testGetToken() {
    check(LlapTokenChecker.getTokenInfoInternal("u", null), "u", null);
    check(LlapTokenChecker.getTokenInfoInternal(null, createTokens("u", null)), "u", null);
    check(LlapTokenChecker.getTokenInfoInternal(null, createTokens("u", "a")), "u", "a");
    check(LlapTokenChecker.getTokenInfoInternal("u", createTokens("u", "a")), "u", "a");
    check(LlapTokenChecker.getTokenInfoInternal("u", createTokens("u", "a", "u", null)),
        "u", "a");
    // Note - some of these scenarios could be handled, but they are not supported right now.
    // The reason is that we bind a query to app/user using the signed token information, and
    // we don't want to bother figuring out which one to use in case of ambiguity w/o a use case.
    checkGetThrows("u", createTokens("u2", "a")); // Ambiguous user.
    checkGetThrows("u2", createTokens("u2", "a", "u3", "a")); // Ambiguous user.
    checkGetThrows(null, createTokens("u2", "a", "u3", "a")); // Ambiguous user.
    checkGetThrows(null, createTokens("u2", "a", "u2", "a1")); // Ambiguous app.
  }

  @Test
  public void testCheckPermissions() {
    LlapTokenChecker.checkPermissionsInternal("u", null, "u", null, null);
    LlapTokenChecker.checkPermissionsInternal(null, createTokens("u", null) , "u", null, null);
    LlapTokenChecker.checkPermissionsInternal("u", createTokens("u", "a") , "u", "a", null);
    // No access.
    checkPrmThrows("u2", null, "u", "a");
    checkPrmThrows("u", null, "u", "a"); // Note - Kerberos user w/o appId doesn't have access.
    checkPrmThrows(null, createTokens("u2", "a"), "u", "a");
    checkPrmThrows(null, createTokens("u", "a2"), "u", "a");
    checkPrmThrows(null, createTokens("u", null), "u", "a");
  }

  private List<LlapTokenIdentifier> createTokens(String... args) {
    List<LlapTokenIdentifier> tokens = new ArrayList<>();
    for (int i = 0; i < args.length; i += 2) {
      tokens.add(new LlapTokenIdentifier(new Text(args[i]), null, null, "c", args[i + 1], false));
    }
    return tokens;
  }

  private void checkGetThrows(String kerberosName, List<LlapTokenIdentifier> tokens) {
    try {
      LlapTokenChecker.getTokenInfoInternal(kerberosName, tokens);
      fail("Didn't throw");
    } catch (SecurityException ex) {
      // Expected.
    }
  }

  private void checkPrmThrows(
      String kerberosName, List<LlapTokenIdentifier> tokens, String userName, String appId) {
    try {
      LlapTokenChecker.checkPermissionsInternal(kerberosName, tokens, userName, appId, null);
      fail("Didn't throw");
    } catch (SecurityException ex) {
      // Expected.
    }
  }

  private void check(LlapTokenChecker.LlapTokenInfo p, String user, String appId) {
    assertEquals(user, p.userName);
    assertEquals(appId, p.appId);
  }
}