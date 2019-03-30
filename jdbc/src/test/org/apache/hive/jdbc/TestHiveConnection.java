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

package org.apache.hive.jdbc;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

public class TestHiveConnection {

  private static final String EXISTING_TOKEN = "ExistingToken";
  public static final String EXPECTED_TOKEN_STRING_FORM = "AAAAAA";
  private static HiveConnection.DelegationTokenFetcher fetcher;

  @BeforeClass
  public static void init() {
    fetcher = new HiveConnection.DelegationTokenFetcher();
  }

  @Test
  public void testIfNPEThrownWhileGettingDelegationToken() throws IOException {
    try {
      String tokenStr = fetcher.getTokenFromCredential(new Credentials(), "hive");
      Assert.assertEquals("Token with id: hive shall not be found.", null, tokenStr);
    } catch (NullPointerException e) {
      Assert.fail("This NPE is not handled in the code elsewhere so user is not notified about it!");
      e.printStackTrace();
    }
  }

  @Test
  public void testIfGettingDelegationTokenFromCredentialWorks() throws IOException {
    Credentials creds = new Credentials();
    creds.addToken(new Text(EXISTING_TOKEN), new Token<>());

    String tokenStr = fetcher.getTokenFromCredential(creds, EXISTING_TOKEN);
    Assert.assertEquals("Token string form is not as expected.", EXPECTED_TOKEN_STRING_FORM, tokenStr);
  }
}
