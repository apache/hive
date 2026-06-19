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

package org.apache.hadoop.hive.metastore.auth.oauth2;

import java.util.Map;
import java.util.regex.Pattern;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.auth.HttpAuthenticationException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MetastoreUnitTest.class)
public class TestRegexOAuth2PrincipalMapper {
  private static final Map<String, String> TEST_CLAIM_SET = Map.of(
      "iss", "https://example.com",
      "email", "alice@example.com",
      "sub", "12345"
  );

  @Test
  public void testSub() throws HttpAuthenticationException {
    var mapper = new RegexOAuth2PrincipalMapper("sub", Pattern.compile("(.*)"));
    Assert.assertEquals("12345", mapper.getUserName(TEST_CLAIM_SET::get));
  }

  @Test
  public void testEmailLocalPart() throws HttpAuthenticationException {
    var mapper = new RegexOAuth2PrincipalMapper("email", Pattern.compile("(.*)@example.com"));
    Assert.assertEquals("alice", mapper.getUserName(TEST_CLAIM_SET::get));
  }

  @Test
  public void testMissingClaim() {
    var mapper = new RegexOAuth2PrincipalMapper("non-existent", Pattern.compile("(.*)"));
    var error = Assert.assertThrows(HttpAuthenticationException.class, () -> mapper.getUserName(TEST_CLAIM_SET::get));
    Assert.assertEquals("Authentication error: Claim 'non-existent' not found in token", error.getMessage());
  }

  @Test
  public void testNoMatching() {
    var mapper = new RegexOAuth2PrincipalMapper("email", Pattern.compile("(.*)@another-domain"));
    var error = Assert.assertThrows(HttpAuthenticationException.class, () -> mapper.getUserName(TEST_CLAIM_SET::get));
    Assert.assertEquals("Authentication error: Claim 'email' does not match (.*)@another-domain", error.getMessage());
  }

  @Test
  public void testMultipleMatching() {
    var mapper = new RegexOAuth2PrincipalMapper("email", Pattern.compile("(.*)@(.*)"));
    var error = Assert.assertThrows(IllegalStateException.class, () -> mapper.getUserName(TEST_CLAIM_SET::get));
    Assert.assertEquals("Pattern must extract exactly one group, but (.*)@(.*) picked up 2 groups", error.getMessage());
  }
}
