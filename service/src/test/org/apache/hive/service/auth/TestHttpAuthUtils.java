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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Tests for {@link HttpAuthUtils}.
 */
public class TestHttpAuthUtils {

  /**
   * A token created for a username must parse back to the same username.
   */
  private void assertRoundTrips(String userName) {
    String token = HttpAuthUtils.createCookieToken(userName);
    assertEquals(userName, HttpAuthUtils.getUserNameFromCookieToken(token));
  }

  /**
   * A malformed or ambiguous token must be rejected by the parser.
   */
  private void assertRejected(String tokenStr) {
    assertNull(HttpAuthUtils.getUserNameFromCookieToken(tokenStr));
  }

  @Test
  public void testSimpleUserNameRoundTrips() {
    assertRoundTrips("alice");
  }

  @Test
  public void testUserNameWithSpaceRoundTrips() {
    assertRoundTrips("alice smith");
  }

  @Test
  public void testUserNameWithPlusRoundTrips() {
    assertRoundTrips("alice+smith");
  }

  @Test
  public void testUserNameWithPercentRoundTrips() {
    assertRoundTrips("100%user");
  }

  /**
   * The key/value separator must be encoded in the username so it cannot
   * forge a second field; the token must parse back to that user.
   */
  @Test
  public void testUserNameWithEqualsRoundTrips() {
    assertRoundTrips("a=b");
  }

  /**
   * A username containing the cookie delimiters must not
   * inject a second {@code cu} field; the token must parse back to that user.
   */
  @Test
  public void testUserNameWithCookieDelimitersDoesNotInjectSecondUser() {
    assertRoundTrips("alice&cu=admin");
  }

  @Test
  public void testUserNameWithTrailingRandFieldDoesNotOverride() {
    assertRoundTrips("alice&cu=admin&rn=0");
  }

  /**
   * A token with a duplicate {@code cu} field is ambiguous and must be rejected,
   * so an injected second username cannot override the authenticated one.
   */
  @Test
  public void testMaliciousDuplicateCuIsRejected() {
    assertRejected("cu=alice&cu=admin&rn=123");
  }

  /**
   * A token whose username contains malformed percent-encoding must be rejected.
   */
  @Test
  public void testMalformedPercentEncodingIsRejected() {
    assertRejected("cu=alice%ZZ&rn=123");
  }

  /**
   * A token missing a required attribute must be rejected.
   */
  @Test
  public void testMissingAttributesRejected() {
    assertRejected("cu=alice"); // no rn
    assertRejected("rn=123");   // no cu
  }

  /**
   * A token part without a key/value separator is malformed and must be rejected.
   */
  @Test
  public void testTokenWithoutSeparatorRejected() {
    assertRejected("aliceadmin");
  }

  /**
   * A token carrying an unknown attribute beyond {@code cu} and {@code rn}
   * must be rejected.
   */
  @Test
  public void testUnknownAttributeRejected() {
    assertRejected("cu=alice&rn=123&x=1");
  }
}



