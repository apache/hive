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

package org.apache.hive.jdbc.saml;

import java.net.URI;

import org.junit.Test;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestSSOControl {

  static boolean checkValid(String uri) {
    try {
      HiveJdbcSamlRedirectStrategy.checkSsoUri(new URI(uri));
      return true;
    } catch(Exception xany) {
      return false;
    }
  }

  @Test
  public void testValidURL() {
    assertTrue(checkValid("https://companya.okta.com"));
    assertTrue(checkValid("https://companyb.okta.com:8080"));
    assertTrue(checkValid("https://companyc.okta.com/testpathvalue"));
  }

  @Test
  public void testInvalidURL() {
    assertFalse(checkValid("-a Calculator"));
    assertFalse(checkValid("This is random text"));
    assertFalse(checkValid("file://randomfile"));
  }
}
