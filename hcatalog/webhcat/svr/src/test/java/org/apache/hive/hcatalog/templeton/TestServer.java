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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.templeton;

import junit.framework.TestCase;

import org.apache.hive.hcatalog.templeton.mock.MockServer;
import java.util.List;

/*
 * Test that the server code exists, and responds to basic requests.
 */
public class TestServer extends TestCase {

  MockServer server;

  public void setUp() {
    new Main(new String[]{});         // Initialize the config
    server = new MockServer();
  }

  public void testServer() {
    assertNotNull(server);
  }

  public void testStatus() {
    assertEquals(server.status().get("status"), "ok");
  }

  public void testVersions() {
    assertEquals(server.version().get("version"), "v1");
  }

  public void testFormats() {
    assertEquals(1, server.requestFormats().size());
    assertEquals( ((List)server.requestFormats().get("responseTypes")).get(0), "application/json");
  }

  public void testVerifyPropertyParam() {
    // HIVE-15410: Though there are not restrictions to Hive table property key and it could be any
    // combination of the letters, digits and even punctuations, we support conventional property
    // name in WebHCat (e.g. prepery name starting with a letter or digit probably with period (.),
    // underscore (_) and hyphen (-) only in the middle like auto.purge, last_modified_by etc)
    String [] validTblProperties = {"abcd", "Abcd", "1Abcd", "abc1d", "Abcd.efgh", "Abcd-efgh",
        "Abcd_efgh", "A", "b", "1"};
    for (String propertyKey : validTblProperties) {
      try {
        server.verifyPropertyParam(propertyKey, ":property");
      } catch (Exception e) {
        fail(propertyKey + " should be a valid table property name in WebHCat.");
      }
    }

    String [] invalidTblProperties = {".abcd", "-Abcd", "_1Abcd", "abc1d.", "Abcd_", "Abcd-",
    "Abcd ", " Abcd", ".", "-", "_", " ", "$"};
    for (String propertyKey : invalidTblProperties) {
      boolean throwException = false;
      try {
        server.verifyPropertyParam(propertyKey, ":property");
      } catch (Exception e) {
        throwException = true;
      }
      if (!throwException) {
        fail(propertyKey + " should not be a valid table property name in WebHCat.");
      }
    }
  }
}
