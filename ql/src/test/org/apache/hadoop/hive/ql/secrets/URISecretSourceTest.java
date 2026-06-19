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
package org.apache.hadoop.hive.ql.secrets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class URISecretSourceTest {
  private MockSecretSource source;

  @Before
  public void setupTest() {
    source = new MockSecretSource();
    URISecretSource.getInstance().register(source);
  }

  @After
  public void tearDownTest() {
    URISecretSource.getInstance().removeForTest(source);
  }

  @Test
  public void testURISecret() throws Exception {
    assertNull(Utilities.getPasswdFromUri(null));
    assertNull(Utilities.getPasswdFromUri(""));
    assertEquals("mysecret", Utilities.getPasswdFromUri("test:///mysecret"));
  }

  @Test(expected = HiveException.class)
  public void testInvalidScheme() throws Exception {
    Utilities.getPasswdFromUri("test1:///mysecret");
  }

  @Test(expected = URISyntaxException.class)
  public void testInvalidUri() throws Exception {
    Utilities.getPasswdFromUri("1test:///mysecret");
  }

  @Test(expected = IOException.class)
  public void testIncorrectUriForScheme() throws Exception {
    Utilities.getPasswdFromUri("test:///mysecret#cry");
  }

  private static class MockSecretSource implements SecretSource {
    @Override
    public String getURIScheme() {
      return "test";
    }

    @Override
    public String getSecret(URI uri) throws IOException {
      if (uri.getFragment() != null) {
        throw new IOException("I cannot handle this");
      }
      return uri.getPath().substring(1);
    }

    @Override
    public void close() throws IOException {
    }
  }
}
