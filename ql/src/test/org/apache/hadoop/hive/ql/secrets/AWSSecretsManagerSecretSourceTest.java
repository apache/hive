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

import com.google.common.cache.LoadingCache;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ExecutionException;

public class AWSSecretsManagerSecretSourceTest {
  private AWSSecretsManagerSecretSource source = new AWSSecretsManagerSecretSource();
  private LoadingCache<String, String> cache;

  @Before
  public void setupTest() {
    cache = Mockito.mock(LoadingCache.class);
    source.setCache(cache);
  }

  @Test
  public void testAWSSecretsManagerScheme() {
    assertEquals("aws-sm", source.getURIScheme());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAWSSecretsManagerInvalidScheme() throws Exception {
    source.getSecret(new URI("awssm:///test"));
  }

  @Test
  public void testAWSSecretsManagerSuccess() throws Exception {
    // Test good case.
    Mockito.when(cache.get("test")).thenReturn("{\"password\":\"super-secret\"}");
    assertEquals("super-secret", source.getSecret(new URI("aws-sm:///test")));
  }

  @Test(expected = IOException.class)
  public void testAWSSecretsManagerServiceException() throws Exception {
    Mockito.when(cache.get("bad")).thenThrow(new ExecutionException(new RuntimeException()));
    source.getSecret(new URI("aws-sm:///bad"));
  }

  @Test(expected = IOException.class)
  public void testAWSSecretsManagerInvalidJson() throws Exception {
    Mockito.when(cache.get("test")).thenReturn("{\"password\":\"super-secret\"");
    source.getSecret(new URI("aws-sm:///test"));
  }

  @Test(expected = IOException.class)
  public void testAWSSecretsManagerArrayJson() throws Exception {
    Mockito.when(cache.get("test")).thenReturn("[]");
    source.getSecret(new URI("aws-sm:///test"));
  }
}
