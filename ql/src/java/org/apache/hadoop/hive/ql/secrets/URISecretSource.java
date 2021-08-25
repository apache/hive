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

import com.google.common.annotations.VisibleForTesting;
import org.apache.curator.utils.CloseableUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * Class provides a way to load passwords using a URI. The secret sources are discovered using java service loader.
 * The
 */
public class URISecretSource {
  private static final URISecretSource INSTANCE = new URISecretSource();

  private final Map<String, SecretSource> sourcesMap = new HashMap<>();
  private URISecretSource() {
    // Find all the registered secretsource.
    ServiceLoader.load(SecretSource.class).forEach(this::register);

    // Cleanup resources.
    Runtime.getRuntime().addShutdownHook(new Thread(this::close));
  }

  public static URISecretSource getInstance() {
    return INSTANCE;
  }

  public String getPasswordFromUri(URI uri) throws IOException, HiveException {
    SecretSource source = sourcesMap.get(uri.getScheme());
    if (source == null) {
      throw new HiveException("Cannot fine secret source for scheme: " + uri.getScheme());
    }
    return source.getSecret(uri);
  }

  public void register(SecretSource source) {
    SecretSource oldSource = sourcesMap.put(source.getURIScheme(), source);
    if (oldSource != null) {
      throw new RuntimeException("Two sources for same scheme: " + source.getURIScheme() +  " [" +
          source.getClass().getName() + " and " + oldSource.getClass().getName() + "]");
    }
  }

  @VisibleForTesting
  void removeForTest(SecretSource source) {
    sourcesMap.remove(source.getURIScheme(), source);
  }

  private void close() {
    sourcesMap.values().forEach(CloseableUtils::closeQuietly);
  }
}
