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

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;

/**
 * Interface representing source of a secret using an uri.
 * The URI scheme is used to match an URI to an implementation scheme. The implementations are discovered and loaded
 * using java service loader. Currently, there isn't a way to initialize or reset a SecretSource after construction.
 *
 * The secret source is expected to be thread-safe.
 */
public interface SecretSource extends Closeable {
  /**
   * The scheme string which this implementation will handle.
   * @return The scheme string.
   */
  String getURIScheme();

  /**
   * Get the secret associated with the given URI.
   * @param uri The uri which has information required to fetch/compute the password.
   * @return The password.
   * @throws IOException
   */
  String getSecret(URI uri) throws IOException;
}