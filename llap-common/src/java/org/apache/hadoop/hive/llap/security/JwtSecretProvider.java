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

package org.apache.hadoop.hive.llap.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;

import java.security.Key;

/**
 * JwtSecretProvider
 *
 * - provides encryption and decryption secrets for generating and parsing JWTs.
 *
 * - Hive internally uses method initAndGet() which initializes providers based on the value of config
 *   {@link HiveConf.ConfVars#LLAP_EXTERNAL_CLIENT_CLOUD_JWT_SHARED_SECRET_PROVIDER}.
 *   It expects implementations to provide default constructor and {@link #init(Configuration)} method.
 */
public interface JwtSecretProvider {

  /**
   * returns secret for signing JWT.
   */
  Key getEncryptionSecret();

  /**
   * returns secret for parsing JWT.
   */
  Key getDecryptionSecret();

  /**
   * Initializes the provider.
   * Should also contain any validations that we want to put on secret, helps us to fail fast.
   * @param conf configuration
   */
  void init(Configuration conf);

  /**
   *  Hive internally uses this method to obtain instance of {@link JwtSecretProvider}
   *
   * @param conf configuration
   * @return implementation of {@link HiveConf.ConfVars#LLAP_EXTERNAL_CLIENT_CLOUD_JWT_SHARED_SECRET_PROVIDER}
   */
  static JwtSecretProvider initAndGet(Configuration conf) {
    final String providerClass =
        HiveConf.getVar(conf, HiveConf.ConfVars.LLAP_EXTERNAL_CLIENT_CLOUD_JWT_SHARED_SECRET_PROVIDER);
    JwtSecretProvider provider;
    try {
      provider = (JwtSecretProvider) Class.forName(providerClass).newInstance();
      provider.init(conf);
    } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
      throw new RuntimeException("Unable to instantiate provider: " + providerClass, e);
    }
    return provider;
  }
}
