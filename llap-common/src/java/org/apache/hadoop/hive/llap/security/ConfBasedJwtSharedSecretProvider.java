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

import com.google.common.base.Preconditions;
import io.jsonwebtoken.security.Keys;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;

import java.security.Key;

/**
 * Default implementation of {@link JwtSecretProvider}.
 * It uses the same encryption and decryption secret which can be used to sign and verify JWT.
 */
public class ConfBasedJwtSharedSecretProvider implements JwtSecretProvider {

  private Key jwtEncryptionKey;

  @Override public Key getEncryptionSecret() {
    return jwtEncryptionKey;
  }

  @Override public Key getDecryptionSecret() {
    return jwtEncryptionKey;
  }

  @Override public void init(final Configuration conf) {
    final String sharedSecret = HiveConf.getVar(conf, HiveConf.ConfVars.LLAP_EXTERNAL_CLIENT_CLOUD_JWT_SHARED_SECRET);
    Preconditions.checkNotNull(sharedSecret,
        "To use: org.apache.hadoop.hive.llap.security.ConfBasedJwtSharedSecretProvider, "
            + "a non-null value of 'hive.llap.external.client.cloud.jwt.shared.secret' must be provided");
    this.jwtEncryptionKey = Keys.hmacShaKeyFor(sharedSecret.getBytes());
  }

}
