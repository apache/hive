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
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.security.Key;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.LLAP_EXTERNAL_CLIENT_CLOUD_DEPLOYMENT_SETUP_ENABLED;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.LLAP_EXTERNAL_CLIENT_CLOUD_JWT_SHARED_SECRET;

/**
 * Default implementation of {@link JwtSecretProvider}.
 *
 * 1. It first tries to get shared secret from conf {@link HiveConf.ConfVars#LLAP_EXTERNAL_CLIENT_CLOUD_JWT_SHARED_SECRET}
 * using {@link Configuration#getPassword(String)}.
 *
 * 2. If not found, it tries to read from env var {@link #LLAP_EXTERNAL_CLIENT_CLOUD_JWT_SHARED_SECRET_ENV_VAR}.
 *
 * If secret is not found even after 1) and 2), {@link #init(Configuration)} methods throws {@link IllegalStateException}.
 *
 * Length of shared secret provided in 1) or 2) should be &gt; 32 bytes.
 *
 * It uses the same encryption and decryption secret which can be used to sign and verify JWT.
 */
public class DefaultJwtSharedSecretProvider implements JwtSecretProvider {

  public static final String LLAP_EXTERNAL_CLIENT_CLOUD_JWT_SHARED_SECRET_ENV_VAR =
      "LLAP_EXTERNAL_CLIENT_CLOUD_JWT_SHARED_SECRET_ENV_VAR";

  private Key jwtEncryptionKey;

  @Override public Key getEncryptionSecret() {
    return jwtEncryptionKey;
  }

  @Override public Key getDecryptionSecret() {
    return jwtEncryptionKey;
  }

  @Override public void init(final Configuration conf) {
    char[] sharedSecret;
    byte[] sharedSecretBytes = null;

    // try getting secret from conf first
    // if not found, get from env var - LLAP_EXTERNAL_CLIENT_CLOUD_JWT_SHARED_SECRET_ENV_VAR
    try {
      sharedSecret = conf.getPassword(LLAP_EXTERNAL_CLIENT_CLOUD_JWT_SHARED_SECRET.varname);
    } catch (IOException e) {
      throw new RuntimeException("Unable to get password [hive.llap.external.client.cloud.jwt.shared.secret] - "
          + e.getMessage(), e);
    }
    if (sharedSecret != null) {
      ByteBuffer bb = StandardCharsets.UTF_8.encode(CharBuffer.wrap(sharedSecret));
      sharedSecretBytes = new byte[bb.remaining()];
      bb.get(sharedSecretBytes);
    } else {
      String sharedSecredFromEnv = System.getenv(LLAP_EXTERNAL_CLIENT_CLOUD_JWT_SHARED_SECRET_ENV_VAR);
      if (StringUtils.isNotBlank(sharedSecredFromEnv)) {
        sharedSecretBytes = sharedSecredFromEnv.getBytes();
      }
    }

    Preconditions.checkState(sharedSecretBytes != null,
        "With: " + LLAP_EXTERNAL_CLIENT_CLOUD_DEPLOYMENT_SETUP_ENABLED.varname + " = true, \n"
            + "To use: org.apache.hadoop.hive.llap.security.DefaultJwtSharedSecretProvider, \n"
            + "1. a non-null value of 'hive.llap.external.client.cloud.jwt.shared.secret' must be provided OR \n"
            + "2. alternatively environment variable "
            + "LLAP_EXTERNAL_CLIENT_CLOUD_JWT_SHARED_SECRET_ENV_VAR can also be set. \n"
            + "Length of the secret provided in 1) or 2) should be > 32 bytes.");

    this.jwtEncryptionKey = Keys.hmacShaKeyFor(sharedSecretBytes);
  }

}
