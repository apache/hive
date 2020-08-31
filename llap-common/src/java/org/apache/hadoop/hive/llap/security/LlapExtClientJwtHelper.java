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

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.Jwts;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;

import java.util.Date;
import java.util.UUID;

/**
 * Contains helper methods for generating and verifying JWTs for external llap clients.
 * Initializes and uses {@link JwtSecretProvider} to obtain encryption and decryption secret.
 */
public class LlapExtClientJwtHelper {

  public static final String LLAP_JWT_SUBJECT = "llap";
  public static final String LLAP_EXT_CLIENT_APP_ID = "llap_ext_client_app_id";
  private final JwtSecretProvider jwtSecretProvider;

  public LlapExtClientJwtHelper(Configuration conf) {
    this.jwtSecretProvider = JwtSecretProvider.initAndGet(conf);
  }

  /**
   * @param extClientAppId application Id - application Id injected by get_splits
   * @return JWT signed with {@link JwtSecretProvider#getEncryptionSecret()}.
   * As of now this JWT contains extClientAppId in claims.
   */
  public String buildJwtForLlap(ApplicationId extClientAppId) {
    return Jwts.builder()
        .setSubject(LLAP_JWT_SUBJECT)
        .setIssuedAt(new Date())
        .setId(UUID.randomUUID().toString())
        .claim(LLAP_EXT_CLIENT_APP_ID, extClientAppId.toString())
        .signWith(jwtSecretProvider.getEncryptionSecret())
        .compact();
  }

  /**
   *
   * @param jwt signed JWT String
   * @return claims present in JWT, this method parses jwt using {@link JwtSecretProvider#getDecryptionSecret()}
   */
  public Jws<Claims> parseClaims(String jwt) {
    return Jwts.parser()
        .setSigningKey(jwtSecretProvider.getDecryptionSecret())
        .parseClaimsJws(jwt);
  }

}
