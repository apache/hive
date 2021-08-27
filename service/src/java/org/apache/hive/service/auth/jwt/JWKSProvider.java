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

package org.apache.hive.service.auth.jwt;

import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.jwk.JWK;

import java.util.List;

/**
 * Provides a way to get JWKS json. Hive will use this to verify the incoming JWTs.
 */
public interface JWKSProvider {

  /**
   * Fetches the JWKS, the JWKS are expected to be in the standard form as defined here -
   * https://datatracker.ietf.org/doc/html/rfc7517#appendix-A
   *
   * @return JWKS Json.
   * @param header
   */
  List<JWK> getJWKs(JWSHeader header);
}
