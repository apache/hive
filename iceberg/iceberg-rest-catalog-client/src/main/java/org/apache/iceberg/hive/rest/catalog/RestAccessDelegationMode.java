/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.hive.rest.catalog;

import java.util.Arrays;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;

/**
 * Values for the Iceberg REST catalog {@code X-Iceberg-Access-Delegation} request header. The header
 * accepts a comma-separated list of these modes; configure via
 * {@link RestCatalogAccessDelegation#ACCESS_DELEGATION_PROPERTY}.
 *
 * @see <a href="https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml">REST catalog spec</a>
 */
public enum RestAccessDelegationMode {
  VENDED_CREDENTIALS("vended-credentials"),
  REMOTE_SIGNING("remote-signing");

  private final String modeName;

  RestAccessDelegationMode(String modeName) {
    this.modeName = modeName;
  }

  /** Spec-defined header token for this delegation mode. */
  public String modeName() {
    return modeName;
  }

  /** Comma-separated list suitable for {@link RestCatalogAccessDelegation#ACCESS_DELEGATION_PROPERTY}. */
  public static String toHeaderValue(RestAccessDelegationMode... modes) {
    return Arrays.stream(modes).map(RestAccessDelegationMode::modeName).collect(Collectors.joining(","));
  }

  /** Parses a single mode name (case-insensitive); throws if unknown. */
  public static RestAccessDelegationMode fromModeName(String modeName) {
    for (RestAccessDelegationMode mode : values()) {
      if (mode.modeName.equalsIgnoreCase(modeName.trim())) {
        return mode;
      }
    }
    throw new IllegalArgumentException(
        String.format(
            "Unknown REST access delegation mode: %s. Valid values are: %s",
            modeName,
            Arrays.stream(values()).map(RestAccessDelegationMode::modeName).collect(Collectors.joining(", "))));
  }

  /** Returns true if the {@code X-Iceberg-Access-Delegation} header value includes vended credentials. */
  public static boolean headerRequestsVendedCredentials(String headerValue) {
    if (StringUtils.isBlank(headerValue)) {
      return false;
    }
    for (String token : headerValue.split(",")) {
      if (VENDED_CREDENTIALS.modeName.equalsIgnoreCase(token.trim())) {
        return true;
      }
    }
    return false;
  }
}
