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

package org.apache.iceberg.hive;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestRestAccessDelegationMode {

  @Test
  void vendedCredentialsModeName() {
    assertThat(RestAccessDelegationMode.VENDED_CREDENTIALS.modeName()).isEqualTo("vended-credentials");
  }

  @Test
  void toHeaderValueJoinsModes() {
    assertThat(
            RestAccessDelegationMode.toHeaderValue(
                RestAccessDelegationMode.VENDED_CREDENTIALS, RestAccessDelegationMode.REMOTE_SIGNING))
        .isEqualTo("vended-credentials,remote-signing");
  }

  @Test
  void fromModeNameParsesCaseInsensitive() {
    assertThat(RestAccessDelegationMode.fromModeName("VENDED-CREDENTIALS"))
        .isEqualTo(RestAccessDelegationMode.VENDED_CREDENTIALS);
  }

  @Test
  void fromModeNameRejectsUnknown() {
    assertThatThrownBy(() -> RestAccessDelegationMode.fromModeName("unknown"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("unknown");
  }

  @Test
  void headerRequestsVendedCredentials() {
    assertThat(RestAccessDelegationMode.headerRequestsVendedCredentials(null)).isFalse();
    assertThat(RestAccessDelegationMode.headerRequestsVendedCredentials("remote-signing")).isFalse();
    assertThat(RestAccessDelegationMode.headerRequestsVendedCredentials("vended-credentials")).isTrue();
    assertThat(RestAccessDelegationMode.headerRequestsVendedCredentials("VENDED-CREDENTIALS,remote-signing"))
        .isTrue();
  }

  @Test
  void requestsVendedCredentialsFromConfiguration() {
    org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
    assertThat(IcebergCatalogProperties.requestsVendedCredentials("ice01", conf)).isFalse();

    conf.set(
        "iceberg.catalog.ice01.header.X-Iceberg-Access-Delegation",
        RestAccessDelegationMode.VENDED_CREDENTIALS.modeName());
    assertThat(IcebergCatalogProperties.requestsVendedCredentials("ice01", conf)).isTrue();
  }
}
