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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hive.IcebergCatalogProperties;

/**
 * Utilities for Iceberg REST catalog access delegation, including the {@code X-Iceberg-Access-Delegation}
 * catalog property and vended-credential configuration.
 */
public final class RestCatalogAccessDelegation {

  /** Iceberg REST catalog property prefix for {@code X-Iceberg-Access-Delegation}. */
  public static final String ACCESS_DELEGATION_PROPERTY = "header.X-Iceberg-Access-Delegation";

  private RestCatalogAccessDelegation() {
  }

  /**
   * Returns true when the catalog is configured to request REST vended storage credentials via
   * {@link #ACCESS_DELEGATION_PROPERTY}.
   */
  public static boolean requestsVendedCredentials(String catalogName, Configuration conf) {
    if (conf == null || StringUtils.isEmpty(catalogName)) {
      return false;
    }
    String headerValue =
        conf.get(IcebergCatalogProperties.catalogPropertyConfigKey(catalogName, ACCESS_DELEGATION_PROPERTY));
    return RestAccessDelegationMode.headerRequestsVendedCredentials(headerValue);
  }
}
