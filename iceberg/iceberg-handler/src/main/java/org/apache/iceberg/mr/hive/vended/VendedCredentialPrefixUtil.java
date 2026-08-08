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

package org.apache.iceberg.mr.hive.vended;

import org.apache.commons.lang3.StringUtils;

/** Helpers for parsing Iceberg {@link org.apache.iceberg.io.StorageCredential#prefix()} values. */
final class VendedCredentialPrefixUtil {

  private VendedCredentialPrefixUtil() {
  }

  static String schemeFromPrefix(String prefix) {
    if (prefix == null) {
      return null;
    }
    int schemeEnd = prefix.indexOf("://");
    if (schemeEnd <= 0) {
      return null;
    }
    return prefix.substring(0, schemeEnd).toLowerCase();
  }

  /**
   * Authority segment of a storage prefix ({@code s3://bucket/path} → {@code bucket},
   * {@code abfss://container@account.dfs.core.windows.net/path} →
   * {@code container@account.dfs.core.windows.net}).
   */
  @SuppressWarnings("java:S1075")
  static String scopeFromPrefix(String prefix) {
    int schemeEnd = prefix == null ? -1 : prefix.indexOf("://");
    if (schemeEnd < 0) {
      return null;
    }
    String withoutScheme = prefix.substring(schemeEnd + 3);
    int slash = withoutScheme.indexOf('/');
    String scope = slash >= 0 ? withoutScheme.substring(0, slash) : withoutScheme;
    return StringUtils.defaultIfBlank(scope, null);
  }

  /** Normalizes a table location to a credential prefix with trailing slash. */
  @SuppressWarnings("java:S1075")
  static String storagePrefixFromLocation(String location) {
    if (StringUtils.isBlank(location)) {
      return "";
    }
    int schemeEnd = location.indexOf("://");
    if (schemeEnd < 0) {
      return location.endsWith("/") ? location : location + "/";
    }
    int pathStart = location.indexOf('/', schemeEnd + 3);
    String base = pathStart >= 0 ? location.substring(0, pathStart) : location;
    return base.endsWith("/") ? base : base + "/";
  }
}
