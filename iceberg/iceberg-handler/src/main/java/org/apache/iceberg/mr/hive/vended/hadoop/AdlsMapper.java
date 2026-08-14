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

package org.apache.iceberg.mr.hive.vended.hadoop;

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.mr.hive.vended.HadoopMapper;
import org.apache.iceberg.mr.hive.vended.PrefixUtil;

/** Maps Iceberg ADLS FileIO properties to Hadoop Azure connector keys (ABFS or WASB). */
public enum AdlsMapper implements HadoopMapper {
  INSTANCE;

  public static final String ADLS_SAS_TOKEN_PREFIX = "adls.sas-token.";
  public static final String ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX = "adls.sas-token-expires-at-ms.";
  public static final String ADLS_SHARED_KEY_ACCOUNT_KEY = "adls.auth.shared-key.account.key";

  private static final String DFS_CORE_WINDOWS_NET_SUFFIX = ".dfs.core.windows.net";
  private static final String BLOB_CORE_WINDOWS_NET_SUFFIX = ".blob.core.windows.net";

  @Override
  public boolean supportsPrefix(String prefix) {
    String scheme = PrefixUtil.schemeFromPrefix(prefix);
    return "abfs".equals(scheme) || "abfss".equals(scheme) || "wasb".equals(scheme) ||
        "wasbs".equals(scheme);
  }

  @Override
  public boolean supportsConfigKey(String icebergKey) {
    return icebergKey.startsWith("adls.");
  }

  @Override
  public String scopeFromPrefix(String prefix) {
    return PrefixUtil.scopeFromPrefix(prefix);
  }

  @Override
  public String toHadoopProperty(String scope, String icebergKey) {
    String authoritySuffix = authoritySuffixFromScope(scope);
    // SAS token: the Iceberg key carries the account (adls.sas-token.<account>), so the
    // account-scoped fixed-token key is derived directly from it. ABFS also requires
    // fs.azure.account.auth.type.<account>...=SAS, emitted by additionalNonSecretHadoopProperties.
    if (icebergKey.startsWith(ADLS_SAS_TOKEN_PREFIX) &&
        !icebergKey.startsWith(ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX)) {
      String account = icebergKey.substring(ADLS_SAS_TOKEN_PREFIX.length());
      return "fs.azure.sas.fixed.token." + account + authoritySuffix;
    }
    // Shared-key: the account name is not in this entry, so it is taken from the storage prefix.
    // SharedKey is the default auth type, so no auth-type companion property is needed.
    if (icebergKey.equals(ADLS_SHARED_KEY_ACCOUNT_KEY)) {
      String account = accountFromScope(scope);
      if (account != null) {
        return "fs.azure.account.key." + account + authoritySuffix;
      }
      return null;
    }
    // adls.token (OAuth bearer) has no plain ABFS config key — ABFS OAuth needs a provider type
    // (client id/secret/endpoint or refresh token). It rides the Iceberg StorageCredential blob.
    return null;
  }

  @Override
  public Map<String, String> additionalNonSecretHadoopProperties(String scope, Map<String, String> config) {
    String authoritySuffix = authoritySuffixFromScope(scope);
    Map<String, String> extra = new LinkedHashMap<>();
    for (String key : config.keySet()) {
      if (key.startsWith(ADLS_SAS_TOKEN_PREFIX) && !key.startsWith(ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX)) {
        String account = key.substring(ADLS_SAS_TOKEN_PREFIX.length());
        extra.put("fs.azure.account.auth.type." + account + authoritySuffix, "SAS");
      }
    }
    return extra;
  }

  private static String authoritySuffixFromScope(String scope) {
    if (StringUtils.isBlank(scope)) {
      return DFS_CORE_WINDOWS_NET_SUFFIX;
    }
    int at = scope.indexOf('@');
    String host = at < 0 ? scope : scope.substring(at + 1);
    if (host.endsWith(BLOB_CORE_WINDOWS_NET_SUFFIX)) {
      return BLOB_CORE_WINDOWS_NET_SUFFIX;
    }
    return DFS_CORE_WINDOWS_NET_SUFFIX;
  }

  /**
   * Storage account from a credential prefix scope such as
   * {@code container@account.dfs.core.windows.net} or {@code container@account.blob.core.windows.net},
   * with the Azure authority suffix stripped.
   */
  private static String accountFromScope(String scope) {
    if (StringUtils.isBlank(scope)) {
      return null;
    }
    int at = scope.indexOf('@');
    String host = at < 0 ? scope : scope.substring(at + 1);
    if (host.endsWith(DFS_CORE_WINDOWS_NET_SUFFIX)) {
      host = host.substring(0, host.length() - DFS_CORE_WINDOWS_NET_SUFFIX.length());
    } else if (host.endsWith(BLOB_CORE_WINDOWS_NET_SUFFIX)) {
      host = host.substring(0, host.length() - BLOB_CORE_WINDOWS_NET_SUFFIX.length());
    }
    return StringUtils.defaultIfBlank(host, null);
  }
}
