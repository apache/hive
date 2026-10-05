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

package org.apache.iceberg.mr.hive.vended.hadoop;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.aliyun.AliyunProperties;
import org.apache.iceberg.io.StorageCredential;
import org.apache.iceberg.mr.hive.vended.CredentialProperties;
import org.apache.iceberg.mr.hive.vended.HadoopMapper;
import org.apache.iceberg.mr.hive.vended.PrefixUtil;

/** Maps Iceberg OSS FileIO properties to Hadoop Aliyun OSS filesystem keys. */
public enum OssMapper implements HadoopMapper {
  INSTANCE;

  @Override
  public boolean supportsPrefix(String prefix) {
    return "oss".equals(PrefixUtil.schemeFromPrefix(prefix));
  }

  @Override
  public boolean supportsConfigKey(String icebergKey) {
    return icebergKey.startsWith("client.") || icebergKey.startsWith("oss.") || icebergKey.startsWith("oss-");
  }

  @Override
  public String scopeFromPrefix(String prefix) {
    return PrefixUtil.scopeFromPrefix(prefix);
  }

  @Override
  public String toHadoopProperty(String bucket, String icebergKey) {
    return switch (icebergKey) {
      case AliyunProperties.CLIENT_ACCESS_KEY_ID, "oss-access-key-id" -> "fs.oss.accessKeyId";
      case AliyunProperties.CLIENT_ACCESS_KEY_SECRET, "oss-secret-access-key" -> "fs.oss.accessKeySecret";
      case AliyunProperties.CLIENT_SECURITY_TOKEN, "oss.security-token" -> "fs.oss.securityToken";
      case AliyunProperties.OSS_ENDPOINT, "oss-endpoint" -> "fs.oss.endpoint";
      default -> null;
    };
  }

  @Override
  public List<StorageCredential> credentialsFromProperties(String prefix, Map<String, String> props) {
    Map<String, String> normalized = normalizeOssProperties(props);
    if (StringUtils.isBlank(normalized.get(AliyunProperties.CLIENT_ACCESS_KEY_ID)) ||
        StringUtils.isBlank(normalized.get(AliyunProperties.CLIENT_ACCESS_KEY_SECRET))) {
      return List.of();
    }
    Map<String, String> config = new LinkedHashMap<>();
    CredentialProperties.putIfPresent(config, normalized, AliyunProperties.CLIENT_ACCESS_KEY_ID);
    CredentialProperties.putIfPresent(config, normalized, AliyunProperties.CLIENT_ACCESS_KEY_SECRET);
    CredentialProperties.putIfPresent(config, normalized, AliyunProperties.CLIENT_SECURITY_TOKEN);
    CredentialProperties.putIfPresent(config, normalized, AliyunProperties.OSS_ENDPOINT);
    return List.of(StorageCredential.create(prefix, config));
  }

  private static Map<String, String> normalizeOssProperties(Map<String, String> props) {
    Map<String, String> normalized = new LinkedHashMap<>(props);
    copyAlias(normalized, "oss-access-key-id", AliyunProperties.CLIENT_ACCESS_KEY_ID);
    copyAlias(normalized, "oss-secret-access-key", AliyunProperties.CLIENT_ACCESS_KEY_SECRET);
    copyAlias(normalized, "oss.security-token", AliyunProperties.CLIENT_SECURITY_TOKEN);
    copyAlias(normalized, "oss-endpoint", AliyunProperties.OSS_ENDPOINT);
    return normalized;
  }

  private static void copyAlias(Map<String, String> target, String alias, String canonical) {
    if (StringUtils.isBlank(target.get(canonical)) && StringUtils.isNotBlank(target.get(alias))) {
      target.put(canonical, target.get(alias));
    }
  }
}
