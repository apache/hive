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
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.io.StorageCredential;
import org.apache.iceberg.mr.hive.vended.CredentialProperties;
import org.apache.iceberg.mr.hive.vended.HadoopMapper;
import org.apache.iceberg.mr.hive.vended.PrefixUtil;

/** Maps Iceberg GCS FileIO properties to Hadoop Google Cloud Storage connector keys. */
public enum GcsMapper implements HadoopMapper {
  INSTANCE;

  @Override
  public boolean supportsPrefix(String prefix) {
    String scheme = PrefixUtil.schemeFromPrefix(prefix);
    return "gs".equals(scheme) || "gcs".equals(scheme);
  }

  @Override
  public boolean supportsConfigKey(String icebergKey) {
    return icebergKey.startsWith("gcs.");
  }

  @Override
  public String scopeFromPrefix(String prefix) {
    return PrefixUtil.scopeFromPrefix(prefix);
  }

  @Override
  public String toHadoopProperty(String bucket, String icebergKey) {
    // The Google Cloud Storage Hadoop connector has no plain config key to inject a raw OAuth
    // token; a vended token requires fs.gs.auth.type=ACCESS_TOKEN_PROVIDER plus an
    // AccessTokenProvider implementation class. Until that is wired, gcs.oauth2.token and its
    // expiry travel only in the serialized Iceberg StorageCredential blob and are consumed by
    // Iceberg GCSFileIO. Only connectivity/config that maps to real connector keys is emitted.
    return switch (icebergKey) {
      case GCPProperties.GCS_PROJECT_ID -> "fs.gs.project.id";
      case GCPProperties.GCS_SERVICE_HOST -> "fs.gs.storage.root.url";
      default -> null;
    };
  }

  @Override
  public List<StorageCredential> credentialsFromProperties(String prefix, Map<String, String> props) {
    if (StringUtils.isBlank(props.get(GCPProperties.GCS_OAUTH2_TOKEN))) {
      return List.of();
    }
    Map<String, String> config = new LinkedHashMap<>();
    CredentialProperties.putIfPresent(config, props, GCPProperties.GCS_OAUTH2_TOKEN);
    CredentialProperties.putIfPresent(config, props, GCPProperties.GCS_OAUTH2_TOKEN_EXPIRES_AT);
    CredentialProperties.putIfPresent(config, props, GCPProperties.GCS_PROJECT_ID);
    CredentialProperties.putIfPresent(config, props, GCPProperties.GCS_SERVICE_HOST);
    return List.of(StorageCredential.create(prefix, config));
  }
}
