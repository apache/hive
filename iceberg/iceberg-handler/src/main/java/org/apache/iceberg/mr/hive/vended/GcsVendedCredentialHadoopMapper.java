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

/** Maps Iceberg GCS FileIO properties to Hadoop Google Cloud Storage connector keys. */
enum GcsVendedCredentialHadoopMapper implements VendedCredentialHadoopMapper {
  INSTANCE;

  static final String GCS_OAUTH2_TOKEN = "gcs.oauth2.token";
  static final String GCS_OAUTH2_TOKEN_EXPIRES_AT = "gcs.oauth2.token-expires-at";
  static final String GCS_PROJECT_ID = "gcs.project-id";
  static final String GCS_SERVICE_HOST = "gcs.service.host";

  @Override
  public boolean supportsPrefix(String prefix) {
    String scheme = VendedCredentialPrefixUtil.schemeFromPrefix(prefix);
    return "gs".equals(scheme) || "gcs".equals(scheme);
  }

  @Override
  public boolean supportsConfigKey(String icebergKey) {
    return icebergKey.startsWith("gcs.");
  }

  @Override
  public String scopeFromPrefix(String prefix) {
    return VendedCredentialPrefixUtil.scopeFromPrefix(prefix);
  }

  @Override
  public String toHadoopProperty(String bucket, String icebergKey) {
    // The Google Cloud Storage Hadoop connector has no plain config key to inject a raw OAuth
    // token; a vended token requires fs.gs.auth.type=ACCESS_TOKEN_PROVIDER plus an
    // AccessTokenProvider implementation class. Until that is wired, gcs.oauth2.token and its
    // expiry travel only in the serialized Iceberg StorageCredential blob and are consumed by
    // Iceberg GCSFileIO. Only connectivity/config that maps to real connector keys is emitted.
    return switch (icebergKey) {
      case GCS_PROJECT_ID -> "fs.gs.project.id";
      case GCS_SERVICE_HOST -> "fs.gs.storage.root.url";
      default -> null;
    };
  }
}
