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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.Table;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.StorageCredential;
import org.apache.iceberg.mr.hive.vended.hadoop.GcsMapper;
import org.apache.iceberg.mr.hive.vended.hadoop.OssMapper;

/** Public helpers for {@link org.apache.iceberg.mr.hive.IcebergVendedCredentialUtil}. */
public final class Support {

  private Support() {
  }

  public static String storagePrefixFromLocation(String location) {
    return PrefixUtil.storagePrefixFromLocation(location);
  }

  public static String scopeFromPrefix(String prefix) {
    return PrefixUtil.scopeFromPrefix(prefix);
  }

  public static HadoopMapper mapperFor(StorageCredential credential) {
    return HadoopMappers.forCredential(credential);
  }

  public static String toHadoopProperty(HadoopMapper mapper, String scope, String icebergKey) {
    if (mapper == null || scope == null) {
      return null;
    }
    return mapper.toHadoopProperty(scope, icebergKey);
  }

  public static Map<String, String> additionalNonSecretHadoopProperties(
      HadoopMapper mapper, String scope, Map<String, String> config) {
    if (mapper == null) {
      return Map.of();
    }
    return mapper.additionalNonSecretHadoopProperties(scope, config);
  }

  public static List<StorageCredential> credentialsFromFileIoProperties(Table table, FileIO io) {
    Map<String, String> props = io.properties();
    if (props == null || props.isEmpty()) {
      return List.of();
    }
    String prefix = storagePrefixFromLocation(table.location());
    List<StorageCredential> s3 = credentialsFromS3Properties(prefix, props);
    if (!s3.isEmpty()) {
      return s3;
    }
    List<StorageCredential> gcs = credentialsFromGcsProperties(prefix, props);
    if (!gcs.isEmpty()) {
      return gcs;
    }
    List<StorageCredential> adls = credentialsFromAdlsProperties(prefix, props);
    if (!adls.isEmpty()) {
      return adls;
    }
    return credentialsFromOssProperties(prefix, props);
  }

  private static List<StorageCredential> credentialsFromS3Properties(
      String prefix, Map<String, String> props) {
    if (StringUtils.isBlank(props.get(S3FileIOProperties.ACCESS_KEY_ID)) ||
        StringUtils.isBlank(props.get(S3FileIOProperties.SECRET_ACCESS_KEY))) {
      return List.of();
    }
    Map<String, String> config = new LinkedHashMap<>();
    putIfPresent(config, props, S3FileIOProperties.ACCESS_KEY_ID);
    putIfPresent(config, props, S3FileIOProperties.SECRET_ACCESS_KEY);
    putIfPresent(config, props, S3FileIOProperties.SESSION_TOKEN);
    putIfPresent(config, props, S3FileIOProperties.ENDPOINT);
    putIfPresent(config, props, S3FileIOProperties.PATH_STYLE_ACCESS);
    putIfPresent(config, props, AwsClientProperties.CLIENT_REGION);
    return List.of(StorageCredential.create(prefix, config));
  }

  private static List<StorageCredential> credentialsFromGcsProperties(
      String prefix, Map<String, String> props) {
    if (StringUtils.isBlank(props.get(GcsMapper.GCS_OAUTH2_TOKEN))) {
      return List.of();
    }
    Map<String, String> config = new LinkedHashMap<>();
    putIfPresent(config, props, GcsMapper.GCS_OAUTH2_TOKEN);
    putIfPresent(config, props, GcsMapper.GCS_OAUTH2_TOKEN_EXPIRES_AT);
    putIfPresent(config, props, GcsMapper.GCS_PROJECT_ID);
    putIfPresent(config, props, GcsMapper.GCS_SERVICE_HOST);
    return List.of(StorageCredential.create(prefix, config));
  }

  private static List<StorageCredential> credentialsFromAdlsProperties(
      String prefix, Map<String, String> props) {
    Map<String, String> config = new LinkedHashMap<>();
    props.forEach((key, value) -> {
      if (key.startsWith("adls.") && StringUtils.isNotBlank(value)) {
        config.put(key, value);
      }
    });
    if (config.isEmpty()) {
      return List.of();
    }
    return List.of(StorageCredential.create(prefix, config));
  }

  private static List<StorageCredential> credentialsFromOssProperties(
      String prefix, Map<String, String> props) {
    if (StringUtils.isBlank(props.get(OssMapper.CLIENT_ACCESS_KEY_ID)) ||
        StringUtils.isBlank(props.get(OssMapper.CLIENT_ACCESS_KEY_SECRET))) {
      return List.of();
    }
    Map<String, String> config = new LinkedHashMap<>();
    putIfPresent(config, props, OssMapper.CLIENT_ACCESS_KEY_ID);
    putIfPresent(config, props, OssMapper.CLIENT_ACCESS_KEY_SECRET);
    putIfPresent(config, props, OssMapper.CLIENT_SECURITY_TOKEN);
    putIfPresent(config, props, OssMapper.OSS_ENDPOINT);
    return List.of(StorageCredential.create(prefix, config));
  }

  private static void putIfPresent(Map<String, String> target, Map<String, String> source, String key) {
    if (source.containsKey(key) && StringUtils.isNotBlank(source.get(key))) {
      target.put(key, source.get(key));
    }
  }
}
