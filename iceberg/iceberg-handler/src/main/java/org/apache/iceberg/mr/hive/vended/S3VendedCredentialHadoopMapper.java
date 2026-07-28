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

import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;

enum S3VendedCredentialHadoopMapper implements VendedCredentialHadoopMapper {
  INSTANCE;

  @Override
  public boolean supportsPrefix(String prefix) {
    String scheme = VendedCredentialPrefixUtil.schemeFromPrefix(prefix);
    return "s3".equals(scheme) || "s3a".equals(scheme) || "s3n".equals(scheme);
  }

  @Override
  public boolean supportsConfigKey(String icebergKey) {
    return icebergKey.startsWith("s3.") || AwsClientProperties.CLIENT_REGION.equals(icebergKey);
  }

  @Override
  public String scopeFromPrefix(String prefix) {
    return VendedCredentialPrefixUtil.scopeFromPrefix(prefix);
  }

  @Override
  public String toHadoopProperty(String bucket, String icebergKey) {
    if (bucket == null) {
      return null;
    }
    String bucketPrefix = "fs.s3a.bucket." + bucket + ".";
    return switch (icebergKey) {
      case S3FileIOProperties.ACCESS_KEY_ID -> bucketPrefix + "access.key";
      case S3FileIOProperties.SECRET_ACCESS_KEY -> bucketPrefix + "secret.key";
      case S3FileIOProperties.SESSION_TOKEN -> bucketPrefix + "session.token";
      case S3FileIOProperties.ENDPOINT -> bucketPrefix + "endpoint";
      case S3FileIOProperties.PATH_STYLE_ACCESS -> bucketPrefix + "path.style.access";
      case AwsClientProperties.CLIENT_REGION -> bucketPrefix + "endpoint.region";
      default -> null;
    };
  }
}
