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

package org.apache.hive.kubernetes.operator.model.spec;

import com.fasterxml.jackson.annotation.JsonPropertyDescription;

/**
 * Configuration for S3-compatible storage backend.
 * Supports any S3-compatible endpoint such as Apache Ozone (via Helm),
 * MinIO, or AWS S3.
 */
public class StorageSpec {

  @JsonPropertyDescription(
      "S3-compatible endpoint URL. "
      + "Example: http://ozone-s3g-rest:9878 or https://s3.amazonaws.com")
  private String endpoint;

  @JsonPropertyDescription(
      "Bucket name for the Hive scratch directory")
  private String bucket = "hive";

  @JsonPropertyDescription(
      "Whether to use path-style access for S3 requests")
  private boolean pathStyleAccess = true;

  @JsonPropertyDescription(
      "S3 access key as plain text (dev/test only)")
  private String accessKey;

  @JsonPropertyDescription(
      "S3 secret key as plain text (dev/test only)")
  private String secretKey;

  @JsonPropertyDescription(
      "Reference to a Kubernetes Secret containing the S3 access key. "
      + "Takes precedence over accessKey.")
  private SecretKeyRef accessKeySecretRef;

  @JsonPropertyDescription(
      "Reference to a Kubernetes Secret containing the S3 secret key. "
      + "Takes precedence over secretKey.")
  private SecretKeyRef secretKeySecretRef;

  public String getEndpoint() {
    return endpoint;
  }

  public void setEndpoint(String endpoint) {
    this.endpoint = endpoint;
  }

  public String getBucket() {
    return bucket;
  }

  public void setBucket(String bucket) {
    this.bucket = bucket;
  }

  public boolean isPathStyleAccess() {
    return pathStyleAccess;
  }

  public void setPathStyleAccess(boolean pathStyleAccess) {
    this.pathStyleAccess = pathStyleAccess;
  }

  public String getAccessKey() {
    return accessKey;
  }

  public void setAccessKey(String accessKey) {
    this.accessKey = accessKey;
  }

  public String getSecretKey() {
    return secretKey;
  }

  public void setSecretKey(String secretKey) {
    this.secretKey = secretKey;
  }

  public SecretKeyRef getAccessKeySecretRef() {
    return accessKeySecretRef;
  }

  public void setAccessKeySecretRef(SecretKeyRef accessKeySecretRef) {
    this.accessKeySecretRef = accessKeySecretRef;
  }

  public SecretKeyRef getSecretKeySecretRef() {
    return secretKeySecretRef;
  }

  public void setSecretKeySecretRef(SecretKeyRef secretKeySecretRef) {
    this.secretKeySecretRef = secretKeySecretRef;
  }
}
