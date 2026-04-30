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

package org.apache.hadoop.hive.metastore.credential.s3;

import com.google.common.base.Preconditions;
import software.amazon.awssdk.arns.Arn;

import java.net.URI;
import java.util.Objects;
import java.util.Set;

/**
 * An S3 location.
 */
class S3Location {
  private static final Set<String> SCHEMES = Set.of("s3", "s3a", "s3n");

  private final String partition;
  private final String bucket;
  private final String path;

  private S3Location(String partition, String bucket, String path) {
    this.partition = partition;
    this.bucket = bucket;
    this.path = path;
  }

  static S3Location create(String partition, URI uri) {
    Preconditions.checkArgument(SCHEMES.contains(uri.getScheme()));
    Objects.requireNonNull(uri);
    final var bucket = Objects.requireNonNull(uri.getAuthority());
    final var rawPath = Objects.requireNonNull(uri.getPath());
    final var path = rawPath.endsWith("/") ? rawPath : rawPath + "/";
    return new S3Location(partition, bucket, path);
  }

  Arn getBucketArn() {
    return Arn.builder().partition(partition).service("s3").resource(bucket).build();
  }

  Arn getWildCardArn() {
    return Arn.builder().partition(partition).service("s3").resource("%s%s*".formatted(bucket, path)).build();
  }

  String getWildCardPath() {
    return path.substring(1) + "*";
  }

  boolean matches(String prefix) {
    final var optionalArn = Arn.tryFromString(prefix);
    if (optionalArn.isPresent()) {
      return getWildCardArn().toString().startsWith(prefix);
    }
    return "%s%s".formatted(bucket, path).startsWith(prefix);
  }
}
