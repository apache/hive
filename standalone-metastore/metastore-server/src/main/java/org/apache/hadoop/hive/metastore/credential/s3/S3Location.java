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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.metastore.credential.s3;

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.Path;
import software.amazon.awssdk.arns.Arn;

import java.util.Optional;
import java.util.Set;

/**
 * An S3 location.
 */
final class S3Location {
  private static final Set<String> SCHEMES = Set.of("s3", "s3a", "s3n");

  private final String partition;
  private final String bucket;
  private final String path;
  private final String escapedPath;

  private S3Location(String partition, String bucket, String path) {
    this.partition = partition;
    this.bucket = bucket;
    Preconditions.checkArgument(path.endsWith(Path.SEPARATOR));
    this.path = path;
    this.escapedPath = escapeIamGlobLiteral(path);
  }

  /**
   * Note that this is critical for security.
   * https://nvd.nist.gov/vuln/detail/cve-2026-42810
   * https://github.com/apache/polaris/blob/apache-polaris-1.7.0/polaris-core/src/main/java/org/apache/polaris/core/storage/aws/AwsCredentialsStorageIntegration.java#L531-L548
   */
  private static String escapeIamGlobLiteral(String value) {
    final var escaped = new StringBuilder(value.length() + 8);
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      switch (c) {
      case '*' -> escaped.append("${*}");
      case '?' -> escaped.append("${?}");
      case '$' -> escaped.append("${$}");
      default -> escaped.append(c);
      }
    }
    return escaped.toString();
  }

  static Optional<S3Location> create(String partition, Path path) {
    final var uri = path.toUri();
    final var scheme = uri.getScheme();
    if (scheme == null) {
      return Optional.empty();
    }
    if (!SCHEMES.contains(scheme)) {
      return Optional.empty();
    }
    final var bucket = uri.getAuthority();
    if (bucket == null) {
      return Optional.empty();
    }
    final var rawPath = uri.getPath();
    if (rawPath == null) {
      return Optional.empty();
    }
    final var dirPath = rawPath.endsWith(Path.SEPARATOR) ? rawPath : rawPath + Path.SEPARATOR;
    return Optional.of(new S3Location(partition, bucket, dirPath));
  }

  Arn getBucketArn() {
    return Arn.builder().partition(partition).service("s3").resource(bucket).build();
  }

  Arn getWildCardArn() {
    return Arn.builder().partition(partition).service("s3").resource("%s%s*".formatted(bucket, escapedPath)).build();
  }

  String getWildCardPath() {
    return escapedPath.substring(1) + "*";
  }

  boolean matches(String prefix) {
    final var optionalArn = Arn.tryFromString(prefix);
    if (optionalArn.isPresent()) {
      return getWildCardArn().toString().startsWith(prefix);
    }
    return "%s%s".formatted(bucket, path).startsWith(prefix);
  }
}
