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
package org.apache.hadoop.hive.ql.metadata;

import java.util.EnumSet;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FileSystem;

/**
 * File systems whose {@link FileSystem#rename(org.apache.hadoop.fs.Path, org.apache.hadoop.fs.Path)}
 * implementation is not atomic-if-absent and can silently overwrite an existing destination when
 * two concurrent writers race between an {@code exists()} probe and the subsequent rename call.
 * <p>
 * Object stores (S3, GCS, etc.) fall in this category: the S3A rename is a copy+delete on the
 * client, with the "does the destination exist?" check performed on the client before the copy;
 * two writers whose probes both fire before either PUT commits will both proceed and one will
 * silently overwrite the other.
 * <p>
 * Callers use this enum to decide whether to apply defensive strategies such as suffixing the
 * destination filename with a per-query tag so concurrent writers pick distinct keys — see
 * {@code Hive#mvFile}. It is intentionally an in-code enum rather than a configuration knob:
 * the set of unsafe filesystems is a property of the filesystem implementation, not something an
 * operator should override.
 */
public enum UnstableRenameFileSystem {
  S3A("s3a"),
  S3N("s3n"),
  S3("s3"),
  // Google Cloud Storage exposes the same "rename is copy+delete" semantics through the Hadoop
  // connector; keep here so multi-cloud deployments are covered without further edits.
  GS("gs");

  private final String scheme;

  UnstableRenameFileSystem(String scheme) {
    this.scheme = scheme;
  }

  public String scheme() {
    return scheme;
  }

  private static final Set<String> SCHEMES = EnumSet.allOf(UnstableRenameFileSystem.class).stream()
      .map(UnstableRenameFileSystem::scheme).collect(Collectors.toSet());

  /**
   * @return {@code true} when {@code scheme} matches one of the known unstable-rename
   *         filesystems; {@code false} otherwise (including {@code null} / empty).
   */
  public static boolean matches(String scheme) {
    return scheme != null && SCHEMES.contains(scheme.toLowerCase(Locale.ROOT));
  }

  /**
   * Convenience overload that inspects a {@link FileSystem}'s URI scheme.
   *
   * @return {@code true} when the filesystem's scheme matches one of the known unstable-rename
   *         filesystems.
   */
  public static boolean matches(FileSystem fs) {
    if (fs == null || fs.getUri() == null) {
      return false;
    }
    return matches(fs.getUri().getScheme());
  }
}
