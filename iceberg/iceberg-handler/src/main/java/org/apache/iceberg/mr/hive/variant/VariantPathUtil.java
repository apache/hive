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

package org.apache.iceberg.mr.hive.variant;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.iceberg.expressions.UnboundExtract;
import org.apache.iceberg.expressions.UnboundPredicate;

public final class VariantPathUtil {

  // Variant field names (matching ParquetVariantVisitor package-protected constants)
  public static final String METADATA = "metadata";
  public static final String VALUE = "value";
  public static final String TYPED_VALUE = "typed_value";
  public static final String TYPED_VALUE_SEGMENT = ".typed_value.";

  private VariantPathUtil() {
  }

  /**
   * Parse a simple object-only JSON path (RFC9535 shorthand, e.g. {@code $.a} or {@code $.a.b}) into path segments.
   * Returns {@code null} when the path is unsupported (arrays, wildcards, recursive descent, or empty).
   */
  public static List<String> parseSimpleObjectPath(String jsonPath) {
    if (jsonPath == null) {
      return null;
    }

    String trimmed = jsonPath.trim();
    if (!trimmed.startsWith("$.") || trimmed.length() <= 2) {
      return null;
    }

    String remaining = trimmed.substring(2);
    if (remaining.contains("[") || remaining.contains("]") || remaining.contains("*") || remaining.contains("..")) {
      return null;
    }

    List<String> segments = splitPath(remaining);
    return segments.isEmpty() ? null : segments;
  }

  /**
   * Builds a shredded pseudo-column path from a base variant column and path segments (e.g. base {@code payload},
   * segments {@code ["a", "b"]} -> {@code payload.typed_value.a.b}).
   */
  public static String shreddedColumnPath(String variantColumn, List<String> segments) {
    Objects.requireNonNull(variantColumn, "variantColumn");
    if (segments == null || segments.isEmpty()) {
      throw new IllegalArgumentException("segments must not be empty for shredded column path");
    }
    return variantColumn + TYPED_VALUE_SEGMENT + String.join(".", segments);
  }

  /**
   * Splits a dot-delimited path into segments, ignoring empty segments.
   * @param path the path string (e.g. {@code a.b} without a leading {@code $.})
   */
  public static List<String> splitPath(String path) {
    if (path == null || path.isEmpty()) {
      return Collections.emptyList();
    }
    return Arrays.stream(path.split("\\.")).filter(s -> !s.isEmpty())
        .toList();
  }

  /**
   * Detects a variant shredded column reference from an unbound predicate: either a typed_value column name, or an
   * extract(...) term.
   */
  public static String extractVariantShreddedColumn(UnboundPredicate<?> predicate) {
    // Common case: predicate already uses shredded column references (e.g. "payload.typed_value.tier").
    String column = predicate.ref().name();
    if (column != null && column.contains(TYPED_VALUE_SEGMENT)) {
      return column;
    }

    // Variant extract case: rewrite extract(payload, "$.tier", "string") into "payload.typed_value.tier".
    if (predicate.term() instanceof UnboundExtract<?> extract) {
      String base = extract.ref().name();
      if (base == null || base.isEmpty()) {
        return null;
      }

      List<String> segments = parseSimpleObjectPath(extract.path());
      if (segments == null || segments.isEmpty()) {
        return null;
      }

      return shreddedColumnPath(base, segments);
    }

    return column;
  }
}
