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
import java.util.Set;
import java.util.function.Predicate;

/**
   * Enumeration of Materialized view query rewrite algorithms.
   */
public enum RewriteAlgorithm {
  /**
   * Query sql text is compared to stored materialized view definition sql texts.
   */
  TEXT,
  /**
   * Use rewriting algorithm provided by Calcite.
   */
  CALCITE;

  public static final EnumSet<RewriteAlgorithm> ALL = EnumSet.allOf(RewriteAlgorithm.class);

  public static final Predicate<Set<RewriteAlgorithm>> ANY =
      rewriteAlgorithms -> true;
  public static final Predicate<Set<RewriteAlgorithm>> NON_CALCITE =
      rewriteAlgorithms -> !rewriteAlgorithms.contains(CALCITE);
}
