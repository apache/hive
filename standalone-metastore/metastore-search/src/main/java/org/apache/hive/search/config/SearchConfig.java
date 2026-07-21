/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.search.config;

import java.time.Duration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hive.search.exception.SearchException;

public record SearchConfig(Configuration configuration) {
  public static final String REFRESH_INTERVAL_SECONDS = "metastore.search.refresh.interval.seconds";
  public static final long REFRESH_INTERVAL_SECONDS_DEFAULT = 1L;

  public static final String DEFAULT_LIMIT = "metastore.search.default.limit";
  public static final int DEFAULT_LIMIT_DEFAULT = 10;

  public static final String HYBRID_SEMANTIC_WEIGHT = "metastore.search.hybrid.semantic.weight";
  public static final float HYBRID_SEMANTIC_WEIGHT_DEFAULT = 0.4f;

  public static final String FUSION_PRIOR = "metastore.search.fusion.prior";
  public static final float FUSION_PRIOR_DEFAULT = 0.5f;

  public static final String BAYESIAN_SAMPLES = "metastore.search.bayesian.samples";
  public static final int BAYESIAN_SAMPLES_DEFAULT = 100;

  public static final String BAYESIAN_TOKENS_PER_QUERY = "metastore.search.bayesian.tokens.per.query";
  public static final int BAYESIAN_TOKENS_PER_QUERY_DEFAULT = 5;

  public static final String SEMANTIC_K_MULTIPLIER = "metastore.search.semantic.k.multiplier";
  public static final int SEMANTIC_K_MULTIPLIER_DEFAULT = 5;

  /** Max semantic segments per table ({@code search_text_0} head + column batches). */
  public static final String SEMANTIC_SEGMENT_MAX = "metastore.search.semantic.segments.max";
  public static final int SEMANTIC_SEGMENT_MAX_DEFAULT = 4;

  /** Soft char limit per segment; keep below embedder {@code maxSeqLen} in token count. */
  public static final String SEMANTIC_SEGMENT_MAX_CHARS = "metastore.search.semantic.segments.maxChars";
  /** Default assumes ~4 chars/token and 512 maxSeqLen (incl. special tokens). */
  public static final int SEMANTIC_SEGMENT_MAX_CHARS_DEFAULT = 1800;

  public static final String BAYESIAN_SEED = "metastore.search.bayesian.seed";
  public static final long BAYESIAN_SEED_DEFAULT = 42L;

  public Duration getRefreshInterval() {
    return Duration.ofSeconds(
        configuration.getLong(REFRESH_INTERVAL_SECONDS, REFRESH_INTERVAL_SECONDS_DEFAULT));
  }

  public int getDefaultLimit() {
    return configuration.getInt(DEFAULT_LIMIT, DEFAULT_LIMIT_DEFAULT);
  }

  public float getHybridSemanticWeight() throws SearchException {
    float weight = configuration.getFloat(HYBRID_SEMANTIC_WEIGHT, HYBRID_SEMANTIC_WEIGHT_DEFAULT);
    if (weight >= 1.0f || weight <= 0.0f) {
      throw new SearchException("Invalid hybrid semantic weight, " +
          "it must be in (0, 1), but got " + weight);
    }
    return weight;
  }

  public float getHybridMatchWeight() throws SearchException {
    return 1.0f - getHybridSemanticWeight();
  }

  public float getFusionPrior() {
    return configuration.getFloat(FUSION_PRIOR, FUSION_PRIOR_DEFAULT);
  }

  public int getBayesianSamples() {
    return configuration.getInt(BAYESIAN_SAMPLES, BAYESIAN_SAMPLES_DEFAULT);
  }

  public int getBayesianTokensPerQuery() {
    return configuration.getInt(BAYESIAN_TOKENS_PER_QUERY, BAYESIAN_TOKENS_PER_QUERY_DEFAULT);
  }

  public long getBayesianSeed() {
    return configuration.getLong(BAYESIAN_SEED, BAYESIAN_SEED_DEFAULT);
  }

  /** kNN candidate count for semantic retrieval (oversamples vs hit limit). */
  public int semanticK(int limit) {
    int effectiveLimit = limit > 0 ? limit : getDefaultLimit();
    int multiplier = Math.max(1, configuration.getInt(SEMANTIC_K_MULTIPLIER, SEMANTIC_K_MULTIPLIER_DEFAULT));
    return Math.max(effectiveLimit, effectiveLimit * multiplier);
  }

  public int getSemanticSegmentMax() {
    return Math.max(1, configuration.getInt(SEMANTIC_SEGMENT_MAX, SEMANTIC_SEGMENT_MAX_DEFAULT));
  }

  public int getSemanticSegmentMaxChars() {
    return Math.max(256, configuration.getInt(SEMANTIC_SEGMENT_MAX_CHARS, SEMANTIC_SEGMENT_MAX_CHARS_DEFAULT));
  }
}
