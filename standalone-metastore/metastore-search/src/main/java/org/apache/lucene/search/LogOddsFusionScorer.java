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
package org.apache.lucene.search;

import java.io.IOException;
import java.util.IdentityHashMap;
import java.util.List;

/**
 * Scorer for {@link LogOddsFusionQuery}. Combines sub-scorer outputs (assumed to be probabilities
 * in (0, 1)) via log-odds fusion with multiplicative confidence scaling.
 *
 * <p>The scoring formula is:
 *
 * <pre>
 *   gatedLogit  = softplus(logit(clamp(subScore)))   for each matching sub-scorer
 *   logitSum    = sum of gatedLogit values
 *   meanLogit   = logitSum / n   (n = total clause count, not just matching)
 *   scaledLogit = meanLogit * pow(n, alpha)
 *   score       = sigmoid(scaledLogit)
 * </pre>
 *
 * <p>Softplus gating ({@code log(1 + exp(x))}) is applied to logit values before aggregation. This
 * distinguishes "absence of evidence" (non-matching sub-scorer, contributes 0) from "evidence of
 * absence" (matching sub-scorer with weak probability, contributes a small positive value). A
 * matching sub-scorer always contributes more than a non-matching one, preserving the ordering
 * among weak matches while ensuring that no match is ever penalized.
 *
 * <p>Non-matching sub-scorers contribute logit(0.5) = 0 (neutral evidence).
 *
 * @lucene.experimental
 */
final class LogOddsFusionScorer extends DisjunctionScorer {
  private static final float CLAMP_MIN = 1e-7f;
  private static final float CLAMP_MAX = 1f - 1e-7f;

  private final List<Scorer> subScorers;
  private final int totalClauses;
  private final float scalingFactor;
  private final float[] signalWeights;
  private final float[] logitMin;
  private final float[] logitMax;
  private final IdentityHashMap<Scorer, Integer> scorerIndexMap;

  private final DisjunctionScoreBlockBoundaryPropagator disjunctionBlockPropagator;

  /**
   * Creates a new LogOddsFusionScorer.
   *
   * @param subScorers the sub scorers to combine
   * @param totalClauses the total number of clauses (including non-matching)
   * @param alpha confidence scaling exponent (0.5 = sqrt(n) law)
   * @param signalWeights per-signal weights parallel to subScorers (null for uniform weighting).
   *     When provided, the scoring formula uses weighted sum instead of mean. Weights must be
   *     non-negative and should sum to 1.
   * @param logitMin per-signal logit lower bounds for normalization (null to use softplus gating).
   *     When provided together with logitMax, logit values are normalized to [0, 1] instead of
   *     applying softplus. This ensures non-negative contributions while preserving learned signal
   *     scale calibration.
   * @param logitMax per-signal logit upper bounds for normalization (null to use softplus gating)
   * @param scoreMode the score mode
   * @param leadCost the lead cost for iteration
   */
  LogOddsFusionScorer(
      List<Scorer> subScorers,
      int totalClauses,
      float alpha,
      float[] signalWeights,
      float[] logitMin,
      float[] logitMax,
      ScoreMode scoreMode,
      long leadCost)
      throws IOException {
    super(subScorers, scoreMode, leadCost);
    this.subScorers = subScorers;
    this.totalClauses = totalClauses;
    this.scalingFactor = (float) Math.pow(totalClauses, alpha);
    this.signalWeights = signalWeights;
    this.logitMin = logitMin;
    this.logitMax = logitMax;
    if (signalWeights != null) {
      this.scorerIndexMap = new IdentityHashMap<>(subScorers.size());
      for (int i = 0; i < subScorers.size(); i++) {
        this.scorerIndexMap.put(subScorers.get(i), i);
      }
    } else {
      this.scorerIndexMap = null;
    }
    if (scoreMode == ScoreMode.TOP_SCORES) {
      this.disjunctionBlockPropagator = new DisjunctionScoreBlockBoundaryPropagator(subScorers);
    } else {
      this.disjunctionBlockPropagator = null;
    }
  }

  static float clampProbability(float p) {
    return Math.clamp(p, CLAMP_MIN, CLAMP_MAX);
  }

  static float logit(float p) {
    float clamped = clampProbability(p);
    return (float) Math.log(clamped / (1f - clamped));
  }

  static float sigmoid(float x) {
    if (x >= 0) {
      return (float) (1.0 / (1.0 + Math.exp(-x)));
    } else {
      double expX = Math.exp(x);
      return (float) (expX / (1.0 + expX));
    }
  }

  /**
   * Softplus function: log(1 + exp(x)). Always positive, smooth approximation of ReLU. For large
   * positive x, approaches x. For large negative x, approaches 0 from above. At x=0, returns log(2)
   * ~ 0.693.
   *
   * <p>Uses a numerically stable formulation: for x &gt; 20, softplus(x) ~ x.
   */
  static float softplus(float x) {
    if (x > 20f) {
      return x;
    }
    return (float) Math.log1p(Math.exp(x));
  }

  /** Applies gating to a logit value: normalization if bounds are set, softplus otherwise. */
  private float gateLogit(float rawLogit, int signalIndex) {
    if (logitMin != null) {
      float range = logitMax[signalIndex] - logitMin[signalIndex];
      if (range > 0) {
        return Math.clamp((rawLogit - logitMin[signalIndex]) / range, 0f, 1f);
      }
      return 0.5f;
    }
    return softplus(rawLogit);
  }

  @Override
  protected float score(DisiWrapper topList) throws IOException {
    double logitSum = 0;
    for (DisiWrapper w = topList; w != null; w = w.next) {
      float subScore = w.scorable.score();
      int idx = scorerIndexMap != null ? scorerIndexMap.get(w.scorer) : -1;
      float gated = gateLogit(logit(subScore), idx >= 0 ? idx : 0);
      if (scorerIndexMap != null) {
        logitSum += signalWeights[idx] * gated;
      } else {
        logitSum += gated;
      }
    }
    // Non-matching sub-scorers contribute 0.
    // With weights: sum(w_i * gated_i) already accounts for the 1/n factor.
    // Without weights: divide by totalClauses to compute the mean.
    float scaledLogit = 0f;
    if (signalWeights != null) {
      scaledLogit = (float) logitSum * scalingFactor;
    } else {
      scaledLogit = (float) (logitSum / totalClauses) * scalingFactor;
    }
    return sigmoid(scaledLogit);
  }

  @Override
  public int advanceShallow(int target) throws IOException {
    if (disjunctionBlockPropagator != null) {
      return disjunctionBlockPropagator.advanceShallow(target);
    }
    return super.advanceShallow(target);
  }

  @Override
  public float getMaxScore(int upTo) throws IOException {
    // Safe upper bound: gateLogit is monotone in p (both softplus and normalize are monotone),
    // weights are non-negative, sum of upper bounds >= sum of actuals, and sigmoid is monotone.
    double maxLogitSum = 0;
    for (int i = 0; i < subScorers.size(); i++) {
      Scorer scorer = subScorers.get(i);
      if (scorer.docID() <= upTo) {
        float maxSubScore = scorer.getMaxScore(upTo);
        float gated = gateLogit(logit(maxSubScore), i);
        if (signalWeights != null) {
          maxLogitSum += signalWeights[i] * gated;
        } else {
          maxLogitSum += gated;
        }
      }
    }
    float scaledLogit = 0f;
    if (signalWeights != null) {
      scaledLogit = (float) maxLogitSum * scalingFactor;
    } else {
      scaledLogit = (float) (maxLogitSum / totalClauses) * scalingFactor;
    }
    return sigmoid(scaledLogit);
  }

  @Override
  public void setMinCompetitiveScore(float minScore) throws IOException {
    if (disjunctionBlockPropagator != null) {
      disjunctionBlockPropagator.setMinCompetitiveScore(minScore);
    }
  }
}