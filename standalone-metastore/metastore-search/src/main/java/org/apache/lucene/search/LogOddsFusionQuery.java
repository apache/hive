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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.lucene.index.LeafReaderContext;

/**
 * A query that combines sub-query probability scores via log-odds fusion. Sub-queries are expected
 * to produce scores in (0, 1) representing probabilities (e.g., from {@link BayesianScoreQuery}
 * wrapping a BM25 query, or KNN cosine similarity).
 *
 * <p>The combination formula resolves the shrinkage problem of naive probabilistic AND by:
 *
 * <ol>
 *   <li>Converting each sub-score to log-odds: logit(p) = log(p / (1 - p))
 *   <li>Computing the mean log-odds across all clauses (non-matching contribute 0 = neutral)
 *   <li>Applying multiplicative confidence scaling: meanLogit * n^alpha
 *   <li>Converting back to probability via sigmoid
 * </ol>
 *
 * <p>The alpha parameter controls the confidence scaling exponent. The default alpha=0.5 implements
 * the sqrt(n) scaling law from "From Bayesian Inference to Neural Computation".
 *
 * <p>Optional per-signal weights enable weighted Log-OP (Logarithmic Opinion Pooling) where each
 * signal's log-odds contribution is scaled by its reliability weight. Weights must be non-negative
 * and sum to 1. When weights are provided, the scoring formula becomes: {@code sigmoid(n^alpha *
 * sum(w_i * softplus(logit(p_i))))} instead of the uniform mean.
 *
 * @see LogOddsFusionScorer
 */
public final class LogOddsFusionQuery extends Query implements Iterable<Query> {

  private final Multiset<Query> clauses = new Multiset<>();
  private final List<Query> orderedClauses;
  private final float alpha;
  private final float[] signalWeights;
  private final float[] logitMin;
  private final float[] logitMax;

  /**
   * Creates a new LogOddsFusionQuery with per-signal weights and optional logit normalization.
   *
   * @param clauses the sub-queries to combine
   * @param alpha confidence scaling exponent (0.5 = sqrt(n) law)
   * @param weights per-signal weights (must be non-negative, finite, and sum to 1.0), or null for
   *     uniform weighting
   * @param logitMin per-signal logit lower bounds for normalization, or null to use softplus gating
   * @param logitMax per-signal logit upper bounds for normalization, or null to use softplus gating
   * @throws IllegalArgumentException if alpha is not in [0, 1], or weights/bounds are invalid
   */
  public LogOddsFusionQuery(
      Collection<? extends Query> clauses,
      float alpha,
      float[] weights,
      float[] logitMin,
      float[] logitMax) {
    Objects.requireNonNull(clauses, "Collection of Queries must not be null");
    if (Float.isNaN(alpha) || alpha < 0 || alpha > 1) {
      throw new IllegalArgumentException("alpha must be in [0, 1], got " + alpha);
    }
    if (weights != null) {
      if (weights.length != clauses.size()) {
        throw new IllegalArgumentException(
            "weights length " + weights.length + " must equal clauses size " + clauses.size());
      }
      float sum = 0;
      for (float w : weights) {
        if (Float.isFinite(w) == false || w < 0) {
          throw new IllegalArgumentException("weights must be non-negative and finite, got " + w);
        }
        sum += w;
      }
      if (Math.abs(sum - 1.0f) > 1e-3f) {
        throw new IllegalArgumentException("weights must sum to 1.0, got " + sum);
      }
      this.signalWeights = weights.clone();
    } else {
      this.signalWeights = null;
    }
    if (logitMin != null && logitMax != null) {
      if (logitMin.length != clauses.size()) {
        throw new IllegalArgumentException(
            "logitMin length " + logitMin.length + " must equal clauses size " + clauses.size());
      }
      if (logitMax.length != clauses.size()) {
        throw new IllegalArgumentException(
            "logitMax length " + logitMax.length + " must equal clauses size " + clauses.size());
      }
      this.logitMin = logitMin.clone();
      this.logitMax = logitMax.clone();
    } else {
      this.logitMin = null;
      this.logitMax = null;
    }
    this.alpha = alpha;
    this.clauses.addAll(clauses);
    this.orderedClauses = new ArrayList<>(clauses);
  }

  /**
   * Creates a new LogOddsFusionQuery with per-signal weights (softplus gating, no normalization).
   *
   * @param clauses the sub-queries to combine
   * @param alpha confidence scaling exponent (0.5 = sqrt(n) law)
   * @param weights per-signal weights, or null for uniform weighting
   * @throws IllegalArgumentException if alpha is not in [0, 1], or weights are invalid
   */
  public LogOddsFusionQuery(Collection<? extends Query> clauses, float alpha, float[] weights) {
    this(clauses, alpha, weights, null, null);
  }

  /**
   * Creates a new LogOddsFusionQuery with uniform weighting and softplus gating.
   *
   * @param clauses the sub-queries to combine
   * @param alpha confidence scaling exponent (0.5 = sqrt(n) law)
   * @throws IllegalArgumentException if alpha is not in [0, 1]
   */
  public LogOddsFusionQuery(Collection<? extends Query> clauses, float alpha) {
    this(clauses, alpha, null, null, null);
  }

  /**
   * Creates a new LogOddsFusionQuery with default alpha=0.5, uniform weighting, and softplus
   * gating.
   *
   * @param clauses the sub-queries to combine
   */
  public LogOddsFusionQuery(Collection<? extends Query> clauses) {
    this(clauses, 0.5f, null, null, null);
  }

  @Override
  public Iterator<Query> iterator() {
    return getClauses().iterator();
  }

  /** Returns the clauses. */
  public Collection<Query> getClauses() {
    return Collections.unmodifiableCollection(orderedClauses);
  }

  /** Returns the alpha (confidence scaling exponent). */
  public float getAlpha() {
    return alpha;
  }

  /**
   * Returns a copy of the per-signal weights, or null if uniform weighting is used.
   *
   * <p>When non-null, the i-th element is the weight for the i-th clause in the order returned by
   * {@link #getClauses()}.
   */
  public float[] getWeights() {
    return signalWeights != null ? signalWeights.clone() : null;
  }

  /** Weight for LogOddsFusionQuery. */
  protected class LogOddsFusionWeight extends Weight {

    protected final ArrayList<Weight> weights = new ArrayList<>();
    private final ScoreMode scoreMode;

    public LogOddsFusionWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
        throws IOException {
      super(LogOddsFusionQuery.this);
      for (Query clauseQuery : orderedClauses) {
        weights.add(searcher.createWeight(clauseQuery, scoreMode, boost));
      }
      this.scoreMode = scoreMode;
    }

    @Override
    public Matches matches(LeafReaderContext context, int doc) throws IOException {
      List<Matches> mis = new ArrayList<>();
      for (Weight weight : weights) {
        Matches mi = weight.matches(context, doc);
        if (mi != null) {
          mis.add(mi);
        }
      }
      return MatchesUtils.fromSubMatches(mis);
    }

    @Override
    public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
      List<ScorerSupplier> scorerSuppliers = new ArrayList<>();
      List<Float> activeWeightsList = signalWeights != null ? new ArrayList<>() : null;
      List<Float> activeLogitMinList = logitMin != null ? new ArrayList<>() : null;
      List<Float> activeLogitMaxList = logitMax != null ? new ArrayList<>() : null;

      for (int i = 0; i < weights.size(); i++) {
        ScorerSupplier ss = weights.get(i).scorerSupplier(context);
        if (ss != null) {
          scorerSuppliers.add(ss);
          if (activeWeightsList != null) {
            activeWeightsList.add(signalWeights[i]);
          }
          if (activeLogitMinList != null) {
            activeLogitMinList.add(logitMin[i]);
            activeLogitMaxList.add(logitMax[i]);
          }
        }
      }

      if (scorerSuppliers.isEmpty()) {
        return null;
      } else if (scorerSuppliers.size() == 1) {
        return scorerSuppliers.get(0);
      } else {
        final int totalClauses = clauses.size();
        final float[] activeWeights = toFloatArray(activeWeightsList);
        final float[] activeMin = toFloatArray(activeLogitMinList);
        final float[] activeMax = toFloatArray(activeLogitMaxList);

        return new ScorerSupplier() {

          private long cost = -1;

          @Override
          public Scorer get(long leadCost) throws IOException {
            List<Scorer> scorers = new ArrayList<>();
            for (ScorerSupplier ss : scorerSuppliers) {
              scorers.add(ss.get(leadCost));
            }
            return new LogOddsFusionScorer(
                scorers,
                totalClauses,
                alpha,
                activeWeights,
                activeMin,
                activeMax,
                scoreMode,
                leadCost);
          }

          @Override
          public long cost() {
            if (cost == -1) {
              long cost = 0;
              for (ScorerSupplier ss : scorerSuppliers) {
                cost += ss.cost();
              }
              this.cost = cost;
            }
            return cost;
          }

          @Override
          public void setTopLevelScoringClause() throws IOException {
            for (ScorerSupplier ss : scorerSuppliers) {
              ss.setTopLevelScoringClause();
            }
          }
        };
      }
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      if (weights.size()
          > AbstractMultiTermQueryConstantScoreWrapper.BOOLEAN_REWRITE_TERM_COUNT_THRESHOLD) {
        return false;
      }
      for (Weight w : weights) {
        if (w.isCacheable(ctx) == false) return false;
      }
      return true;
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
      boolean match = false;
      List<Explanation> subsOnMatch = new ArrayList<>();
      List<Explanation> subsOnNoMatch = new ArrayList<>();
      double logitSum = 0;
      int totalClauses = weights.size();

      for (int i = 0; i < weights.size(); i++) {
        Explanation e = weights.get(i).explain(context, doc);
        if (e.isMatch()) {
          match = true;
          subsOnMatch.add(e);
          float subScore = e.getValue().floatValue();
          float rawLogit = LogOddsFusionScorer.logit(subScore);
          float gated;
          if (logitMin != null) {
            float range = logitMax[i] - logitMin[i];
            gated = range > 0 ? Math.clamp((rawLogit - logitMin[i]) / range, 0f, 1f) : 0.5f;
          } else {
            gated = LogOddsFusionScorer.softplus(rawLogit);
          }
          if (signalWeights != null) {
            logitSum += signalWeights[i] * gated;
          } else {
            logitSum += gated;
          }
        } else if (match == false) {
          subsOnNoMatch.add(e);
        }
      }

      if (match) {
        float scalingFactor = (float) Math.pow(totalClauses, alpha);
        float scaledLogit = 0f;
        String description;
        if (signalWeights != null) {
          scaledLogit = (float) logitSum * scalingFactor;
          description =
              "weighted log-odds fusion, computed as sigmoid(weightedLogit * n^alpha) from:";
        } else {
          scaledLogit = (float) (logitSum / totalClauses) * scalingFactor;
          description = "log-odds fusion, computed as sigmoid(meanLogit * n^alpha) from:";
        }
        float score = LogOddsFusionScorer.sigmoid(scaledLogit);
        return Explanation.match(score, description, subsOnMatch);
      } else {
        return Explanation.noMatch("No matching clause", subsOnNoMatch);
      }
    }
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {
    return new LogOddsFusionWeight(searcher, scoreMode, boost);
  }

  @Override
  public Query rewrite(IndexSearcher indexSearcher) throws IOException {
    if (clauses.isEmpty()) {
      return new MatchNoDocsQuery("empty LogOddsFusionQuery");
    }

    if (clauses.size() == 1) {
      return orderedClauses.get(0);
    }

    boolean actuallyRewritten = false;
    List<Query> rewrittenClauses = new ArrayList<>();
    List<Float> newWeights = signalWeights != null ? new ArrayList<>() : null;
    List<Float> newLogitMin = logitMin != null ? new ArrayList<>() : null;
    List<Float> newLogitMax = logitMax != null ? new ArrayList<>() : null;

    for (int i = 0; i < orderedClauses.size(); i++) {
      Query sub = orderedClauses.get(i);
      Query rewrittenSub = sub.rewrite(indexSearcher);
      if (rewrittenSub != sub || sub.getClass() == MatchNoDocsQuery.class) {
        actuallyRewritten = true;
      }
      if (rewrittenSub.getClass() != MatchNoDocsQuery.class) {
        rewrittenClauses.add(rewrittenSub);
        if (newWeights != null) {
          newWeights.add(signalWeights[i]);
        }
        if (newLogitMin != null) {
          newLogitMin.add(logitMin[i]);
          newLogitMax.add(logitMax[i]);
        }
      }
    }

    if (actuallyRewritten == false) {
      return super.rewrite(indexSearcher);
    }
    if (rewrittenClauses.isEmpty()) {
      return new MatchNoDocsQuery("empty LogOddsFusionQuery");
    }
    if (rewrittenClauses.size() == 1) {
      return rewrittenClauses.get(0);
    }

    float[] filteredWeights = toFloatArray(newWeights);
    if (filteredWeights != null) {
      float sum = 0;
      for (float w : filteredWeights) {
        sum += w;
      }
      if (sum > 0) {
        for (int i = 0; i < filteredWeights.length; i++) {
          filteredWeights[i] /= sum;
        }
      }
    }

    return new LogOddsFusionQuery(
        rewrittenClauses,
        alpha,
        filteredWeights,
        toFloatArray(newLogitMin),
        toFloatArray(newLogitMax));
  }

  @Override
  public void visit(QueryVisitor visitor) {
    QueryVisitor v = visitor.getSubVisitor(BooleanClause.Occur.SHOULD, this);
    for (Query q : clauses) {
      q.visit(v);
    }
  }

  @Override
  public String toString(String field) {
    String base =
        this.orderedClauses.stream()
            .map(
                subquery -> {
                  if (subquery instanceof BooleanQuery) {
                    return "(" + subquery.toString(field) + ")";
                  }
                  return subquery.toString(field);
                })
            .collect(Collectors.joining(" & ", "LogOdds(", ")^" + alpha));
    if (signalWeights != null) {
      return base + " w=" + Arrays.toString(signalWeights);
    }
    return base;
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) && equalsTo(getClass().cast(other));
  }

  private boolean equalsTo(LogOddsFusionQuery other) {
    return alpha == other.alpha
        && Objects.equals(clauses, other.clauses)
        && Arrays.equals(signalWeights, other.signalWeights)
        && Arrays.equals(logitMin, other.logitMin)
        && Arrays.equals(logitMax, other.logitMax);
  }

  @Override
  public int hashCode() {
    int h = classHash();
    h = 31 * h + Float.floatToIntBits(alpha);
    h = 31 * h + Objects.hashCode(clauses);
    h = 31 * h + Arrays.hashCode(signalWeights);
    h = 31 * h + Arrays.hashCode(logitMin);
    h = 31 * h + Arrays.hashCode(logitMax);
    return h;
  }

  private static float[] toFloatArray(List<Float> list) {
    if (list == null) {
      return null;
    }
    float[] result = new float[list.size()];
    for (int i = 0; i < list.size(); i++) {
      result[i] = list.get(i);
    }
    return result;
  }
}