package org.apache.hadoop.hive.ql.metadata;

import org.apache.calcite.plan.RelOptMaterialization;

import java.util.EnumSet;

import static org.apache.commons.collections.CollectionUtils.intersection;

/**
 * Wrapper class of {@link RelOptMaterialization} and corresponding flags.
 */
public class Materialization {

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
  }

  private final RelOptMaterialization relOptMaterialization;
  private final EnumSet<RewriteAlgorithm> scope;

  public Materialization(RelOptMaterialization relOptMaterialization, EnumSet<RewriteAlgorithm> scope) {
    this.relOptMaterialization = relOptMaterialization;
    this.scope = scope;
  }

  public RelOptMaterialization getRelOptMaterialization() {
    return relOptMaterialization;
  }

  public EnumSet<RewriteAlgorithm> getScope() {
    return scope;
  }

  /**
   * Is this materialized view applicable to the specified scope.
   * @param scope Set of algorithms
   * @return true if applicable false otherwise
   */
  public boolean isSupported(EnumSet<RewriteAlgorithm> scope) {
    return !intersection(this.scope, scope).isEmpty();
  }
}
