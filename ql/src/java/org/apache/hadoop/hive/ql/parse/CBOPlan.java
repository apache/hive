package org.apache.hadoop.hive.ql.parse;

import org.apache.calcite.rel.RelNode;

/**
 * Wrapper of Calcite plan.
 */
public class CBOPlan {
  private final RelNode plan;
  private final String invalidAutomaticRewritingMaterializationReason;

  public CBOPlan(RelNode plan, String invalidAutomaticRewritingMaterializationReason) {
    this.plan = plan;
    this.invalidAutomaticRewritingMaterializationReason = invalidAutomaticRewritingMaterializationReason;
  }

  /**
   * Root node of plan.
   * @return Root {@link RelNode}
   */
  public RelNode getPlan() {
    return plan;
  }

  /**
   * Returns an error message if this plan can not be a definition of a Materialized view which is an input of
   * Calcite based materialized view query rewrite.
   * null or empty string otherwise.
   * @return
   */
  public String getInvalidAutomaticRewritingMaterializationReason() {
    return invalidAutomaticRewritingMaterializationReason;
  }
}
