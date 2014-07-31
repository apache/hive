package org.apache.hadoop.hive.ql.optimizer.optiq.reloperators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.ql.optimizer.optiq.TraitsUtil;
import org.apache.hadoop.hive.ql.optimizer.optiq.cost.HiveCostUtil;
import org.eigenbase.rel.InvalidRelException;
import org.eigenbase.rel.JoinRelBase;
import org.eigenbase.rel.JoinRelType;
import org.eigenbase.rel.RelFactories.JoinFactory;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.rex.RexNode;

//TODO: Should we convert MultiJoin to be a child of HiveJoinRelBase
public class HiveJoinRel extends JoinRelBase implements HiveRel {
  // NOTE: COMMON_JOIN & SMB_JOIN are Sort Merge Join (in case of COMMON_JOIN
  // each parallel computation handles multiple splits where as in case of SMB
  // each parallel computation handles one bucket). MAP_JOIN and BUCKET_JOIN is
  // hash joins where MAP_JOIN keeps the whole data set of non streaming tables
  // in memory where as BUCKET_JOIN keeps only the b
  public enum JoinAlgorithm {
    NONE, COMMON_JOIN, MAP_JOIN, BUCKET_JOIN, SMB_JOIN
  }

  public enum MapJoinStreamingRelation {
    NONE, LEFT_RELATION, RIGHT_RELATION
  }
  
  public static final JoinFactory HIVE_JOIN_FACTORY = new HiveJoinFactoryImpl();

  private final boolean m_leftSemiJoin;
  private final JoinAlgorithm      m_joinAlgorithm;
  private MapJoinStreamingRelation m_mapJoinStreamingSide = MapJoinStreamingRelation.NONE;

  public static HiveJoinRel getJoin(RelOptCluster cluster, RelNode left, RelNode right,
      RexNode condition, JoinRelType joinType, boolean leftSemiJoin) {
    try {
      Set<String> variablesStopped = Collections.emptySet();
      return new HiveJoinRel(cluster, null, left, right, condition, joinType, variablesStopped,
          JoinAlgorithm.NONE, null, leftSemiJoin);
    } catch (InvalidRelException e) {
      throw new RuntimeException(e);
    }
  }

  protected HiveJoinRel(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right,
      RexNode condition, JoinRelType joinType, Set<String> variablesStopped,
      JoinAlgorithm joinAlgo, MapJoinStreamingRelation streamingSideForMapJoin, boolean leftSemiJoin)
      throws InvalidRelException {
    super(cluster, TraitsUtil.getJoinTraitSet(cluster, traits), left, right, condition, joinType,
        variablesStopped);

    final List<RexNode> leftKeys = new ArrayList<RexNode>();
    final List<RexNode> rightKeys = new ArrayList<RexNode>();
    List<Integer> filterNulls = new LinkedList<Integer>();
    RexNode remaining = null;
    if (condition != null) {
      remaining = RelOptUtil.splitJoinCondition(getSystemFieldList(), left,
          right, condition, leftKeys, rightKeys, filterNulls, null);

      if (!remaining.isAlwaysTrue()) {
        throw new InvalidRelException(
            "EnumerableJoinRel only supports equi-join");
      }
    }
    this.m_joinAlgorithm = joinAlgo;
    m_leftSemiJoin = leftSemiJoin;
  }

  @Override
  public void implement(Implementor implementor) {
  }

  @Override
  public final HiveJoinRel copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left,
      RelNode right, JoinRelType joinType, boolean semiJoinDone) {
    return copy(traitSet, conditionExpr, left, right, m_joinAlgorithm, m_mapJoinStreamingSide, m_leftSemiJoin);
  }

  public HiveJoinRel copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left, RelNode right,
      JoinAlgorithm joinalgo, MapJoinStreamingRelation streamingSide, boolean semiJoinDone) {
    try {
      return new HiveJoinRel(getCluster(), traitSet, left, right, conditionExpr, joinType,
          variablesStopped, joinalgo, streamingSide, semiJoinDone);
    } catch (InvalidRelException e) {
      // Semantic error not possible. Must be a bug. Convert to
      // internal error.
      throw new AssertionError(e);
    }
  }

  public JoinAlgorithm getJoinAlgorithm() {
    return m_joinAlgorithm;
  }

  public boolean isLeftSemiJoin() {
    return m_leftSemiJoin;
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    return HiveCostUtil.computCardinalityBasedCost(this);
  }

  /**
   * @return returns rowtype representing only the left join input
   */
  public RelDataType deriveRowType() {
    if (m_leftSemiJoin) {
      return deriveJoinRowType(left.getRowType(), null, JoinRelType.INNER,
          getCluster().getTypeFactory(), null,
          Collections.<RelDataTypeField> emptyList());
    }
    return super.deriveRowType();
  }

  private static class HiveJoinFactoryImpl implements JoinFactory {
    /**
     * Creates a join.
     * 
     * @param left
     *          Left input
     * @param right
     *          Right input
     * @param condition
     *          Join condition
     * @param joinType
     *          Join type
     * @param variablesStopped
     *          Set of names of variables which are set by the LHS and used by
     *          the RHS and are not available to nodes above this JoinRel in the
     *          tree
     * @param semiJoinDone
     *          Whether this join has been translated to a semi-join
     */
    @Override
    public RelNode createJoin(RelNode left, RelNode right, RexNode condition, JoinRelType joinType,
        Set<String> variablesStopped, boolean semiJoinDone) {
      return getJoin(left.getCluster(), left, right, condition, joinType, false);
    }
  }
}
