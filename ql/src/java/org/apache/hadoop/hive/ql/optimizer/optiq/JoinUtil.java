package org.apache.hadoop.hive.ql.optimizer.optiq;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveJoinRel;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.relopt.RelOptUtil.InputReferencedVisitor;
import org.eigenbase.rex.RexNode;
import org.eigenbase.sql.SqlKind;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/**
 * Utility for inspecting Join Conditions.<br>
 * <p>
 * Main Elements:<br>
 * 1. JoinPredicateInfo - represents Join Condition.<br>
 * 2. JoinLeafPredicateInfo - represents leaf predicates with in join condition.
 * 
 * TODO: Move this to Optiq Framework
 */
public class JoinUtil {

  /**
   * JoinPredicateInfo represents Join condition; JoinPredicate Info uses
   * JoinLeafPredicateInfo to represent individual conjunctive elements in the
   * predicate.<br>
   * JoinPredicateInfo = JoinLeafPredicateInfo1 and JoinLeafPredicateInfo2...<br>
   * <p>
   * JoinPredicateInfo:<br>
   * 1. preserves the order of conjuctive elements for
   * equi-join(m_equiJoinPredicateElements)<br>
   * 2. Stores set of projection indexes from left and right child which is part
   * of equi join keys; the indexes are both in child and Join node schema.<br>
   * 3. Keeps a map of projection indexes that are part of join keys to list of
   * conjuctive elements(JoinLeafPredicateInfo) that uses them.
   * 
   */
  public static class JoinPredicateInfo {
    private final ImmutableList<JoinLeafPredicateInfo>                        m_nonEquiJoinPredicateElements;
    private final ImmutableList<JoinLeafPredicateInfo>                        m_equiJoinPredicateElements;
    private final ImmutableSet<Integer>                                       m_projsFromLeftPartOfJoinKeysInChildSchema;
    private final ImmutableSet<Integer>                                       m_projsFromRightPartOfJoinKeysInChildSchema;
    private final ImmutableSet<Integer>                                       m_projsFromRightPartOfJoinKeysInJoinSchema;
    private final ImmutableMap<Integer, ImmutableList<JoinLeafPredicateInfo>> m_mapOfProjIndxInJoinSchemaToLeafPInfo;

    public JoinPredicateInfo(List<JoinLeafPredicateInfo> nonEquiJoinPredicateElements,
        List<JoinLeafPredicateInfo> equiJoinPredicateElements,
        Set<Integer> projsFromLeftPartOfJoinKeysInChildSchema,
        Set<Integer> projsFromRightPartOfJoinKeysInChildSchema,
        Set<Integer> projsFromRightPartOfJoinKeysInJoinSchema,
        Map<Integer, ImmutableList<JoinLeafPredicateInfo>> mapOfProjIndxInJoinSchemaToLeafPInfo) {
      m_nonEquiJoinPredicateElements = ImmutableList.copyOf(nonEquiJoinPredicateElements);
      m_equiJoinPredicateElements = ImmutableList.copyOf(equiJoinPredicateElements);
      m_projsFromLeftPartOfJoinKeysInChildSchema = ImmutableSet
          .copyOf(projsFromLeftPartOfJoinKeysInChildSchema);
      m_projsFromRightPartOfJoinKeysInChildSchema = ImmutableSet
          .copyOf(projsFromRightPartOfJoinKeysInChildSchema);
      m_projsFromRightPartOfJoinKeysInJoinSchema = ImmutableSet
          .copyOf(projsFromRightPartOfJoinKeysInJoinSchema);
      m_mapOfProjIndxInJoinSchemaToLeafPInfo = ImmutableMap
          .copyOf(mapOfProjIndxInJoinSchemaToLeafPInfo);
    }

    public List<JoinLeafPredicateInfo> getNonEquiJoinPredicateElements() {
      return m_nonEquiJoinPredicateElements;
    }

    public List<JoinLeafPredicateInfo> getEquiJoinPredicateElements() {
      return m_equiJoinPredicateElements;
    }

    public Set<Integer> getProjsFromLeftPartOfJoinKeysInChildSchema() {
      return m_projsFromLeftPartOfJoinKeysInChildSchema;
    }

    public Set<Integer> getProjsFromRightPartOfJoinKeysInChildSchema() {
      return m_projsFromRightPartOfJoinKeysInChildSchema;
    }

    /**
     * NOTE: Join Schema = left Schema + (right Schema offset by
     * left.fieldcount). Hence its ok to return projections from left in child
     * schema.
     */
    public Set<Integer> getProjsFromLeftPartOfJoinKeysInJoinSchema() {
      return m_projsFromLeftPartOfJoinKeysInChildSchema;
    }

    public Set<Integer> getProjsFromRightPartOfJoinKeysInJoinSchema() {
      return m_projsFromRightPartOfJoinKeysInJoinSchema;
    }

    public Map<Integer, ImmutableList<JoinLeafPredicateInfo>> getMapOfProjIndxToLeafPInfo() {
      return m_mapOfProjIndxInJoinSchemaToLeafPInfo;
    }

    public static JoinPredicateInfo constructJoinPredicateInfo(HiveJoinRel j) {
      return constructJoinPredicateInfo(j, j.getCondition());
    }

    public static JoinPredicateInfo constructJoinPredicateInfo(HiveJoinRel j, RexNode predicate) {
      JoinPredicateInfo jpi = null;
      JoinLeafPredicateInfo jlpi = null;
      List<JoinLeafPredicateInfo> equiLPIList = new ArrayList<JoinLeafPredicateInfo>();
      List<JoinLeafPredicateInfo> nonEquiLPIList = new ArrayList<JoinLeafPredicateInfo>();
      Set<Integer> projsFromLeftPartOfJoinKeys = new HashSet<Integer>();
      Set<Integer> projsFromRightPartOfJoinKeys = new HashSet<Integer>();
      Set<Integer> projsFromRightPartOfJoinKeysInJoinSchema = new HashSet<Integer>();
      Map<Integer, List<JoinLeafPredicateInfo>> tmpMapOfProjIndxInJoinSchemaToLeafPInfo = new HashMap<Integer, List<JoinLeafPredicateInfo>>();
      Map<Integer, ImmutableList<JoinLeafPredicateInfo>> mapOfProjIndxInJoinSchemaToLeafPInfo = new HashMap<Integer, ImmutableList<JoinLeafPredicateInfo>>();
      List<JoinLeafPredicateInfo> tmpJLPILst = null;
      int rightOffSet = j.getLeft().getRowType().getFieldCount();
      int projIndxInJoin;
      List<RexNode> conjuctiveElements;

      todo("Move this to Optiq");

      // 1. Decompose Join condition to a number of leaf predicates
      // (conjuctive elements)
      conjuctiveElements = RelOptUtil.conjunctions(predicate);

      // 2. Walk through leaf predicates building up JoinLeafPredicateInfo
      for (RexNode ce : conjuctiveElements) {
        // 2.1 Construct JoinLeafPredicateInfo
        jlpi = JoinLeafPredicateInfo.constructJoinLeafPredicateInfo(j, ce);

        // 2.2 Classify leaf predicate as Equi vs Non Equi
        if (jlpi.m_comparisonType.equals(SqlKind.EQUALS)) {
          equiLPIList.add(jlpi);
        } else {
          nonEquiLPIList.add(jlpi);
        }

        // 2.3 Maintain join keys coming from left vs right (in child &
        // Join Schema)
        projsFromLeftPartOfJoinKeys.addAll(jlpi.getProjsFromLeftPartOfJoinKeysInChildSchema());
        projsFromRightPartOfJoinKeys.addAll(jlpi.getProjsFromRightPartOfJoinKeysInChildSchema());
        projsFromRightPartOfJoinKeysInJoinSchema.addAll(jlpi
            .getProjsFromRightPartOfJoinKeysInJoinSchema());

        // 2.4 Update Join Key to JoinLeafPredicateInfo map with keys
        // from left
        for (Integer projIndx : jlpi.getProjsFromLeftPartOfJoinKeysInChildSchema()) {
          tmpJLPILst = tmpMapOfProjIndxInJoinSchemaToLeafPInfo.get(projIndx);
          if (tmpJLPILst == null)
            tmpJLPILst = new ArrayList<JoinLeafPredicateInfo>();
          tmpJLPILst.add(jlpi);
          tmpMapOfProjIndxInJoinSchemaToLeafPInfo.put(projIndx, tmpJLPILst);
        }

        // 2.5 Update Join Key to JoinLeafPredicateInfo map with keys
        // from right
        for (Integer projIndx : jlpi.getProjsFromRightPartOfJoinKeysInChildSchema()) {
          projIndxInJoin = projIndx + rightOffSet;
          tmpJLPILst = tmpMapOfProjIndxInJoinSchemaToLeafPInfo.get(projIndxInJoin);
          if (tmpJLPILst == null)
            tmpJLPILst = new ArrayList<JoinLeafPredicateInfo>();
          tmpJLPILst.add(jlpi);
          tmpMapOfProjIndxInJoinSchemaToLeafPInfo.put(projIndxInJoin, tmpJLPILst);
        }

      }

      // 3. Update Update Join Key to List<JoinLeafPredicateInfo> to use
      // ImmutableList
      for (Entry<Integer, List<JoinLeafPredicateInfo>> e : tmpMapOfProjIndxInJoinSchemaToLeafPInfo
          .entrySet()) {
        mapOfProjIndxInJoinSchemaToLeafPInfo.put(e.getKey(), ImmutableList.copyOf(e.getValue()));
      }

      // 4. Construct JoinPredicateInfo
      jpi = new JoinPredicateInfo(nonEquiLPIList, equiLPIList, projsFromLeftPartOfJoinKeys,
          projsFromRightPartOfJoinKeys, projsFromRightPartOfJoinKeysInJoinSchema,
          mapOfProjIndxInJoinSchemaToLeafPInfo);
      return jpi;
    }
  }

  /**
   * JoinLeafPredicateInfo represents leaf predicate in Join condition
   * (conjuctive lement).<br>
   * <p>
   * JoinLeafPredicateInfo:<br>
   * 1. Stores list of expressions from left and right child which is part of
   * equi join keys.<br>
   * 2. Stores set of projection indexes from left and right child which is part
   * of equi join keys; the indexes are both in child and Join node schema.<br>
   */
  public static class JoinLeafPredicateInfo {
    private final SqlKind                m_comparisonType;
    private final ImmutableList<RexNode> m_joinKeyExprsFromLeft;
    private final ImmutableList<RexNode> m_joinKeyExprsFromRight;
    private final ImmutableSet<Integer>  m_projsFromLeftPartOfJoinKeysInChildSchema;
    private final ImmutableSet<Integer>  m_projsFromRightPartOfJoinKeysInChildSchema;
    private final ImmutableSet<Integer>  m_projsFromRightPartOfJoinKeysInJoinSchema;

    public JoinLeafPredicateInfo(SqlKind comparisonType, List<RexNode> joinKeyExprsFromLeft,
        List<RexNode> joinKeyExprsFromRight, Set<Integer> projsFromLeftPartOfJoinKeysInChildSchema,
        Set<Integer> projsFromRightPartOfJoinKeysInChildSchema,
        Set<Integer> projsFromRightPartOfJoinKeysInJoinSchema) {
      m_comparisonType = comparisonType;
      m_joinKeyExprsFromLeft = ImmutableList.copyOf(joinKeyExprsFromLeft);
      m_joinKeyExprsFromRight = ImmutableList.copyOf(joinKeyExprsFromRight);
      m_projsFromLeftPartOfJoinKeysInChildSchema = ImmutableSet
          .copyOf(projsFromLeftPartOfJoinKeysInChildSchema);
      m_projsFromRightPartOfJoinKeysInChildSchema = ImmutableSet
          .copyOf(projsFromRightPartOfJoinKeysInChildSchema);
      m_projsFromRightPartOfJoinKeysInJoinSchema = ImmutableSet
          .copyOf(projsFromRightPartOfJoinKeysInJoinSchema);
    }

    public List<RexNode> getJoinKeyExprsFromLeft() {
      return m_joinKeyExprsFromLeft;
    }

    public List<RexNode> getJoinKeyExprsFromRight() {
      return m_joinKeyExprsFromRight;
    }

    public Set<Integer> getProjsFromLeftPartOfJoinKeysInChildSchema() {
      return m_projsFromLeftPartOfJoinKeysInChildSchema;
    }

    /**
     * NOTE: Join Schema = left Schema + (right Schema offset by
     * left.fieldcount). Hence its ok to return projections from left in child
     * schema.
     */
    public Set<Integer> getProjsFromLeftPartOfJoinKeysInJoinSchema() {
      return m_projsFromLeftPartOfJoinKeysInChildSchema;
    }

    public Set<Integer> getProjsFromRightPartOfJoinKeysInChildSchema() {
      return m_projsFromRightPartOfJoinKeysInChildSchema;
    }

    public Set<Integer> getProjsFromRightPartOfJoinKeysInJoinSchema() {
      return m_projsFromRightPartOfJoinKeysInJoinSchema;
    }

    public static JoinLeafPredicateInfo constructJoinLeafPredicateInfo(HiveJoinRel j, RexNode pe) {
      JoinLeafPredicateInfo jlpi = null;
      List<Integer> filterNulls = new ArrayList<Integer>();
      List<RexNode> joinKeyExprsFromLeft = new ArrayList<RexNode>();
      List<RexNode> joinKeyExprsFromRight = new ArrayList<RexNode>();
      Set<Integer> projsFromLeftPartOfJoinKeysInChildSchema = new HashSet<Integer>();
      Set<Integer> projsFromRightPartOfJoinKeysInChildSchema = new HashSet<Integer>();
      Set<Integer> projsFromRightPartOfJoinKeysInJoinSchema = new HashSet<Integer>();
      int rightOffSet = j.getLeft().getRowType().getFieldCount();

      todo("Move this to Optiq");

      // 1. Split leaf join predicate to expressions from left, right
      @SuppressWarnings("unused")
      RexNode nonEquiPredicate = RelOptUtil.splitJoinCondition(j.getSystemFieldList(), j.getLeft(),
          j.getRight(), pe, joinKeyExprsFromLeft, joinKeyExprsFromRight, filterNulls, null);

      // 2. For left expressions, collect child projection indexes used
      InputReferencedVisitor irvLeft = new InputReferencedVisitor();
      irvLeft.apply(joinKeyExprsFromLeft);
      projsFromLeftPartOfJoinKeysInChildSchema.addAll(irvLeft.inputPosReferenced);

      // 3. For right expressions, collect child projection indexes used
      InputReferencedVisitor irvRight = new InputReferencedVisitor();
      irvRight.apply(joinKeyExprsFromRight);
      projsFromRightPartOfJoinKeysInChildSchema.addAll(irvRight.inputPosReferenced);

      // 3. Translate projection indexes from right to join schema, by adding
      // offset.
      for (Integer indx : projsFromRightPartOfJoinKeysInChildSchema) {
        projsFromRightPartOfJoinKeysInJoinSchema.add(indx + rightOffSet);
      }

      // 4. Construct JoinLeafPredicateInfo
      jlpi = new JoinLeafPredicateInfo(pe.getKind(), joinKeyExprsFromLeft, joinKeyExprsFromRight,
          projsFromLeftPartOfJoinKeysInChildSchema, projsFromRightPartOfJoinKeysInChildSchema,
          projsFromRightPartOfJoinKeysInJoinSchema);

      return jlpi;
    }
  }

  @Deprecated
  public static void todo(String s) {
  }
}
