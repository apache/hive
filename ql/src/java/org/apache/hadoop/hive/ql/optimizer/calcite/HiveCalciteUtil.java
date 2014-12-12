/**
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
package org.apache.hadoop.hive.ql.optimizer.calcite;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelOptUtil.InputReferencedVisitor;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories.ProjectFactory;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.parse.ASTNode;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

/**
 * Generic utility functions needed for Calcite based Hive CBO.
 */

public class HiveCalciteUtil {

  /**
   * Get list of virtual columns from the given list of projections.
   * <p>
   *
   * @param exps
   *          list of rex nodes representing projections
   * @return List of Virtual Columns, will not be null.
   */
  public static List<Integer> getVirtualCols(List<? extends RexNode> exps) {
    List<Integer> vCols = new ArrayList<Integer>();

    for (int i = 0; i < exps.size(); i++) {
      if (!(exps.get(i) instanceof RexInputRef)) {
        vCols.add(i);
      }
    }

    return vCols;
  }

  public static boolean validateASTForUnsupportedTokens(ASTNode ast) {
    String astTree = ast.toStringTree();
    // if any of following tokens are present in AST, bail out
    String[] tokens = { "TOK_CHARSETLITERAL", "TOK_TABLESPLITSAMPLE" };
    for (String token : tokens) {
      if (astTree.contains(token)) {
        return false;
      }
    }
    return true;
  }

  public static List<RexNode> getProjsFromBelowAsInputRef(final RelNode rel) {
    List<RexNode> projectList = Lists.transform(rel.getRowType().getFieldList(),
        new Function<RelDataTypeField, RexNode>() {
          @Override
          public RexNode apply(RelDataTypeField field) {
            return rel.getCluster().getRexBuilder().makeInputRef(field.getType(), field.getIndex());
          }
        });
    return projectList;
  }

  public static List<Integer> translateBitSetToProjIndx(ImmutableBitSet projBitSet) {
    List<Integer> projIndxLst = new ArrayList<Integer>();

    for (int i = 0; i < projBitSet.length(); i++) {
      if (projBitSet.get(i)) {
        projIndxLst.add(i);
      }
    }

    return projIndxLst;
  }

  /**
   * Push any equi join conditions that are not column references as Projections
   * on top of the children.
   *
   * @param factory
   *          Project factory to use.
   * @param inputRels
   *          inputs to a join
   * @param leftJoinKeys
   *          expressions for LHS of join key
   * @param rightJoinKeys
   *          expressions for RHS of join key
   * @param systemColCount
   *          number of system columns, usually zero. These columns are
   *          projected at the leading edge of the output row.
   * @param leftKeys
   *          on return this contains the join key positions from the new
   *          project rel on the LHS.
   * @param rightKeys
   *          on return this contains the join key positions from the new
   *          project rel on the RHS.
   * @return the join condition after the equi expressions pushed down.
   */
  public static RexNode projectNonColumnEquiConditions(ProjectFactory factory, RelNode[] inputRels,
      List<RexNode> leftJoinKeys, List<RexNode> rightJoinKeys, int systemColCount,
      List<Integer> leftKeys, List<Integer> rightKeys) {
    RelNode leftRel = inputRels[0];
    RelNode rightRel = inputRels[1];
    RexBuilder rexBuilder = leftRel.getCluster().getRexBuilder();
    RexNode outJoinCond = null;

    int origLeftInputSize = leftRel.getRowType().getFieldCount();
    int origRightInputSize = rightRel.getRowType().getFieldCount();

    List<RexNode> newLeftFields = new ArrayList<RexNode>();
    List<String> newLeftFieldNames = new ArrayList<String>();

    List<RexNode> newRightFields = new ArrayList<RexNode>();
    List<String> newRightFieldNames = new ArrayList<String>();
    int leftKeyCount = leftJoinKeys.size();
    int i;

    for (i = 0; i < origLeftInputSize; i++) {
      final RelDataTypeField field = leftRel.getRowType().getFieldList().get(i);
      newLeftFields.add(rexBuilder.makeInputRef(field.getType(), i));
      newLeftFieldNames.add(field.getName());
    }

    for (i = 0; i < origRightInputSize; i++) {
      final RelDataTypeField field = rightRel.getRowType().getFieldList().get(i);
      newRightFields.add(rexBuilder.makeInputRef(field.getType(), i));
      newRightFieldNames.add(field.getName());
    }

    int newKeyCount = 0;
    List<Pair<Integer, Integer>> origColEqConds = new ArrayList<Pair<Integer, Integer>>();
    for (i = 0; i < leftKeyCount; i++) {
      RexNode leftKey = leftJoinKeys.get(i);
      RexNode rightKey = rightJoinKeys.get(i);

      if (leftKey instanceof RexInputRef && rightKey instanceof RexInputRef) {
        origColEqConds.add(Pair.of(((RexInputRef) leftKey).getIndex(),
            ((RexInputRef) rightKey).getIndex()));
      } else {
        newLeftFields.add(leftKey);
        newLeftFieldNames.add(null);
        newRightFields.add(rightKey);
        newRightFieldNames.add(null);
        newKeyCount++;
      }
    }

    for (i = 0; i < origColEqConds.size(); i++) {
      Pair<Integer, Integer> p = origColEqConds.get(i);
      RexNode leftKey = leftJoinKeys.get(i);
      RexNode rightKey = rightJoinKeys.get(i);
      leftKeys.add(p.left);
      rightKeys.add(p.right);
      RexNode cond = rexBuilder.makeCall(
          SqlStdOperatorTable.EQUALS,
          rexBuilder.makeInputRef(leftKey.getType(), systemColCount + p.left),
          rexBuilder.makeInputRef(rightKey.getType(), systemColCount + origLeftInputSize
              + newKeyCount + p.right));
      if (outJoinCond == null) {
        outJoinCond = cond;
      } else {
        outJoinCond = rexBuilder.makeCall(SqlStdOperatorTable.AND, outJoinCond, cond);
      }
    }

    if (newKeyCount == 0) {
      return outJoinCond;
    }

    int newLeftOffset = systemColCount + origLeftInputSize;
    int newRightOffset = systemColCount + origLeftInputSize + origRightInputSize + newKeyCount;
    for (i = 0; i < newKeyCount; i++) {
      leftKeys.add(origLeftInputSize + i);
      rightKeys.add(origRightInputSize + i);
      RexNode cond = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
          rexBuilder.makeInputRef(newLeftFields.get(i).getType(), newLeftOffset + i),
          rexBuilder.makeInputRef(newLeftFields.get(i).getType(), newRightOffset + i));
      if (outJoinCond == null) {
        outJoinCond = cond;
      } else {
        outJoinCond = rexBuilder.makeCall(SqlStdOperatorTable.AND, outJoinCond, cond);
      }
    }

    // added project if need to produce new keys than the original input
    // fields
    if (newKeyCount > 0) {
      leftRel = factory.createProject(leftRel, newLeftFields,
          SqlValidatorUtil.uniquify(newLeftFieldNames));
      rightRel = factory.createProject(rightRel, newRightFields,
          SqlValidatorUtil.uniquify(newRightFieldNames));
    }

    inputRels[0] = leftRel;
    inputRels[1] = rightRel;

    return outJoinCond;
  }

  /**
   * JoinPredicateInfo represents Join condition; JoinPredicate Info uses
   * JoinLeafPredicateInfo to represent individual conjunctive elements in the
   * predicate.<br>
   * JoinPredicateInfo = JoinLeafPredicateInfo1 and JoinLeafPredicateInfo2...<br>
   * <p>
   * JoinPredicateInfo:<br>
   * 1. preserves the order of conjuctive elements for
   * equi-join(equiJoinPredicateElements)<br>
   * 2. Stores set of projection indexes from left and right child which is part
   * of equi join keys; the indexes are both in child and Join node schema.<br>
   * 3. Keeps a map of projection indexes that are part of join keys to list of
   * conjuctive elements(JoinLeafPredicateInfo) that uses them.
   *
   */
  public static class JoinPredicateInfo {
    private final ImmutableList<JoinLeafPredicateInfo>                        nonEquiJoinPredicateElements;
    private final ImmutableList<JoinLeafPredicateInfo>                        equiJoinPredicateElements;
    private final ImmutableSet<Integer>                                       projsFromLeftPartOfJoinKeysInChildSchema;
    private final ImmutableSet<Integer>                                       projsFromRightPartOfJoinKeysInChildSchema;
    private final ImmutableSet<Integer>                                       projsFromRightPartOfJoinKeysInJoinSchema;
    private final ImmutableMap<Integer, ImmutableList<JoinLeafPredicateInfo>> mapOfProjIndxInJoinSchemaToLeafPInfo;

    public JoinPredicateInfo(List<JoinLeafPredicateInfo> nonEquiJoinPredicateElements,
        List<JoinLeafPredicateInfo> equiJoinPredicateElements,
        Set<Integer> projsFromLeftPartOfJoinKeysInChildSchema,
        Set<Integer> projsFromRightPartOfJoinKeysInChildSchema,
        Set<Integer> projsFromRightPartOfJoinKeysInJoinSchema,
        Map<Integer, ImmutableList<JoinLeafPredicateInfo>> mapOfProjIndxInJoinSchemaToLeafPInfo) {
      this.nonEquiJoinPredicateElements = ImmutableList.copyOf(nonEquiJoinPredicateElements);
      this.equiJoinPredicateElements = ImmutableList.copyOf(equiJoinPredicateElements);
      this.projsFromLeftPartOfJoinKeysInChildSchema = ImmutableSet
          .copyOf(projsFromLeftPartOfJoinKeysInChildSchema);
      this.projsFromRightPartOfJoinKeysInChildSchema = ImmutableSet
          .copyOf(projsFromRightPartOfJoinKeysInChildSchema);
      this.projsFromRightPartOfJoinKeysInJoinSchema = ImmutableSet
          .copyOf(projsFromRightPartOfJoinKeysInJoinSchema);
      this.mapOfProjIndxInJoinSchemaToLeafPInfo = ImmutableMap
          .copyOf(mapOfProjIndxInJoinSchemaToLeafPInfo);
    }

    public List<JoinLeafPredicateInfo> getNonEquiJoinPredicateElements() {
      return this.nonEquiJoinPredicateElements;
    }

    public List<JoinLeafPredicateInfo> getEquiJoinPredicateElements() {
      return this.equiJoinPredicateElements;
    }

    public Set<Integer> getProjsFromLeftPartOfJoinKeysInChildSchema() {
      return this.projsFromLeftPartOfJoinKeysInChildSchema;
    }

    public Set<Integer> getProjsFromRightPartOfJoinKeysInChildSchema() {
      return this.projsFromRightPartOfJoinKeysInChildSchema;
    }

    /**
     * NOTE: Join Schema = left Schema + (right Schema offset by
     * left.fieldcount). Hence its ok to return projections from left in child
     * schema.
     */
    public Set<Integer> getProjsFromLeftPartOfJoinKeysInJoinSchema() {
      return this.projsFromLeftPartOfJoinKeysInChildSchema;
    }

    public Set<Integer> getProjsFromRightPartOfJoinKeysInJoinSchema() {
      return this.projsFromRightPartOfJoinKeysInJoinSchema;
    }

    public Map<Integer, ImmutableList<JoinLeafPredicateInfo>> getMapOfProjIndxToLeafPInfo() {
      return this.mapOfProjIndxInJoinSchemaToLeafPInfo;
    }

    public static JoinPredicateInfo constructJoinPredicateInfo(HiveJoin j) {
      return constructJoinPredicateInfo(j, j.getCondition());
    }

    public static JoinPredicateInfo constructJoinPredicateInfo(HiveJoin j, RexNode predicate) {
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

      // 1. Decompose Join condition to a number of leaf predicates
      // (conjuctive elements)
      conjuctiveElements = RelOptUtil.conjunctions(predicate);

      // 2. Walk through leaf predicates building up JoinLeafPredicateInfo
      for (RexNode ce : conjuctiveElements) {
        // 2.1 Construct JoinLeafPredicateInfo
        jlpi = JoinLeafPredicateInfo.constructJoinLeafPredicateInfo(j, ce);

        // 2.2 Classify leaf predicate as Equi vs Non Equi
        if (jlpi.comparisonType.equals(SqlKind.EQUALS)) {
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
    private final SqlKind                comparisonType;
    private final ImmutableList<RexNode> joinKeyExprsFromLeft;
    private final ImmutableList<RexNode> joinKeyExprsFromRight;
    private final ImmutableSet<Integer>  projsFromLeftPartOfJoinKeysInChildSchema;
    private final ImmutableSet<Integer>  projsFromRightPartOfJoinKeysInChildSchema;
    private final ImmutableSet<Integer>  projsFromRightPartOfJoinKeysInJoinSchema;

    public JoinLeafPredicateInfo(SqlKind comparisonType, List<RexNode> joinKeyExprsFromLeft,
        List<RexNode> joinKeyExprsFromRight, Set<Integer> projsFromLeftPartOfJoinKeysInChildSchema,
        Set<Integer> projsFromRightPartOfJoinKeysInChildSchema,
        Set<Integer> projsFromRightPartOfJoinKeysInJoinSchema) {
      this.comparisonType = comparisonType;
      this.joinKeyExprsFromLeft = ImmutableList.copyOf(joinKeyExprsFromLeft);
      this.joinKeyExprsFromRight = ImmutableList.copyOf(joinKeyExprsFromRight);
      this.projsFromLeftPartOfJoinKeysInChildSchema = ImmutableSet
          .copyOf(projsFromLeftPartOfJoinKeysInChildSchema);
      this.projsFromRightPartOfJoinKeysInChildSchema = ImmutableSet
          .copyOf(projsFromRightPartOfJoinKeysInChildSchema);
      this.projsFromRightPartOfJoinKeysInJoinSchema = ImmutableSet
          .copyOf(projsFromRightPartOfJoinKeysInJoinSchema);
    }

    public List<RexNode> getJoinKeyExprsFromLeft() {
      return this.joinKeyExprsFromLeft;
    }

    public List<RexNode> getJoinKeyExprsFromRight() {
      return this.joinKeyExprsFromRight;
    }

    public Set<Integer> getProjsFromLeftPartOfJoinKeysInChildSchema() {
      return this.projsFromLeftPartOfJoinKeysInChildSchema;
    }

    /**
     * NOTE: Join Schema = left Schema + (right Schema offset by
     * left.fieldcount). Hence its ok to return projections from left in child
     * schema.
     */
    public Set<Integer> getProjsFromLeftPartOfJoinKeysInJoinSchema() {
      return this.projsFromLeftPartOfJoinKeysInChildSchema;
    }

    public Set<Integer> getProjsFromRightPartOfJoinKeysInChildSchema() {
      return this.projsFromRightPartOfJoinKeysInChildSchema;
    }

    public Set<Integer> getProjsFromRightPartOfJoinKeysInJoinSchema() {
      return this.projsFromRightPartOfJoinKeysInJoinSchema;
    }

    private static JoinLeafPredicateInfo constructJoinLeafPredicateInfo(HiveJoin j, RexNode pe) {
      JoinLeafPredicateInfo jlpi = null;
      List<Integer> filterNulls = new ArrayList<Integer>();
      List<RexNode> joinKeyExprsFromLeft = new ArrayList<RexNode>();
      List<RexNode> joinKeyExprsFromRight = new ArrayList<RexNode>();
      Set<Integer> projsFromLeftPartOfJoinKeysInChildSchema = new HashSet<Integer>();
      Set<Integer> projsFromRightPartOfJoinKeysInChildSchema = new HashSet<Integer>();
      Set<Integer> projsFromRightPartOfJoinKeysInJoinSchema = new HashSet<Integer>();
      int rightOffSet = j.getLeft().getRowType().getFieldCount();

      // 1. Split leaf join predicate to expressions from left, right
      RelOptUtil.splitJoinCondition(j.getSystemFieldList(), j.getLeft(), j.getRight(), pe,
          joinKeyExprsFromLeft, joinKeyExprsFromRight, filterNulls, null);

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

  public static boolean limitRelNode(RelNode rel) {
    if ((rel instanceof Sort) && ((Sort) rel).getCollation().getFieldCollations().isEmpty())
      return true;

    return false;
  }

  public static boolean orderRelNode(RelNode rel) {
    if ((rel instanceof Sort) && !((Sort) rel).getCollation().getFieldCollations().isEmpty())
      return true;

    return false;
  }

  /**
   * Get top level select starting from root. Assumption here is root can only
   * be Sort & Project. Also the top project should be at most 2 levels below
   * Sort; i.e Sort(Limit)-Sort(OB)-Select
   *
   * @param rootRel
   * @return
   */
  public static Pair<RelNode, RelNode> getTopLevelSelect(final RelNode rootRel) {
    RelNode tmpRel = rootRel;
    RelNode parentOforiginalProjRel = rootRel;
    HiveProject originalProjRel = null;

    while (tmpRel != null) {
      if (tmpRel instanceof HiveProject) {
        originalProjRel = (HiveProject) tmpRel;
        break;
      }
      parentOforiginalProjRel = tmpRel;
      tmpRel = tmpRel.getInput(0);
    }

    return (new Pair<RelNode, RelNode>(parentOforiginalProjRel, originalProjRel));
  }

  public static boolean isDeterministic(RexNode expr) {
    boolean deterministic = true;

    RexVisitor<Void> visitor = new RexVisitorImpl<Void>(true) {
      public Void visitCall(org.apache.calcite.rex.RexCall call) {
        if (!call.getOperator().isDeterministic()) {
          throw new Util.FoundOne(call);
        }
        return super.visitCall(call);
      }
    };

    try {
      expr.accept(visitor);
    } catch (Util.FoundOne e) {
        deterministic = false;
    }

    return deterministic;
  }
}
