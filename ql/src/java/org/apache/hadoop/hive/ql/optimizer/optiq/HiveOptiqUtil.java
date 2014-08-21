package org.apache.hadoop.hive.ql.optimizer.optiq;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.eigenbase.rel.RelFactories.ProjectFactory;
import org.eigenbase.rel.RelNode;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.rex.RexBuilder;
import org.eigenbase.rex.RexInputRef;
import org.eigenbase.rex.RexNode;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.validate.SqlValidatorUtil;
import org.eigenbase.util.Pair;

/**
 * Generic utility functions needed for Optiq based Hive CBO.
 */

public class HiveOptiqUtil {

  /**
   * Get list of virtual columns from the given list of projections.
   * <p>
   * 
   * @param exps
   *          list of rex nodes representing projections
   * @return List of Virtual Columns, will not be null.
   */
  public static List<Integer> getVirtualCols(List<RexNode> exps) {
    List<Integer> vCols = new ArrayList<Integer>();

    for (int i = 0; i < exps.size(); i++) {
      if (!(exps.get(i) instanceof RexInputRef)) {
        vCols.add(i);
      }
    }

    return vCols;
  }

  public static List<Integer> translateBitSetToProjIndx(BitSet projBitSet) {
    List<Integer> projIndxLst = new ArrayList<Integer>();

    for (int i = 0; i < projBitSet.length(); i++) {
      if (projBitSet.get(i)) {
        projIndxLst.add(i);
      }
    }

    return projIndxLst;
  }

  @Deprecated
  public static void todo(String s) {
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
   * @param leftKeys       on return this contains the join key positions from
   *                       the new project rel on the LHS.
   * @param rightKeys      on return this contains the join key positions from
   *                       the new project rel on the RHS.
   * @return the join condition after the equi expressions pushed down.
   */
  public static RexNode projectNonColumnEquiConditions(ProjectFactory factory,
      RelNode[] inputRels, List<RexNode> leftJoinKeys,
      List<RexNode> rightJoinKeys, int systemColCount, List<Integer> leftKeys,
      List<Integer> rightKeys) {
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
      final RelDataTypeField field = rightRel.getRowType().getFieldList()
          .get(i);
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
      RexNode leftKey = leftJoinKeys.get(p.left);
      RexNode rightKey = rightJoinKeys.get(p.right);
      leftKeys.add(p.left);
      rightKeys.add(p.right);
      RexNode cond = rexBuilder.makeCall(
          SqlStdOperatorTable.EQUALS,
          rexBuilder.makeInputRef(leftKey.getType(), systemColCount + p.left),
          rexBuilder.makeInputRef(rightKey.getType(), systemColCount
              + origLeftInputSize + newKeyCount + p.right));
      if (outJoinCond == null) {
        outJoinCond = cond;
      } else {
        outJoinCond = rexBuilder.makeCall(SqlStdOperatorTable.AND, outJoinCond,
            cond);
      }
    }

    if (newKeyCount == 0) {
      return outJoinCond;
    }

    int newLeftOffset = systemColCount + origLeftInputSize;
    int newRightOffset = systemColCount + origLeftInputSize
        + origRightInputSize + newKeyCount;
    for (i = 0; i < newKeyCount; i++) {
      leftKeys.add(origLeftInputSize + i);
      rightKeys.add(origRightInputSize + i);
      RexNode cond = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, rexBuilder
          .makeInputRef(newLeftFields.get(i).getType(), newLeftOffset + i),
          rexBuilder.makeInputRef(newLeftFields.get(i).getType(),
              newRightOffset + i));
      if (outJoinCond == null) {
        outJoinCond = cond;
      } else {
        outJoinCond = rexBuilder.makeCall(SqlStdOperatorTable.AND, outJoinCond,
            cond);
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
}
