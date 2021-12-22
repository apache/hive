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
package org.apache.hadoop.hive.ql.optimizer.calcite.stats;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.BitSets;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableSet;
import org.apache.calcite.util.Util;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAntiJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSemiJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortLimit;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;
import org.apache.hadoop.hive.ql.plan.ColStatistics;

import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * EstimateUniqueKeys provides an ability to estimate unique keys based on statistics.
 */
//TODO: Ideally RelMdUniqueKeys should be modified (on Calcite side) to accept a parameter based on which
// this logic whoud be implemented
public final class EstimateUniqueKeys {
  //~ Constructors -----------------------------------------------------------
  private EstimateUniqueKeys() {
  }

  //~ Methods ----------------------------------------------------------------

  private static Set<ImmutableBitSet> getUniqueKeys(HiveFilter rel) {
    return getUniqueKeys(rel.getInput());
  }

  private static Set<ImmutableBitSet> getUniqueKeys(HiveSortLimit rel) {
    return getUniqueKeys(rel.getInput());
  }

  private static Set<ImmutableBitSet> getUniqueKeys(Correlate rel) {
    return getUniqueKeys(rel.getLeft());
  }


  //Infer Uniquenes if: - rowCount(col) = ndv(col) - TBD for numerics: max(col)
  // - min(col) = rowCount(col)
  private static Set<ImmutableBitSet> generateKeysUsingStatsEstimation(Project rel,
                                                                       HiveTableScan tScan) {
    Map<Integer, Integer> posMap = new HashMap<Integer, Integer>();
    int projectPos = 0;
    int colStatsPos = 0;

    BitSet projectedCols = new BitSet();
    for (RexNode r : rel.getProjects()) {
      if (r instanceof RexInputRef) {
        projectedCols.set(((RexInputRef) r).getIndex());
        posMap.put(colStatsPos, projectPos);
        colStatsPos++;
      }
      projectPos++;
    }

    RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    double numRows = mq.getRowCount(tScan);
    List<ColStatistics> colStats = tScan.getColStat(BitSets.toList(projectedCols));
    Set<ImmutableBitSet> keys = new HashSet<ImmutableBitSet>();

    colStatsPos = 0;
    for (ColStatistics cStat : colStats) {
      boolean isKey = false;
      if (!cStat.isEstimated()) {
        if (cStat.getCountDistint() >= numRows) {
          isKey = true;
        }
        if (!isKey && cStat.getRange() != null && cStat.getRange().maxValue != null
            && cStat.getRange().minValue != null) {
          double r = cStat.getRange().maxValue.doubleValue() - cStat.getRange().minValue.doubleValue() + 1;
          isKey = (Math.abs(numRows - r) < RelOptUtil.EPSILON);
        }
        if (isKey) {
          ImmutableBitSet key = ImmutableBitSet.of(posMap.get(colStatsPos));
          keys.add(key);
        }
      }
      colStatsPos++;
    }

    return keys;

  }

  private static Set<ImmutableBitSet> getUniqueKeys(HiveProject rel) {

    HiveTableScan tScan = getTableScan(rel.getInput(), false);
    if (tScan != null) {
      return generateKeysUsingStatsEstimation(rel, tScan);
    }

    // LogicalProject maps a set of rows to a different set;
    // Without knowledge of the mapping function(whether it
    // preserves uniqueness), it is only safe to derive uniqueness
    // info from the child of a project when the mapping is f(a) => a.
    //
    // Further more, the unique bitset coming from the child needs
    // to be mapped to match the output of the project.
    final Map<Integer, Integer> mapInToOutPos = new HashMap<>();
    final List<RexNode> projExprs = rel.getProjects();
    final Set<ImmutableBitSet> projUniqueKeySet = new HashSet<>();

    // Build an input to output position map.
    for (int i = 0; i < projExprs.size(); i++) {
      RexNode projExpr = projExprs.get(i);
      if (projExpr instanceof RexInputRef) {
        mapInToOutPos.put(((RexInputRef) projExpr).getIndex(), i);
      }
    }

    if (mapInToOutPos.isEmpty()) {
      // if there's no RexInputRef in the projected expressions
      // return empty set.
      return projUniqueKeySet;
    }

    Set<ImmutableBitSet> childUniqueKeySet =
        getUniqueKeys(rel.getInput());

    if (childUniqueKeySet != null) {
      // Now add to the projUniqueKeySet the child keys that are fully
      // projected.
      for (ImmutableBitSet colMask : childUniqueKeySet) {
        ImmutableBitSet.Builder tmpMask = ImmutableBitSet.builder();
        boolean completeKeyProjected = true;
        for (int bit : colMask) {
          if (mapInToOutPos.containsKey(bit)) {
            tmpMask.set(mapInToOutPos.get(bit));
          } else {
            // Skip the child unique key if part of it is not
            // projected.
            completeKeyProjected = false;
            break;
          }
        }
        if (completeKeyProjected) {
          projUniqueKeySet.add(tmpMask.build());
        }
      }
    }

    return projUniqueKeySet;

  }

  private static RelNode getRelNode(RelNode rel) {
    if (rel != null && rel instanceof HepRelVertex) {
      rel = ((HepRelVertex) rel).getCurrentRel();
    } else if (rel != null && rel instanceof RelSubset) {
      rel = Util.first(((RelSubset)rel).getBest(), ((RelSubset) rel).getOriginal());
    }
    return rel;
  }

  private static Set<ImmutableBitSet> getUniqueKeys(HiveJoin rel) {
    RelNode left = getRelNode(rel.getLeft());
    RelNode right = getRelNode(rel.getRight());

    // first add the different combinations of concatenated unique keys
    // from the left and the right, adjusting the right hand side keys to
    // reflect the addition of the left hand side
    //
    // NOTE zfong 12/18/06 - If the number of tables in a join is large,
    // the number of combinations of unique key sets will explode.  If
    // that is undesirable, use RelMetadataQuery.areColumnsUnique() as
    // an alternative way of getting unique key information.

    final Set<ImmutableBitSet> retSet = new HashSet<>();
    final Set<ImmutableBitSet> leftSet = getUniqueKeys(left);
    Set<ImmutableBitSet> rightSet = null;

    final Set<ImmutableBitSet> tmpRightSet = getUniqueKeys(right);
    int nFieldsOnLeft = left.getRowType().getFieldCount();

    if (tmpRightSet != null) {
      rightSet = new HashSet<>();
      for (ImmutableBitSet colMask : tmpRightSet) {
        ImmutableBitSet.Builder tmpMask = ImmutableBitSet.builder();
        for (int bit : colMask) {
          tmpMask.set(bit + nFieldsOnLeft);
        }
        rightSet.add(tmpMask.build());
      }

      if (leftSet != null) {
        for (ImmutableBitSet colMaskRight : rightSet) {
          for (ImmutableBitSet colMaskLeft : leftSet) {
            retSet.add(colMaskLeft.union(colMaskRight));
          }
        }
      }
    }

    // locate the columns that participate in equijoins
    final JoinInfo joinInfo = rel.analyzeCondition();
    RelMetadataQuery mq = rel.getCluster().getMetadataQuery();

    // determine if either or both the LHS and RHS are unique on the
    // equijoin columns
    final Boolean leftUnique =
        mq.areColumnsUnique(left, joinInfo.leftSet());
    final Boolean rightUnique =
        mq.areColumnsUnique(right, joinInfo.rightSet());

    // if the right hand side is unique on its equijoin columns, then we can
    // add the unique keys from left if the left hand side is not null
    // generating
    if ((rightUnique != null)
        && rightUnique
        && (leftSet != null)
        && !(rel.getJoinType().generatesNullsOnLeft())) {
      retSet.addAll(leftSet);
    }

    // same as above except left and right are reversed
    if ((leftUnique != null)
        && leftUnique
        && (rightSet != null)
        && !(rel.getJoinType().generatesNullsOnRight())) {
      retSet.addAll(rightSet);
    }

    return retSet;
  }

  private static Set<ImmutableBitSet> getUniqueKeys(HiveSemiJoin rel) {
    // only return the unique keys from the LHS since a semijoin only
    // returns the LHS
    return getUniqueKeys(rel.getLeft());
  }

  private static Set<ImmutableBitSet> getUniqueKeys(HiveAntiJoin rel) {
    // only return the unique keys from the LHS since a anti join only
    // returns the LHS
    return getUniqueKeys(rel.getLeft());
  }

  private static Set<ImmutableBitSet> getUniqueKeys(HiveAggregate rel) {
    // group by keys form a unique key
    return ImmutableSet.of(rel.getGroupSet());
  }

  private static Set<ImmutableBitSet> getUniqueKeys(SetOp rel) {
    if (!rel.all) {
      return ImmutableSet.of(
          ImmutableBitSet.range(rel.getRowType().getFieldCount()));
    }
    return ImmutableSet.of();
  }

  // Catch-all rule when none of the others apply.
  public static Set<ImmutableBitSet> getUniqueKeys(RelNode rel) {
    rel = getRelNode(rel);
    if (rel instanceof HiveFilter) {
      return getUniqueKeys((HiveFilter) rel);
    } else if (rel instanceof HiveSortLimit) {
      return getUniqueKeys((HiveSortLimit) rel);
    } else if (rel instanceof Correlate) {
      return getUniqueKeys((Correlate) rel);
    } else if (rel instanceof HiveProject) {
      return getUniqueKeys((HiveProject) rel);
    } else if (rel instanceof HiveJoin) {
      return getUniqueKeys((HiveJoin) rel);
    } else if (rel instanceof HiveSemiJoin) {
      return getUniqueKeys((HiveSemiJoin) rel);
    } else if (rel instanceof HiveAntiJoin) {
      return getUniqueKeys((HiveAntiJoin) rel);
    } else if (rel instanceof HiveAggregate) {
      return getUniqueKeys((HiveAggregate) rel);
    } else if (rel instanceof SetOp) {
      return getUniqueKeys((SetOp) rel);
    } else {
      return null;
    }
  }

  /*
   * traverse a path of Filter, Projects to get to the TableScan.
   * In case of Unique keys, stop if you reach a Project, it will be handled
   * by the invocation on the Project.
   * In case of getting the base rowCount of a Path, keep going past a Project.
   */
  static HiveTableScan getTableScan(RelNode r, boolean traverseProject) {

    while (r != null && !(r instanceof HiveTableScan)) {
      if (r instanceof HepRelVertex) {
        r = ((HepRelVertex) r).getCurrentRel();
      } else if (r instanceof Filter) {
        r = ((Filter) r).getInput();
      } else if (traverseProject && r instanceof Project) {
        r = ((Project) r).getInput();
      } else {
        r = null;
      }
    }
    return r == null ? null : (HiveTableScan) r;
  }
}

// End EstimateUniqueKeys.java
