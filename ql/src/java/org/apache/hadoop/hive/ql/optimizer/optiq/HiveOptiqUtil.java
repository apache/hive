package org.apache.hadoop.hive.ql.optimizer.optiq;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.eigenbase.rex.RexInputRef;
import org.eigenbase.rex.RexNode;

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
}
