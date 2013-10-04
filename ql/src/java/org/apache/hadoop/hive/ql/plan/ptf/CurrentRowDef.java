package org.apache.hadoop.hive.ql.plan.ptf;

import org.apache.hadoop.hive.ql.parse.WindowingSpec.Direction;

public class CurrentRowDef extends BoundaryDef {

  public int compareTo(BoundaryDef other) {
    return getDirection().compareTo(other.getDirection());
  }

  @Override
  public Direction getDirection() {
    return Direction.CURRENT;
  }

  @Override
  public int getAmt() {
    return 0;
  }
}