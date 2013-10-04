package org.apache.hadoop.hive.ql.plan.ptf;

import org.apache.hadoop.hive.ql.parse.WindowingSpec.Direction;

public abstract class BoundaryDef {
  Direction direction;

  public Direction getDirection() {
    return direction;
  }

  public void setDirection(Direction direction) {
    this.direction = direction;
  }

  public abstract int getAmt();
}