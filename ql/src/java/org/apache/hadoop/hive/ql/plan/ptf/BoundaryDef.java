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

package org.apache.hadoop.hive.ql.plan.ptf;

import org.apache.hadoop.hive.ql.parse.WindowingSpec.BoundarySpec;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.Direction;

public class BoundaryDef {
  Direction direction;
  private int amt;
  private final int relativeOffset;

  public BoundaryDef(Direction direction, int amt) {
    this.direction = direction;
    this.amt = amt;

    // Calculate relative offset
    switch(this.direction) {
    case PRECEDING:
      relativeOffset = -amt;
      break;
    case FOLLOWING:
      relativeOffset = amt;
      break;
    default:
      relativeOffset = 0;
    }
  }

  public Direction getDirection() {
    return direction;
  }

  /**
   * Returns if the bound is PRECEDING.
   * @return if the bound is PRECEDING
   */
  public boolean isPreceding() {
    return this.direction == Direction.PRECEDING;
  }

  /**
   * Returns if the bound is FOLLOWING.
   * @return if the bound is FOLLOWING
   */
  public boolean isFollowing() {
    return this.direction == Direction.FOLLOWING;
  }

  /**
   * Returns if the bound is CURRENT ROW.
   * @return if the bound is CURRENT ROW
   */
  public boolean isCurrentRow() {
    return this.direction == Direction.CURRENT;
  }

  /**
   * Returns offset from XX PRECEDING/FOLLOWING.
   *
   * @return offset from XX PRECEDING/FOLLOWING
   */
  public int getAmt() {
    return amt;
  }

  /**
   * Returns signed offset from XX PRECEDING/FOLLOWING. Negative for preceding.
   *
   * @return signed offset from XX PRECEDING/FOLLOWING
   */
  public int getRelativeOffset() {
    return relativeOffset;
  }


  public boolean isUnbounded() {
    return this.getAmt() == BoundarySpec.UNBOUNDED_AMOUNT;
  }

  public int compareTo(BoundaryDef other) {
    int c = getDirection().compareTo(other.getDirection());
    if (c != 0) {
      return c;
    }

    return this.direction == Direction.PRECEDING ? other.amt - this.amt : this.amt - other.amt;
  }

  @Override
  public String toString() {
    if (direction == null) return "";
    if (direction == Direction.CURRENT) {
      return Direction.CURRENT.toString();
    }

    return direction + "(" + (getAmt() == Integer.MAX_VALUE ? "MAX" : getAmt()) + ")";
  }
}
