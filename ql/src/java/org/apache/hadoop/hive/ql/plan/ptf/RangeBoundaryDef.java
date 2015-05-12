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

import org.apache.hadoop.hive.ql.parse.WindowingSpec.Direction;

public class RangeBoundaryDef extends BoundaryDef {
  private int amt;
  private final int relativeOffset;

  public RangeBoundaryDef(Direction direction, int amt) {
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

  public int compareTo(BoundaryDef other) {
    int c = getDirection().compareTo(other.getDirection());
    if (c != 0) {
      return c;
    }
    RangeBoundaryDef rb = (RangeBoundaryDef) other;
    return this.direction == Direction.PRECEDING ? rb.amt - this.amt : this.amt - rb.amt;
  }

  @Override
  public int getAmt() {
    return amt;
  }

  @Override
  public boolean isPreceding() {
    return this.direction == Direction.PRECEDING;
  }

  @Override
  public boolean isFollowing() {
    return this.direction == Direction.FOLLOWING;
  }

  @Override
  public int getRelativeOffset() {
    return relativeOffset;
  }
}
