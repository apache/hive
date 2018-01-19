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

package org.apache.hadoop.hive.ql.plan.ptf;

import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.classification.InterfaceStability;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.WindowType;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class WindowFrameDef {
  private WindowType windowType;
  private BoundaryDef start;
  private BoundaryDef end;
  private final int windowSize;
  private OrderDef orderDef;    // Order expressions which will only get set and used for RANGE windowing type

  public WindowFrameDef(WindowType windowType, BoundaryDef start, BoundaryDef end) {
    this.windowType = windowType;
    this.start = start;
    this.end = end;

    // Calculate window size
    if (start.getDirection() == end.getDirection()) {
      windowSize =  Math.abs(end.getAmt() - start.getAmt()) + 1;
    } else {
      windowSize =  end.getAmt() + start.getAmt() + 1;
    }
  }

  public BoundaryDef getStart() {
    return start;
  }

  public BoundaryDef getEnd() {
    return end;
  }

  public WindowType getWindowType() {
    return windowType;
  }

  public void setOrderDef(OrderDef orderDef) {
    this.orderDef = orderDef;
  }

  public OrderDef getOrderDef() throws HiveException {
    if (this.windowType != WindowType.RANGE) {
      throw new HiveException("Order expressions should only be used for RANGE windowing type");
    }
    return orderDef;
  }

  public boolean isStartUnbounded() {
    return start.isUnbounded();
  }

  public boolean isEndUnbounded() {
    return end.isUnbounded();
  }

  public int getWindowSize() {
    return windowSize;
  }

  @Override
  public String toString() {
    return windowType + " " + start + "~" + end;
  }
}
