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
package org.apache.hadoop.hive.ql.udf.ptf;

import org.apache.hadoop.hive.ql.exec.PTFPartition;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.BoundarySpec;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.Direction;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.WindowType;
import org.apache.hadoop.hive.ql.plan.ptf.BoundaryDef;
import org.apache.hadoop.hive.ql.plan.ptf.WindowFrameDef;

public class PTFRangeUtil {

  public static Range getRange(WindowFrameDef winFrame, int currRow, PTFPartition p,
      boolean nullsLast) throws HiveException {
    BoundaryDef startB = winFrame.getStart();
    BoundaryDef endB = winFrame.getEnd();

    int start, end;
    if (winFrame.getWindowType() == WindowType.ROWS) {
      start = getRowBoundaryStart(startB, currRow);
      end = getRowBoundaryEnd(endB, currRow, p);
    } else {
      ValueBoundaryScanner vbs = ValueBoundaryScanner.getScanner(winFrame, nullsLast);
      vbs.handleCache(currRow, p);
      start = vbs.computeStart(currRow, p);
      end = vbs.computeEnd(currRow, p);
    }
    start = start < 0 ? 0 : start;
    end = end > p.size() ? p.size() : end;
    return new Range(start, end, p);
  }

  private static int getRowBoundaryStart(BoundaryDef b, int currRow) throws HiveException {
    Direction d = b.getDirection();
    int amt = b.getAmt();
    switch(d) {
    case PRECEDING:
      if (amt == BoundarySpec.UNBOUNDED_AMOUNT) {
        return 0;
      }
      else {
        return currRow - amt;
      }
    case CURRENT:
      return currRow;
    case FOLLOWING:
      return currRow + amt;
    }
    throw new HiveException("Unknown Start Boundary Direction: " + d);
  }

  private static int getRowBoundaryEnd(BoundaryDef b, int currRow, PTFPartition p) throws HiveException {
    Direction d = b.getDirection();
    int amt = b.getAmt();
    switch(d) {
    case PRECEDING:
      if ( amt == 0 ) {
        return currRow + 1;
      }
      return currRow - amt + 1;
    case CURRENT:
      return currRow + 1;
    case FOLLOWING:
      if (amt == BoundarySpec.UNBOUNDED_AMOUNT) {
        return p.size();
      }
      else {
        return currRow + amt + 1;
      }
    }
    throw new HiveException("Unknown End Boundary Direction: " + d);
  }
}
