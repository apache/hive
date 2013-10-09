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

package org.apache.hadoop.hive.ql.exec.vector.expressions;

import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;

/**
 * Math expression evaluation helper functions.
 * Some of these are referenced from ColumnUnaryFunc.txt.
 */
public class MathExpr {

  // Round using the "half-up" method used in Hive.
  public static double round(double d) {
    if (d > 0.0) {
      return (double) ((long) (d + 0.5d));
    } else {
      return (double) ((long) (d - 0.5d));
    }
  }

  public static double log2(double d) {
    return Math.log(d) / Math.log(2);
  }

  public static long abs(long v) {
    return v >= 0 ? v : -v;
  }

  public static double sign(double v) {
    return v >= 0 ? 1.0 : -1.0;
  }

  public static double sign(long v) {
    return v >= 0 ? 1.0 : -1.0;
  }

  // Convert all NaN values in vector v to NULL. Should only be used if n > 0.
  public static void NaNToNull(DoubleColumnVector v, int[] sel, boolean selectedInUse, int n) {

    // handle repeating case
    if (v.isRepeating) {
      if (Double.isNaN(v.vector[0])){
        v.isNull[0] = true;
        v.noNulls = false;
      }
      return;
    }

    if (v.noNulls) {
      if (selectedInUse) {
        for(int j = 0; j != n; j++) {
          int i = sel[j];
          if (Double.isNaN(v.vector[i])) {
            v.isNull[i] = true;
            v.noNulls = false;
          } else {

            // Must set isNull[i] to false to make sure
            // it gets initialized, in case we set noNulls to true.
            v.isNull[i] = false;
          }
        }
      } else {
        for(int i = 0; i != n; i++) {
          if (Double.isNaN(v.vector[i])) {
            v.isNull[i] = true;
            v.noNulls = false;
          } else {
            v.isNull[i] = false;
          }
        }
      }
    } else {  // there are nulls, so null array entries are already initialized
      if (selectedInUse) {
        for (int j = 0; j != n; j++) {
          int i = sel[j];
          if(Double.isNaN(v.vector[i])) {
            v.isNull[i] = true;
          }
        }
      } else {
        for (int i = 0; i != n; i++) {
          if(Double.isNaN(v.vector[i])) {
            v.isNull[i] = true;
          }
        }
      }
    }
  }
}
