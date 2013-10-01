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
}
