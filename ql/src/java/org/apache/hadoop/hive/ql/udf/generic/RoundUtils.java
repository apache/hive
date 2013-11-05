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

package org.apache.hadoop.hive.ql.udf.generic;

import java.math.BigDecimal;
import java.math.RoundingMode;

import org.apache.hadoop.hive.common.type.HiveDecimal;

/**
 * Utility class for generic round UDF.
 *
 */
public class RoundUtils {

  private RoundUtils() {
  }

  /**
   * Rounding a double is approximate, as the double value itself is approximate.
   * A double literal, such as 3.15, may not be represented internally exactly as
   * 3.15. thus, the rounding value of it can be off on the surface. For accurate
   * rounding, consider using decimal type.
   *
   * @param input input value
   * @param scale decimal place
   * @return rounded value
   */
  public static double round(double input, int scale) {
    if (Double.isNaN(input) || Double.isInfinite(input)) {
      return input;
    }
    return BigDecimal.valueOf(input).setScale(scale, RoundingMode.HALF_UP).doubleValue();
  }

  public static long round(long input, int scale) {
    return BigDecimal.valueOf(input).setScale(scale, RoundingMode.HALF_UP).longValue();
  }

  public static HiveDecimal round(HiveDecimal input, int scale) {
    return input.setScale(scale, HiveDecimal.ROUND_HALF_UP);
  }

}
