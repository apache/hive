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

package org.apache.hadoop.hive.metastore;

import java.util.HashMap;
import java.util.Map;

public interface IExtrapolatePartStatus {
  /**
   * The sequence of colStatNames.
   */
  static String[] colStatNames = new String[] { "LONG_LOW_VALUE",
      "LONG_HIGH_VALUE", "DOUBLE_LOW_VALUE", "DOUBLE_HIGH_VALUE",
      "BIG_DECIMAL_LOW_VALUE", "BIG_DECIMAL_HIGH_VALUE", "NUM_NULLS",
      "NUM_DISTINCTS", "AVG_COL_LEN", "MAX_COL_LEN", "NUM_TRUES", "NUM_FALSES" };
  
  /**
   * The indexes for colstats.
   */
  static HashMap<String, Integer[]> indexMaps = new HashMap<String, Integer []>(){{
    put("long", new Integer [] {0,1,6,7});
    put("double", new Integer [] {2,3,6,7});
    put("string", new Integer [] {8,9,6,7});
    put("boolean", new Integer [] {10,11,6});
    put("binary", new Integer [] {8,9,6});
    put("decimal", new Integer [] {4,5,6,7});
    put("default", new Integer [] {0,1,2,3,4,5,6,7,8,9,10,11});
}};

  /**
   * The sequence of colStatTypes.
   */
  static enum ColStatType {
    Long, Double, Decimal
  }

  static ColStatType[] colStatTypes = new ColStatType[] { ColStatType.Long,
      ColStatType.Long, ColStatType.Double, ColStatType.Double,
      ColStatType.Decimal, ColStatType.Decimal, ColStatType.Long,
      ColStatType.Long, ColStatType.Double, ColStatType.Long, ColStatType.Long,
      ColStatType.Long };

  /**
   * The sequence of aggregation function on colStats.
   */
  static enum AggrType {
    Min, Max, Sum
  }

  static AggrType[] aggrTypes = new AggrType[] { AggrType.Min, AggrType.Max,
      AggrType.Min, AggrType.Max, AggrType.Min, AggrType.Max, AggrType.Sum,
      AggrType.Max, AggrType.Max, AggrType.Max, AggrType.Sum, AggrType.Sum };
  
  public Object extrapolate(Object[] min, Object[] max, int colStatIndex,
      Map<String, Integer> indexMap);

}
