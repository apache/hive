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

import org.apache.hadoop.hive.common.type.HiveDecimal;

public class DecimalNumDistinctValueEstimator extends NumDistinctValueEstimator {

  public DecimalNumDistinctValueEstimator(int numBitVectors) {
    super(numBitVectors);
  }

  public DecimalNumDistinctValueEstimator(String s, int numBitVectors) {
    super(s, numBitVectors);
  }

  public void addToEstimator(HiveDecimal decimal) {
    int v = decimal.hashCode();
    super.addToEstimator(v);
  }

  public void addToEstimatorPCSA(HiveDecimal decimal) {
    int v = decimal.hashCode();
    super.addToEstimatorPCSA(v);
  }
}
