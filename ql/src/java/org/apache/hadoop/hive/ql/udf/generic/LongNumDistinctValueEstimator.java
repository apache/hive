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

public class LongNumDistinctValueEstimator extends NumDistinctValueEstimator {

  public LongNumDistinctValueEstimator(int numBitVectors) {
    super(numBitVectors);
  }

  public LongNumDistinctValueEstimator(String s, int numVectors) {
    super(s, numVectors);
  }

   @Override
   public void addToEstimator(long v) {
    /* Update summary bitVector :
     * Generate hash value of the long value and mod it by 2^bitVectorSize-1.
     * In this implementation bitVectorSize is 31.
     */
     super.addToEstimator(v);
  }

  @Override
  public void addToEstimatorPCSA(long v) {
    super.addToEstimatorPCSA(v);
  }
}
