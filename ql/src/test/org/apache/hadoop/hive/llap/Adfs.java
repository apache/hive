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

package org.apache.hadoop.hive.llap;

import org.apache.hadoop.hive.common.ndv.NumDistinctValueEstimator;
import org.apache.hadoop.hive.common.ndv.hll.HyperLogLog;
import org.junit.Test;

public class Adfs {

  @Test
  public void asd() {

    System.out.println("asd");
    HyperLogLog h = b70();
    HyperLogLog h2 = HyperLogLog.builder().build();
    //h.squash(p0)
    NumDistinctValueEstimator h3 = h2.deserialize(h.serialize());
    h3.mergeEstimators(h2.deserialize(h.serialize()));
    System.out.println(h3.estimateNumDistinctValues());
  }

  private HyperLogLog b70() {
    HyperLogLog h = HyperLogLog.builder().build();
    for (int ia = 0; ia <= 70; ia++)
    {
      for (int i = 1; i <= 70; i++) {
        h.addLong(i);
      }
    }
    return h;
  }

}
