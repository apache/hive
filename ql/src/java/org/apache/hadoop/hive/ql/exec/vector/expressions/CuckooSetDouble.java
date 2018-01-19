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

package org.apache.hadoop.hive.ql.exec.vector.expressions;


/**
 * A high-performance set implementation used to support fast set membership testing,
 * using Cuckoo hashing. This is used to support fast tests of the form
 *
 *       column IN ( <list-of-values )
 *
 * For double, we simply layer over the implementation for long. Double.doubleToRawLongBits
 * is used to convert a 64-bit double to a 64-bit long with bit-for-bit fidelity.
 */
public class CuckooSetDouble {
  CuckooSetLong setLong;

  public CuckooSetDouble(int expectedSize) {
    setLong = new CuckooSetLong(expectedSize);
  }

  /**
   * Return true if and only if the value x is present in the set.
   */
  public boolean lookup(double x) {
    return setLong.lookup(Double.doubleToRawLongBits(x));
  }

  /**
   * Insert a single value into the set.
   */
  public void insert(double x) {
    setLong.insert(Double.doubleToRawLongBits(x));
  }

  /**
   * Insert all values in the input array into the set.
   */
  public void load(double[] a) {
    for (Double x : a) {
      insert(x);
    }
  }
}
