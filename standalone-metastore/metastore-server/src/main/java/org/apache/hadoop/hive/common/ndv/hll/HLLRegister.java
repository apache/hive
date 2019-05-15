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

package org.apache.hadoop.hive.common.ndv.hll;

public interface HLLRegister {

  /**
   * Specify a hashcode to add to hyperloglog register.
   * @param hashcode
   *          - hashcode to add
   * @return true if register value is updated else false
   */
  boolean add(long hashcode);

  /**
   * Instead of specifying hashcode, this interface can be used to directly
   * specify the register index and register value. This interface is useful
   * when reconstructing hyperloglog from a serialized representation where its
   * not possible to regenerate the hashcode.
   * @param idx
   *          - register index
   * @param value
   *          - register value
   * @return true if register value is updated else false
   */
  boolean set(int idx, byte value);

  /**
   * Merge hyperloglog registers of the same type (SPARSE or DENSE register)
   * @param reg
   *          - register to be merged
   */
  void merge(HLLRegister reg);
}
