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

package org.apache.hadoop.hive.common;

/**
 * Collection of {@link Number} manipulation utilities common across Hive.
 */
public final class NumberUtils {

  private NumberUtils() {
  }

  /**
   * Store two ints in a single long value.
   *
   * @param i1 First int to store
   * @param i2 Second int to store
   * @return The combined value stored in a long
   */
  public static long makeIntPair(int i1, int i2) {
    return (((long) i1) << 32) | (i2 & 0xffffffffL);
  }

  /**
   * Get the first int stored in a long value.
   *
   * @param pair The pair generated from makeIntPair
   * @return The first value stored in the long
   */
  public static int getFirstInt(long pair) {
    return (int) (pair >> 32);
  }

  /**
   * Get the second int stored in a long value.
   *
   * @param pair The pair generated from makeIntPair
   * @return The first value stored in the long
   */
  public static int getSecondInt(long pair) {
    return (int) pair;
  }

}
