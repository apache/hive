/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.accumulo.predicate.compare;

/**
 * Handles different types of comparisons in hive predicates. Filter iterator delegates value
 * acceptance to the CompareOpt.
 *
 * Used by {@link org.apache.hadoop.hive.accumulo.predicate.PrimitiveComparisonFilter}. Works with
 * {@link PrimitiveComparison}
 */
public interface CompareOp {
  /**
   * Sets the PrimitiveComparison for this CompareOp
   */
  public void setPrimitiveCompare(PrimitiveComparison comp);

  /**
   * @return The PrimitiveComparison this CompareOp is a part of
   */
  public PrimitiveComparison getPrimitiveCompare();

  /**
   * @param val The bytes from the Accumulo Value
   * @return true if the value is accepted by this CompareOp
   */
  public boolean accept(byte[] val);
}
