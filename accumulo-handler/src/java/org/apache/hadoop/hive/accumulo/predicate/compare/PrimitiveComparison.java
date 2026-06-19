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
 * Wraps type-specific comparison operations over a constant value. Methods take raw byte from
 * incoming Accumulo values.
 *
 * The CompareOpt instance in the iterator uses one or more methods from a PrimitiveCompare
 * implementation to perform type-specific comparisons and determine acceptances.
 *
 * Used by {@link org.apache.hadoop.hive.accumulo.predicate.PrimitiveComparisonFilter}. Works with
 * {@link CompareOp}
 */
public interface PrimitiveComparison {

  public boolean isEqual(byte[] value);

  public boolean isNotEqual(byte[] value);

  public boolean greaterThanOrEqual(byte[] value);

  public boolean greaterThan(byte[] value);

  public boolean lessThanOrEqual(byte[] value);

  public boolean lessThan(byte[] value);

  public boolean like(byte[] value);

  public Object serialize(byte[] value);

  public void init(byte[] constant);
}
