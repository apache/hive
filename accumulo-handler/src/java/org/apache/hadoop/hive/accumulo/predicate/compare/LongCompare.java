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

import java.nio.ByteBuffer;

/**
 * Set of comparison operations over a long constant. Used for Hive predicates involving long
 * comparison.
 *
 * Used by {@link org.apache.hadoop.hive.accumulo.predicate.PrimitiveComparisonFilter}
 */
public class LongCompare implements PrimitiveComparison {

  private long constant;

  @Override
  public void init(byte[] constant) {
    this.constant = serialize(constant);
  }

  @Override
  public boolean isEqual(byte[] value) {
    long lonVal = serialize(value);
    return lonVal == constant;
  }

  @Override
  public boolean isNotEqual(byte[] value) {
    return serialize(value) != constant;
  }

  @Override
  public boolean greaterThanOrEqual(byte[] value) {
    return serialize(value) >= constant;
  }

  @Override
  public boolean greaterThan(byte[] value) {
    return serialize(value) > constant;
  }

  @Override
  public boolean lessThanOrEqual(byte[] value) {
    return serialize(value) <= constant;
  }

  @Override
  public boolean lessThan(byte[] value) {
    return serialize(value) < constant;
  }

  @Override
  public boolean like(byte[] value) {
    throw new UnsupportedOperationException("Like not supported for " + getClass().getName());
  }

  public Long serialize(byte[] value) {
    try {
      return ByteBuffer.wrap(value).asLongBuffer().get();
    } catch (Exception e) {
      throw new RuntimeException(e.toString() + " occurred trying to build long value. "
          + "Make sure the value type for the byte[] is long ");
    }
  }
}
