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
 * Wraps call to like over {@link PrimitiveComparison} instance. Currently only supported by
 * StringCompare.
 *
 * Used by {@link org.apache.hadoop.hive.accumulo.predicate.PrimitiveComparisonFilter}
 */
public class Like implements CompareOp {

  PrimitiveComparison comp;

  public Like() {}

  public Like(PrimitiveComparison comp) {
    this.comp = comp;
  }

  @Override
  public void setPrimitiveCompare(PrimitiveComparison comp) {
    this.comp = comp;
  }

  @Override
  public PrimitiveComparison getPrimitiveCompare() {
    return comp;
  }

  @Override
  public boolean accept(byte[] val) {
    return comp.like(val);
  }
}
