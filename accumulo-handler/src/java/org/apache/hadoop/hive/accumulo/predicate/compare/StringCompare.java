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

import java.util.regex.Pattern;

/**
 * Set of comparison operations over a string constant. Used for Hive predicates involving string
 * comparison.
 *
 * Used by {@link org.apache.hadoop.hive.accumulo.predicate.PrimitiveComparisonFilter}
 */
public class StringCompare implements PrimitiveComparison {
  @SuppressWarnings("unused")

  private String constant;

  @Override
  public void init(byte[] constant) {
    this.constant = serialize(constant);
  }

  @Override
  public boolean isEqual(byte[] value) {
    return serialize(value).equals(constant);
  }

  @Override
  public boolean isNotEqual(byte[] value) {
    return !isEqual(value);
  }

  @Override
  public boolean greaterThanOrEqual(byte[] value) {
    return serialize(value).compareTo(constant) >= 0;
  }

  @Override
  public boolean greaterThan(byte[] value) {
    return serialize(value).compareTo(constant) > 0;
  }

  @Override
  public boolean lessThanOrEqual(byte[] value) {
    return serialize(value).compareTo(constant) <= 0;
  }

  @Override
  public boolean lessThan(byte[] value) {
    return serialize(value).compareTo(constant) < 0;
  }

  @Override
  public boolean like(byte[] value) {
    String temp = new String(value).replace("%", "[\\\\\\w]+?");
    Pattern pattern = Pattern.compile(temp);
    boolean match = pattern.matcher(constant).matches();
    return match;
  }

  public String serialize(byte[] value) {
    return new String(value);
  }
}
