/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.wm;

import java.util.Objects;

/**
 * Simple trigger expression for a rule.
 */
public class TriggerExpression implements Expression {
  private CounterLimit counterLimit;
  private Predicate predicate;

  public TriggerExpression(final CounterLimit counter, final Predicate predicate) {
    this.counterLimit = counter;
    this.predicate = predicate;
  }

  @Override
  public boolean evaluate(final long current) {
    if (counterLimit.getLimit() > 0) {
      if (predicate.equals(Predicate.GREATER_THAN)) {
        return current > counterLimit.getLimit();
      }
    }
    return false;
  }

  @Override
  public CounterLimit getCounterLimit() {
    return counterLimit;
  }

  @Override
  public Predicate getPredicate() {
    return predicate;
  }

  @Override
  public Expression clone() {
    return new TriggerExpression(counterLimit.clone(), predicate);
  }

  @Override
  public String toString() {
    return counterLimit.getName() + " " + predicate.getSymbol() + " " + counterLimit.getLimit();
  }

  @Override
  public int hashCode() {
    int hash = counterLimit == null ? 31 : 31 * counterLimit.hashCode();
    hash += predicate == null ? 31 * hash : 31 * hash * predicate.hashCode();
    return 31 * hash;
  }

  @Override
  public boolean equals(final Object other) {
    if (other == null || !(other instanceof TriggerExpression)) {
      return false;
    }

    if (other == this) {
      return true;
    }

    return Objects.equals(counterLimit, ((TriggerExpression) other).counterLimit) &&
      Objects.equals(predicate, ((TriggerExpression) other).predicate);
  }
}
