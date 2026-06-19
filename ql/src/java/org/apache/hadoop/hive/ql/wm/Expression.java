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

/**
 * Expression that is defined in triggers.
 * Most expressions will get triggered only after exceeding a limit. As a result, only greater than (&gt;) expression
 * is supported.
 */
public interface Expression {

  enum Predicate {
    GREATER_THAN(">");

    String symbol;

    Predicate(final String symbol) {
      this.symbol = symbol;
    }

    public String getSymbol() {
      return symbol;
    }
  }

  interface Builder {
    Builder greaterThan(CounterLimit counter);

    Expression build();
  }

  /**
   * Evaluate current value against this expression. Return true if expression evaluates to true (current &gt; limit)
   * else false otherwise
   *
   * @param current - current value against which expression will be evaluated
   * @return true if current value exceeds limit
   */
  boolean evaluate(final long current);

  /**
   * Return counter limit
   *
   * @return counter limit
   */
  CounterLimit getCounterLimit();

  /**
   * Return predicate defined in the expression.
   *
   * @return predicate
   */
  Predicate getPredicate();

  /**
   * Return cloned copy of this expression.
   *
   * @return cloned copy
   */
  Expression clone();
}
