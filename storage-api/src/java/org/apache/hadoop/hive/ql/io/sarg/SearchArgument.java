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

package org.apache.hadoop.hive.ql.io.sarg;

import java.util.List;

/**
 * Primary interface for <a href="http://en.wikipedia.org/wiki/Sargable">
 *   SearchArgument</a>, which are the subset of predicates
 * that can be pushed down to the RecordReader. Each SearchArgument consists
 * of a series of SearchClauses that must each be true for the row to be
 * accepted by the filter.
 *
 * This requires that the filter be normalized into conjunctive normal form
 * (<a href="http://en.wikipedia.org/wiki/Conjunctive_normal_form">CNF</a>).
 */
public interface SearchArgument {

  /**
   * The potential result sets of logical operations.
   */
  public static enum TruthValue {
    YES, NO, NULL, YES_NULL, NO_NULL, YES_NO, YES_NO_NULL;

    /**
     * Compute logical or between the two values.
     * @param right the other argument or null
     * @return the result
     */
    public TruthValue or(TruthValue right) {
      if (right == null || right == this) {
        return this;
      }
      if (right == YES || this == YES) {
        return YES;
      }
      if (right == YES_NULL || this == YES_NULL) {
        return YES_NULL;
      }
      if (right == NO) {
        return this;
      }
      if (this == NO) {
        return right;
      }
      if (this == NULL) {
        if (right == NO_NULL) {
          return NULL;
        } else {
          return YES_NULL;
        }
      }
      if (right == NULL) {
        if (this == NO_NULL) {
          return NULL;
        } else {
          return YES_NULL;
        }
      }
      return YES_NO_NULL;
    }

    /**
     * Compute logical AND between the two values.
     * @param right the other argument or null
     * @return the result
     */
    public TruthValue and(TruthValue right) {
      if (right == null || right == this) {
        return this;
      }
      if (right == NO || this == NO) {
        return NO;
      }
      if (right == NO_NULL || this == NO_NULL) {
        return NO_NULL;
      }
      if (right == YES) {
        return this;
      }
      if (this == YES) {
        return right;
      }
      if (this == NULL) {
        if (right == YES_NULL) {
          return NULL;
        } else {
          return NO_NULL;
        }
      }
      if (right == NULL) {
        if (this == YES_NULL) {
          return NULL;
        } else {
          return NO_NULL;
        }
      }
      return YES_NO_NULL;
    }

    public TruthValue not() {
      switch (this) {
        case NO:
          return YES;
        case YES:
          return NO;
        case NULL:
        case YES_NO:
        case YES_NO_NULL:
          return this;
        case NO_NULL:
          return YES_NULL;
        case YES_NULL:
          return NO_NULL;
        default:
          throw new IllegalArgumentException("Unknown value: " + this);
      }
    }

    /**
     * Does the RecordReader need to include this set of records?
     * @return true unless none of the rows qualify
     */
    public boolean isNeeded() {
      switch (this) {
        case NO:
        case NULL:
        case NO_NULL:
          return false;
        default:
          return true;
      }
    }
  }

  /**
   * Get the leaf predicates that are required to evaluate the predicate. The
   * list will have the duplicates removed.
   * @return the list of leaf predicates
   */
  public List<PredicateLeaf> getLeaves();

  /**
   * Get the expression tree. This should only needed for file formats that
   * need to translate the expression to an internal form.
   */
  public ExpressionTree getExpression();

  /**
   * Evaluate the entire predicate based on the values for the leaf predicates.
   * @param leaves the value of each leaf predicate
   * @return the value of hte entire predicate
   */
  public TruthValue evaluate(TruthValue[] leaves);

  /**
   * A builder object for contexts outside of Hive where it isn't easy to
   * get a ExprNodeDesc. The user must call startOr, startAnd, or startNot
   * before adding any leaves.
   */
  public interface Builder {

    /**
     * Start building an or operation and push it on the stack.
     * @return this
     */
    public Builder startOr();

    /**
     * Start building an and operation and push it on the stack.
     * @return this
     */
    public Builder startAnd();

    /**
     * Start building a not operation and push it on the stack.
     * @return this
     */
    public Builder startNot();

    /**
     * Finish the current operation and pop it off of the stack. Each start
     * call must have a matching end.
     * @return this
     */
    public Builder end();

    /**
     * Add a less than leaf to the current item on the stack.
     * @param column the name of the column
     * @param type the type of the expression
     * @param literal the literal
     * @return this
     */
    public Builder lessThan(String column, PredicateLeaf.Type type,
                            Object literal);

    /**
     * Add a less than equals leaf to the current item on the stack.
     * @param column the name of the column
     * @param type the type of the expression
     * @param literal the literal
     * @return this
     */
    public Builder lessThanEquals(String column, PredicateLeaf.Type type,
                                  Object literal);

    /**
     * Add an equals leaf to the current item on the stack.
     * @param column the name of the column
     * @param type the type of the expression
     * @param literal the literal
     * @return this
     */
    public Builder equals(String column, PredicateLeaf.Type type,
                          Object literal);

    /**
     * Add a null safe equals leaf to the current item on the stack.
     * @param column the name of the column
     * @param type the type of the expression
     * @param literal the literal
     * @return this
     */
    public Builder nullSafeEquals(String column, PredicateLeaf.Type type,
                                  Object literal);

    /**
     * Add an in leaf to the current item on the stack.
     * @param column the name of the column
     * @param type the type of the expression
     * @param literal the literal
     * @return this
     */
    public Builder in(String column, PredicateLeaf.Type type,
                      Object... literal);

    /**
     * Add an is null leaf to the current item on the stack.
     * @param column the name of the column
     * @param type the type of the expression
     * @return this
     */
    public Builder isNull(String column, PredicateLeaf.Type type);

    /**
     * Add a between leaf to the current item on the stack.
     * @param column the name of the column
     * @param type the type of the expression
     * @param lower the literal
     * @param upper the literal
     * @return this
     */
    public Builder between(String column, PredicateLeaf.Type type,
                           Object lower, Object upper);

    /**
     * Add a truth value to the expression.
     * @param truth
     * @return this
     */
    public Builder literal(TruthValue truth);

    /**
     * Build and return the SearchArgument that has been defined. All of the
     * starts must have been ended before this call.
     * @return the new SearchArgument
     */
    public SearchArgument build();
  }
}
