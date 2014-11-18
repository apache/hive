/**
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
 * The primitive predicates that form a SearchArgument.
 */
public interface PredicateLeaf {

  /**
   * The possible operators for predicates. To get the opposites, construct
   * an expression with a not operator.
   */
  public static enum Operator {
    EQUALS,
    NULL_SAFE_EQUALS,
    LESS_THAN,
    LESS_THAN_EQUALS,
    IN,
    BETWEEN,
    IS_NULL
  }

  /**
   * The possible types for sargs.
   */
  public static enum Type {
    INTEGER, // all of the integer types except long
    LONG,
    FLOAT,   // float and double
    STRING,  // string, char, varchar
    DATE,
    DECIMAL,
    TIMESTAMP,
    BOOLEAN
  }

  /**
   * file format which supports search arguments
   */
  public static enum FileFormat {
    ORC,
    PARQUET
  }

  /**
   * Get the operator for the leaf.
   */
  public Operator getOperator();

  /**
   * Get the type of the column and literal by the file format.
   */
  public Type getType();

  /**
   * Get the simple column name.
   * @return the column name
   */
  public String getColumnName();

  /**
   * Get the literal half of the predicate leaf. Adapt the original type for what orc needs
   * @return a Long, Double, or String for Orc and a Int, Long, Double, or String for parquet
   */
  public Object getLiteral(FileFormat format);

  /**
   * For operators with multiple literals (IN and BETWEEN), get the literals.
   *
   * @return the list of literals (Longs, Doubles, or Strings) for orc or the list of literals
   * (Integer, Longs, Doubles, or String) for parquet
   */
  public List<Object> getLiteralList(FileFormat format);

}
