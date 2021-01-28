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

import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;

import java.sql.Date;
import java.sql.Timestamp;
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
    LONG(Long.class),      // all of the integer types
    FLOAT(Double.class),   // float and double
    STRING(String.class),  // string, char, varchar
    DATE(Date.class),
    DECIMAL(HiveDecimalWritable.class),
    TIMESTAMP(Timestamp.class),
    BOOLEAN(Boolean.class);

    private final Class cls;
    Type(Class cls) {
      this.cls = cls;
    }

    /**
     * For all SARG leaves, the values must be the matching class.
     * @return the value class
     */
    public Class getValueClass() {
      return cls;
    }
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
   *
   * @return an Integer, Long, Double, or String
   */
  public Object getLiteral();

  /**
   * For operators with multiple literals (IN and BETWEEN), get the literals.
   *
   * @return the list of literals (Integer, Longs, Doubles, or Strings)
   *
   */
  public List<Object> getLiteralList();

  /**
   * Get the id of the leaf.
   * The ids are assigned sequentially from 0.
   * @return the offset in the list returned from {@link SearchArgument#getLeaves}
   */
  int getId();
}
