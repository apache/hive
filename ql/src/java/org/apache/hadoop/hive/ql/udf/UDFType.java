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
package org.apache.hadoop.hive.ql.udf;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.apache.hadoop.hive.common.classification.InterfaceAudience.Public;
import org.apache.hadoop.hive.common.classification.InterfaceStability.Evolving;

/**
 * UDFType annotations are used to describe properties of a UDF. This gives
 * important information to the optimizer.
 * If the UDF is not deterministic, or if it is stateful, it is necessary to
 * annotate it as such for correctness.
 *
 */
@Public
@Evolving
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface UDFType {
  /**
   * Certain optimizations should not be applied if UDF is not deterministic.
   * Deterministic UDF returns same result each time it is invoked with a
   * particular input. This determinism just needs to hold within the context of
   * a query.
   *
   * @return true if the UDF is deterministic
   */
  boolean deterministic() default true;

  /**
   * If a UDF stores state based on the sequence of records it has processed, it
   * is stateful. A stateful UDF cannot be used in certain expressions such as
   * case statement and certain optimizations such as AND/OR short circuiting
   * don't apply for such UDFs, as they need to be invoked for each record.
   * row_sequence is an example of stateful UDF. A stateful UDF is considered to
   * be non-deterministic, irrespective of what deterministic() returns.
   *
   * @return true
   */
  boolean stateful() default false;

  /**
   * A UDF is considered distinctLike if the UDF can be evaluated on just the
   * distinct values of a column. Examples include min and max UDFs. This
   * information is used by metadata-only optimizer.
   *
   * @return true if UDF is distinctLike
   */
  boolean distinctLike() default false;

  /**
   * Using in analytical functions to specify that UDF implies an ordering
   *
   * @return true if the function implies order
   */
  boolean impliesOrder() default false;
}
