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

package org.apache.hadoop.hive.ql.exec;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver2;
import org.apache.hadoop.hive.ql.udf.ptf.WindowingTableFunction;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Documented
public @interface WindowFunctionDescription {
  /**
   * controls whether this function can be applied to a Window.
   * <p>
   * Ranking function: Rank, Dense_Rank, Percent_Rank and Cume_Dist don't operate on Windows.
   * Why? a window specification implies a row specific range i.e. every row gets its own set of rows to process the UDAF on.
   * For ranking defining a set of rows for every row makes no sense.
   * <p>
   * All other UDAFs can be computed for a Window.
   */
  boolean supportsWindow() default true;
  /**
   * A WindowFunc is implemented as {@link GenericUDAFResolver2}. It returns only one value.
   * If this is true then the function must return a List which is taken to be the column for this function in the Output table returned by the
   * {@link WindowingTableFunction}. Otherwise the output is assumed to be a single value, the column of the Output will contain the same value
   * for all the rows.
   */
  boolean pivotResult() default false;

  /**
   * Used in translations process to validate arguments.
   * @return true if ranking function
   */
  boolean rankingFunction() default false;

  /**
  * Using in analytical functions to specify that UDF implies an ordering.
  * @return true if the function implies order
  */
  boolean impliesOrder() default false;

  /**
   * This property specifies whether the UDAF is an Ordered-set aggregate function.
   * &lt;ordered-set aggregate functions&gt; ::=
   *   &lt;hypothetical set function&gt; |
   *   &lt;inverse distribution function&gt;
   *
   * &lt;hypothetical set function&gt; ::=
   *   &lt;rank function type&gt; &lt;left paren&gt;
   *   &lt;hypothetical set function value expression list&gt; &lt;right paren&gt;
   *   &lt;within group specification&gt;
   *
   * &lt;rank function type&gt; ::= RANK | DENSE_RANK | PERCENT_RANK | CUME_DIST
   *
   * &lt;inverse distribution function&gt; ::=
   *   &lt;inverse distribution function type&gt; &lt;left paren&gt;
   *   &lt;inverse distribution function argument&gt; &lt;right paren&gt;
   *   &lt;within group specification&gt;
   *
   * &lt;inverse distribution function type&gt; ::= PERCENTILE_CONT | PERCENTILE_DISC
   *
   * @return true if the function can be used as an ordered-set aggregate
   */
  boolean orderedAggregate() default false;

  /**
   * Some aggregate functions allow specifying null treatment.
   * Example:
   *   SELECT last_value(b) IGNORE NULLS OVER (ORDER BY b) FROM table1;
   *
   * @return true if this aggregate functions support null treatment.
   */
  boolean supportsNullTreatment() default false;
}
