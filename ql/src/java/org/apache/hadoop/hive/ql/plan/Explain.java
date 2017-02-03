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

package org.apache.hadoop.hive.ql.plan;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Explain.
 *
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface Explain {
  public enum Level {
    USER, DEFAULT, EXTENDED;
    public boolean in(Level[] levels) {
      for (Level level : levels) {
        if (level.equals(this)) {
          return true;
        }
      }
      return false;
    }
  };
  String displayName() default "";

  Level[] explainLevels() default { Level.DEFAULT, Level.EXTENDED };

  boolean displayOnlyOnTrue() default false;

  boolean skipHeader() default false;

  // By default, many existing @Explain classes/methods are NON_VECTORIZED.
  //
  // Vectorized methods/classes have detail levels:
  //     SUMMARY, OPERATOR, EXPRESSION, or DETAIL.
  // As you go to the right you get more detail and the information for the previous level(s) is
  // included.  The default is SUMMARY.
  //
  // The "path" enumerations are used to mark methods/classes that lead to vectorization specific
  // ones so we can avoid displaying headers for things that have no vectorization information
  // below.
  //
  // For example, the TezWork class is marked SUMMARY_PATH because it leads to both
  // SUMMARY and OPERATOR methods/classes. And, MapWork.getAllRootOperators is marked OPERATOR_PATH
  // because we only display operator information for OPERATOR.
  //
  // EXPRESSION and DETAIL typically live inside SUMMARY or OPERATOR classes.
  //
  public enum Vectorization {
    SUMMARY_PATH(4), OPERATOR_PATH(3),
    SUMMARY(4), OPERATOR(3), EXPRESSION(2), DETAIL(1),
    NON_VECTORIZED(Integer.MAX_VALUE);

    public final int rank;
    Vectorization(int rank) {
      this.rank = rank;
    }
  };
  Vectorization vectorization() default Vectorization.NON_VECTORIZED;
}
