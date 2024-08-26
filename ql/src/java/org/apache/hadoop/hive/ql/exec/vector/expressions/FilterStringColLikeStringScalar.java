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

package org.apache.hadoop.hive.ql.exec.vector.expressions;

import org.apache.hadoop.hive.ql.udf.UDFLike;

import com.google.common.collect.ImmutableList;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Evaluate LIKE filter on a batch for a vector of strings.
 */
public class FilterStringColLikeStringScalar extends AbstractFilterStringColLikeStringScalar {
  private static final long serialVersionUID = 1L;

  private static final List<CheckerFactory> CHECKER_FACTORIES = ImmutableList.of(
    pattern -> {
      UDFLikePattern udfLike = UDFLikePattern.matcher(pattern);
      try {
        return udfLike.checker.getConstructor(String.class).newInstance(
          udfLike.format(pattern));
      } catch (Exception e) {
        throw new IllegalArgumentException("unable to initialize Checker");
      }
    });

  public FilterStringColLikeStringScalar() {
    super();
  }

  public FilterStringColLikeStringScalar(int colNum, byte[] likePattern) {
    super(colNum, null);
    super.setPattern(new String(likePattern, StandardCharsets.UTF_8));
  }

  @Override
  protected List<CheckerFactory> getCheckerFactories() {
    return CHECKER_FACTORIES;
  }

  private enum UDFLikePattern {
    // Accepts simple LIKE patterns like "abc%" and creates corresponding checkers.
    BEGIN(BeginChecker.class) {
      @Override
      String format(String pattern) {
        return pattern.substring(0, pattern.length() - 1);
      }
    },
    // Accepts simple LIKE patterns like "%abc" and creates a corresponding checkers.
    END(EndChecker.class) {
      @Override
      String format(String pattern) {
        return pattern.substring(1);
      }
    },
    // Accepts simple LIKE patterns like "%abc%" and creates a corresponding checkers.
    MIDDLE(MiddleChecker.class) {
      @Override
      String format(String pattern) {
        return pattern.substring(1, pattern.length() - 1);
      }
    },
    // Accepts any LIKE patterns and creates corresponding checkers.
    COMPLEX(ComplexChecker.class) {
      @Override
      String format(String pattern) {
        return "^" + UDFLike.likePatternToRegExp(pattern) + "$";
      }
    },
    // Accepts chained LIKE patterns without escaping like "abc%def%ghi%" and
    // creates corresponding checkers.
    CHAINED(ChainedChecker.class),
    // Accepts simple LIKE patterns like "abc" and creates corresponding checkers.
    NONE(NoneChecker.class);

    Class<? extends Checker> checker;

    UDFLikePattern(Class<? extends Checker> checker) {
      this.checker = checker;
    }

    private static UDFLikePattern matcher(String pattern) {
      UDFLikePattern lastType = NONE;
      int length = pattern.length();
      char lastChar = 0;

      for (int i = 0; i < length; i++) {
        char n = pattern.charAt(i);
        if (n == '_' && lastChar != '\\') { // such as "a_bc"
          return COMPLEX;
        } else if (n == '%') {
          if (i == 0) { // such as "%abc"
            lastType = END;
          } else if (i < length - 1) {
            if (lastChar != '\\') { // such as "a%bc"
              lastType = CHAINED;
            }
          } else {
            if (lastChar != '\\') {
              if (lastType == END) { // such as "%abc%"
                lastType = MIDDLE;
              } else if (lastType != CHAINED) {
                lastType = BEGIN; // such as "abc%"
              }
            }
          }
        }
        lastChar = n;
      }
      return lastType;
    }

    String format(String pattern) {
      return pattern;
    }
  }
}
