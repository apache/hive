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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

/**
 * Evaluate LIKE filter on a batch for a vector of strings.
 */
public class FilterStringColLikeStringScalar extends AbstractFilterStringColLikeStringScalar {
  private static final long serialVersionUID = 1L;

  private transient final static List<CheckerFactory> checkerFactories = Arrays.asList(
      new BeginCheckerFactory(),
      new EndCheckerFactory(),
      new MiddleCheckerFactory(),
      new NoneCheckerFactory(),
      new ChainedCheckerFactory(),
      new ComplexCheckerFactory());

  public FilterStringColLikeStringScalar() {
    super();
  }

  public FilterStringColLikeStringScalar(int colNum, byte[] likePattern) {
    super(colNum, null);
    super.setPattern(new String(likePattern, StandardCharsets.UTF_8));
  }

  @Override
  protected List<CheckerFactory> getCheckerFactories() {
    return checkerFactories;
  }

  /**
   * Accepts simple LIKE patterns like "abc%" and creates corresponding checkers.
   */
  private static class BeginCheckerFactory implements CheckerFactory {
    public Checker tryCreate(String pattern) {
      return UDFLikePattern.BEGIN.apply(pattern);
    }
  }

  /**
   * Accepts simple LIKE patterns like "%abc" and creates a corresponding checkers.
   */
  private static class EndCheckerFactory implements CheckerFactory {
    public Checker tryCreate(String pattern) {
      return UDFLikePattern.END.apply(pattern);
    }
  }

  /**
   * Accepts simple LIKE patterns like "%abc%" and creates a corresponding checkers.
   */
  private static class MiddleCheckerFactory implements CheckerFactory {
    public Checker tryCreate(String pattern) {
      return UDFLikePattern.MIDDLE.apply(pattern);
    }
  }

  /**
   * Accepts simple LIKE patterns like "abc" and creates corresponding checkers.
   */
  private static class NoneCheckerFactory implements CheckerFactory {
    public Checker tryCreate(String pattern) {
      return UDFLikePattern.NONE.apply(pattern);
    }
  }

  /**
   * Accepts chained LIKE patterns without escaping like "abc%def%ghi%" and creates corresponding
   * checkers.
   *
   */
  private static class ChainedCheckerFactory implements CheckerFactory {
    public Checker tryCreate(String pattern) {
      return UDFLikePattern.CHAINED.apply(pattern);
    }
  }

  /**
   * Accepts any LIKE patterns and creates corresponding checkers.
   */
  private static class ComplexCheckerFactory implements CheckerFactory {
    public Checker tryCreate(String pattern) {
      // anchor the pattern to the start:end of the whole string.
      return new ComplexChecker("^" + UDFLike.likePatternToRegExp(pattern) + "$");
    }
  }

  private enum UDFLikePattern {
    BEGIN(BeginChecker.class, UDFLike.PatternType.BEGIN),
    END(EndChecker.class, UDFLike.PatternType.END),
    MIDDLE(MiddleChecker.class, UDFLike.PatternType.MIDDLE),
    NONE(NoneChecker.class, UDFLike.PatternType.NONE),
    CHAINED(ChainedChecker.class, UDFLike.PatternType.CHAINED);

    Class<? extends Checker> checker;
    UDFLike.PatternType type;

    UDFLikePattern(Class<? extends Checker> checker, UDFLike.PatternType type) {
      this.checker = checker;
      this.type = type;
    }

    private String get(String pattern) {
      int startIndex = 0;
      int endIndex = pattern.length();

      switch (type) {
      case BEGIN:
        endIndex--;
        break;
      case END:
        startIndex++;
        break;
      case MIDDLE:
        startIndex++;
        endIndex--;
        break;
      default:
        break;
      }

      return pattern.substring(startIndex, endIndex);
    }

    public Checker apply(String pattern) {
      if (matches(pattern)) {
        try {
          return checker.getConstructor(String.class).newInstance(get(pattern));
        } catch (Exception e) {
          throw new IllegalArgumentException("unable to initialize Checker");
        }
      }

      return null;
    }

    private boolean matches(String pattern) {
      UDFLike.PatternType lastType = UDFLike.PatternType.NONE;
      int length = pattern.length();
      char lastChar = 0;

      for (int i = 0; i < length; i++) {
        char n = pattern.charAt(i);
        if (n == '_' && lastChar != '\\') { // such as "a_bc"
          return false;
        } else if (n == '%') {
          if (i == 0) { // such as "%abc"
            lastType = UDFLike.PatternType.END;
          } else if (i < length - 1) {
            if (lastChar != '\\') { // such as "a%bc"
              lastType = UDFLike.PatternType.CHAINED;
            }
          } else {
            if (lastChar != '\\') {
              if (lastType == UDFLike.PatternType.END) { // such as "%abc%"
                lastType = UDFLike.PatternType.MIDDLE;
              } else if (lastType != UDFLike.PatternType.CHAINED) {
                lastType = UDFLike.PatternType.BEGIN; // such as "abc%"
              }
            }
          }
        }
        lastChar = n;
      }
      return type == lastType;
    }
  }
}
