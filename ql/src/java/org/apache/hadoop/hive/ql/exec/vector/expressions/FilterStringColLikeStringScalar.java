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
      CheckerPattern checkerPattern = new CheckerPattern(PatternType.BEGIN, pattern);
      String matche = checkerPattern.check();
      if (matche != null) {
        return new BeginChecker(matche);
      }
      return null;
    }
  }

  /**
   * Accepts simple LIKE patterns like "%abc" and creates a corresponding checkers.
   */
  private static class EndCheckerFactory implements CheckerFactory {
    public Checker tryCreate(String pattern) {
      CheckerPattern checkerPattern = new CheckerPattern(PatternType.END, pattern);
      String matche = checkerPattern.check();
      if (matche != null) {
        return new EndChecker(matche);
      }
      return null;
    }
  }

  /**
   * Accepts simple LIKE patterns like "%abc%" and creates a corresponding checkers.
   */
  private static class MiddleCheckerFactory implements CheckerFactory {
    public Checker tryCreate(String pattern) {
      CheckerPattern checkerPattern = new CheckerPattern(PatternType.MIDDLE, pattern);
      String matche = checkerPattern.check();
      if (matche != null) {
        return new MiddleChecker(matche);
      }
      return null;
    }
  }

  /**
   * Accepts simple LIKE patterns like "abc" and creates corresponding checkers.
   */
  private static class NoneCheckerFactory implements CheckerFactory {
    public Checker tryCreate(String pattern) {
      CheckerPattern checkerPattern = new CheckerPattern(PatternType.NONE, pattern);
      String matche = checkerPattern.check();
      if (matche != null) {
        return new NoneChecker(matche);
      }
      return null;
    }
  }

  /**
   * Accepts chained LIKE patterns without escaping like "abc%def%ghi%" and creates corresponding
   * checkers.
   *
   */
  private static class ChainedCheckerFactory implements CheckerFactory {
    public Checker tryCreate(String pattern) {
      CheckerPattern checkerPattern = new CheckerPattern(PatternType.CHAINED, pattern);
      String matche = checkerPattern.check();
      if (matche != null) {
        return new ChainedChecker(matche);
      }
      return null;
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

  private enum PatternType {
    NONE, // "abc"
    BEGIN, // "abc%"
    END, // "%abc"
    MIDDLE, // "%abc%"
    CHAINED, // "abc%def%ghi%"
    COMPLEX, // all other cases, such as "ab%c_de"
  }

  /**
   * Check the LIKE pattern..
   */
  private static class CheckerPattern {
    private final String pattern;
    private final PatternType type;

    public CheckerPattern(PatternType type, String pattern) {
      this.type = type;
      this.pattern = pattern;
    }

    public String check() {
      PatternType lastType = PatternType.NONE;
      int length = pattern.length();
      int beginIndex = 0;
      int endIndex = length;
      char lastChar = 'a';
      String strPattern = new String();
      String simplePattern;

      for (int i = 0; i < length; i++) {
        char n = pattern.charAt(i);
        if (n == '_') { // such as "a_b"
          if (lastChar != '\\') { // such as "a%bc"
            return null;
          } else { // such as "abc\%de%"
            strPattern += pattern.substring(beginIndex, i - 1);
            beginIndex = i;
          }
        } else if (n == '%') {
          if (i == 0) { // such as "%abc"
            lastType = PatternType.END;
            beginIndex = 1;
          } else if (i < length - 1) {
            if (lastChar != '\\') { // such as "a%bc"
              lastType = PatternType.CHAINED;
            } else if (lastChar == '\\') { // such as "a\%bc"
              strPattern += pattern.substring(beginIndex, i - 1);
              beginIndex = i;
              if (lastType == PatternType.CHAINED) {
                lastType = PatternType.COMPLEX;
              }
            } else {
              lastType = PatternType.COMPLEX;
            }
          } else {
            if (lastChar != '\\') {
              endIndex = length - 1;
              if (lastType == PatternType.END) { // such as "%abc%"
                lastType = PatternType.MIDDLE;
              } else if (lastType != PatternType.CHAINED &&
                         lastType != PatternType.COMPLEX) {
                lastType = PatternType.BEGIN; // such as "abc%"
              }
            } else { // such as "abc\%"
              strPattern += pattern.substring(beginIndex, i - 1);
              beginIndex = i;
              endIndex = length;
            }
          }
        }
        lastChar = n;
      }

      strPattern += pattern.substring(beginIndex, endIndex);
      if (lastType == PatternType.CHAINED) {
        simplePattern = pattern;
      } else {
        simplePattern = strPattern;
      }

      if (type == lastType) {
        return simplePattern;
      } else {
        return null;
      }
    }
  }
}
