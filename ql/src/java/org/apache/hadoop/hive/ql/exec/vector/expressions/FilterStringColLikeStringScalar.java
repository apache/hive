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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
    private static final Pattern BEGIN_PATTERN = Pattern.compile("([^_%]+)%");

    public Checker tryCreate(String pattern) {
      Matcher matcher = BEGIN_PATTERN.matcher(pattern);
      if (matcher.matches()) {
        return new BeginChecker(matcher.group(1));
      }
      return null;
    }
  }

  /**
   * Accepts simple LIKE patterns like "%abc" and creates a corresponding checkers.
   */
  private static class EndCheckerFactory implements CheckerFactory {
    private static final Pattern END_PATTERN = Pattern.compile("%([^_%]+)");

    public Checker tryCreate(String pattern) {
      Matcher matcher = END_PATTERN.matcher(pattern);
      if (matcher.matches()) {
        return new EndChecker(matcher.group(1));
      }
      return null;
    }
  }

  /**
   * Accepts simple LIKE patterns like "%abc%" and creates a corresponding checkers.
   */
  private static class MiddleCheckerFactory implements CheckerFactory {
    private static final Pattern MIDDLE_PATTERN = Pattern.compile("%([^_%]+)%");

    public Checker tryCreate(String pattern) {
      Matcher matcher = MIDDLE_PATTERN.matcher(pattern);
      if (matcher.matches()) {
        return new MiddleChecker(matcher.group(1));
      }
      return null;
    }
  }

  /**
   * Accepts simple LIKE patterns like "abc" and creates corresponding checkers.
   */
  private static class NoneCheckerFactory implements CheckerFactory {
    private static final Pattern NONE_PATTERN = Pattern.compile("[^%_]+");

    public Checker tryCreate(String pattern) {
      Matcher matcher = NONE_PATTERN.matcher(pattern);
      if (matcher.matches()) {
        return new NoneChecker(pattern);
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
    private static final Pattern CHAIN_PATTERN = Pattern.compile("(%?[^%_\\\\]+%?)+");

    public Checker tryCreate(String pattern) {
      Matcher matcher = CHAIN_PATTERN.matcher(pattern);
      if (matcher.matches()) {
        return new ChainedChecker(pattern);
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
}
