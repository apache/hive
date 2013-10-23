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

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.io.Text;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Evaluate REGEXP filter on a batch for a vector of strings.
 */
public class FilterStringColRegExpStringScalar extends AbstractFilterStringColLikeStringScalar {
  private static final long serialVersionUID = 1L;

  private static final String LITERAL_CHAR = "[^\\[\\]\\\\(){}*?+|$^.]";
  private static final String LITERAL_CHAR_GROUP = "(" + LITERAL_CHAR + "+)";

  private transient static List<CheckerFactory> checkerFactories = Arrays.asList(
      new BeginCheckerFactory(),
      new EndCheckerFactory(),
      new MiddleCheckerFactory(),
      new PhoneNumberCheckerFactory(),
      new NoneCheckerFactory(),
      new ComplexCheckerFactory());

  public FilterStringColRegExpStringScalar() {
    super();
  }

  public FilterStringColRegExpStringScalar(int colNum, byte [] regExpPattern) throws HiveException {
    super(colNum, null);
    try {
      super.setPattern(new String(regExpPattern, "UTF-8"));
    } catch (Exception ex) {
      throw new HiveException(ex);
    }
  }

  @Override
  protected List<CheckerFactory> getCheckerFactories() {
    return checkerFactories;
  }

  /**
   * Accepts simple REGEXP patterns like "abc.*" and creates corresponding checkers.
   */
  private static class BeginCheckerFactory implements CheckerFactory {
    private static final Pattern BEGIN_PATTERN = Pattern.compile(LITERAL_CHAR_GROUP + "\\.\\*");

    public Checker tryCreate(String pattern) {
      Matcher matcher = BEGIN_PATTERN.matcher(pattern);
      if (matcher.matches()) {
        return new BeginChecker(matcher.group(1));
      }
      return null;
    }
  }

  /**
   * Accepts simple REGEXP patterns like ".*abc" and creates corresponding checkers.
   */
  private static class EndCheckerFactory implements CheckerFactory {
    private static final Pattern END_PATTERN = Pattern.compile("\\.\\*" + LITERAL_CHAR_GROUP);

    public Checker tryCreate(String pattern) {
      Matcher matcher = END_PATTERN.matcher(pattern);
      if (matcher.matches()) {
        return new EndChecker(matcher.group(1));
      }
      return null;
    }
  }

  /**
   * Accepts simple REGEXP patterns like ".*abc.*" and creates corresponding checkers.
   */
  private static class MiddleCheckerFactory implements CheckerFactory {
    private static final Pattern MIDDLE_PATTERN = Pattern.compile("\\.\\*" + LITERAL_CHAR_GROUP + "\\.\\*");

    public Checker tryCreate(String pattern) {
      Matcher matcher = MIDDLE_PATTERN.matcher(pattern);
      if (matcher.matches()) {
        return new MiddleChecker(matcher.group(1));
      }
      return null;
    }
  }

  /**
   * Accepts simple phone number regular expressions consisted only with "\(", "\)", "-", " ", "\d".
   * For example, it accepts "(\d\d\d) \d\d\d-\d\d\d\d" then matches "(012) 345-6789".
   */
  private static class PhoneNumberChecker implements Checker {
    byte[] byteSub;

    PhoneNumberChecker(String pattern) {
      this.byteSub = pattern.getBytes();
    }

    public boolean check(byte[] byteS, int start, int len) {
      for (int i = 0; i < len; i++) {
        byte c = byteS[start + i];
        byte p = byteSub[i];
        switch (p) {
          // For pattern 'd', find digits.
          case 'd':
            if (!('0' <= c && c <= '9')) {
              return false;
            }
            break;
          
          // For other registered patterns, find exact matches.
          case '-':
          case ' ':
          case '(':
          case ')':
            if (c != p) {
              return false;
            }
            break;
          
          // For unregistered patterns, fail.
          default:
            return false;
        }
      }
      return true;
    }
  }

  /**
   * Accepts phone number REGEXP patterns like "\(\d\d\d\) \d\d\d-\d\d\d\d" and creates
   * corresponding checkers.
   */
  private static class PhoneNumberCheckerFactory implements CheckerFactory {
    public Checker tryCreate(String pattern) {
      if (pattern.matches("(\\\\d|\\\\\\(|\\\\\\)|-| )+")) {
        return new PhoneNumberChecker(pattern.replaceAll("\\\\d", "d").replaceAll("\\\\\\(", "(").replaceAll("\\\\\\)", ")"));
      }
      return null;
    }
  }

  /**
   * Accepts simple REGEXP patterns like "abc" and creates corresponding checkers.
   */
  private static class NoneCheckerFactory implements CheckerFactory {
    private static final Pattern NONE_PATTERN = Pattern.compile(LITERAL_CHAR_GROUP);

    public Checker tryCreate(String pattern) {
      Matcher matcher = NONE_PATTERN.matcher(pattern);
      if (matcher.matches()) {
        return new NoneChecker(matcher.group(1));
      }
      return null;
    }
  }

  /**
   * Accepts any REGEXP patterns and creates corresponding checkers.
   */
  private static class ComplexCheckerFactory implements CheckerFactory {
    public Checker tryCreate(String pattern) {
      return new ComplexChecker(pattern);
    }
  }
}
