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

package org.apache.hadoop.hive.ql.anon.anonymize;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

public final class PathStep {

  public enum Op { EQ, NEQ, GT, LT, GTE, LTE }

  private enum Kind { ALL, INDEX, FILTER }

  public final String field;

  private final Kind kind;
  private final int index;
  private final String subfield;
  private final Op op;
  private final String litString;
  private final Long litNumber;

  private PathStep(String field, Kind kind, int index, String subfield, Op op,
                   String litString, Long litNumber) {
    this.field = field;
    this.kind = kind;
    this.index = index;
    this.subfield = subfield;
    this.op = op;
    this.litString = litString;
    this.litNumber = litNumber;
  }

  public boolean selectsAll() {
    return kind == Kind.ALL;
  }


  public static PathStep[] compile(String path) {
    final List<String> raw = splitSteps(path);
    final PathStep[] out = new PathStep[raw.size()];
    for (int i = 0; i < out.length; i++) {
      out[i] = parse(raw.get(i));
    }
    return out;
  }

  static List<String> splitSteps(String path) {
    final List<String> steps = new ArrayList<>();
    final StringBuilder cur = new StringBuilder();
    int depth = 0;
    boolean inStr = false;
    for (int i = 0; i < path.length(); i++) {
      final char c = path.charAt(i);
      if (inStr) {
        cur.append(c);
        if (c == '\\' && i + 1 < path.length()) {
          cur.append(path.charAt(++i));
        } else if (c == '\'') {
          inStr = false;
        }
      } else if (c == '\'') {
        inStr = true;
        cur.append(c);
      } else if (c == '[') {
        depth++;
        cur.append(c);
      } else if (c == ']') {
        depth--;
        cur.append(c);
      } else if (c == ':' && depth == 0) {
        steps.add(cur.toString());
        cur.setLength(0);
      } else {
        cur.append(c);
      }
    }
    steps.add(cur.toString());
    return steps;
  }

  public static PathStep parse(String step) {
    final int open = step.indexOf('[');
    if (open < 0) {
      return new PathStep(step, Kind.ALL, 0, null, null, null, null);
    }
    final String field = step.substring(0, open);
    final int close = closeBracket(step, open);
    final String content = step.substring(open + 1, close).trim();
    final String tail = step.substring(close + 1).trim();
    if (!tail.isEmpty()) {
      throw new IllegalArgumentException(
          "at most one predicate is allowed per path step, found more in: " + step);
    }

    if (content.equals("*")) {
      return new PathStep(field, Kind.ALL, 0, null, null, null, null);
    }
    if (content.matches("\\d+")) {
      return new PathStep(field, Kind.INDEX, Integer.parseInt(content), null, null, null, null);
    }
    return parseFilter(field, content, step);
  }

  private static PathStep parseFilter(String field, String content, String step) {
    int p = 0;
    while (p < content.length()
        && (Character.isLetterOrDigit(content.charAt(p)) || content.charAt(p) == '_')) {
      p++;
    }
    final String sub = content.substring(0, p);
    final String rest = content.substring(p).trim();
    final Op op;
    final String opStr;
    if (rest.startsWith("!=")) {
      op = Op.NEQ; opStr = "!=";
    } else if (rest.startsWith(">=")) {
      op = Op.GTE; opStr = ">=";
    } else if (rest.startsWith("<=")) {
      op = Op.LTE; opStr = "<=";
    } else if (rest.startsWith("=")) {
      op = Op.EQ; opStr = "=";
    } else if (rest.startsWith(">")) {
      op = Op.GT; opStr = ">";
    } else if (rest.startsWith("<")) {
      op = Op.LT; opStr = "<";
    } else {
      throw new IllegalArgumentException("bad filter operator in predicate step: " + step);
    }
    final String litText = rest.substring(opStr.length()).trim();
    if (sub.isEmpty() || litText.isEmpty()) {
      throw new IllegalArgumentException("malformed filter predicate step: " + step);
    }
    if (litText.startsWith("'") && litText.endsWith("'") && litText.length() >= 2) {
      return new PathStep(field, Kind.FILTER, 0, sub, op, unquote(litText), null);
    }
    final String n = (litText.endsWith("L") || litText.endsWith("l"))
        ? litText.substring(0, litText.length() - 1) : litText;
    return new PathStep(field, Kind.FILTER, 0, sub, op, null, Long.valueOf(Long.parseLong(n)));
  }

  private static int closeBracket(String step, int open) {
    boolean inStr = false;
    for (int i = open + 1; i < step.length(); i++) {
      final char c = step.charAt(i);
      if (inStr) {
        if (c == '\\') {
          i++;
        } else if (c == '\'') {
          inStr = false;
        }
      } else if (c == '\'') {
        inStr = true;
      } else if (c == ']') {
        return i;
      }
    }
    throw new IllegalArgumentException("unterminated predicate in step: " + step);
  }

  private static String unquote(String s) {
    final String inner = s.substring(1, s.length() - 1);
    final StringBuilder b = new StringBuilder();
    for (int i = 0; i < inner.length(); i++) {
      final char c = inner.charAt(i);
      if (c == '\\' && i + 1 < inner.length()) {
        b.append(inner.charAt(++i));
      } else {
        b.append(c);
      }
    }
    return b.toString();
  }


  List<Integer> selectIndices(List<?> lst) {
    final List<Integer> out = new ArrayList<>();
    if (lst == null) {
      return out;
    }
    switch (kind) {
      case INDEX:
        if (index >= 0 && index < lst.size()) {
          out.add(index);
        }
        break;
      case FILTER:
        for (int i = 0; i < lst.size(); i++) {
          final Object e = lst.get(i);
          if (e != null && matches(e)) {
            out.add(i);
          }
        }
        break;
      case ALL:
      default:
        for (int i = 0; i < lst.size(); i++) {
          out.add(i);
        }
    }
    return out;
  }

  List<Integer> selectScalarIndices(int size) {
    if (kind == Kind.FILTER) {
      throw new RuntimeException("filter predicate " + this
          + " is not valid on a scalar list; use an index or wildcard");
    }
    final List<Integer> out = new ArrayList<>();
    if (kind == Kind.INDEX) {
      if (index >= 0 && index < size) {
        out.add(index);
      }
    } else {
      for (int i = 0; i < size; i++) {
        out.add(i);
      }
    }
    return out;
  }

  boolean matches(Object element) {
    final Object v = subfieldValue(element, subfield);
    final int cmp;
    if (litNumber != null) {
      final Long n = asLong(v);
      if (n == null) {
        return op == Op.NEQ;
      }
      cmp = Long.compare(n, litNumber);
    } else {
      final String vs = String.valueOf(v);
      final String ls = litString != null ? litString : String.valueOf(litNumber);
      cmp = vs.compareTo(ls);
    }
    switch (op) {
      case EQ:  return cmp == 0;
      case NEQ: return cmp != 0;
      case GT:  return cmp > 0;
      case LT:  return cmp < 0;
      case GTE: return cmp >= 0;
      case LTE: return cmp <= 0;
      default:  return false;
    }
  }

  private static Long asLong(final Object v) {
    if (v instanceof Number) {
      return ((Number) v).longValue();
    }
    if (v == null) {
      return null;
    }
    final String s = String.valueOf(v).trim();
    if (s.isEmpty()) {
      return null;
    }
    try {
      return Long.valueOf(Long.parseLong(s));
    } catch (NumberFormatException nfe) {
      try {
        return (long) Double.parseDouble(s);
      } catch (NumberFormatException nfe2) {
        return null;
      }
    }
  }

  private static Object subfieldValue(Object element, String name) {
    for (final Method m : element.getClass().getMethods()) {
      if (m.getParameterCount() == 0
          && Modifier.isPublic(m.getModifiers()) && !Modifier.isStatic(m.getModifiers())
          && m.getName().equalsIgnoreCase("get" + name)) {
        try {
          return m.invoke(element);
        } catch (Exception e) {
          throw new RuntimeException("reading filter field '" + name + "': " + e.getMessage(), e);
        }
      }
    }
    throw new RuntimeException("filter predicate field '" + name
        + "' not found on " + element.getClass().getSimpleName());
  }

  @Override
  public String toString() {
    switch (kind) {
      case INDEX:  return field + "[" + index + "]";
      case FILTER: return field + "[" + subfield + " " + op + " "
          + (litString != null ? "'" + litString + "'" : litNumber) + "]";
      default:     return field;
    }
  }
}
