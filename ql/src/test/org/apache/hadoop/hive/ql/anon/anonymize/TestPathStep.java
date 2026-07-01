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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

public class TestPathStep {

  public static final class Elem {
    private final String type;
    private final int num;
    public Elem(String type, int num) { this.type = type; this.num = num; }
    public String getType() { return type; }
    public int getNum() { return num; }
    public String getCodeStr() { return String.valueOf(num); }
  }

  private static List<Elem> elems() {
    return Arrays.asList(
        new Elem("guest", 1), new Elem("member", 7),
        new Elem("guest", 12), new Elem("admin", 3));
  }

  @Test
  public void splitsOnColonOutsideBracketsAndQuotes() {
    Assertions.assertEquals(List.of("a", "b", "c"), PathStep.splitSteps("a:b:c"));
    Assertions.assertEquals(List.of("xs[*]", "y"), PathStep.splitSteps("xs[*]:y"));
    Assertions.assertEquals(List.of("xs[k='a:b']", "y"), PathStep.splitSteps("xs[k='a:b']:y"));
  }

  @Test
  public void parsesPlainWildcardIndexFilter() {
    Assertions.assertEquals("xs", PathStep.parse("xs").field);
    Assertions.assertTrue(PathStep.parse("xs").selectsAll());
    Assertions.assertTrue(PathStep.parse("xs[*]").selectsAll());
    Assertions.assertEquals("xs", PathStep.parse("xs[2]").field);
    Assertions.assertFalse(PathStep.parse("xs[2]").selectsAll());
    Assertions.assertFalse(PathStep.parse("xs[k='v']").selectsAll());
    Assertions.assertEquals("*", PathStep.parse("*").field);
  }

  @Test
  public void indexSelectsSingleElement() {
    final List<Elem> l = elems();
    Assertions.assertEquals(List.of(0), PathStep.parse("xs[0]").selectIndices(l));
    Assertions.assertEquals(List.of(3), PathStep.parse("xs[3]").selectIndices(l));
    Assertions.assertTrue(PathStep.parse("xs[9]").selectIndices(l).isEmpty(), "out-of-range index selects nothing");
  }

  @Test
  public void wildcardAndPlainSelectAll() {
    final List<Elem> l = elems();
    Assertions.assertEquals(List.of(0, 1, 2, 3), PathStep.parse("xs").selectIndices(l));
    Assertions.assertEquals(List.of(0, 1, 2, 3), PathStep.parse("xs[*]").selectIndices(l));
  }

  @Test
  public void stringFilterEqualityAndInequality() {
    final List<Elem> l = elems();
    Assertions.assertEquals(List.of(0, 2), PathStep.parse("xs[type='guest']").selectIndices(l));
    Assertions.assertEquals(List.of(1, 3), PathStep.parse("xs[type!='guest']").selectIndices(l));
    final List<Elem> c = List.of(new Elem("a:b", 0), new Elem("c", 1));
    Assertions.assertEquals(List.of(0), PathStep.parse("xs[type='a:b']").selectIndices(c));
  }

  @Test
  public void numericFilterComparisons() {
    final List<Elem> l = elems();
    Assertions.assertEquals(List.of(1, 2), PathStep.parse("xs[num>5]").selectIndices(l));
    Assertions.assertEquals(List.of(0, 3), PathStep.parse("xs[num<5]").selectIndices(l));
    Assertions.assertEquals(List.of(1, 2), PathStep.parse("xs[num>=7]").selectIndices(l));
    Assertions.assertEquals(List.of(0, 3), PathStep.parse("xs[num<=3]").selectIndices(l));
    Assertions.assertEquals(List.of(2), PathStep.parse("xs[num=12]").selectIndices(l));
    Assertions.assertEquals(List.of(0, 1, 3), PathStep.parse("xs[num!=12]").selectIndices(l));
  }

  @Test
  public void numericFilterAgainstNumericStringComparesNumerically() {
    final List<Elem> l = elems();
    Assertions.assertEquals(List.of(1, 2), PathStep.parse("xs[codeStr>5]").selectIndices(l),
        "numeric-string field must compare numerically (7 and 12 > 5), not lexicographically");
    Assertions.assertEquals(List.of(0, 3), PathStep.parse("xs[codeStr<5]").selectIndices(l));
    Assertions.assertEquals(List.of(1, 2), PathStep.parse("xs[codeStr>=7]").selectIndices(l));
    Assertions.assertEquals(List.of(2), PathStep.parse("xs[codeStr=12]").selectIndices(l));
  }

  @Test
  public void scalarIndexSelectionByCount() {
    Assertions.assertEquals(List.of(0, 1, 2), PathStep.parse("xs").selectScalarIndices(3));
    Assertions.assertEquals(List.of(0, 1, 2), PathStep.parse("xs[*]").selectScalarIndices(3));
    Assertions.assertEquals(List.of(1), PathStep.parse("xs[1]").selectScalarIndices(3));
    Assertions.assertTrue(PathStep.parse("xs[5]").selectScalarIndices(3).isEmpty());
    Assertions.assertThrows(RuntimeException.class,
        () -> PathStep.parse("xs[k='v']").selectScalarIndices(3));
  }

  @Test
  public void rejectsMoreThanOnePredicatePerStep() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> PathStep.parse("xs[0][1]"));
    Assertions.assertThrows(IllegalArgumentException.class, () -> PathStep.parse("xs[*][2]"));
    Assertions.assertThrows(IllegalArgumentException.class,
        () -> PathStep.parse("xs[type='guest'][0]"));
    final List<Elem> c = List.of(new Elem("a]b", 0), new Elem("c", 1));
    Assertions.assertEquals(List.of(0), PathStep.parse("xs[type='a]b']").selectIndices(c));
  }
}
