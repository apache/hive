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

package org.apache.hadoop.hive.ql.io.parquet.read;

import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;

public class TestParquetFilterPredicate {
  @Test
  public void testFilterColumnsThatDoNoExistOnSchema() {
    MessageType schema = MessageTypeParser.parseMessageType("message test { required int32 a; required binary stinger; }");
    SearchArgument sarg = SearchArgumentFactory.newBuilder()
            .startNot()
            .startOr()
            .isNull("a", PredicateLeaf.Type.LONG)
            .between("y", PredicateLeaf.Type.LONG, 10L, 20L) // Column will be removed from filter
            .in("z", PredicateLeaf.Type.LONG, 1L, 2L, 3L) // Column will be removed from filter
            .nullSafeEquals("stinger", PredicateLeaf.Type.STRING, "stinger")
            .end()
            .end()
            .build();

    FilterPredicate p = ParquetFilterPredicateConverter.toFilterPredicate(sarg, schema);

    String expected = "and(not(eq(a, null)), not(eq(stinger, Binary{\"stinger\"})))";
    assertEquals(expected, p.toString());
  }

  @Test
  public void testFilterColumnsThatDoNoExistOnSchemaHighOrder1() {
    MessageType schema = MessageTypeParser.parseMessageType("message test { required int32 a; required int32 b; }");
    SearchArgument sarg = SearchArgumentFactory.newBuilder()
        .startOr()
        .startAnd()
        .equals("a", PredicateLeaf.Type.LONG, 1L)
        .equals("none", PredicateLeaf.Type.LONG, 1L)
        .end()
        .startAnd()
        .equals("a", PredicateLeaf.Type.LONG, 999L)
        .equals("none", PredicateLeaf.Type.LONG, 999L)
        .end()
        .end()
        .build();

    FilterPredicate p = ParquetFilterPredicateConverter.toFilterPredicate(sarg, schema);

    String expected = "or(eq(a, 1), eq(a, 999))";
    assertEquals(expected, p.toString());
  }

  @Test
  public void testFilterColumnsThatDoNoExistOnSchemaHighOrder2() {
    MessageType schema = MessageTypeParser.parseMessageType("message test { required int32 a; required int32 b; }");
    SearchArgument sarg = SearchArgumentFactory.newBuilder()
        .startAnd()
        .startOr()
        .equals("a", PredicateLeaf.Type.LONG, 1L)
        .equals("b", PredicateLeaf.Type.LONG, 1L)
        .end()
        .startOr()
        .equals("a", PredicateLeaf.Type.LONG, 999L)
        .equals("none", PredicateLeaf.Type.LONG, 999L)
        .end()
        .end()
        .build();

    FilterPredicate p = ParquetFilterPredicateConverter.toFilterPredicate(sarg, schema);

    String expected = "or(eq(a, 1), eq(b, 1))";
    assertEquals(expected, p.toString());
  }

  @Test
  public void testFilterFloatColumns() {
    MessageType schema =
        MessageTypeParser.parseMessageType("message test {  required float a; required int32 b; }");
    SearchArgument sarg = SearchArgumentFactory.newBuilder()
        .startNot()
        .startOr()
        .isNull("a", PredicateLeaf.Type.FLOAT)
        .between("a", PredicateLeaf.Type.FLOAT, 10.2, 20.3)
        .in("b", PredicateLeaf.Type.LONG, 1L, 2L, 3L)
        .end()
        .end()
        .build();

    FilterPredicate p = ParquetFilterPredicateConverter.toFilterPredicate(sarg, schema);

    String expected =
        "and(and(not(eq(a, null)), not(and(lteq(a, 20.3), not(lt(a, 10.2))))), not(or(or(eq(b, 1), eq(b, 2)), eq(b, 3))))";
    assertEquals(expected, p.toString());
  }

  @Test
  public void testFilterBetween() {
    MessageType schema =
        MessageTypeParser.parseMessageType("message test {  required int32 bCol; }");
    SearchArgument sarg = SearchArgumentFactory.newBuilder()
        .between("bCol", PredicateLeaf.Type.LONG, 1L, 5L)
        .build();
    FilterPredicate p = ParquetFilterPredicateConverter.toFilterPredicate(sarg, schema);
    String expected =
        "and(lteq(bCol, 5), not(lt(bCol, 1)))";
    assertEquals(expected, p.toString());

    sarg = SearchArgumentFactory.newBuilder()
            .between("bCol", PredicateLeaf.Type.LONG, 5L, 1L)
            .build();
    p = ParquetFilterPredicateConverter.toFilterPredicate(sarg, schema);
    expected =
            "and(lteq(bCol, 1), not(lt(bCol, 5)))";
    assertEquals(expected, p.toString());

    sarg = SearchArgumentFactory.newBuilder()
            .between("bCol", PredicateLeaf.Type.LONG, 1L, 1L)
            .build();
    p = ParquetFilterPredicateConverter.toFilterPredicate(sarg, schema);
    expected =
            "and(lteq(bCol, 1), not(lt(bCol, 1)))";
    assertEquals(expected, p.toString());
  }
}
