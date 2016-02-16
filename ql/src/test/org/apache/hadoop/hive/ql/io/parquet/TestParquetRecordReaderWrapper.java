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

package org.apache.hadoop.hive.ql.io.parquet;

import static junit.framework.Assert.assertEquals;

import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.ql.io.parquet.read.ParquetFilterPredicateConverter;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument.TruthValue;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.junit.Test;

import java.sql.Date;

import parquet.filter2.predicate.FilterPredicate;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;

/**
 * These tests test the conversion to Parquet's sarg implementation.
 */
public class TestParquetRecordReaderWrapper {

  private static TruthValue[] values(TruthValue... vals) {
    return vals;
  }

  @Test
  public void testBuilder() throws Exception {
     SearchArgument sarg = SearchArgumentFactory.newBuilder()
        .startNot()
        .startOr()
        .isNull("x")
        .between("y", 10, 20)
        .in("z", 1, 2, 3)
        .nullSafeEquals("a", "stinger")
        .end()
        .end()
        .build();

    MessageType schema = MessageTypeParser.parseMessageType("message test {" +
        " optional int32 x; required int32 y; required int32 z;" +
        " optional binary a;}");
    FilterPredicate p =
        ParquetFilterPredicateConverter.toFilterPredicate(sarg, schema);
    String expected =
      "and(and(and(not(eq(x, null)), not(and(lteq(y, 20), not(lt(y, 10))))), not(or(or(eq(z, 1), " +
        "eq(z, 2)), eq(z, 3)))), not(eq(a, Binary{\"stinger\"})))";
    assertEquals(expected, p.toString());
  }

  @Test
  public void testBuilderComplexTypes() throws Exception {
    SearchArgument sarg =
        SearchArgumentFactory.newBuilder()
            .startAnd()
            .lessThanEquals("y", new HiveChar("hi", 10).toString())
            .end()
            .build();
    MessageType schema = MessageTypeParser.parseMessageType("message test {" +
        " required int32 x; required binary y; required binary z;}");
    assertEquals("lteq(y, Binary{\"hi        \"})",
        ParquetFilterPredicateConverter.toFilterPredicate(sarg, schema).toString());

    sarg = SearchArgumentFactory.newBuilder()
        .startNot()
        .startOr()
        .isNull("x")
        .in("z", 1, 2, 3)
        .nullSafeEquals("a", new HiveVarchar("stinger", 100).toString())
        .end()
        .end()
        .build();

    schema = MessageTypeParser.parseMessageType("message test {" +
        " optional int32 x; required binary y; required int32 z;" +
        " optional binary a;}");
    FilterPredicate p = ParquetFilterPredicateConverter.toFilterPredicate(sarg, schema);
    String expected =
        "and(and(not(eq(x, null)), not(or(or(eq(z, 1), eq(z, 2)), eq(z, 3)))), " +
        "not(eq(a, Binary{\"stinger\"})))";
    assertEquals(expected, p.toString());
  }

  @Test
  public void testBuilderComplexTypes2() throws Exception {
    SearchArgument sarg =
        SearchArgumentFactory.newBuilder()
            .startAnd()
            .lessThanEquals("y", new HiveChar("hi", 10).toString())
            .end()
            .build();
    MessageType schema = MessageTypeParser.parseMessageType("message test {" +
        " required int32 x; required binary y; required binary z;}");
    assertEquals("lteq(y, Binary{\"hi        \"})",
        ParquetFilterPredicateConverter.toFilterPredicate(sarg, schema).toString());

    sarg = SearchArgumentFactory.newBuilder()
        .startNot()
        .startOr()
        .isNull("x")
        .in("z", 1, 2, 3)
        .nullSafeEquals("a", new HiveVarchar("stinger", 100).toString())
        .end()
        .end()
        .build();
    schema = MessageTypeParser.parseMessageType("message test {" +
        " optional int32 x; required binary y; required int32 z;" +
        " optional binary a;}");

    FilterPredicate p = ParquetFilterPredicateConverter.toFilterPredicate(sarg, schema);
    String expected = "and(and(not(eq(x, null)), not(or(or(eq(z, 1), eq(z, 2)), eq(z, 3)))), " +
        "not(eq(a, Binary{\"stinger\"})))";
    assertEquals(expected, p.toString());
  }

  @Test
  public void testBuilderFloat() throws Exception {
    SearchArgument sarg =
        SearchArgumentFactory.newBuilder()
            .startAnd()
            .lessThan("x", new Integer((short) 22))
            .lessThan("x1", new Integer(22))
            .lessThanEquals("y", new HiveChar("hi", 10).toString())
            .equals("z", new Double(0.22))
            .equals("z1", new Double(0.22))
            .end()
            .build();
    MessageType schema = MessageTypeParser.parseMessageType("message test {" +
        " required int32 x; required int32 x1;" +
        " required binary y; required float z; required float z1;}");

    FilterPredicate p = ParquetFilterPredicateConverter.toFilterPredicate(sarg, schema);
    String expected = "and(and(and(and(lt(x, 22), lt(x1, 22))," +
        " lteq(y, Binary{\"hi        \"})), eq(z, " +
        "0.22)), eq(z1, 0.22))";
    assertEquals(expected, p.toString());
  }
}
