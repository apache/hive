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

import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.ql.io.parquet.read.ParquetFilterPredicateConverter;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

import java.sql.Date;

import org.apache.parquet.filter2.predicate.FilterPredicate;

/**
 * These tests test the conversion to Parquet's sarg implementation.
 */
public class TestParquetRecordReaderWrapper {

  @Test
  public void testBuilder() throws Exception {
     SearchArgument sarg = SearchArgumentFactory.newBuilder()
        .startNot()
        .startOr()
        .isNull("x", PredicateLeaf.Type.LONG)
        .between("y", PredicateLeaf.Type.LONG, 10L, 20L)
        .in("z", PredicateLeaf.Type.LONG, 1L, 2L, 3L)
        .nullSafeEquals("a", PredicateLeaf.Type.STRING, "stinger")
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

  /**
   * Check the converted filter predicate is null if unsupported types are included
   * @throws Exception
   */
  @Test
  public void testBuilderComplexTypes() throws Exception {
    SearchArgument sarg =
        SearchArgumentFactory.newBuilder()
            .startAnd()
            .lessThan("x", PredicateLeaf.Type.DATE,
                Date.valueOf("1970-1-11"))
            .lessThanEquals("y", PredicateLeaf.Type.STRING,
                new HiveChar("hi", 10).toString())
            .equals("z", PredicateLeaf.Type.DECIMAL, new HiveDecimalWritable("1.0"))
            .end()
            .build();
    MessageType schema = MessageTypeParser.parseMessageType("message test {" +
        " required int32 x; required binary y; required binary z;}");
    assertEquals(null,
        ParquetFilterPredicateConverter.toFilterPredicate(sarg, schema));

    sarg = SearchArgumentFactory.newBuilder()
        .startNot()
        .startOr()
        .isNull("x", PredicateLeaf.Type.LONG)
        .between("y", PredicateLeaf.Type.DECIMAL,
            new HiveDecimalWritable("10"), new HiveDecimalWritable("20.0"))
        .in("z", PredicateLeaf.Type.LONG, 1L, 2L, 3L)
        .nullSafeEquals("a", PredicateLeaf.Type.STRING,
            new HiveVarchar("stinger", 100).toString())
        .end()
        .end()
        .build();
    schema = MessageTypeParser.parseMessageType("message test {" +
        " optional int32 x; required binary y; required int32 z;" +
        " optional binary a;}");
    assertEquals(null,
        ParquetFilterPredicateConverter.toFilterPredicate(sarg, schema));
  }

  /**
   * Check the converted filter predicate is null if unsupported types are included
   * @throws Exception
   */
  @Test
  public void testBuilderComplexTypes2() throws Exception {
    SearchArgument sarg =
        SearchArgumentFactory.newBuilder()
            .startAnd()
            .lessThan("x", PredicateLeaf.Type.DATE, Date.valueOf("2005-3-12"))
            .lessThanEquals("y", PredicateLeaf.Type.STRING,
                new HiveChar("hi", 10).toString())
            .equals("z", PredicateLeaf.Type.DECIMAL,
                new HiveDecimalWritable("1.0"))
            .end()
            .build();
    MessageType schema = MessageTypeParser.parseMessageType("message test {" +
        " required int32 x; required binary y; required binary z;}");
    assertEquals(null,
        ParquetFilterPredicateConverter.toFilterPredicate(sarg, schema));

    sarg = SearchArgumentFactory.newBuilder()
        .startNot()
        .startOr()
        .isNull("x", PredicateLeaf.Type.LONG)
        .between("y", PredicateLeaf.Type.DECIMAL, new HiveDecimalWritable("10"),
            new HiveDecimalWritable("20.0"))
        .in("z", PredicateLeaf.Type.LONG, 1L, 2L, 3L)
        .nullSafeEquals("a", PredicateLeaf.Type.STRING,
            new HiveVarchar("stinger", 100).toString())
        .end()
        .end()
        .build();
    schema = MessageTypeParser.parseMessageType("message test {" +
        " optional int32 x; required binary y; required int32 z;" +
        " optional binary a;}");
    assertEquals(null,
        ParquetFilterPredicateConverter.toFilterPredicate(sarg, schema));
  }

  @Test
  public void testBuilderFloat() throws Exception {
    SearchArgument sarg =
        SearchArgumentFactory.newBuilder()
            .startAnd()
            .lessThan("x", PredicateLeaf.Type.LONG, 22L)
            .lessThan("x1", PredicateLeaf.Type.LONG, 22L)
            .lessThanEquals("y", PredicateLeaf.Type.STRING,
                new HiveChar("hi", 10).toString())
            .equals("z", PredicateLeaf.Type.FLOAT, new Double(0.22))
            .equals("z1", PredicateLeaf.Type.FLOAT, new Double(0.22))
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
