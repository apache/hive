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

import org.apache.parquet.filter2.predicate.FilterPredicate;

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
        .isNull("x", PredicateLeaf.Type.INTEGER)
        .between("y", PredicateLeaf.Type.INTEGER, 10, 20)
        .in("z", PredicateLeaf.Type.INTEGER, 1, 2, 3)
        .nullSafeEquals("a", PredicateLeaf.Type.STRING, "stinger")
        .end()
        .end()
        .build();

    FilterPredicate p = ParquetFilterPredicateConverter.toFilterPredicate(sarg);
    String expected =
      "and(and(and(not(eq(x, null)), not(and(lt(y, 20), not(lteq(y, 10))))), not(or(or(eq(z, 1), " +
        "eq(z, 2)), eq(z, 3)))), not(eq(a, Binary{\"stinger\"})))";
    assertEquals(expected, p.toString());
  }

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
    assertEquals("lteq(y, Binary{\"hi        \"})",
        ParquetFilterPredicateConverter.toFilterPredicate(sarg).toString());

    sarg = SearchArgumentFactory.newBuilder()
        .startNot()
        .startOr()
        .isNull("x", PredicateLeaf.Type.INTEGER)
        .between("y", PredicateLeaf.Type.DECIMAL,
            new HiveDecimalWritable("10"), new HiveDecimalWritable("20.0"))
        .in("z", PredicateLeaf.Type.INTEGER, 1, 2, 3)
        .nullSafeEquals("a", PredicateLeaf.Type.STRING,
            new HiveVarchar("stinger", 100).toString())
        .end()
        .end()
        .build();

    FilterPredicate p = ParquetFilterPredicateConverter.toFilterPredicate(sarg);
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
            .lessThan("x", PredicateLeaf.Type.DATE, Date.valueOf("2005-3-12"))
            .lessThanEquals("y", PredicateLeaf.Type.STRING,
                new HiveChar("hi", 10).toString())
            .equals("z", PredicateLeaf.Type.DECIMAL,
                new HiveDecimalWritable("1.0"))
            .end()
            .build();
    assertEquals("lteq(y, Binary{\"hi        \"})",
        ParquetFilterPredicateConverter.toFilterPredicate(sarg).toString());

    sarg = SearchArgumentFactory.newBuilder()
        .startNot()
        .startOr()
        .isNull("x", PredicateLeaf.Type.INTEGER)
        .between("y", PredicateLeaf.Type.DECIMAL, new HiveDecimalWritable("10"),
            new HiveDecimalWritable("20.0"))
        .in("z", PredicateLeaf.Type.INTEGER, 1, 2, 3)
        .nullSafeEquals("a", PredicateLeaf.Type.STRING,
            new HiveVarchar("stinger", 100).toString())
        .end()
        .end()
        .build();

    FilterPredicate p = ParquetFilterPredicateConverter.toFilterPredicate(sarg);
    String expected = "and(and(not(eq(x, null)), not(or(or(eq(z, 1), eq(z, 2)), eq(z, 3)))), " +
        "not(eq(a, Binary{\"stinger\"})))";
    assertEquals(expected, p.toString());
  }

  @Test
  public void testBuilderFloat() throws Exception {
    SearchArgument sarg =
        SearchArgumentFactory.newBuilder()
            .startAnd()
            .lessThan("x", PredicateLeaf.Type.INTEGER, new Integer((short) 22))
            .lessThan("x1", PredicateLeaf.Type.INTEGER, new Integer(22))
            .lessThanEquals("y", PredicateLeaf.Type.STRING,
                new HiveChar("hi", 10).toString())
            .equals("z", PredicateLeaf.Type.FLOAT, new Double(0.22))
            .equals("z1", PredicateLeaf.Type.FLOAT, new Double(0.22))
            .end()
            .build();

    FilterPredicate p = ParquetFilterPredicateConverter.toFilterPredicate(sarg);
    String expected = "and(and(and(and(lt(x, 22), lt(x1, 22))," +
        " lteq(y, Binary{\"hi        \"})), eq(z, " +
        "0.22)), eq(z1, 0.22))";
    assertEquals(expected, p.toString());
  }
}
