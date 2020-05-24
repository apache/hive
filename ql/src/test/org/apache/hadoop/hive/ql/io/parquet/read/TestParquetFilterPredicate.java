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

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.Test;

import java.sql.Date;
import java.util.HashMap;
import java.util.Map;

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

    Map<String, TypeInfo> columnTypes = new HashMap<>();
    columnTypes.put("a", TypeInfoFactory.getPrimitiveTypeInfo("int"));
    columnTypes.put("y", TypeInfoFactory.getPrimitiveTypeInfo("int"));
    columnTypes.put("z", TypeInfoFactory.getPrimitiveTypeInfo("int"));
    columnTypes.put("stinger", TypeInfoFactory.getPrimitiveTypeInfo("string"));

    FilterPredicate p = ParquetFilterPredicateConverter.toFilterPredicate(sarg, schema, columnTypes);

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

    Map<String, TypeInfo> columnTypes = new HashMap<>();
    columnTypes.put("a", TypeInfoFactory.getPrimitiveTypeInfo("int"));
    columnTypes.put("none", TypeInfoFactory.getPrimitiveTypeInfo("int"));

    FilterPredicate p = ParquetFilterPredicateConverter.toFilterPredicate(sarg, schema, columnTypes);

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

    Map<String, TypeInfo> columnTypes = new HashMap<>();
    columnTypes.put("a", TypeInfoFactory.getPrimitiveTypeInfo("int"));
    columnTypes.put("b", TypeInfoFactory.getPrimitiveTypeInfo("int"));
    columnTypes.put("none", TypeInfoFactory.getPrimitiveTypeInfo("int"));

    FilterPredicate p = ParquetFilterPredicateConverter.toFilterPredicate(sarg, schema, columnTypes);

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

    Map<String, TypeInfo> columnTypes = new HashMap<>();
    columnTypes.put("a", TypeInfoFactory.getPrimitiveTypeInfo("float"));
    columnTypes.put("b", TypeInfoFactory.getPrimitiveTypeInfo("int"));

    FilterPredicate p = ParquetFilterPredicateConverter.toFilterPredicate(sarg, schema, columnTypes);

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
    Map<String, TypeInfo> columnTypes = new HashMap<>();
    columnTypes.put("bCol", TypeInfoFactory.getPrimitiveTypeInfo("int"));
    FilterPredicate p = ParquetFilterPredicateConverter.toFilterPredicate(sarg, schema, columnTypes);
    String expected =
        "and(lteq(bCol, 5), not(lt(bCol, 1)))";
    assertEquals(expected, p.toString());

    sarg = SearchArgumentFactory.newBuilder()
            .between("bCol", PredicateLeaf.Type.LONG, 5L, 1L)
            .build();
    p = ParquetFilterPredicateConverter.toFilterPredicate(sarg, schema, columnTypes);
    expected =
            "and(lteq(bCol, 1), not(lt(bCol, 5)))";
    assertEquals(expected, p.toString());

    sarg = SearchArgumentFactory.newBuilder()
            .between("bCol", PredicateLeaf.Type.LONG, 1L, 1L)
            .build();
    p = ParquetFilterPredicateConverter.toFilterPredicate(sarg, schema, columnTypes);
    expected =
            "and(lteq(bCol, 1), not(lt(bCol, 1)))";
    assertEquals(expected, p.toString());
  }

  @Test
  public void testFilter() throws Exception {
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
    Map<String, TypeInfo> columnTypes = new HashMap<>();
    columnTypes.put("x", TypeInfoFactory.getPrimitiveTypeInfo("int"));
    columnTypes.put("y", TypeInfoFactory.getPrimitiveTypeInfo("int"));
    columnTypes.put("z", TypeInfoFactory.getPrimitiveTypeInfo("int"));
    columnTypes.put("a", TypeInfoFactory.getPrimitiveTypeInfo("string"));
    FilterPredicate p =
        ParquetFilterPredicateConverter.toFilterPredicate(sarg, schema, columnTypes);
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
  public void testFilterComplexTypes() throws Exception {
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
    Map<String, TypeInfo> columnTypes = new HashMap<>();
    columnTypes.put("x", TypeInfoFactory.getPrimitiveTypeInfo("date"));
    columnTypes.put("y", TypeInfoFactory.getCharTypeInfo(10));
    columnTypes.put("z", TypeInfoFactory.getDecimalTypeInfo(4, 2));
    assertEquals(null,
        ParquetFilterPredicateConverter.toFilterPredicate(sarg, schema, columnTypes));

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
    columnTypes = new HashMap<>();
    columnTypes.put("x", TypeInfoFactory.getPrimitiveTypeInfo("int"));
    columnTypes.put("y", TypeInfoFactory.getDecimalTypeInfo(4, 2));
    columnTypes.put("z", TypeInfoFactory.getPrimitiveTypeInfo("int"));
    columnTypes.put("z", TypeInfoFactory.getCharTypeInfo(100));
    assertEquals(null,
        ParquetFilterPredicateConverter.toFilterPredicate(sarg, schema, columnTypes));
  }

  /**
   * Check the converted filter predicate is null if unsupported types are included
   * @throws Exception
   */
  @Test
  public void testFilterComplexTypes2() throws Exception {
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
    Map<String, TypeInfo> columnTypes = new HashMap<>();
    columnTypes.put("x", TypeInfoFactory.getPrimitiveTypeInfo("date"));
    columnTypes.put("y", TypeInfoFactory.getCharTypeInfo(10));
    columnTypes.put("z", TypeInfoFactory.getDecimalTypeInfo(4, 2));
    assertEquals(null,
        ParquetFilterPredicateConverter.toFilterPredicate(sarg, schema, columnTypes));

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
    columnTypes = new HashMap<>();
    columnTypes.put("x", TypeInfoFactory.getPrimitiveTypeInfo("int"));
    columnTypes.put("y", TypeInfoFactory.getDecimalTypeInfo(4, 2));
    columnTypes.put("z", TypeInfoFactory.getPrimitiveTypeInfo("int"));
    columnTypes.put("a", TypeInfoFactory.getCharTypeInfo(100));
    assertEquals(null,
        ParquetFilterPredicateConverter.toFilterPredicate(sarg, schema, columnTypes));
  }

  @Test
  public void testFilterFloatColumn() throws Exception {
    SearchArgument sarg =
        SearchArgumentFactory.newBuilder()
            .startAnd()
            .lessThan("x", PredicateLeaf.Type.LONG, 22L)
            .lessThan("x1", PredicateLeaf.Type.LONG, 22L)
            .lessThanEquals("y", PredicateLeaf.Type.STRING,
                new HiveChar("hi", 10).toString())
            .equals("z", PredicateLeaf.Type.FLOAT, Double.valueOf(0.22))
            .equals("z1", PredicateLeaf.Type.FLOAT, Double.valueOf(0.22))
            .end()
            .build();
    MessageType schema = MessageTypeParser.parseMessageType("message test {" +
        " required int32 x; required int32 x1;" +
        " required binary y; required float z; required float z1;}");
    Map<String, TypeInfo> columnTypes = new HashMap<>();
    columnTypes.put("x", TypeInfoFactory.getPrimitiveTypeInfo("int"));
    columnTypes.put("x1", TypeInfoFactory.getPrimitiveTypeInfo("int"));
    columnTypes.put("y", TypeInfoFactory.getCharTypeInfo(10));
    columnTypes.put("z", TypeInfoFactory.getPrimitiveTypeInfo("float"));
    columnTypes.put("z1", TypeInfoFactory.getPrimitiveTypeInfo("float"));

    FilterPredicate p = ParquetFilterPredicateConverter.toFilterPredicate(sarg, schema, columnTypes);
    String expected = "and(and(and(and(lt(x, 22), lt(x1, 22))," +
        " lteq(y, Binary{\"hi\"})), eq(z, " +
        "0.22)), eq(z1, 0.22))";
    assertEquals(expected, p.toString());
  }

  @Test
  public void testFilterCharColumnLessThan() throws Exception {
    SearchArgument sarg =
        SearchArgumentFactory.newBuilder()
        .lessThan("a", PredicateLeaf.Type.STRING, new HiveChar("apple", 10).toString())
        .build();
    MessageType schema = MessageTypeParser.parseMessageType(
        "message test {required binary a;}");

    Map<String, TypeInfo> columnTypes = new HashMap<>();
    columnTypes.put("a", TypeInfoFactory.getCharTypeInfo(10));

    FilterPredicate p = ParquetFilterPredicateConverter.toFilterPredicate(sarg, schema, columnTypes);
    String expected = "lt(a, Binary{\"apple\"})";
    assertEquals(expected, p.toString());
  }

  @Test
  public void testFilterCharColumnLessThanEquals() throws Exception {
    SearchArgument sarg =
        SearchArgumentFactory.newBuilder()
        .lessThanEquals("a", PredicateLeaf.Type.STRING, new HiveChar("apple", 10).toString())
        .build();
    MessageType schema = MessageTypeParser.parseMessageType(
        "message test {required binary a;}");

    Map<String, TypeInfo> columnTypes = new HashMap<>();
    columnTypes.put("a", TypeInfoFactory.getCharTypeInfo(10));

    FilterPredicate p = ParquetFilterPredicateConverter.toFilterPredicate(sarg, schema, columnTypes);
    String expected = "lteq(a, Binary{\"apple\"})";
    assertEquals(expected, p.toString());
  }

  @Test
  public void testFilterCharColumnEquals() throws Exception {
    SearchArgument sarg =
        SearchArgumentFactory.newBuilder()
        .equals("a", PredicateLeaf.Type.STRING, new HiveChar("apple", 10).toString())
        .build();
    MessageType schema = MessageTypeParser.parseMessageType(
        "message test {required binary a;}");

    Map<String, TypeInfo> columnTypes = new HashMap<>();
    columnTypes.put("a", TypeInfoFactory.getCharTypeInfo(10));

    FilterPredicate p = ParquetFilterPredicateConverter.toFilterPredicate(sarg, schema, columnTypes);
    String expected = "eq(a, Binary{\"apple\"})";
    assertEquals(expected, p.toString());
  }

  @Test
  public void testFilterCharColumnNullSafeEquals() throws Exception {
    SearchArgument sarg =
        SearchArgumentFactory.newBuilder()
        .nullSafeEquals("a", PredicateLeaf.Type.STRING, new HiveChar("apple", 10).toString())
        .build();
    MessageType schema = MessageTypeParser.parseMessageType(
        "message test {required binary a;}");

    Map<String, TypeInfo> columnTypes = new HashMap<>();
    columnTypes.put("a", TypeInfoFactory.getCharTypeInfo(10));

    FilterPredicate p = ParquetFilterPredicateConverter.toFilterPredicate(sarg, schema, columnTypes);
    String expected = "eq(a, Binary{\"apple\"})";
    assertEquals(expected, p.toString());
  }

  @Test
  public void testFilterCharColumnIn() throws Exception {
    SearchArgument sarg =
        SearchArgumentFactory.newBuilder()
        .in("a", PredicateLeaf.Type.STRING, new HiveChar("cherry", 10).toString(), new HiveChar("orange", 10).toString())
        .build();
    MessageType schema = MessageTypeParser.parseMessageType(
        "message test {required binary a;}");

    Map<String, TypeInfo> columnTypes = new HashMap<>();
    columnTypes.put("a", TypeInfoFactory.getCharTypeInfo(10));

    FilterPredicate p = ParquetFilterPredicateConverter.toFilterPredicate(sarg, schema, columnTypes);
    String expected = "or(eq(a, Binary{\"cherry\"}), eq(a, Binary{\"orange\"}))";
    assertEquals(expected, p.toString());
  }

  @Test
  public void testFilterCharColumnBetween() throws Exception {
    SearchArgument sarg =
        SearchArgumentFactory.newBuilder()
        .between("a", PredicateLeaf.Type.STRING, new HiveChar("apple", 10).toString(), new HiveChar("pear", 10).toString())
        .build();
    MessageType schema = MessageTypeParser.parseMessageType(
        "message test {required binary a;}");

    Map<String, TypeInfo> columnTypes = new HashMap<>();
    columnTypes.put("a", TypeInfoFactory.getCharTypeInfo(10));

    FilterPredicate p = ParquetFilterPredicateConverter.toFilterPredicate(sarg, schema, columnTypes);
    String expected = "and(lteq(a, Binary{\"pear\"}), not(lt(a, Binary{\"apple\"})))";
    assertEquals(expected, p.toString());
  }

  @Test
  public void testFilterCharColumnIsNull() throws Exception {
    SearchArgument sarg =
        SearchArgumentFactory.newBuilder()
        .isNull("a", PredicateLeaf.Type.STRING)
        .build();
    MessageType schema = MessageTypeParser.parseMessageType(
        "message test {required binary a;}");

    Map<String, TypeInfo> columnTypes = new HashMap<>();
    columnTypes.put("a", TypeInfoFactory.getCharTypeInfo(10));

    FilterPredicate p = ParquetFilterPredicateConverter.toFilterPredicate(sarg, schema, columnTypes);
    String expected = "eq(a, null)";
    assertEquals(expected, p.toString());
  }

  @Test
  public void testFilterCharColumnGreaterThan() throws Exception {
    SearchArgument sarg =
        SearchArgumentFactory.newBuilder()
        .startNot()
        .lessThanEquals("a", PredicateLeaf.Type.STRING, new HiveChar("apple", 10).toString())
        .end()
        .build();
    MessageType schema = MessageTypeParser.parseMessageType(
        "message test {required binary a;}");

    Map<String, TypeInfo> columnTypes = new HashMap<>();
    columnTypes.put("a", TypeInfoFactory.getCharTypeInfo(10));

    FilterPredicate p = ParquetFilterPredicateConverter.toFilterPredicate(sarg, schema, columnTypes);
    String expected = "not(lteq(a, Binary{\"apple\"}))";
    assertEquals(expected, p.toString());
  }

  @Test
  public void testFilterCharColumnWhiteSpacePrefix() throws Exception {
    SearchArgument sarg =
        SearchArgumentFactory.newBuilder()
        .startAnd()
        .lessThan("a", PredicateLeaf.Type.STRING, new HiveChar("  apple", 10).toString())
        .lessThanEquals("b", PredicateLeaf.Type.STRING, new HiveChar("  pear", 10).toString())
        .equals("c", PredicateLeaf.Type.STRING, new HiveChar("  orange", 10).toString())
        .nullSafeEquals("d", PredicateLeaf.Type.STRING, new HiveChar(" pineapple", 10).toString())
        .end()
        .build();
    MessageType schema = MessageTypeParser.parseMessageType("message test {" +
        " required binary a; required binary b;"
        + " required binary c; required binary d;}");
    Map<String, TypeInfo> columnTypes = new HashMap<>();
    columnTypes.put("a", TypeInfoFactory.getCharTypeInfo(10));
    columnTypes.put("b", TypeInfoFactory.getCharTypeInfo(10));
    columnTypes.put("c", TypeInfoFactory.getCharTypeInfo(10));
    columnTypes.put("d", TypeInfoFactory.getCharTypeInfo(10));

    FilterPredicate p = ParquetFilterPredicateConverter.toFilterPredicate(sarg, schema, columnTypes);
    String expected = "and(and(and("
        + "lt(a, Binary{\"  apple\"}), "
        + "lteq(b, Binary{\"  pear\"})), "
        + "eq(c, Binary{\"  orange\"})), "
        + "eq(d, Binary{\" pineapple\"}))";
    assertEquals(expected, p.toString());
  }

  @Test
  public void testFilterCharColumnWhiteSpacePostfix() throws Exception {
    SearchArgument sarg =
        SearchArgumentFactory.newBuilder()
        .startAnd()
        .lessThan("a", PredicateLeaf.Type.STRING, new HiveChar("apple  ", 10).toString())
        .lessThanEquals("b", PredicateLeaf.Type.STRING, new HiveChar("pear  ", 10).toString())
        .equals("c", PredicateLeaf.Type.STRING, new HiveChar("orange  ", 10).toString())
        .nullSafeEquals("d", PredicateLeaf.Type.STRING, new HiveChar("pineapple ", 10).toString())
        .end()
        .build();
    MessageType schema = MessageTypeParser.parseMessageType("message test {" +
        " required binary a; required binary b;"
        + " required binary c; required binary d;}");
    Map<String, TypeInfo> columnTypes = new HashMap<>();
    columnTypes.put("a", TypeInfoFactory.getCharTypeInfo(10));
    columnTypes.put("b", TypeInfoFactory.getCharTypeInfo(10));
    columnTypes.put("c", TypeInfoFactory.getCharTypeInfo(10));
    columnTypes.put("d", TypeInfoFactory.getCharTypeInfo(10));

    FilterPredicate p = ParquetFilterPredicateConverter.toFilterPredicate(sarg, schema, columnTypes);
    String expected = "and(and(and("
        + "lt(a, Binary{\"apple\"}), "
        + "lteq(b, Binary{\"pear\"})), "
        + "eq(c, Binary{\"orange\"})), "
        + "eq(d, Binary{\"pineapple\"}))";
    assertEquals(expected, p.toString());
  }
  @Test
  public void testFilterMoreComplexCharColumn() throws Exception {
    //((a=pear or a<=cherry) and (b=orange)) and (c=banana or d<cherry)
    SearchArgument sarg =
        SearchArgumentFactory.newBuilder()
        .startAnd()
        .startAnd()
        .startOr()
        .equals("a", PredicateLeaf.Type.STRING, new HiveChar("pear", 10).toString())
        .lessThanEquals("a", PredicateLeaf.Type.STRING, new HiveChar("cherry", 10).toString())
        .end()
        .equals("b", PredicateLeaf.Type.STRING, new HiveChar("orange", 10).toString())
        .end()
        .startOr()
        .equals("c", PredicateLeaf.Type.STRING, new HiveChar("banana", 10).toString())
        .lessThan("d", PredicateLeaf.Type.STRING, new HiveChar("cherry", 10).toString())
        .end()
        .end()
        .build();
    MessageType schema = MessageTypeParser.parseMessageType("message test {" +
        " required binary a; required binary b;"
        + " required binary c; required binary d;}");
    Map<String, TypeInfo> columnTypes = new HashMap<>();
    columnTypes.put("a", TypeInfoFactory.getCharTypeInfo(10));
    columnTypes.put("b", TypeInfoFactory.getCharTypeInfo(10));
    columnTypes.put("c", TypeInfoFactory.getCharTypeInfo(10));
    columnTypes.put("d", TypeInfoFactory.getCharTypeInfo(10));

    FilterPredicate p = ParquetFilterPredicateConverter.toFilterPredicate(sarg, schema, columnTypes);
    String expected = "and(and(or("
        + "eq(a, Binary{\"pear\"}), "
        + "lteq(a, Binary{\"cherry\"})), "
        + "eq(b, Binary{\"orange\"})), "
        + "or(eq(c, Binary{\"banana\"}), lt(d, Binary{\"cherry\"})))";
    assertEquals(expected, p.toString());
  }

  @Test
  public void testFilterVarCharColumn() throws Exception {
    SearchArgument sarg =
        SearchArgumentFactory.newBuilder()
        .startAnd()
        .lessThan("a", PredicateLeaf.Type.STRING, new HiveVarchar("apple", 10).toString())
        .lessThanEquals("b", PredicateLeaf.Type.STRING, new HiveVarchar("pear", 10).toString())
        .equals("c", PredicateLeaf.Type.STRING, new HiveVarchar("orange", 10).toString())
        .nullSafeEquals("d", PredicateLeaf.Type.STRING, new HiveVarchar("pineapple", 9).toString())
        .in("e", PredicateLeaf.Type.STRING, new HiveVarchar("cherry", 10).toString(), new HiveVarchar("orange", 10).toString())
        .between("f", PredicateLeaf.Type.STRING, new HiveVarchar("apple", 10).toString(), new HiveVarchar("pear", 10).toString())
        .isNull("g", PredicateLeaf.Type.STRING)
        .end()
        .build();
    MessageType schema = MessageTypeParser.parseMessageType("message test {" +
        " required binary a; required binary b;"
        + " required binary c; required binary d;"
        + " required binary e; required binary f;"
        + " required binary g;}");

    Map<String, TypeInfo> columnTypes = new HashMap<>();
    columnTypes.put("a", TypeInfoFactory.getVarcharTypeInfo(10));
    columnTypes.put("b", TypeInfoFactory.getVarcharTypeInfo(10));
    columnTypes.put("c", TypeInfoFactory.getVarcharTypeInfo(10));
    columnTypes.put("d", TypeInfoFactory.getVarcharTypeInfo(10));
    columnTypes.put("e", TypeInfoFactory.getVarcharTypeInfo(10));
    columnTypes.put("f", TypeInfoFactory.getVarcharTypeInfo(10));
    columnTypes.put("g", TypeInfoFactory.getVarcharTypeInfo(10));

    FilterPredicate p = ParquetFilterPredicateConverter.toFilterPredicate(sarg, schema, columnTypes);
    String expected = "and(and(and(and(and(and("
        + "lt(a, Binary{\"apple\"}), "
        + "lteq(b, Binary{\"pear\"})), "
        + "eq(c, Binary{\"orange\"})), "
        + "eq(d, Binary{\"pineapple\"})), "
        + "or(eq(e, Binary{\"cherry\"}), eq(e, Binary{\"orange\"}))), "
        + "and(lteq(f, Binary{\"pear\"}), not(lt(f, Binary{\"apple\"})))), "
        + "eq(g, null))";
    assertEquals(expected, p.toString());
  }

  @Test
  public void testFilterVarCharColumnWithWhiteSpaces() throws Exception {
    SearchArgument sarg =
        SearchArgumentFactory.newBuilder()
        .startAnd()
        .lessThan("a", PredicateLeaf.Type.STRING, new HiveVarchar(" apple  ", 10).toString())
        .lessThanEquals("b", PredicateLeaf.Type.STRING, new HiveVarchar(" pear", 10).toString())
        .equals("c", PredicateLeaf.Type.STRING, new HiveVarchar("orange ", 10).toString())
        .end()
        .build();
    MessageType schema = MessageTypeParser.parseMessageType("message test {" +
        " required binary a; required binary b;"
        + " required binary c;}");
    Map<String, TypeInfo> columnTypes = new HashMap<>();
    columnTypes.put("a", TypeInfoFactory.getVarcharTypeInfo(10));
    columnTypes.put("b", TypeInfoFactory.getVarcharTypeInfo(10));
    columnTypes.put("c", TypeInfoFactory.getVarcharTypeInfo(10));

    FilterPredicate p = ParquetFilterPredicateConverter.toFilterPredicate(sarg, schema, columnTypes);
    String expected = "and(and("
        + "lt(a, Binary{\" apple  \"}), "
        + "lteq(b, Binary{\" pear\"})), "
        + "eq(c, Binary{\"orange \"}))";
    assertEquals(expected, p.toString());
  }

  @Test
  public void testFilterStringColumnWithWhiteSpaces() throws Exception {
    SearchArgument sarg =
        SearchArgumentFactory.newBuilder()
        .startAnd()
        .lessThan("a", PredicateLeaf.Type.STRING, new HiveVarchar(" apple  ", 10).toString())
        .lessThanEquals("b", PredicateLeaf.Type.STRING, new HiveVarchar(" pear", 10).toString())
        .equals("c", PredicateLeaf.Type.STRING, new HiveVarchar("orange ", 10).toString())
        .end()
        .build();
    MessageType schema = MessageTypeParser.parseMessageType("message test {" +
        " required binary a; required binary b;"
        + " required binary c;}");
    Map<String, TypeInfo> columnTypes = new HashMap<>();
    columnTypes.put("a", TypeInfoFactory.getPrimitiveTypeInfo("string"));
    columnTypes.put("b", TypeInfoFactory.getPrimitiveTypeInfo("string"));
    columnTypes.put("c", TypeInfoFactory.getPrimitiveTypeInfo("string"));

    FilterPredicate p = ParquetFilterPredicateConverter.toFilterPredicate(sarg, schema, columnTypes);
    String expected = "and(and("
        + "lt(a, Binary{\" apple  \"}), "
        + "lteq(b, Binary{\" pear\"})), "
        + "eq(c, Binary{\"orange \"}))";
    assertEquals(expected, p.toString());
  }

  @Test
  public void testFilterMoreComplexVarCharColumn() throws Exception {
    //((a=pear or a<=cherry) and (b=orange)) and (c=banana or d<cherry)
    SearchArgument sarg =
        SearchArgumentFactory.newBuilder()
        .startAnd()
        .startAnd()
        .startOr()
        .equals("a", PredicateLeaf.Type.STRING, new HiveVarchar("pear", 10).toString())
        .lessThanEquals("a", PredicateLeaf.Type.STRING, new HiveVarchar("cherry", 10).toString())
        .end()
        .equals("b", PredicateLeaf.Type.STRING, new HiveVarchar("orange", 10).toString())
        .end()
        .startOr()
        .equals("c", PredicateLeaf.Type.STRING, new HiveVarchar("banana", 10).toString())
        .lessThan("d", PredicateLeaf.Type.STRING, new HiveVarchar("cherry", 10).toString())
        .end()
        .end()
        .build();
    MessageType schema = MessageTypeParser.parseMessageType("message test {" +
        " required binary a; required binary b;"
        + " required binary c; required binary d;}");

    Map<String, TypeInfo> columnTypes = new HashMap<>();
    columnTypes.put("a", TypeInfoFactory.getVarcharTypeInfo(10));
    columnTypes.put("b", TypeInfoFactory.getVarcharTypeInfo(10));
    columnTypes.put("c", TypeInfoFactory.getVarcharTypeInfo(10));
    columnTypes.put("d", TypeInfoFactory.getVarcharTypeInfo(10));

    FilterPredicate p = ParquetFilterPredicateConverter.toFilterPredicate(sarg, schema, columnTypes);
    String expected = "and(and(or(eq(a, Binary{\"pear\"}), lteq(a, Binary{\"cherry\"})), "
        + "eq(b, Binary{\"orange\"})), "
        + "or(eq(c, Binary{\"banana\"}), lt(d, Binary{\"cherry\"})))";
    assertEquals(expected, p.toString());
  }
}
