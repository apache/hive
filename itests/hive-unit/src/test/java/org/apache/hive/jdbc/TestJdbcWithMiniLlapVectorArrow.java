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

package org.apache.hive.jdbc;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;

import java.math.BigDecimal;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.Timestamp;

import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.llap.FieldDesc;
import org.apache.hadoop.hive.llap.Row;
import org.apache.hadoop.io.NullWritable;
import org.junit.BeforeClass;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;

import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.hive.llap.LlapArrowRowInputFormat;
import org.junit.Ignore;
import org.junit.Test;

/**
 * TestJdbcWithMiniLlap for Arrow format with vectorized output sink
 */
@Ignore("unstable HIVE-23549")
public class TestJdbcWithMiniLlapVectorArrow extends BaseJdbcWithMiniLlap {


  @BeforeClass
  public static void beforeTest() throws Exception {
    HiveConf conf = defaultConf();
    conf.setBoolVar(ConfVars.LLAP_OUTPUT_FORMAT_ARROW, true);
    conf.setBoolVar(ConfVars.HIVE_VECTORIZATION_FILESINK_ARROW_NATIVE_ENABLED, true);
    BaseJdbcWithMiniLlap.beforeTest(conf);
  }

  @Override
  protected InputFormat<NullWritable, Row> getInputFormat() {
    //For unit testing, no harm in hard-coding allocator ceiling to LONG.MAX_VALUE
    return new LlapArrowRowInputFormat(Long.MAX_VALUE);
  }

  @Override
  public void testDataTypes() throws Exception {
    createDataTypesTable("datatypes");
    RowCollector2 rowCollector = new RowCollector2();
    String query = "select * from datatypes";
    int rowCount = processQuery(query, 1, rowCollector);
    assertEquals(3, rowCount);

    // Verify schema
    String[][] colNameTypes = new String[][] {
        {"datatypes.c1", "int"},
        {"datatypes.c2", "boolean"},
        {"datatypes.c3", "double"},
        {"datatypes.c4", "string"},
        {"datatypes.c5", "array<int>"},
        {"datatypes.c6", "map<int,string>"},
        {"datatypes.c7", "map<string,string>"},
        {"datatypes.c8", "struct<r:string,s:int,t:double>"},
        {"datatypes.c9", "tinyint"},
        {"datatypes.c10", "smallint"},
        {"datatypes.c11", "float"},
        {"datatypes.c12", "bigint"},
        {"datatypes.c13", "array<array<string>>"},
        {"datatypes.c14", "map<int,map<int,int>>"},
        {"datatypes.c15", "struct<r:int,s:struct<a:int,b:string>>"},
        {"datatypes.c16", "array<struct<m:map<string,string>,n:int>>"},
        {"datatypes.c17", "timestamp"},
        {"datatypes.c18", "decimal(16,7)"},
        {"datatypes.c19", "binary"},
        {"datatypes.c20", "date"},
        {"datatypes.c21", "varchar(20)"},
        {"datatypes.c22", "char(15)"},
        {"datatypes.c23", "binary"},
    };
    FieldDesc fieldDesc;
    assertEquals(23, rowCollector.numColumns);
    for (int idx = 0; idx < rowCollector.numColumns; ++idx) {
      fieldDesc = rowCollector.schema.getColumns().get(idx);
      assertEquals("ColName idx=" + idx, colNameTypes[idx][0], fieldDesc.getName());
      assertEquals("ColType idx=" + idx, colNameTypes[idx][1], fieldDesc.getTypeInfo().getTypeName());
    }

    // First row is all nulls
    Object[] rowValues = rowCollector.rows.get(0);
    for (int idx = 0; idx < rowCollector.numColumns; ++idx) {
      assertEquals("idx=" + idx, null, rowValues[idx]);
    }

    // Second Row
    rowValues = rowCollector.rows.get(1);
    assertEquals(Integer.valueOf(-1), rowValues[0]);
    assertEquals(Boolean.FALSE, rowValues[1]);
    assertEquals(Double.valueOf(-1.1d), rowValues[2]);
    assertEquals("", rowValues[3]);

    List<?> c5Value = (List<?>) rowValues[4];
    assertEquals(0, c5Value.size());

    Map<?,?> c6Value = (Map<?,?>) rowValues[5];
    assertEquals(1, c6Value.size());
    assertEquals(null, c6Value.get(1));

    Map<?,?> c7Value = (Map<?,?>) rowValues[6];
    assertEquals(1, c7Value.size());
    assertEquals("b", c7Value.get("a"));

    List<?> c8Value = (List<?>) rowValues[7];
    assertEquals(null, c8Value.get(0));
    assertEquals(null, c8Value.get(1));
    assertEquals(null, c8Value.get(2));

    assertEquals(Byte.valueOf((byte) -1), rowValues[8]);
    assertEquals(Short.valueOf((short) -1), rowValues[9]);
    assertEquals(Float.valueOf(-1.0f), rowValues[10]);
    assertEquals(Long.valueOf(-1l), rowValues[11]);

    List<?> c13Value = (List<?>) rowValues[12];
    assertEquals(0, c13Value.size());

    Map<?,?> c14Value = (Map<?,?>) rowValues[13];
    assertEquals(1, c14Value.size());
    Map<?,?> mapVal = (Map<?,?>) c14Value.get(Integer.valueOf(1));
    assertEquals(1, mapVal.size());
    assertEquals(100, mapVal.get(Integer.valueOf(10)));

    List<?> c15Value = (List<?>) rowValues[14];
    assertEquals(null, c15Value.get(0));
    assertEquals(null, c15Value.get(1));

    List<?> c16Value = (List<?>) rowValues[15];
    assertEquals(0, c16Value.size());

    assertEquals(null, rowValues[16]);
    assertEquals(null, rowValues[17]);
    assertEquals(null, rowValues[18]);
    assertEquals(null, rowValues[19]);
    assertEquals(null, rowValues[20]);
    assertEquals(null, rowValues[21]);
    assertEquals(null, rowValues[22]);

    // Third row
    rowValues = rowCollector.rows.get(2);
    assertEquals(Integer.valueOf(1), rowValues[0]);
    assertEquals(Boolean.TRUE, rowValues[1]);
    assertEquals(Double.valueOf(1.1d), rowValues[2]);
    assertEquals("1", rowValues[3]);

    c5Value = (List<?>) rowValues[4];
    assertEquals(2, c5Value.size());
    assertEquals(Integer.valueOf(1), c5Value.get(0));
    assertEquals(Integer.valueOf(2), c5Value.get(1));

    c6Value = (Map<?,?>) rowValues[5];
    assertEquals(2, c6Value.size());
    assertEquals("x", c6Value.get(Integer.valueOf(1)));
    assertEquals("y", c6Value.get(Integer.valueOf(2)));

    c7Value = (Map<?,?>) rowValues[6];
    assertEquals(2, c7Value.size());
    assertEquals("v", c7Value.get("k"));
    assertEquals("c", c7Value.get("b"));

    c8Value = (List<?>) rowValues[7];
    assertEquals("a", c8Value.get(0));
    assertEquals(Integer.valueOf(9), c8Value.get(1));
    assertEquals(Double.valueOf(2.2d), c8Value.get(2));

    assertEquals(Byte.valueOf((byte) 1), rowValues[8]);
    assertEquals(Short.valueOf((short) 1), rowValues[9]);
    assertEquals(Float.valueOf(1.0f), rowValues[10]);
    assertEquals(Long.valueOf(1l), rowValues[11]);

    c13Value = (List<?>) rowValues[12];
    assertEquals(2, c13Value.size());
    List<?> listVal = (List<?>) c13Value.get(0);
    assertEquals("a", listVal.get(0));
    assertEquals("b", listVal.get(1));
    listVal = (List<?>) c13Value.get(1);
    assertEquals("c", listVal.get(0));
    assertEquals("d", listVal.get(1));

    c14Value = (Map<?,?>) rowValues[13];
    assertEquals(2, c14Value.size());
    mapVal = (Map<?,?>) c14Value.get(Integer.valueOf(1));
    assertEquals(2, mapVal.size());
    assertEquals(Integer.valueOf(12), mapVal.get(Integer.valueOf(11)));
    assertEquals(Integer.valueOf(14), mapVal.get(Integer.valueOf(13)));
    mapVal = (Map<?,?>) c14Value.get(Integer.valueOf(2));
    assertEquals(1, mapVal.size());
    assertEquals(Integer.valueOf(22), mapVal.get(Integer.valueOf(21)));

    c15Value = (List<?>) rowValues[14];
    assertEquals(Integer.valueOf(1), c15Value.get(0));
    listVal = (List<?>) c15Value.get(1);
    assertEquals(2, listVal.size());
    assertEquals(Integer.valueOf(2), listVal.get(0));
    assertEquals("x", listVal.get(1));

    c16Value = (List<?>) rowValues[15];
    assertEquals(2, c16Value.size());
    listVal = (List<?>) c16Value.get(0);
    assertEquals(2, listVal.size());
    mapVal = (Map<?,?>) listVal.get(0);
    assertEquals(0, mapVal.size());
    assertEquals(Integer.valueOf(1), listVal.get(1));
    listVal = (List<?>) c16Value.get(1);
    mapVal = (Map<?,?>) listVal.get(0);
    assertEquals(2, mapVal.size());
    assertEquals("b", mapVal.get("a"));
    assertEquals("d", mapVal.get("c"));
    assertEquals(Integer.valueOf(2), listVal.get(1));

    assertEquals(Timestamp.valueOf("2012-04-22 09:00:00.123456"), rowValues[16]);
    assertEquals(new BigDecimal("123456789.123456"), rowValues[17]);
    assertArrayEquals("abcd".getBytes("UTF-8"), (byte[]) rowValues[18]);
    assertEquals(Date.valueOf("2013-01-01"), rowValues[19]);
    assertEquals("abc123", rowValues[20]);
    assertEquals("abc123         ", rowValues[21]);

    assertArrayEquals("X'01FF'".getBytes("UTF-8"), (byte[]) rowValues[22]);
  }


  // ToDo: Fix me
  @Ignore
  @Test
  public void testTypesNestedInListWithLimitAndFilters() throws Exception {
    try (Statement statement = hs2Conn.createStatement()) {
      statement.execute("CREATE TABLE complex_tbl(c1 array<string>, " +
          "c2 array<struct<f1:string,f2:string>>, " +
          "c3 array<array<struct<f1:string,f2:string>>>, " +
          "c4 int) STORED AS ORC");

      statement.executeUpdate("INSERT INTO complex_tbl VALUES " +
          "(" +
          "ARRAY('a1', 'a2', 'a3', null), " +
          "ARRAY(NAMED_STRUCT('f1','a1', 'f2','a2'), NAMED_STRUCT('f1','a3', 'f2','a4')), " +
          "ARRAY((ARRAY(NAMED_STRUCT('f1','a1', 'f2','a2'), NAMED_STRUCT('f1','a3', 'f2','a4')))), " +
          "1),      " +
          "(" +
          "ARRAY('b1'), " +
          "ARRAY(NAMED_STRUCT('f1','b1', 'f2','b2'), NAMED_STRUCT('f1','b3', 'f2','b4')), " +
          "ARRAY((ARRAY(NAMED_STRUCT('f1','b1', 'f2','b2'), NAMED_STRUCT('f1','b3', 'f2','b4'))), " +
          "(ARRAY(NAMED_STRUCT('f1','b5', 'f2','b6'), NAMED_STRUCT('f1','b7', 'f2','b8')))), " +
          "2), " +
          "(" +
          "ARRAY('c1', 'c2'), ARRAY(NAMED_STRUCT('f1','c1', 'f2','c2'), NAMED_STRUCT('f1','c3', 'f2','c4'), " +
          "NAMED_STRUCT('f1','c5', 'f2','c6')), ARRAY((ARRAY(NAMED_STRUCT('f1','c1', 'f2','c2'), " +
          "NAMED_STRUCT('f1','c3', 'f2','c4'))), (ARRAY(NAMED_STRUCT('f1','c5', 'f2','c6'), " +
          "NAMED_STRUCT('f1','c7', 'f2','c8'))), (ARRAY(NAMED_STRUCT('f1','c9', 'f2','c10'), " +
          "NAMED_STRUCT('f1','c11', 'f2','c12')))), " +
          "3), " +
          "(" +
          "ARRAY(null), " +
          "ARRAY(NAMED_STRUCT('f1','d1', 'f2','d2'), NAMED_STRUCT('f1','d3', 'f2','d4'), " +
          "NAMED_STRUCT('f1','d5', 'f2','d6'), NAMED_STRUCT('f1','d7', 'f2','d8')), " +
          "ARRAY((ARRAY(NAMED_STRUCT('f1','d1', 'f2', 'd2')))), " +
          "4)");

    }

    List<Object[]> expected = new ArrayList<>();
    expected.add(new Object[]{
        asList("a1", "a2", "a3", null),
        asList(asList("a1", "a2"), asList("a3", "a4")),
        asList(asList(asList("a1", "a2"), asList("a3", "a4"))),
        1
    });
    expected.add(new Object[]{
        asList("b1"),
        asList(asList("b1", "b2"), asList("b3", "b4")),
        asList(asList(asList("b1", "b2"), asList("b3", "b4")), asList(asList("b5", "b6"), asList("b7", "b8"))),
        2
    });
    expected.add(new Object[]{
        asList("c1", "c2"),
        asList(asList("c1", "c2"), asList("c3", "c4"), asList("c5", "c6")),
        asList(asList(asList("c1", "c2"), asList("c3", "c4")), asList(asList("c5", "c6"), asList("c7", "c8")),
            asList(asList("c9", "c10"), asList("c11", "c12"))),
        3
    });
    List<String> nullList = new ArrayList<>();
    nullList.add(null);
    expected.add(new Object[]{
        nullList,
        asList(asList("d1", "d2"), asList("d3", "d4"), asList("d5", "d6"), asList("d7", "d8")),
        asList(asList(asList("d1", "d2"))),
        4
    });

    // test without limit and filters (i.e VectorizedRowBatch#selectedInUse=false)
    RowCollector2 rowCollector = new RowCollector2();
    String query = "select * from complex_tbl";
    processQuery(query, 1, rowCollector);
    verifyResult(rowCollector.rows, expected.get(0),
        expected.get(1),
        expected.get(2),
        expected.get(3));

    // test with filter
    rowCollector = new RowCollector2();
    query = "select * from complex_tbl where c4 > 1 ";
    processQuery(query, 1, rowCollector);
    verifyResult(rowCollector.rows, expected.get(1), expected.get(2), expected.get(3));

    // test with limit
    rowCollector = new RowCollector2();
    query = "select * from complex_tbl limit 3";
    processQuery(query, 1, rowCollector);
    verifyResult(rowCollector.rows, expected.get(0), expected.get(1), expected.get(2));

    // test with filters and limit
    rowCollector = new RowCollector2();
    query = "select * from complex_tbl where c4 > 1 limit 2";
    processQuery(query, 1, rowCollector);
    verifyResult(rowCollector.rows, expected.get(1), expected.get(2));

  }

  // ToDo: Fix me
  @Ignore
  @Test
  public void testTypesNestedInMapWithLimitAndFilters() throws Exception {
    try (Statement statement = hs2Conn.createStatement()) {
      statement.execute("CREATE TABLE complex_tbl2(c1 map<int, string>," +
          " c2 map<int, array<string>>, " +
          " c3 map<int, struct<f1:string,f2:string>>, c4 int) STORED AS ORC");

      statement.executeUpdate("INSERT INTO complex_tbl2 VALUES " +
          "(MAP(1, 'a1'), MAP(1, ARRAY('a1', 'a2')), MAP(1, NAMED_STRUCT('f1','a1', 'f2','a2')), " +
          "1), " +
          "(MAP(1, 'b1',2, 'b2'), MAP(1, ARRAY('b1', 'b2'), 2, ARRAY('b3') ), " +
          "MAP(1, NAMED_STRUCT('f1','b1', 'f2','b2')), " +
          "2), " +
          "(MAP(1, 'c1',2, 'c2'), MAP(1, ARRAY('c1', 'c2'), 2, ARRAY('c3') ), " +
          "MAP(1, NAMED_STRUCT('f1','c1', 'f2','c2'), 2, NAMED_STRUCT('f1', 'c3', 'f2', 'c4') ), " +
          "3)");

    }

    List<Object[]> expected = new ArrayList<>();
    expected.add(new Object[]{
        ImmutableMap.of(1, "a1"),
        ImmutableMap.of(1, asList("a1", "a2")),
        ImmutableMap.of(1, asList("a1", "a2")),
        1,
    });
    expected.add(new Object[]{
        ImmutableMap.of(1, "b1", 2, "b2"),
        ImmutableMap.of(1, asList("b1", "b2"), 2, asList("b3")),
        ImmutableMap.of(1, asList("b1", "b2")),
        2,
    });
    expected.add(new Object[]{
        ImmutableMap.of(1, "c1", 2, "c2"),
        ImmutableMap.of(1, asList("c1", "c2"), 2, asList("c3")),
        ImmutableMap.of(1, asList("c1", "c2"), 2, asList("c3", "c4")),
        3,
    });


    // test without limit and filters (i.e. VectorizedRowBatch#selectedInUse=false)
    RowCollector2 rowCollector = new RowCollector2();
    String query = "select * from complex_tbl2";
    processQuery(query, 1, rowCollector);
    verifyResult(rowCollector.rows, expected.get(0), expected.get(1), expected.get(2));

    // test with filter
    rowCollector = new RowCollector2();
    query = "select * from complex_tbl2 where c4 > 1 ";
    processQuery(query, 1, rowCollector);
    verifyResult(rowCollector.rows, expected.get(1), expected.get(2));

    // test with limit
    rowCollector = new RowCollector2();
    query = "select * from complex_tbl2 limit 2";
    processQuery(query, 1, rowCollector);
    verifyResult(rowCollector.rows, expected.get(0), expected.get(1));

    // test with filters and limit
    rowCollector = new RowCollector2();
    query = "select * from complex_tbl2 where c4 > 1 limit 1";
    processQuery(query, 1, rowCollector);
    verifyResult(rowCollector.rows, expected.get(1));

  }

  private void verifyResult(List<Object[]> actual, Object[]... expected) {
    assertEquals(expected.length, actual.size());
    for (int i = 0; i < expected.length; i++) {
      assertArrayEquals(expected[i], actual.get(i));
    }
  }

  @Override
  @Ignore
  public void testMultipleBatchesOfComplexTypes() {
    // ToDo: FixMe
  }

  @Override
  @Ignore
  public void testComplexQuery() {
    // ToDo: FixMe
  }

  @Override
  @Ignore
  public void testLlapInputFormatEndToEnd() {
    // ToDo: FixMe
  }
}

