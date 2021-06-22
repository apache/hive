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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.FieldDesc;
import org.apache.hadoop.hive.llap.LlapArrowRowInputFormat;
import org.apache.hadoop.hive.llap.Row;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputFormat;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * TestMiniLlapVectorArrowWithLlapIODisabled - turns off llap io while testing LLAP external client flow.
 * The aim of turning off LLAP IO is -
 * when we create table through this test, LLAP caches them and returns the same
 * when we do a read query, due to this we miss some code paths which may have been hit otherwise.
 */
public class TestMiniLlapVectorArrowWithLlapIODisabled extends BaseJdbcWithMiniLlap {

  @BeforeClass
  public static void beforeTest() throws Exception {
    HiveConf conf = defaultConf();
    conf.setBoolVar(ConfVars.LLAP_OUTPUT_FORMAT_ARROW, true);
    conf.setBoolVar(ConfVars.HIVE_VECTORIZATION_FILESINK_ARROW_NATIVE_ENABLED, true);
    conf.set(ConfVars.LLAP_IO_ENABLED.varname, "false");
    BaseJdbcWithMiniLlap.beforeTest(conf);
  }

  @Override
  protected InputFormat<NullWritable, Row> getInputFormat() {
    //For unit testing, no harm in hard-coding allocator ceiling to LONG.MAX_VALUE
    return new LlapArrowRowInputFormat(Long.MAX_VALUE);
  }

  @Test
  public void testNullsInStructFields() throws Exception {
    createDataTypesTable("datatypes");
    RowCollector2 rowCollector = new RowCollector2();
    // c1 int
    // c8 struct<r:string,s:int,t:double>
    // c15 struct<r:int,s:struct<a:int,b:string>>
    // c16 array<struct<m:map<string,string>,n:int>>
    String query = "select c1, c8, c15, c16 from datatypes";
    int rowCount = processQuery(query, 1, rowCollector);
    assertEquals(4, rowCollector.numColumns);
    assertEquals(3, rowCount);

    FieldDesc fieldDesc = rowCollector.schema.getColumns().get(0);
    assertEquals("c1", fieldDesc.getName());
    assertEquals("int", fieldDesc.getTypeInfo().getTypeName());

    fieldDesc = rowCollector.schema.getColumns().get(1);
    assertEquals("c8", fieldDesc.getName());
    assertEquals("struct<r:string,s:int,t:double>", fieldDesc.getTypeInfo().getTypeName());

    fieldDesc = rowCollector.schema.getColumns().get(2);
    assertEquals("c15", fieldDesc.getName());
    assertEquals("struct<r:int,s:struct<a:int,b:string>>", fieldDesc.getTypeInfo().getTypeName());

    fieldDesc = rowCollector.schema.getColumns().get(3);
    assertEquals("c16", fieldDesc.getName());
    assertEquals("array<struct<m:map<string,string>,n:int>>", fieldDesc.getTypeInfo().getTypeName());

    // First row is all nulls
    Object[] rowValues = rowCollector.rows.get(0);
    for (int idx = 0; idx < rowCollector.numColumns; ++idx) {
      assertNull("idx=" + idx, rowValues[idx]);
    }

    // Second Row
    rowValues = rowCollector.rows.get(1);
    assertEquals(-1, rowValues[0]);

    List<?> c8Value = (List<?>) rowValues[1];
    assertNull(c8Value.get(0));
    assertNull(c8Value.get(1));
    assertNull(c8Value.get(2));

    List<?> c15Value = (List<?>) rowValues[2];
    assertNull(c15Value.get(0));
    assertNull(c15Value.get(1));

    List<?> c16Value = (List<?>) rowValues[3];
    assertEquals(0, c16Value.size());

    // Third row
    rowValues = rowCollector.rows.get(2);
    assertEquals(1, rowValues[0]);

    c8Value = (List<?>) rowValues[1];
    assertEquals("a", c8Value.get(0));
    assertEquals(9, c8Value.get(1));
    assertEquals(2.2d, c8Value.get(2));


    c15Value = (List<?>) rowValues[2];
    assertEquals(1, c15Value.get(0));
    List<?> listVal = (List<?>) c15Value.get(1);
    assertEquals(2, listVal.size());
    assertEquals(2, listVal.get(0));
    assertEquals("x", listVal.get(1));

    c16Value = (List<?>) rowValues[3];
    assertEquals(2, c16Value.size());
    listVal = (List<?>) c16Value.get(0);
    assertEquals(2, listVal.size());
    Map<?,?> mapVal = (Map<?,?>) listVal.get(0);
    assertEquals(0, mapVal.size());
    assertEquals(1, listVal.get(1));
    listVal = (List<?>) c16Value.get(1);
    mapVal = (Map<?,?>) listVal.get(0);
    assertEquals(2, mapVal.size());
    assertEquals("b", mapVal.get("a"));
    assertEquals("d", mapVal.get("c"));
    assertEquals(2, listVal.get(1));
  }

  @Override
  @Ignore
  public void testDataTypes() throws Exception {
    // To be implemented
  }

  @Override
  @Ignore
  public void testLlapInputFormatEndToEnd() throws Exception {
    // To be implemented
  }

  @Override
  @Ignore
  public void testMultipleBatchesOfComplexTypes() throws Exception {
    // To be implemented
  }

  @Override
  @Ignore
  public void testLlapInputFormatEndToEndWithMultipleBatches() throws Exception {
    // To be implemented
  }

  @Override
  @Ignore
  public void testInvalidReferenceCountScenario() throws Exception {
    // To be implemented
  }

  @Override
  @Ignore
  public void testNonAsciiStrings() throws Exception {
    // To be implemented
  }

  @Override
  @Ignore
  public void testEscapedStrings() throws Exception {
    // To be implemented
  }


  @Override
  @Ignore
  public void testComplexQuery() throws Exception {
    // To be implemented
  }

  @Override
  @Ignore
  public void testKillQuery() throws Exception {
    // To be implemented
  }
}

