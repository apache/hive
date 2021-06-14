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
import org.apache.hadoop.hive.llap.LlapArrowRowInputFormat;
import org.apache.hadoop.hive.llap.Row;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputFormat;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

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
    // c8 struct<r:string,s:int,t:double>
    String query = "select c8 from datatypes";
    int rowCount = processQuery(query, 1, rowCollector);
    assertEquals(3, rowCount);
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
  public void testDataTypes() throws Exception {
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

