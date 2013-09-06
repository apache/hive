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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hive.hcatalog.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hive.hcatalog.common.ErrorType;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchemaUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestHCatNonPartitioned extends HCatMapReduceTest {

  private static List<HCatRecord> writeRecords;
  static List<HCatFieldSchema> partitionColumns;

  @BeforeClass
  public static void oneTimeSetUp() throws Exception {

    dbName = null; //test if null dbName works ("default" is used)
    tableName = "testHCatNonPartitionedTable";

    writeRecords = new ArrayList<HCatRecord>();

    for (int i = 0; i < 20; i++) {
      List<Object> objList = new ArrayList<Object>();

      objList.add(i);
      objList.add("strvalue" + i);
      writeRecords.add(new DefaultHCatRecord(objList));
    }

    partitionColumns = new ArrayList<HCatFieldSchema>();
    partitionColumns.add(HCatSchemaUtils.getHCatFieldSchema(new FieldSchema("c1", serdeConstants.INT_TYPE_NAME, "")));
    partitionColumns.add(HCatSchemaUtils.getHCatFieldSchema(new FieldSchema("c2", serdeConstants.STRING_TYPE_NAME, "")));
  }

  @Override
  protected List<FieldSchema> getPartitionKeys() {
    List<FieldSchema> fields = new ArrayList<FieldSchema>();
    //empty list, non partitioned
    return fields;
  }

  @Override
  protected List<FieldSchema> getTableColumns() {
    List<FieldSchema> fields = new ArrayList<FieldSchema>();
    fields.add(new FieldSchema("c1", serdeConstants.INT_TYPE_NAME, ""));
    fields.add(new FieldSchema("c2", serdeConstants.STRING_TYPE_NAME, ""));
    return fields;
  }


  @Test
  public void testHCatNonPartitionedTable() throws Exception {

    Map<String, String> partitionMap = new HashMap<String, String>();
    runMRCreate(null, partitionColumns, writeRecords, 10, true);

    //Test for duplicate publish
    IOException exc = null;
    try {
      runMRCreate(null, partitionColumns, writeRecords, 20, true);
    } catch (IOException e) {
      exc = e;
    }

    assertTrue(exc != null);
    assertTrue(exc instanceof HCatException);
    assertEquals(ErrorType.ERROR_NON_EMPTY_TABLE, ((HCatException) exc).getErrorType());

    //Test for publish with invalid partition key name
    exc = null;
    partitionMap.clear();
    partitionMap.put("px", "p1value2");

    try {
      runMRCreate(partitionMap, partitionColumns, writeRecords, 20, true);
    } catch (IOException e) {
      exc = e;
    }

    assertTrue(exc != null);
    assertTrue(exc instanceof HCatException);
    assertEquals(ErrorType.ERROR_INVALID_PARTITION_VALUES, ((HCatException) exc).getErrorType());

    //Read should get 10 rows
    runMRRead(10);

    hiveReadTest();
  }

  //Test that data inserted through hcatoutputformat is readable from hive
  private void hiveReadTest() throws Exception {

    String query = "select * from " + tableName;
    int retCode = driver.run(query).getResponseCode();

    if (retCode != 0) {
      throw new Exception("Error " + retCode + " running query " + query);
    }

    ArrayList<String> res = new ArrayList<String>();
    driver.getResults(res);
    assertEquals(10, res.size());
  }
}
