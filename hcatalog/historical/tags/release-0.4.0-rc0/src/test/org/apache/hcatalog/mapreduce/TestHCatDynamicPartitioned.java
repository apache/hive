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

package org.apache.hcatalog.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hcatalog.common.ErrorType;
import org.apache.hcatalog.common.HCatConstants;
import org.apache.hcatalog.common.HCatException;
import org.apache.hcatalog.data.DefaultHCatRecord;
import org.apache.hcatalog.data.HCatRecord;
import org.apache.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hcatalog.data.schema.HCatSchemaUtils;

public class TestHCatDynamicPartitioned extends HCatMapReduceTest {

  private List<HCatRecord> writeRecords;
  private List<HCatFieldSchema> dataColumns;

  @Override
  protected void initialize() throws Exception {

    tableName = "testHCatDynamicPartitionedTable";
    generateWriteRecords(20,5,0);
    generateDataColumns();
  }

  private void generateDataColumns() throws HCatException {
    dataColumns = new ArrayList<HCatFieldSchema>();
    dataColumns.add(HCatSchemaUtils.getHCatFieldSchema(new FieldSchema("c1", Constants.INT_TYPE_NAME, "")));
    dataColumns.add(HCatSchemaUtils.getHCatFieldSchema(new FieldSchema("c2", Constants.STRING_TYPE_NAME, "")));
    dataColumns.add(HCatSchemaUtils.getHCatFieldSchema(new FieldSchema("p1", Constants.STRING_TYPE_NAME, "")));
  }

  private void generateWriteRecords(int max, int mod,int offset) {
    writeRecords = new ArrayList<HCatRecord>();

    for(int i = 0;i < max;i++) {
      List<Object> objList = new ArrayList<Object>();

      objList.add(i);
      objList.add("strvalue" + i);
      objList.add(String.valueOf((i % mod)+offset));
      writeRecords.add(new DefaultHCatRecord(objList));
    }
  }

  @Override
  protected List<FieldSchema> getPartitionKeys() {
    List<FieldSchema> fields = new ArrayList<FieldSchema>();
    fields.add(new FieldSchema("p1", Constants.STRING_TYPE_NAME, ""));
    return fields;
  }

  @Override
  protected List<FieldSchema> getTableColumns() {
    List<FieldSchema> fields = new ArrayList<FieldSchema>();
    fields.add(new FieldSchema("c1", Constants.INT_TYPE_NAME, ""));
    fields.add(new FieldSchema("c2", Constants.STRING_TYPE_NAME, ""));
    return fields;
  }


  public void testHCatDynamicPartitionedTable() throws Exception {

    generateWriteRecords(20,5,0);
    runMRCreate(null, dataColumns, writeRecords, 20,true);

    runMRRead(20);

    //Read with partition filter
    runMRRead(4, "p1 = \"0\"");
    runMRRead(8, "p1 = \"1\" or p1 = \"3\"");
    runMRRead(4, "p1 = \"4\"");

    // read from hive to test
    
    String query = "select * from " + tableName;
    int retCode = driver.run(query).getResponseCode();

    if( retCode != 0 ) {
      throw new Exception("Error " + retCode + " running query " + query);
    }

    ArrayList<String> res = new ArrayList<String>();
    driver.getResults(res);
    assertEquals(20, res.size());

    
    //Test for duplicate publish
    IOException exc = null;
    try {
      generateWriteRecords(20,5,0);
      runMRCreate(null, dataColumns, writeRecords, 20,false);
    } catch(IOException e) {
      exc = e;
    }

    assertTrue(exc != null);
    assertTrue(exc instanceof HCatException);
    assertTrue( "Got exception of type ["+((HCatException) exc).getErrorType().toString()
        + "] Expected ERROR_PUBLISHING_PARTITION or ERROR_MOVE_FAILED",
        (ErrorType.ERROR_PUBLISHING_PARTITION == ((HCatException) exc).getErrorType()) 
        || (ErrorType.ERROR_MOVE_FAILED == ((HCatException) exc).getErrorType()) 
        );
  }

//TODO 1.0 miniCluster is slow this test times out, make it work
// renaming test to make test framework skip it
  public void _testHCatDynamicPartitionMaxPartitions() throws Exception {
    HiveConf hc = new HiveConf(this.getClass());

    int maxParts = hiveConf.getIntVar(HiveConf.ConfVars.DYNAMICPARTITIONMAXPARTS);
    System.out.println("Max partitions allowed = " + maxParts);

    IOException exc = null;
    try {
      generateWriteRecords(maxParts+5,maxParts+2,10);
      runMRCreate(null,dataColumns,writeRecords,maxParts+5,false);
    } catch(IOException e) {
      exc = e;
    }

    if (HCatConstants.HCAT_IS_DYNAMIC_MAX_PTN_CHECK_ENABLED){
      assertTrue(exc != null);
      assertTrue(exc instanceof HCatException);
      assertEquals(ErrorType.ERROR_TOO_MANY_DYNAMIC_PTNS, ((HCatException) exc).getErrorType());
    }else{
      assertTrue(exc == null);
      runMRRead(maxParts+5);
    }
  }
}
