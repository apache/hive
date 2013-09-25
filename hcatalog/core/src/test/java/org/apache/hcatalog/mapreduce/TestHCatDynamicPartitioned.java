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

package org.apache.hcatalog.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hcatalog.common.HCatUtil;
import org.apache.hcatalog.common.ErrorType;
import org.apache.hcatalog.common.HCatConstants;
import org.apache.hcatalog.common.HCatException;
import org.apache.hcatalog.data.DefaultHCatRecord;
import org.apache.hcatalog.data.HCatRecord;
import org.apache.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hcatalog.data.schema.HCatSchemaUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

/**
 * @deprecated Use/modify {@link org.apache.hive.hcatalog.mapreduce.TestHCatDynamicPartitioned} instead
 */
public class TestHCatDynamicPartitioned extends HCatMapReduceTest {

  private static List<HCatRecord> writeRecords;
  private static List<HCatFieldSchema> dataColumns;
  private static final Logger LOG = LoggerFactory.getLogger(TestHCatDynamicPartitioned.class);
  protected static final int NUM_RECORDS = 20;
  protected static final int NUM_PARTITIONS = 5;

  @BeforeClass
  public static void generateInputData() throws Exception {
    tableName = "testHCatDynamicPartitionedTable";
    generateWriteRecords(NUM_RECORDS, NUM_PARTITIONS, 0);
    generateDataColumns();
  }

  protected static void generateDataColumns() throws HCatException {
    dataColumns = new ArrayList<HCatFieldSchema>();
    dataColumns.add(HCatSchemaUtils.getHCatFieldSchema(new FieldSchema("c1", serdeConstants.INT_TYPE_NAME, "")));
    dataColumns.add(HCatSchemaUtils.getHCatFieldSchema(new FieldSchema("c2", serdeConstants.STRING_TYPE_NAME, "")));
    dataColumns.add(HCatSchemaUtils.getHCatFieldSchema(new FieldSchema("p1", serdeConstants.STRING_TYPE_NAME, "")));
  }

  protected static void generateWriteRecords(int max, int mod, int offset) {
    writeRecords = new ArrayList<HCatRecord>();

    for (int i = 0; i < max; i++) {
      List<Object> objList = new ArrayList<Object>();

      objList.add(i);
      objList.add("strvalue" + i);
      objList.add(String.valueOf((i % mod) + offset));
      writeRecords.add(new DefaultHCatRecord(objList));
    }
  }

  @Override
  protected List<FieldSchema> getPartitionKeys() {
    List<FieldSchema> fields = new ArrayList<FieldSchema>();
    fields.add(new FieldSchema("p1", serdeConstants.STRING_TYPE_NAME, ""));
    return fields;
  }

  @Override
  protected List<FieldSchema> getTableColumns() {
    List<FieldSchema> fields = new ArrayList<FieldSchema>();
    fields.add(new FieldSchema("c1", serdeConstants.INT_TYPE_NAME, ""));
    fields.add(new FieldSchema("c2", serdeConstants.STRING_TYPE_NAME, ""));
    return fields;
  }

  /**
   * Run the dynamic partitioning test but with single map task
   * @throws Exception
   */
  @Test
  public void testHCatDynamicPartitionedTable() throws Exception {
    runHCatDynamicPartitionedTable(true);
  }

  /**
   * Run the dynamic partitioning test but with multiple map task. See HCATALOG-490
   * @throws Exception
   */
  @Test
  public void testHCatDynamicPartitionedTableMultipleTask() throws Exception {
    runHCatDynamicPartitionedTable(false);
  }

  protected void runHCatDynamicPartitionedTable(boolean asSingleMapTask) throws Exception {
    generateWriteRecords(NUM_RECORDS, NUM_PARTITIONS, 0);
    runMRCreate(null, dataColumns, writeRecords, NUM_RECORDS, true, asSingleMapTask);

    runMRRead(NUM_RECORDS);

    //Read with partition filter
    runMRRead(4, "p1 = \"0\"");
    runMRRead(8, "p1 = \"1\" or p1 = \"3\"");
    runMRRead(4, "p1 = \"4\"");

    // read from hive to test

    String query = "select * from " + tableName;
    int retCode = driver.run(query).getResponseCode();

    if (retCode != 0) {
      throw new Exception("Error " + retCode + " running query " + query);
    }

    ArrayList<String> res = new ArrayList<String>();
    driver.getResults(res);
    assertEquals(NUM_RECORDS, res.size());


    //Test for duplicate publish
    IOException exc = null;
    try {
      generateWriteRecords(NUM_RECORDS, NUM_PARTITIONS, 0);
      Job job = runMRCreate(null, dataColumns, writeRecords, NUM_RECORDS, false);

      if (HCatUtil.isHadoop23()) {
        Assert.assertTrue(job.isSuccessful()==false);
      }
    } catch (IOException e) {
      exc = e;
    }

    if (!HCatUtil.isHadoop23()) {
      assertTrue(exc != null);
      assertTrue(exc instanceof HCatException);
      assertTrue("Got exception of type [" + ((HCatException) exc).getErrorType().toString()
          + "] Expected ERROR_PUBLISHING_PARTITION or ERROR_MOVE_FAILED",
          (ErrorType.ERROR_PUBLISHING_PARTITION == ((HCatException) exc).getErrorType())
              || (ErrorType.ERROR_MOVE_FAILED == ((HCatException) exc).getErrorType())
      );
    }

    query = "show partitions " + tableName;
    retCode = driver.run(query).getResponseCode();
    if (retCode != 0) {
      throw new Exception("Error " + retCode + " running query " + query);
    }
    res = new ArrayList<String>();
    driver.getResults(res);
    assertEquals(NUM_PARTITIONS, res.size());

    query = "select * from " + tableName;
    retCode = driver.run(query).getResponseCode();
    if (retCode != 0) {
      throw new Exception("Error " + retCode + " running query " + query);
    }
    res = new ArrayList<String>();
    driver.getResults(res);
    assertEquals(NUM_RECORDS, res.size());
  }

  //TODO 1.0 miniCluster is slow this test times out, make it work
// renaming test to make test framework skip it
  public void _testHCatDynamicPartitionMaxPartitions() throws Exception {
    HiveConf hc = new HiveConf(this.getClass());

    int maxParts = hiveConf.getIntVar(HiveConf.ConfVars.DYNAMICPARTITIONMAXPARTS);
    LOG.info("Max partitions allowed = {}", maxParts);

    IOException exc = null;
    try {
      generateWriteRecords(maxParts + 5, maxParts + 2, 10);
      runMRCreate(null, dataColumns, writeRecords, maxParts + 5, false);
    } catch (IOException e) {
      exc = e;
    }

    if (HCatConstants.HCAT_IS_DYNAMIC_MAX_PTN_CHECK_ENABLED) {
      assertTrue(exc != null);
      assertTrue(exc instanceof HCatException);
      assertEquals(ErrorType.ERROR_TOO_MANY_DYNAMIC_PTNS, ((HCatException) exc).getErrorType());
    } else {
      assertTrue(exc == null);
      runMRRead(maxParts + 5);
    }
  }
}
