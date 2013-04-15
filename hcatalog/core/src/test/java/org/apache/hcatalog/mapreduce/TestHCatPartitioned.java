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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hcatalog.common.ErrorType;
import org.apache.hcatalog.common.HCatException;
import org.apache.hcatalog.data.DefaultHCatRecord;
import org.apache.hcatalog.data.HCatRecord;
import org.apache.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hcatalog.data.schema.HCatSchema;
import org.apache.hcatalog.data.schema.HCatSchemaUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestHCatPartitioned extends HCatMapReduceTest {

    private static List<HCatRecord> writeRecords;
    private static List<HCatFieldSchema> partitionColumns;

    @BeforeClass
    public static void oneTimeSetUp() throws Exception {

        tableName = "testHCatPartitionedTable";
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
        //Defining partition names in unsorted order
        fields.add(new FieldSchema("PaRT1", serdeConstants.STRING_TYPE_NAME, ""));
        fields.add(new FieldSchema("part0", serdeConstants.STRING_TYPE_NAME, ""));
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
    public void testHCatPartitionedTable() throws Exception {

        Map<String, String> partitionMap = new HashMap<String, String>();
        partitionMap.put("part1", "p1value1");
        partitionMap.put("part0", "p0value1");

        runMRCreate(partitionMap, partitionColumns, writeRecords, 10, true);

        partitionMap.clear();
        partitionMap.put("PART1", "p1value2");
        partitionMap.put("PART0", "p0value2");

        runMRCreate(partitionMap, partitionColumns, writeRecords, 20, true);

        //Test for duplicate publish
        IOException exc = null;
        try {
            runMRCreate(partitionMap, partitionColumns, writeRecords, 20, true);
        } catch (IOException e) {
            exc = e;
        }

        assertTrue(exc != null);
        assertTrue(exc instanceof HCatException);
        assertEquals(ErrorType.ERROR_DUPLICATE_PARTITION, ((HCatException) exc).getErrorType());

        //Test for publish with invalid partition key name
        exc = null;
        partitionMap.clear();
        partitionMap.put("px1", "p1value2");
        partitionMap.put("px0", "p0value2");

        try {
            runMRCreate(partitionMap, partitionColumns, writeRecords, 20, true);
        } catch (IOException e) {
            exc = e;
        }

        assertTrue(exc != null);
        assertTrue(exc instanceof HCatException);
        assertEquals(ErrorType.ERROR_MISSING_PARTITION_KEY, ((HCatException) exc).getErrorType());

        //Test for publish with missing partition key values
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


        //Test for null partition value map
        exc = null;
        try {
            runMRCreate(null, partitionColumns, writeRecords, 20, false);
        } catch (IOException e) {
            exc = e;
        }

        assertTrue(exc == null);
//    assertTrue(exc instanceof HCatException);
//    assertEquals(ErrorType.ERROR_PUBLISHING_PARTITION, ((HCatException) exc).getErrorType());
        // With Dynamic partitioning, this isn't an error that the keyValues specified didn't values

        //Read should get 10 + 20 rows
        runMRRead(30);

        //Read with partition filter
        runMRRead(10, "part1 = \"p1value1\"");
        runMRRead(20, "part1 = \"p1value2\"");
        runMRRead(30, "part1 = \"p1value1\" or part1 = \"p1value2\"");
        runMRRead(10, "part0 = \"p0value1\"");
        runMRRead(20, "part0 = \"p0value2\"");
        runMRRead(30, "part0 = \"p0value1\" or part0 = \"p0value2\"");

        tableSchemaTest();
        columnOrderChangeTest();
        hiveReadTest();
    }


    //test that new columns gets added to table schema
    private void tableSchemaTest() throws Exception {

        HCatSchema tableSchema = getTableSchema();

        assertEquals(4, tableSchema.getFields().size());

        //Update partition schema to have 3 fields
        partitionColumns.add(HCatSchemaUtils.getHCatFieldSchema(new FieldSchema("c3", serdeConstants.STRING_TYPE_NAME, "")));

        writeRecords = new ArrayList<HCatRecord>();

        for (int i = 0; i < 20; i++) {
            List<Object> objList = new ArrayList<Object>();

            objList.add(i);
            objList.add("strvalue" + i);
            objList.add("str2value" + i);

            writeRecords.add(new DefaultHCatRecord(objList));
        }

        Map<String, String> partitionMap = new HashMap<String, String>();
        partitionMap.put("part1", "p1value5");
        partitionMap.put("part0", "p0value5");

        runMRCreate(partitionMap, partitionColumns, writeRecords, 10, true);

        tableSchema = getTableSchema();

        //assert that c3 has got added to table schema
        assertEquals(5, tableSchema.getFields().size());
        assertEquals("c1", tableSchema.getFields().get(0).getName());
        assertEquals("c2", tableSchema.getFields().get(1).getName());
        assertEquals("c3", tableSchema.getFields().get(2).getName());
        assertEquals("part1", tableSchema.getFields().get(3).getName());
        assertEquals("part0", tableSchema.getFields().get(4).getName());

        //Test that changing column data type fails
        partitionMap.clear();
        partitionMap.put("part1", "p1value6");
        partitionMap.put("part0", "p0value6");

        partitionColumns = new ArrayList<HCatFieldSchema>();
        partitionColumns.add(HCatSchemaUtils.getHCatFieldSchema(new FieldSchema("c1", serdeConstants.INT_TYPE_NAME, "")));
        partitionColumns.add(HCatSchemaUtils.getHCatFieldSchema(new FieldSchema("c2", serdeConstants.INT_TYPE_NAME, "")));

        IOException exc = null;
        try {
            runMRCreate(partitionMap, partitionColumns, writeRecords, 20, true);
        } catch (IOException e) {
            exc = e;
        }

        assertTrue(exc != null);
        assertTrue(exc instanceof HCatException);
        assertEquals(ErrorType.ERROR_SCHEMA_TYPE_MISMATCH, ((HCatException) exc).getErrorType());

        //Test that partition key is not allowed in data
        partitionColumns = new ArrayList<HCatFieldSchema>();
        partitionColumns.add(HCatSchemaUtils.getHCatFieldSchema(new FieldSchema("c1", serdeConstants.INT_TYPE_NAME, "")));
        partitionColumns.add(HCatSchemaUtils.getHCatFieldSchema(new FieldSchema("c2", serdeConstants.STRING_TYPE_NAME, "")));
        partitionColumns.add(HCatSchemaUtils.getHCatFieldSchema(new FieldSchema("c3", serdeConstants.STRING_TYPE_NAME, "")));
        partitionColumns.add(HCatSchemaUtils.getHCatFieldSchema(new FieldSchema("part1", serdeConstants.STRING_TYPE_NAME, "")));

        List<HCatRecord> recordsContainingPartitionCols = new ArrayList<HCatRecord>(20);
        for (int i = 0; i < 20; i++) {
            List<Object> objList = new ArrayList<Object>();

            objList.add(i);
            objList.add("c2value" + i);
            objList.add("c3value" + i);
            objList.add("p1value6");

            recordsContainingPartitionCols.add(new DefaultHCatRecord(objList));
        }

        exc = null;
        try {
            runMRCreate(partitionMap, partitionColumns, recordsContainingPartitionCols, 20, true);
        } catch (IOException e) {
            exc = e;
        }

        List<HCatRecord> records = runMRRead(20, "part1 = \"p1value6\"");
        assertEquals(20, records.size());
        records = runMRRead(20, "part0 = \"p0value6\"");
        assertEquals(20, records.size());
        Integer i = 0;
        for (HCatRecord rec : records) {
            assertEquals(5, rec.size());
            assertTrue(rec.get(0).equals(i));
            assertTrue(rec.get(1).equals("c2value" + i));
            assertTrue(rec.get(2).equals("c3value" + i));
            assertTrue(rec.get(3).equals("p1value6"));
            assertTrue(rec.get(4).equals("p0value6"));
            i++;
        }
    }

    //check behavior while change the order of columns
    private void columnOrderChangeTest() throws Exception {

        HCatSchema tableSchema = getTableSchema();

        assertEquals(5, tableSchema.getFields().size());

        partitionColumns = new ArrayList<HCatFieldSchema>();
        partitionColumns.add(HCatSchemaUtils.getHCatFieldSchema(new FieldSchema("c1", serdeConstants.INT_TYPE_NAME, "")));
        partitionColumns.add(HCatSchemaUtils.getHCatFieldSchema(new FieldSchema("c3", serdeConstants.STRING_TYPE_NAME, "")));
        partitionColumns.add(HCatSchemaUtils.getHCatFieldSchema(new FieldSchema("c2", serdeConstants.STRING_TYPE_NAME, "")));


        writeRecords = new ArrayList<HCatRecord>();

        for (int i = 0; i < 10; i++) {
            List<Object> objList = new ArrayList<Object>();

            objList.add(i);
            objList.add("co strvalue" + i);
            objList.add("co str2value" + i);

            writeRecords.add(new DefaultHCatRecord(objList));
        }

        Map<String, String> partitionMap = new HashMap<String, String>();
        partitionMap.put("part1", "p1value8");
        partitionMap.put("part0", "p0value8");

        Exception exc = null;
        try {
            runMRCreate(partitionMap, partitionColumns, writeRecords, 10, true);
        } catch (IOException e) {
            exc = e;
        }

        assertTrue(exc != null);
        assertTrue(exc instanceof HCatException);
        assertEquals(ErrorType.ERROR_SCHEMA_COLUMN_MISMATCH, ((HCatException) exc).getErrorType());


        partitionColumns = new ArrayList<HCatFieldSchema>();
        partitionColumns.add(HCatSchemaUtils.getHCatFieldSchema(new FieldSchema("c1", serdeConstants.INT_TYPE_NAME, "")));
        partitionColumns.add(HCatSchemaUtils.getHCatFieldSchema(new FieldSchema("c2", serdeConstants.STRING_TYPE_NAME, "")));

        writeRecords = new ArrayList<HCatRecord>();

        for (int i = 0; i < 10; i++) {
            List<Object> objList = new ArrayList<Object>();

            objList.add(i);
            objList.add("co strvalue" + i);

            writeRecords.add(new DefaultHCatRecord(objList));
        }

        runMRCreate(partitionMap, partitionColumns, writeRecords, 10, true);

        //Read should get 10 + 20 + 10 + 10 + 20 rows
        runMRRead(70);
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
        assertEquals(70, res.size());
    }
}
