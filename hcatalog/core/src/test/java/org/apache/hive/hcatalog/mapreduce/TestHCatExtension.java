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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hive.hcatalog.mapreduce;

import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchemaUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestHCatExtension extends HCatMapReduceTest {
    private static List<HCatRecord> writeRecords;
    private static List<HCatFieldSchema> dataColumns;
    protected static final int NUM_RECORDS = 20;
    protected static final int NUM_TOP_PARTITIONS = 5;

    public TestHCatExtension(String formatName, String serdeClass, String inputFormatClass, String outputFormatClass) throws Exception {
        super(formatName, serdeClass, inputFormatClass, outputFormatClass);
        tableName = "testHCatExtension_" + formatName;
        generateWriteRecords(NUM_RECORDS, NUM_TOP_PARTITIONS, 0);
        generateDataColumns();
    }

    protected static void generateDataColumns() throws HCatException {
        dataColumns = new ArrayList<HCatFieldSchema>();
        dataColumns.add(HCatSchemaUtils.getHCatFieldSchema(new FieldSchema("c1", serdeConstants.INT_TYPE_NAME, "")));
        dataColumns.add(HCatSchemaUtils.getHCatFieldSchema(new FieldSchema("c2", serdeConstants.STRING_TYPE_NAME, "")));
        dataColumns.add(HCatSchemaUtils.getHCatFieldSchema(new FieldSchema("p1", serdeConstants.STRING_TYPE_NAME, "")));
        dataColumns.add(HCatSchemaUtils.getHCatFieldSchema(new FieldSchema("p2", serdeConstants.STRING_TYPE_NAME, "")));

    }

    protected static void generateWriteRecords(int max, int mod, int offset) {
        writeRecords = new ArrayList<HCatRecord>();

        for (int i = 0; i < max; i++) {
            List<Object> objList = new ArrayList<Object>();

            objList.add(i);
            objList.add("strvalue" + i);
            objList.add(String.valueOf((i % mod) + offset));
            objList.add(String.valueOf((i / (max / 2)) + offset));
            writeRecords.add(new DefaultHCatRecord(objList));
        }
    }

    @Override
    protected List<FieldSchema> getPartitionKeys() {
        List<FieldSchema> fields = new ArrayList<FieldSchema>();
        fields.add(new FieldSchema("p1", serdeConstants.STRING_TYPE_NAME, ""));
        fields.add(new FieldSchema("p2", serdeConstants.STRING_TYPE_NAME, ""));
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
    public void testHCatExtension() throws Exception {
        Map<String, String> properties = new HashMap<String, String>();
        properties.put(HiveConf.ConfVars.OUTPUT_FILE_EXTENSION.varname, ".test");
        generateWriteRecords(NUM_RECORDS, NUM_TOP_PARTITIONS, 0);
        runMRCreate(null, dataColumns, writeRecords.subList(0,NUM_RECORDS/2), NUM_RECORDS/2,
                true, false, properties);
        runMRCreate(null, dataColumns, writeRecords.subList(NUM_RECORDS/2,NUM_RECORDS), NUM_RECORDS/2,
                true, false, properties);
        String databaseName = (dbName == null) ? Warehouse.DEFAULT_DATABASE_NAME : dbName;
        Table tbl = client.getTable(databaseName, tableName);
        RemoteIterator<LocatedFileStatus> ls = fs.listFiles(new Path(tbl.getSd().getLocation()), true);
        while (ls.hasNext()) {
            LocatedFileStatus lfs = ls.next();
            Assert.assertFalse(lfs.isFile() && lfs.getPath().getName().startsWith("part") && !lfs.getPath().toString().endsWith(".test"));
        }
    }
}
