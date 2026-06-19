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
package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.builder.PartitionBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_CATALOG_NAME;
import static org.junit.Assert.assertTrue;

/**
 * TestHMSFetchPartitionsWithoutCols. Test case for HMS and HMSClient
 * calls in {@link org.apache.hadoop.hive.metastore.HiveMetaStore} to fetch
 * partitions without column schema in the storage descriptor.
 */
@Category(MetastoreUnitTest.class)
public class TestHMSFetchPartitionsWithoutCols {
    private Configuration conf;
    private HiveMetaStoreClient msc;
    private final Database db = new Database();
    private Table table;

    private static final String dbName = "hmsPartitionTestDb";
    private static final String tblName = "partitionTestTable";

    @Before
    public void setUp() throws Exception {
        conf = MetastoreConf.newMetastoreConf();
        MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
        MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.METASTORE_CLIENT_FIELD_SCHEMA_FOR_PARTITIONS, true);
        msc = new HiveMetaStoreClient(conf);
        msc.dropDatabase(dbName, true, true, true);
        db.setName(dbName);
        db.setCatalogName(DEFAULT_CATALOG_NAME);
        table = new TableBuilder()
                .setDbName(dbName)
                .setTableName(tblName)
                .addTableParam("a", "string")
                .addPartCol("year", "string")
                .addCol("a", "string")
                .addCol("b", "string")
                .build(conf);
        msc.createDatabase(db);
        msc.createTable(table);
        table = msc.getTable(dbName, tblName);
    }

    @Test
    public void testPartitionsWithoutCols() throws Exception {
        // Test-1: check Column schema for add_partitions is not empty
        Partition[] addParts = new Partition[2];
        for (int i = 0; i < addParts.length; i++) {
            addParts[i] = new PartitionBuilder()
                    .inTable(table)
                    .addValue("202" + i)
                    .build(conf);
        }
        List<Partition> add_partitions_result= msc.add_partitions(Arrays.asList(addParts), true, true);
        checkColumnSchemaForPartitions(add_partitions_result, false);

        // Test-2: check column schema for list partitions is empty
        List<String> partVals = new ArrayList<>();
        partVals.add("2022");
        msc.appendPartition(dbName, tblName, partVals);
        List<Partition> parts = msc.listPartitions(dbName, tblName, (short)-1);
        checkColumnSchemaForPartitions(parts, true);

        // Test-3: check column schema for list partitions by names is empty
        List<Partition> getParts = msc.listPartitions(dbName, tblName, partVals, (short) -1);
        checkColumnSchemaForPartitions(getParts, true);
    }

    private void checkColumnSchemaForPartitions(List<Partition> partitionList, boolean isEmpty) {
        assertTrue(!partitionList.isEmpty());
        if (isEmpty) {
            for (Partition part : partitionList) {
                assertTrue(part.getSd().getCols().isEmpty());
            }
        } else {
            for (Partition part : partitionList) {
                assertTrue(!part.getSd().getCols().isEmpty());
            }
        }
    }
}
