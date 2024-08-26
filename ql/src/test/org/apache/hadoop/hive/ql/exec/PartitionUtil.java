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

package org.apache.hadoop.hive.ql.exec;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.CheckResult;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.builder.PartitionBuilder;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.stats.StatsUtils;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.StringUtils;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.fail;

public class PartitionUtil {

    public static Table createPartitionedTable(IMetaStoreClient db, String catName, String dbName, String tableName)
            throws Exception {
        try {
            db.dropTable(catName, dbName, tableName);
            Table table = new Table();
            table.setCatName(catName);
            table.setDbName(dbName);
            table.setTableName(tableName);
            FieldSchema col1 = new FieldSchema("key", "string", "");
            FieldSchema col2 = new FieldSchema("value", "int", "");
            FieldSchema col3 = new FieldSchema("city", "string", "");
            StorageDescriptor sd = new StorageDescriptor();
            sd.setSerdeInfo(new SerDeInfo());
            sd.setInputFormat(TextInputFormat.class.getCanonicalName());
            sd.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
            sd.setCols(Arrays.asList(col1, col2));
            table.setPartitionKeys(Arrays.asList(col3));
            table.setSd(sd);
            db.createTable(table);
            return db.getTable(catName, dbName, tableName);
        } catch (Exception exception) {
            fail("Unable to drop and create table " + StatsUtils.getFullyQualifiedTableName(dbName, tableName) + " because "
                    + StringUtils.stringifyException(exception));
            throw exception;
        }
    }

    public static void cleanUpTableQuietly(IMetaStoreClient db, String catName, String dbName, String tableName) {
        try {
            db.dropTable(catName, dbName, tableName);
        } catch (Exception exception) {
            fail("Unexpected exception: " + StringUtils.stringifyException(exception));
        }
    }

    public static Set<CheckResult.PartitionResult> createPartsNotInMs(int numOfParts) {
        Set<CheckResult.PartitionResult> partsNotInMs = new HashSet<>();
        for (int i = 0; i < numOfParts; i++) {
            CheckResult.PartitionResult result = new CheckResult.PartitionResult();
            result.setPartitionName("city=dummyCity_" + i);
            result.setTableName("dummyTable");
            partsNotInMs.add(result);
        }
        return partsNotInMs;
    }


    public static void addPartitions(IMetaStoreClient db, String dbName, String tableName, String location,
         HiveConf hiveConf, int numPartitions) throws Exception {
        List<Partition> partitions = new ArrayList<>();
        for (int i = 0; i < numPartitions; i++) {
            partitions.add(buildPartition(dbName, tableName, String.valueOf(i), location + "/city=" + i, hiveConf));
        }
        db.add_partitions(partitions, true, true);
    }

    protected static Partition buildPartition(String dbName, String tableName, String value,
         String location, HiveConf hiveConf) throws MetaException {
        return new PartitionBuilder()
                .setDbName(dbName)
                .setTableName(tableName)
                .addValue(value)
                .addCol("test_id", "int", "test col id")
                .addCol("test_value", "string", "test col value")
                .setLocation(location)
                .build(hiveConf);
    }
}
