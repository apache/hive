/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.ColumnType;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.PartitionBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;

/**
 * This class runs a few simple operations against the server to make sure it runs reasonably.
 * Even though it is a test its in the main tree because it needs to be deployed with the server
 * to allow smoke testing once the server is installed.
 */
public class SmokeTest {
  private static final Logger LOG = LoggerFactory.getLogger(SmokeTest.class);
  private static final String dbName = "internal_smoke_test";
  private static final String tableName = "internal_smoke_test_table";
  private static final String partValue = "internal_smoke_test_val1";

  private static Configuration conf;

  private SmokeTest() {

  }

  private void runTest(IMetaStoreClient client) throws TException {
    LOG.info("Starting smoke test");

    File dbDir = new File(System.getProperty("java.io.tmpdir"), "internal_smoke_test");
    if (!dbDir.mkdir()) {
      throw new RuntimeException("Unable to create direcotory " + dbDir.getAbsolutePath());
    }
    dbDir.deleteOnExit();

    LOG.info("Going to create database " + dbName);
    Database db = new DatabaseBuilder()
        .setName(dbName)
        .setLocation(dbDir.getAbsolutePath())
        .create(client, conf);

    LOG.info("Going to create table " + tableName);
    Table table = new TableBuilder()
        .inDb(db)
        .setTableName(tableName)
        .addCol("col1", ColumnType.INT_TYPE_NAME)
        .addCol("col2", ColumnType.TIMESTAMP_TYPE_NAME)
        .addPartCol("pcol1", ColumnType.STRING_TYPE_NAME)
        .create(client, conf);

    LOG.info("Going to create partition with value " + partValue);
    Partition part = new PartitionBuilder()
        .inTable(table)
        .addValue("val1")
        .addToTable(client, conf);

    LOG.info("Going to list the partitions");
    List<Partition> parts = client.listPartitions(dbName, tableName, (short)-1);
    LOG.info("Fetched: { " + parts.toString() + "}");

    LOG.info("Going to drop database");
    client.dropDatabase(dbName, true, false, true);


    LOG.info("Completed smoke test");
  }

  public static void main(String[] args) throws Exception {
    SmokeTest test = new SmokeTest();
    conf = MetastoreConf.newMetastoreConf();
    IMetaStoreClient client = new HiveMetaStoreClient(conf);
    test.runTest(client);
  }
}
