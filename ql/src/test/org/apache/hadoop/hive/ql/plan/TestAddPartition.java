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
package org.apache.hadoop.hive.ql.plan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.MetadataTypedColumnsetSerDe;

public class TestAddPartition extends TestCase {

  private static final String PART1_NAME = "part1";
  private static final String PART2_NAME = "part2";

  public TestAddPartition() throws Exception {
    super();
  }

  public void testAddPartition() throws Exception {
    Configuration conf = new Configuration();
    HiveConf hiveConf = new HiveConf(conf, TestAddPartition.class);
    HiveMetaStoreClient client = null;

    try {
      client = new HiveMetaStoreClient(hiveConf);

      String dbName = "testdb";
      String tableName = "tablename";

      Table tbl = new Table();
      tbl.setTableName(tableName);
      tbl.setDbName(dbName);
      tbl.setParameters(new HashMap<String, String>());

      StorageDescriptor sd = new StorageDescriptor();
      sd.setSerdeInfo(new SerDeInfo());
      sd.getSerdeInfo().setName(tbl.getTableName());
      sd.getSerdeInfo().setSerializationLib(MetadataTypedColumnsetSerDe.class.getName());

      List<FieldSchema> fss = new ArrayList<FieldSchema>();
      fss.add(new FieldSchema("name", Constants.STRING_TYPE_NAME, ""));
      sd.setCols(fss);
      tbl.setSd(sd);

      tbl.setPartitionKeys(new ArrayList<FieldSchema>());
      tbl.getPartitionKeys().add(
          new FieldSchema(PART1_NAME, Constants.STRING_TYPE_NAME, ""));
      tbl.getPartitionKeys().add(
          new FieldSchema(PART2_NAME, Constants.STRING_TYPE_NAME, ""));

      client.dropTable(dbName, tableName);
      client.dropDatabase(dbName);

      client.createDatabase(dbName, "newloc");
      client.createTable(tbl);

      tbl = client.getTable(dbName, tableName);

      List<String> partValues = new ArrayList<String>();
      partValues.add("value1");
      partValues.add("value2");

      Map<String, String> part1 = new HashMap<String, String>();
      part1.put(PART1_NAME, "value1");
      part1.put(PART2_NAME, "value2");
      
      List<Map<String, String>> partitions = new ArrayList<Map<String, String>>();
      partitions.add(part1);
      
      // no partitions yet
      List<Partition> parts = client.listPartitions(dbName, tableName,
          (short) -1);
      assertTrue(parts.isEmpty());

      String partitionLocation = PART1_NAME + Path.SEPARATOR + PART2_NAME;
      // add the partitions
      for (Map<String,String> map : partitions) {
        AddPartitionDesc addPartition = new AddPartitionDesc(dbName, 
            tableName, map, partitionLocation);
        Task<DDLWork> task = TaskFactory.get(new DDLWork(addPartition), hiveConf);
        task.initialize(hiveConf);
        assertEquals(0, task.execute());
      }

      // should have one
      parts = client.listPartitions(dbName, tableName, (short) -1);
      assertEquals(1, parts.size());
      Partition insertedPart = parts.get(0);
      assertEquals(tbl.getSd().getLocation() + Path.SEPARATOR + partitionLocation, 
          insertedPart.getSd().getLocation());

      client.dropPartition(dbName, tableName, insertedPart.getValues());

      // add without location specified

      AddPartitionDesc addPartition = new AddPartitionDesc(dbName, tableName, part1, null);
      Task<DDLWork> task = TaskFactory.get(new DDLWork(addPartition), hiveConf);
      task.initialize(hiveConf);
      assertEquals(0, task.execute());
      parts = client.listPartitions(dbName, tableName, (short) -1);
      assertEquals(1, parts.size());

      // see that this fails properly
      addPartition = new AddPartitionDesc(dbName, "doesnotexist", part1, null);
      task = TaskFactory.get(new DDLWork(addPartition), hiveConf);
      task.initialize(hiveConf);
      assertEquals(1, task.execute());
    } finally {
      if (client != null) {
        client.close();
      }
    }
  }

}