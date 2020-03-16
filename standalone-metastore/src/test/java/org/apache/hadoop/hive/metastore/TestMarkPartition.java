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

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidPartitionException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionEventType;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.PartitionBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MetastoreCheckinTest.class)
public class TestMarkPartition {

  protected Configuration conf;

  @Before
  public void setUp() throws Exception {

    System.setProperty("hive.metastore.event.clean.freq", "1s");
    System.setProperty("hive.metastore.event.expiry.duration", "2s");
    conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    MetaStoreTestUtils.setConfForStandloneMode(conf);

  }

  @Test
  public void testMarkingPartitionSet() throws TException, InterruptedException {
    HiveMetaStoreClient msc = new HiveMetaStoreClient(conf);

    final String dbName = "hive2215";
    msc.dropDatabase(dbName, true, true, true);
    Database db = new DatabaseBuilder()
        .setName(dbName)
        .create(msc, conf);

    final String tableName = "tmptbl";
    msc.dropTable(dbName, tableName, true, true);
    Table table = new TableBuilder()
        .setDbName(dbName)
        .setTableName(tableName)
        .addCol("a", "string")
        .addPartCol("b", "string")
        .create(msc, conf);

    Partition part = new PartitionBuilder()
        .inTable(table)
        .addValue("2011")
        .build(conf);
    msc.add_partition(part);
    Map<String,String> kvs = new HashMap<>();
    kvs.put("b", "'2011'");
    msc.markPartitionForEvent(dbName, tableName, kvs, PartitionEventType.LOAD_DONE);
    Assert.assertTrue(msc.isPartitionMarkedForEvent(dbName, tableName, kvs, PartitionEventType.LOAD_DONE));
    Thread.sleep(10000);
    Assert.assertFalse(msc.isPartitionMarkedForEvent(dbName, tableName, kvs, PartitionEventType.LOAD_DONE));

    kvs.put("b", "'2012'");
    Assert.assertFalse(msc.isPartitionMarkedForEvent(dbName, tableName, kvs, PartitionEventType.LOAD_DONE));
    try {
      msc.markPartitionForEvent(dbName, "tmptbl2", kvs, PartitionEventType.LOAD_DONE);
      Assert.fail("Expected UnknownTableException");
    } catch (UnknownTableException e) {
      // All good
    } catch(Exception e){
      Assert.fail("Expected UnknownTableException");
    }
    try{
      msc.isPartitionMarkedForEvent(dbName, "tmptbl2", kvs, PartitionEventType.LOAD_DONE);
      Assert.fail("Expected UnknownTableException");
    } catch (UnknownTableException e) {
      // All good
    } catch(Exception e){
      Assert.fail("Expected UnknownTableException, received " + e.getClass().getName());
    }
    kvs.put("a", "'2012'");
    try {
      msc.isPartitionMarkedForEvent(dbName, tableName, kvs, PartitionEventType.LOAD_DONE);
      Assert.fail("Expected InvalidPartitionException");
    } catch (InvalidPartitionException e) {
      // All good
    } catch(Exception e){
      Assert.fail("Expected InvalidPartitionException, received " + e.getClass().getName());
    }
  }

}
