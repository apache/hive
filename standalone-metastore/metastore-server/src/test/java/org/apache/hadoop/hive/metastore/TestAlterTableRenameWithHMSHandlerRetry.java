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

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jdo.JDODataStoreException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.PartitionBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Renaming a managed partitioned table must stay consistent when {@link RetryingHMSHandler}
 * retries the call after a recoverable datastore error.
 *
 * The first attempt fails while updating the partition locations, after
 * {@link HiveAlterHandler} has already rewritten the location of the new table object passed
 * in by the caller. The retry is invoked with that same object.
 */
@Category(MetastoreCheckinTest.class)
public class TestAlterTableRenameWithHMSHandlerRetry {
  private static final String DB_NAME = "rename_retry_db";
  private static final String OLD_TBL_NAME = "rename_retry_src";
  private static final String NEW_TBL_NAME = "rename_retry_dst";

  private Configuration conf;
  private HiveMetaStoreClient msc;

  @Before
  public void setUp() throws Exception {
    conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setVar(conf, ConfVars.RAW_STORE_IMPL, InjectableBehaviourObjectStore.class.getName());
    MetastoreConf.setLongVar(conf, ConfVars.HMS_HANDLER_ATTEMPTS, 3);
    MetastoreConf.setTimeVar(conf, ConfVars.HMS_HANDLER_INTERVAL, 0, TimeUnit.MILLISECONDS);
    MetastoreConf.setBoolVar(conf, ConfVars.HMS_HANDLER_FORCE_RELOAD_CONF, false);
    // Keep the table a plain managed table.
    MetastoreConf.setVar(conf, ConfVars.METASTORE_METADATA_TRANSFORMER_CLASS, " ");
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    MetaStoreTestUtils.startMetaStoreWithRetry(HadoopThriftAuthBridge.getBridge(), conf);
    msc = new HiveMetaStoreClient(conf);
  }

  @After
  public void tearDown() throws Exception {
    InjectableBehaviourObjectStore.setAlterPartitionsBehaviour(null);
    if (msc != null) {
      msc.dropDatabase(DB_NAME, true, true, true);
      msc.close();
    }
  }

  @Test
  public void testRenameIsConsistentAfterRetry() throws Exception {
    new DatabaseBuilder().setName(DB_NAME).create(msc, conf);
    Table table = new TableBuilder()
        .setDbName(DB_NAME)
        .setTableName(OLD_TBL_NAME)
        .addCol("c1", ColumnType.STRING_TYPE_NAME)
        .addPartCol("p", ColumnType.STRING_TYPE_NAME)
        .create(msc, conf);
    new PartitionBuilder().inTable(table).addValue("1").addToTable(msc, conf);
    new PartitionBuilder().inTable(table).addValue("2").addToTable(msc, conf);

    Table oldTable = msc.getTable(DB_NAME, OLD_TBL_NAME);
    Path oldTablePath = new Path(oldTable.getSd().getLocation());
    FileSystem fs = oldTablePath.getFileSystem(conf);
    Assert.assertTrue("Table directory should exist before the rename", fs.exists(oldTablePath));

    // Fail the first partition update with an error RetryingHMSHandler treats as recoverable.
    AtomicInteger alterPartitionsCalls = new AtomicInteger();
    InjectableBehaviourObjectStore.setAlterPartitionsBehaviour(parts -> {
      if (alterPartitionsCalls.getAndIncrement() == 0) {
        throw new JDODataStoreException("Lock wait timeout exceeded; try restarting transaction");
      }
      return true;
    });

    Table newTable = oldTable.deepCopy();
    newTable.setTableName(NEW_TBL_NAME);
    msc.alter_table(DB_NAME, OLD_TBL_NAME, newTable);

    Assert.assertTrue("The first attempt should have reached the partition update",
        alterPartitionsCalls.get() >= 1);

    Table renamed = msc.getTable(DB_NAME, NEW_TBL_NAME);
    Path newTablePath = new Path(renamed.getSd().getLocation());
    Assert.assertNotEquals("Table location should follow the rename", oldTablePath, newTablePath);

    Assert.assertTrue("Data should have been moved to the new table location " + newTablePath,
        fs.exists(newTablePath));
    Assert.assertFalse("Old table location should be gone " + oldTablePath, fs.exists(oldTablePath));

    List<Partition> partitions = msc.listPartitions(DB_NAME, NEW_TBL_NAME, (short) -1);
    Assert.assertEquals(2, partitions.size());
    String newTableLocationPrefix = newTablePath.toUri().getPath() + Path.SEPARATOR;
    for (Partition partition : partitions) {
      String partitionPath = new Path(partition.getSd().getLocation()).toUri().getPath();
      Assert.assertTrue("Partition " + partition.getValues() + " location " + partitionPath
              + " should be under the renamed table location " + newTablePath,
          partitionPath.startsWith(newTableLocationPrefix));
    }
  }
}
