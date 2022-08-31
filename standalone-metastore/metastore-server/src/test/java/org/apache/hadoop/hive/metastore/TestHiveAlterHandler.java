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
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.List;

import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getDefaultCatalog;

@Category(MetastoreUnitTest.class)
public class TestHiveAlterHandler {

  private Configuration conf = MetastoreConf.newMetastoreConf();

  @Test
  public void testAlterTableAddColNotUpdateStats() throws MetaException, InvalidObjectException, NoSuchObjectException, InvalidInputException {
    FieldSchema col1 = new FieldSchema("col1", "string", "col1 comment");
    FieldSchema col2 = new FieldSchema("col2", "string", "col2 comment");
    FieldSchema col3 = new FieldSchema("col3", "string", "col3 comment");
    FieldSchema col4 = new FieldSchema("col4", "string", "col4 comment");

    StorageDescriptor oldSd = new StorageDescriptor();
    oldSd.setCols(Arrays.asList(col1, col2, col3));
    Table oldTable = new Table();
    oldTable.setDbName("default");
    oldTable.setTableName("test_table");
    oldTable.setSd(oldSd);

    StorageDescriptor newSd = new StorageDescriptor(oldSd);
    newSd.setCols(Arrays.asList(col1, col2, col3, col4));
    Table newTable = new Table(oldTable);
    newTable.setSd(newSd);

    RawStore msdb = Mockito.mock(RawStore.class);
    Mockito.doThrow(new RuntimeException("shouldn't be called")).when(msdb).updateTableColumnStatistics(
        Mockito.any(), Mockito.eq(null), Mockito.anyLong());
    HiveAlterHandler handler = new HiveAlterHandler();
    handler.setConf(conf);
    Deadline.registerIfNot(100_000);
    Deadline.startTimer("updateTableColumnStats");
    List<ColumnStatistics> colstats = handler.deleteTableColumnStats(msdb, oldTable, newTable, handler.getColumnStats(msdb, oldTable));
    handler.updateTableColumnStats(msdb, newTable, null, colstats);
  }

  @Test
  public void testAlterTableDelColUpdateStats() throws Exception {
    FieldSchema col1 = new FieldSchema("col1", "string", "col1 comment");
    FieldSchema col2 = new FieldSchema("col2", "string", "col2 comment");
    FieldSchema col3 = new FieldSchema("col3", "string", "col3 comment");
    FieldSchema col4 = new FieldSchema("col4", "string", "col4 comment");

    StorageDescriptor oldSd = new StorageDescriptor();
    oldSd.setCols(Arrays.asList(col1, col2, col3, col4));
    Table oldTable = new Table();
    oldTable.setDbName("default");
    oldTable.setTableName("test_table");
    oldTable.setSd(oldSd);

    StorageDescriptor newSd = new StorageDescriptor(oldSd);
    newSd.setCols(Arrays.asList(col1, col2, col3));
    Table newTable = new Table(oldTable);
    newTable.setSd(newSd);

    RawStore msdb = Mockito.mock(RawStore.class);
    HiveAlterHandler handler = new HiveAlterHandler();
    handler.setConf(conf);
    Deadline.registerIfNot(100_000);
    Deadline.startTimer("updateTableColumnStats");
    try {
      List<ColumnStatistics> colstats = handler.deleteTableColumnStats(msdb, oldTable, newTable, handler.getColumnStats(msdb, oldTable));
      handler.updateTableColumnStats(msdb, newTable, null, colstats);
    } catch (Throwable t) {
      System.err.println(t);
      t.printStackTrace(System.err);
      throw t;
    }
    Mockito.verify(msdb, Mockito.times(1)).getTableColumnStatistics(
        getDefaultCatalog(conf), oldTable.getDbName(), oldTable.getTableName(), Arrays.asList("col1", "col2", "col3", "col4")
    );
  }

  @Test
  public void testAlterTableChangePosNotUpdateStats() throws MetaException, InvalidObjectException, NoSuchObjectException, InvalidInputException {
    FieldSchema col1 = new FieldSchema("col1", "string", "col1 comment");
    FieldSchema col2 = new FieldSchema("col2", "string", "col2 comment");
    FieldSchema col3 = new FieldSchema("col3", "string", "col3 comment");
    FieldSchema col4 = new FieldSchema("col4", "string", "col4 comment");

    StorageDescriptor oldSd = new StorageDescriptor();
    oldSd.setCols(Arrays.asList(col1, col2, col3, col4));
    Table oldTable = new Table();
    oldTable.setDbName("default");
    oldTable.setTableName("test_table");
    oldTable.setSd(oldSd);

    StorageDescriptor newSd = new StorageDescriptor(oldSd);
    newSd.setCols(Arrays.asList(col1, col4, col2, col3));
    Table newTable = new Table(oldTable);
    newTable.setSd(newSd);

    RawStore msdb = Mockito.mock(RawStore.class);
    Mockito.doThrow(new RuntimeException("shouldn't be called")).when(msdb).updateTableColumnStatistics(
        Mockito.any(), Mockito.eq(null), Mockito.anyLong());
    HiveAlterHandler handler = new HiveAlterHandler();
    handler.setConf(conf);
    Deadline.registerIfNot(100_000);
    Deadline.startTimer("updateTableColumnStats");
    List<ColumnStatistics> colstats = handler.deleteTableColumnStats(msdb, oldTable, newTable, handler.getColumnStats(msdb, oldTable));
    handler.updateTableColumnStats(msdb, newTable, null, colstats);
  }

}
