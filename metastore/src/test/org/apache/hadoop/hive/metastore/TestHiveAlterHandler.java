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

package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.hive.metastore.api.*;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;

public class TestHiveAlterHandler {

  @Test
  public void testAlterTableAddColNotUpdateStats() throws MetaException, InvalidObjectException, NoSuchObjectException {
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
    Mockito.doThrow(new RuntimeException("shouldn't be called")).when(msdb).getTableColumnStatistics(
        oldTable.getDbName(), oldTable.getTableName(), Arrays.asList("col1", "col2", "col3"));
    HiveAlterHandler handler = new HiveAlterHandler();
    handler.alterTableUpdateTableColumnStats(msdb, oldTable, newTable);
  }

  @Test
  public void testAlterTableDelColUpdateStats() throws MetaException, InvalidObjectException, NoSuchObjectException {
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
    handler.alterTableUpdateTableColumnStats(msdb, oldTable, newTable);
    Mockito.verify(msdb, Mockito.times(1)).getTableColumnStatistics(
        oldTable.getDbName(), oldTable.getTableName(), Arrays.asList("col1", "col2", "col3", "col4")
    );
  }

  @Test
  public void testAlterTableChangePosNotUpdateStats() throws MetaException, InvalidObjectException, NoSuchObjectException {
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
    Mockito.doThrow(new RuntimeException("shouldn't be called")).when(msdb).getTableColumnStatistics(
        oldTable.getDbName(), oldTable.getTableName(), Arrays.asList("col1", "col2", "col3", "col4"));
    HiveAlterHandler handler = new HiveAlterHandler();
    handler.alterTableUpdateTableColumnStats(msdb, oldTable, newTable);
  }

}
