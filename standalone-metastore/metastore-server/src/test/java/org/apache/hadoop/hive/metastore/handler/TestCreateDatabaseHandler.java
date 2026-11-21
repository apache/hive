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

package org.apache.hadoop.hive.metastore.handler;

import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.HMSHandler;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.CreateDatabaseRequest;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Category(MetastoreUnitTest.class)
public class TestCreateDatabaseHandler {

  @Test
  public void failedCreateDoesNotDeleteDatabaseDirectories() throws Exception {
    Configuration conf = MetastoreConf.newMetastoreConf();
    RawStore rawStore = mock(RawStore.class);
    Warehouse warehouse = mock(Warehouse.class);
    HMSHandler handler = mock(HMSHandler.class);

    Path externalPath = new Path("file:/warehouse/external.db");
    Path managedPath = new Path("file:/warehouse/managed.db");
    when(handler.getConf()).thenReturn(conf);
    when(handler.getMS()).thenReturn(rawStore);
    when(handler.getWh()).thenReturn(warehouse);
    when(handler.getTransactionalListeners()).thenReturn(Collections.emptyList());
    when(handler.getListeners()).thenReturn(Collections.emptyList());
    when(rawStore.getCatalog("hive")).thenReturn(new Catalog("hive", null));
    when(warehouse.getDefaultDatabasePath(anyString(), anyBoolean()))
        .thenReturn(externalPath, managedPath);
    when(warehouse.getDnsPath(any(Path.class))).thenAnswer(invocation -> invocation.getArgument(0));
    when(warehouse.isDir(any(Path.class))).thenReturn(false);
    when(warehouse.mkdirs(any(Path.class))).thenReturn(true);
    doThrow(new MetaException("duplicate database")).when(rawStore).createDatabase(any(Database.class));

    CreateDatabaseRequest request = new CreateDatabaseRequest("race_db");
    request.setCatalogName("hive");
    request.setLocationUri(externalPath.toString());
    request.setManagedLocationUri(managedPath.toString());

    CreateDatabaseHandler operation = new CreateDatabaseHandler(handler, request);
    assertThrows(MetaException.class, operation::getRequestStatus);

    verify(rawStore).rollbackTransaction();
    verify(rawStore).createDatabase(any(Database.class));
    verify(warehouse, never()).deleteDir(any(Path.class), anyBoolean(), any(Database.class));
  }
}
