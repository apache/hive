/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.hive;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.thrift.TException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;

public class TestHiveIcebergRESTCatalogClientAdapter {

  static HiveIcebergRESTCatalogClientAdapter hiveIcebergRESTCatalogClientAdapter;
  static RESTCatalog restCatalog;
  private static MockedStatic<CatalogUtil> catalogUtilMockedStatic;

  @BeforeAll
  public static void before() throws MetaException {
    Configuration configuration = new Configuration();
    configuration.set("iceberg.catalog.type", "rest");
    configuration.set("iceberg.rest-catalog.uri", "http://localhost");
    catalogUtilMockedStatic = Mockito.mockStatic(CatalogUtil.class);
    restCatalog = Mockito.mock(RESTCatalog.class);
    catalogUtilMockedStatic.when(() -> CatalogUtil.buildIcebergCatalog(any(), any(), any())).thenReturn(restCatalog);
    hiveIcebergRESTCatalogClientAdapter = Mockito.spy(new HiveIcebergRESTCatalogClientAdapter(configuration, null));
    hiveIcebergRESTCatalogClientAdapter.reconnect();
    TableOperations ops = new TableOperations() {
      @Override
      public TableMetadata current() {
        return TableMetadata.newTableMetadata(new Schema(), PartitionSpec.unpartitioned(), "location",
            Maps.newHashMap());
      }

      @Override
      public TableMetadata refresh() {
        return null;
      }

      @Override
      public void commit(TableMetadata base, TableMetadata metadata) {

      }

      @Override
      public FileIO io() {
        return null;
      }

      @Override
      public String metadataFileLocation(String fileName) {
        return null;
      }

      @Override
      public LocationProvider locationProvider() {
        return null;
      }
    };
    Mockito.doReturn(new BaseTable(ops, "tableName")).when(restCatalog).loadTable(any());
    Namespace namespace = Namespace.of("default");
    Mockito.doReturn(Arrays.asList(namespace)).when(restCatalog).listNamespaces(any());
    Mockito.doReturn(new BaseTable(ops, "tableName")).when(restCatalog).createTable(any(), any(), any(), any());
    Mockito.doReturn(tableBuilder).when(restCatalog).buildTable(any(), any());
  }
  static Catalog.TableBuilder tableBuilder = new Catalog.TableBuilder() {
    @Override
    public Catalog.TableBuilder withPartitionSpec(PartitionSpec spec) {
      return this;
    }

    @Override
    public Catalog.TableBuilder withSortOrder(SortOrder sortOrder) {
      return this;
    }

    @Override
    public Catalog.TableBuilder withLocation(String location) {
      return this;
    }

    @Override
    public Catalog.TableBuilder withProperties(Map<String, String> properties) {
      return this;
    }

    @Override
    public Catalog.TableBuilder withProperty(String key, String value) {
      return this;
    }

    @Override
    public org.apache.iceberg.Table create() {
      return null;
    }

    @Override
    public Transaction createTransaction() {
      return null;
    }

    @Override
    public Transaction replaceTransaction() {
      return null;
    }

    @Override
    public Transaction createOrReplaceTransaction() {
      return null;
    }
  };

  @AfterEach
  public void after() {

  }

  @Test
  public void testGetTable() throws TException {
    hiveIcebergRESTCatalogClientAdapter.getTable("default", "tableName");
    Mockito.verify(restCatalog).loadTable(TableIdentifier.of("default", "tableName"));
  }

  @Test
  public void testCreateTable() throws TException {
    Table table = new Table();
    table.setTableName("tableName");
    table.setDbName("default");
    table.setSd(new StorageDescriptor());
    table.getSd().setCols(new LinkedList<FieldSchema>());
    table.setParameters(Maps.newHashMap());
    hiveIcebergRESTCatalogClientAdapter.createTable(table);
    Mockito.verify(restCatalog).buildTable(any(), any());
  }

  @Test
  public void testGetDatabase() throws TException {
    Database aDefault = hiveIcebergRESTCatalogClientAdapter.getDatabase("default");
    assertThat(aDefault.getName()).isEqualTo("default");
    Mockito.verify(restCatalog).listNamespaces(Namespace.empty());
  }
}
