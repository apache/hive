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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.hive.client;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.CreateTableRequest;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveSchemaUtil;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.thrift.TException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;

public class TestHiveRESTCatalogClient {

  private static HiveRESTCatalogClient spyHiveRESTCatalogClient;
  private static RESTCatalog mockRestCatalog;
  private static Catalog.TableBuilder mockTableBuilder;
  private static MockedStatic<CatalogUtil> mockCatalogUtil;

  private static final TableOperations ops = new TableOperations() {
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

  @BeforeAll
  public static void before() throws MetaException {
    Configuration configuration = new Configuration();
    configuration.set("iceberg.catalog", "ice01");
    configuration.set("iceberg.catalog.ice01.uri", "http://localhost");
    mockCatalogUtil = Mockito.mockStatic(CatalogUtil.class);
    mockRestCatalog = Mockito.mock(RESTCatalog.class);
    mockCatalogUtil.when(() -> CatalogUtil.buildIcebergCatalog(any(), any(), any())).thenReturn(mockRestCatalog);
    spyHiveRESTCatalogClient = Mockito.spy(new HiveRESTCatalogClient(configuration));
    spyHiveRESTCatalogClient.reconnect();
  }

  @BeforeEach
  public void resetMocks() {
    Mockito.reset(mockRestCatalog);

    mockTableBuilder = Mockito.mock(Catalog.TableBuilder.class);
    Mockito.when(mockTableBuilder.withPartitionSpec(any(PartitionSpec.class))).thenReturn(mockTableBuilder);
    Mockito.when(mockTableBuilder.withLocation(any())).thenReturn(mockTableBuilder);
    Mockito.when(mockTableBuilder.withSortOrder(any())).thenReturn(mockTableBuilder);
    Mockito.when(mockTableBuilder.withProperties(any())).thenReturn(mockTableBuilder);
    Mockito.doReturn(mockTableBuilder).when(mockRestCatalog).buildTable(any(), any());

    Mockito.doReturn(new BaseTable(ops, "tableName")).when(mockRestCatalog).loadTable(any());
    Namespace namespace = Namespace.of("default");
    Mockito.doReturn(Collections.singletonList(namespace)).when(mockRestCatalog).listNamespaces(any());
    Mockito.doReturn("hive").when(mockRestCatalog).name();
    Mockito.doReturn(new BaseTable(ops, "tableName")).when(mockRestCatalog).createTable(any(), any(), any(),
        any());
  }

  @AfterEach
  public void after() {

  }

  @Test
  public void testGetTable() throws TException {
    spyHiveRESTCatalogClient.getTable("default", "tableName");
    Mockito.verify(mockRestCatalog).loadTable(TableIdentifier.of("default", "tableName"));
  }

  @Test
  public void testCreateTable() throws TException {
    Table table = new Table();
    table.setTableName("tableName");
    table.setDbName("default");
    table.setSd(new StorageDescriptor());
    table.getSd().setCols(new LinkedList<>());
    table.setParameters(Maps.newHashMap());
    spyHiveRESTCatalogClient.createTable(table);
    Mockito.verify(mockRestCatalog).buildTable(any(), any());
  }

  @Test
  public void testCreatePartitionedTable() throws TException {
    Table table = new Table();
    table.setTableName("tableName");
    table.setDbName("default");
    table.setParameters(Maps.newHashMap());

    FieldSchema col1 = new FieldSchema("id", "string", "");
    FieldSchema col2 = new FieldSchema("city", "string", "");
    List<FieldSchema> cols = Arrays.asList(col1, col2);

    table.setSd(new StorageDescriptor());
    table.getSd().setCols(cols);

    Schema schema = HiveSchemaUtil.convert(cols, Collections.emptyMap(), false);
    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("city").build();
    String specString = PartitionSpecParser.toJson(spec);

    CreateTableRequest request = new CreateTableRequest(table);
    request.setEnvContext(new EnvironmentContext(
        Map.ofEntries(Map.entry(TableProperties.DEFAULT_PARTITION_SPEC, specString))));
    spyHiveRESTCatalogClient.createTable(request);

    ArgumentCaptor<PartitionSpec> captor = ArgumentCaptor.forClass(PartitionSpec.class);
    // Verify buildTable was called,
    Mockito.verify(mockRestCatalog).buildTable(any(), any());

    // Verify that withPartitionSpec was called
    Mockito.verify(mockTableBuilder).withPartitionSpec(captor.capture());

    // Assert that the correct PartitionSpec was passed to .withPartitionSpec()
    PartitionSpec capturedSpec = captor.getValue();
    assertThat(capturedSpec.isPartitioned()).isTrue();
    assertThat(capturedSpec.fields()).hasSize(1);
    assertThat(capturedSpec.fields().getFirst().sourceId()).isEqualTo(schema.findField("city").fieldId());
  }

  @Test
  public void testGetDatabase() throws TException {
    Database aDefault = spyHiveRESTCatalogClient.getDatabase("default");
    assertThat(aDefault.getName()).isEqualTo("default");
    Mockito.verify(mockRestCatalog).listNamespaces(Namespace.empty());
  }
}
