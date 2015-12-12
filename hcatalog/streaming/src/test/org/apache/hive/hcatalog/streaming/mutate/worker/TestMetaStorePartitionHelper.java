/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.hcatalog.streaming.mutate.worker;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TestMetaStorePartitionHelper {

  private static final Path TABLE_PATH = new Path("table");
  private static final String TABLE_LOCATION = TABLE_PATH.toString();

  private static final FieldSchema PARTITION_KEY_A = new FieldSchema("A", "string", null);
  private static final FieldSchema PARTITION_KEY_B = new FieldSchema("B", "string", null);
  private static final List<FieldSchema> PARTITION_KEYS = Arrays.asList(PARTITION_KEY_A, PARTITION_KEY_B);
  private static final Path PARTITION_PATH = new Path(TABLE_PATH, "a=1/b=2");
  private static final String PARTITION_LOCATION = PARTITION_PATH.toString();

  private static final String DATABASE_NAME = "db";
  private static final String TABLE_NAME = "one";

  private static final List<String> UNPARTITIONED_VALUES = Collections.emptyList();
  private static final List<String> PARTITIONED_VALUES = Arrays.asList("1", "2");

  @Mock
  private IMetaStoreClient mockClient;
  @Mock
  private Table mockTable;
  private StorageDescriptor tableStorageDescriptor = new StorageDescriptor();

  @Mock
  private Partition mockPartition;
  @Mock
  private StorageDescriptor mockPartitionStorageDescriptor;
  @Captor
  private ArgumentCaptor<Partition> partitionCaptor;

  private PartitionHelper helper;

  @Before
  public void injectMocks() throws Exception {
    when(mockClient.getTable(DATABASE_NAME, TABLE_NAME)).thenReturn(mockTable);
    when(mockTable.getDbName()).thenReturn(DATABASE_NAME);
    when(mockTable.getTableName()).thenReturn(TABLE_NAME);
    when(mockTable.getPartitionKeys()).thenReturn(PARTITION_KEYS);
    when(mockTable.getSd()).thenReturn(tableStorageDescriptor);
    tableStorageDescriptor.setLocation(TABLE_LOCATION);

    when(mockClient.getPartition(DATABASE_NAME, TABLE_NAME, PARTITIONED_VALUES)).thenReturn(mockPartition);
    when(mockPartition.getSd()).thenReturn(mockPartitionStorageDescriptor);
    when(mockPartitionStorageDescriptor.getLocation()).thenReturn(PARTITION_LOCATION);

    helper = new MetaStorePartitionHelper(mockClient, DATABASE_NAME, TABLE_NAME, TABLE_PATH);
  }

  @Test
  public void getPathForUnpartitionedTable() throws Exception {
    Path path = helper.getPathForPartition(UNPARTITIONED_VALUES);
    assertThat(path, is(TABLE_PATH));
    verifyZeroInteractions(mockClient);
  }

  @Test
  public void getPathForPartitionedTable() throws Exception {
    Path path = helper.getPathForPartition(PARTITIONED_VALUES);
    assertThat(path, is(PARTITION_PATH));
  }

  @Test
  public void createOnUnpartitionTableDoesNothing() throws Exception {
    helper.createPartitionIfNotExists(UNPARTITIONED_VALUES);
    verifyZeroInteractions(mockClient);
  }

  @Test
  public void createOnPartitionTable() throws Exception {
    helper.createPartitionIfNotExists(PARTITIONED_VALUES);

    verify(mockClient).add_partition(partitionCaptor.capture());
    Partition actual = partitionCaptor.getValue();
    assertThat(actual.getSd().getLocation(), is(PARTITION_LOCATION));
    assertThat(actual.getValues(), is(PARTITIONED_VALUES));
  }

  @Test
  public void closeSucceeds() throws IOException {
    helper.close();
    verify(mockClient).close();
  }

}
