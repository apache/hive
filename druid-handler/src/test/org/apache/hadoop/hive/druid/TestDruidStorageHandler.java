/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.druid;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.druid.indexer.JobHelper;
import io.druid.indexer.SQLMetadataStorageUpdaterJobHandler;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;
import org.skife.jdbi.v2.Handle;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;

public class TestDruidStorageHandler {

  @Rule
  public final DerbyConnectorTestUtility.DerbyConnectorRule derbyConnectorRule = new DerbyConnectorTestUtility.DerbyConnectorRule();

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private static final String DATA_SOURCE_NAME = "testName";

  private String segmentsTable;

  private String tableWorkingPath;

  private DataSegment dataSegment = DataSegment.builder().dataSource(DATA_SOURCE_NAME).version("v1")
          .interval(new Interval(100, 170)).shardSpec(NoneShardSpec.instance()).build();

  @Before
  public void before() throws Throwable {
    tableWorkingPath = temporaryFolder.newFolder().getAbsolutePath();
    segmentsTable = derbyConnectorRule.metadataTablesConfigSupplier().get().getSegmentsTable();
    Map<String, String> mockMap = ImmutableMap.of(Constants.DRUID_DATA_SOURCE, DATA_SOURCE_NAME);
    Mockito.when(tableMock.getParameters()).thenReturn(mockMap);
    Mockito.when(tableMock.getPartitionKeysSize()).thenReturn(0);
    StorageDescriptor storageDes = Mockito.mock(StorageDescriptor.class);
    Mockito.when(storageDes.getBucketColsSize()).thenReturn(0);
    Mockito.when(tableMock.getSd()).thenReturn(storageDes);
    Mockito.when(tableMock.getDbName()).thenReturn(DATA_SOURCE_NAME);
  }

  Table tableMock = Mockito.mock(Table.class);

  @Test
  public void testPreCreateTableWillCreateSegmentsTable() throws MetaException {
    DruidStorageHandler druidStorageHandler = new DruidStorageHandler(
            derbyConnectorRule.getConnector(),
            new SQLMetadataStorageUpdaterJobHandler(derbyConnectorRule.getConnector()),
            derbyConnectorRule.metadataTablesConfigSupplier().get(),
            null
    );

    try (Handle handle = derbyConnectorRule.getConnector().getDBI().open()) {
      Assert.assertFalse(derbyConnectorRule.getConnector()
              .tableExists(handle,
                      segmentsTable
              ));
      druidStorageHandler.preCreateTable(tableMock);
      Assert.assertTrue(derbyConnectorRule.getConnector()
              .tableExists(handle,
                      segmentsTable
              ));
    }

  }

  @Test(expected = MetaException.class)
  public void testPreCreateTableWhenDataSourceExists() throws MetaException {
    derbyConnectorRule.getConnector().createSegmentTable();
    SQLMetadataStorageUpdaterJobHandler sqlMetadataStorageUpdaterJobHandler = new SQLMetadataStorageUpdaterJobHandler(
            derbyConnectorRule.getConnector());
    sqlMetadataStorageUpdaterJobHandler.publishSegments(segmentsTable, Arrays.asList(dataSegment),
            DruidStorageHandlerUtils.JSON_MAPPER
    );
    DruidStorageHandler druidStorageHandler = new DruidStorageHandler(
            derbyConnectorRule.getConnector(),
            new SQLMetadataStorageUpdaterJobHandler(derbyConnectorRule.getConnector()),
            derbyConnectorRule.metadataTablesConfigSupplier().get(),
            null
    );
    druidStorageHandler.preCreateTable(tableMock);
  }

  @Test
  public void testCommitCreateTablePlusCommitDropTableWithoutPurge()
          throws MetaException, IOException {
    DruidStorageHandler druidStorageHandler = new DruidStorageHandler(
            derbyConnectorRule.getConnector(),
            new SQLMetadataStorageUpdaterJobHandler(derbyConnectorRule.getConnector()),
            derbyConnectorRule.metadataTablesConfigSupplier().get(),
            null
    );
    druidStorageHandler.preCreateTable(tableMock);
    Configuration config = new Configuration();
    config.set(String.valueOf(HiveConf.ConfVars.HIVEQUERYID), UUID.randomUUID().toString());
    config.set(String.valueOf(HiveConf.ConfVars.DRUID_WORKING_DIR), tableWorkingPath);
    druidStorageHandler.setConf(config);
    LocalFileSystem localFileSystem = FileSystem.getLocal(config);
    Path taskDirPath = new Path(tableWorkingPath, druidStorageHandler.makeStagingName());
    Path descriptorPath = DruidStorageHandlerUtils.makeSegmentDescriptorOutputPath(dataSegment,
            new Path(taskDirPath, DruidStorageHandler.SEGMENTS_DESCRIPTOR_DIR_NAME)
    );
    DruidStorageHandlerUtils.writeSegmentDescriptor(localFileSystem, dataSegment, descriptorPath);
    druidStorageHandler.commitCreateTable(tableMock);
    Assert.assertArrayEquals(Lists.newArrayList(DATA_SOURCE_NAME).toArray(), Lists.newArrayList(
            DruidStorageHandlerUtils.getAllDataSourceNames(derbyConnectorRule.getConnector(),
                    derbyConnectorRule.metadataTablesConfigSupplier().get()
            )).toArray());
    druidStorageHandler.commitDropTable(tableMock, false);
    Assert.assertArrayEquals(Lists.newArrayList().toArray(), Lists.newArrayList(
            DruidStorageHandlerUtils.getAllDataSourceNames(derbyConnectorRule.getConnector(),
                    derbyConnectorRule.metadataTablesConfigSupplier().get()
            )).toArray());

  }

  @Test
  public void testCommitInsertTable() throws MetaException, IOException {
    DruidStorageHandler druidStorageHandler = new DruidStorageHandler(
            derbyConnectorRule.getConnector(),
            new SQLMetadataStorageUpdaterJobHandler(derbyConnectorRule.getConnector()),
            derbyConnectorRule.metadataTablesConfigSupplier().get(),
            null
    );
    druidStorageHandler.preCreateTable(tableMock);
    Configuration config = new Configuration();
    config.set(String.valueOf(HiveConf.ConfVars.HIVEQUERYID), UUID.randomUUID().toString());
    config.set(String.valueOf(HiveConf.ConfVars.DRUID_WORKING_DIR), tableWorkingPath);
    druidStorageHandler.setConf(config);
    LocalFileSystem localFileSystem = FileSystem.getLocal(config);
    Path taskDirPath = new Path(tableWorkingPath, druidStorageHandler.makeStagingName());
    Path descriptorPath = DruidStorageHandlerUtils.makeSegmentDescriptorOutputPath(dataSegment,
            new Path(taskDirPath, DruidStorageHandler.SEGMENTS_DESCRIPTOR_DIR_NAME)
    );
    DruidStorageHandlerUtils.writeSegmentDescriptor(localFileSystem, dataSegment, descriptorPath);
    druidStorageHandler.commitCreateTable(tableMock);
    Assert.assertArrayEquals(Lists.newArrayList(DATA_SOURCE_NAME).toArray(), Lists.newArrayList(
            DruidStorageHandlerUtils.getAllDataSourceNames(derbyConnectorRule.getConnector(),
                    derbyConnectorRule.metadataTablesConfigSupplier().get()
            )).toArray());
  }

  @Test
  public void testDeleteSegment() throws IOException, SegmentLoadingException {
    DruidStorageHandler druidStorageHandler = new DruidStorageHandler(
            derbyConnectorRule.getConnector(),
            new SQLMetadataStorageUpdaterJobHandler(derbyConnectorRule.getConnector()),
            derbyConnectorRule.metadataTablesConfigSupplier().get(),
            null
    );

    String segmentRootPath = temporaryFolder.newFolder().getAbsolutePath();
    Configuration config = new Configuration();
    druidStorageHandler.setConf(config);
    LocalFileSystem localFileSystem = FileSystem.getLocal(config);

    Path segmentOutputPath = JobHelper
            .makeSegmentOutputPath(new Path(segmentRootPath), localFileSystem, dataSegment);
    Path indexPath = new Path(segmentOutputPath, "index.zip");
    DataSegment dataSegmentWithLoadspect = DataSegment.builder(dataSegment).loadSpec(
            ImmutableMap.<String, Object>of("path", indexPath)).build();
    OutputStream outputStream = localFileSystem.create(indexPath, true);
    outputStream.close();
    Assert.assertTrue("index file is not created ??", localFileSystem.exists(indexPath));
    Assert.assertTrue(localFileSystem.exists(segmentOutputPath));

    druidStorageHandler.deleteSegment(dataSegmentWithLoadspect);
    // path format -- > .../dataSource/interval/version/partitionNum/xxx.zip
    Assert.assertFalse("Index file still there ??", localFileSystem.exists(indexPath));
    // path format of segmentOutputPath -- > .../dataSource/interval/version/partitionNum/
    Assert.assertFalse("PartitionNum directory still there ??",
            localFileSystem.exists(segmentOutputPath)
    );
    Assert.assertFalse("Version directory still there ??",
            localFileSystem.exists(segmentOutputPath.getParent())
    );
    Assert.assertFalse("Interval directory still there ??",
            localFileSystem.exists(segmentOutputPath.getParent().getParent())
    );
    Assert.assertFalse("Data source directory still there ??",
            localFileSystem.exists(segmentOutputPath.getParent().getParent().getParent())
    );
  }
}
