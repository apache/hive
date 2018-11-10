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

import io.druid.indexer.JobHelper;
import io.druid.indexer.SQLMetadataStorageUpdaterJobHandler;
import io.druid.metadata.MetadataStorageTablesConfig;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.storage.hdfs.HdfsDataSegmentPusher;
import io.druid.storage.hdfs.HdfsDataSegmentPusherConfig;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.LinearShardSpec;
import io.druid.timeline.partition.NoneShardSpec;
import io.druid.timeline.partition.ShardSpec;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

public class TestDruidStorageHandler {

  @Rule
  public final DerbyConnectorTestUtility.DerbyConnectorRule derbyConnectorRule = new DerbyConnectorTestUtility.DerbyConnectorRule();

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private static final String DB_NAME = "default";
  private static final String TABLE_NAME = "testName";
  private static final String DATA_SOURCE_NAME = "default.testName";

  private String segmentsTable;

  private String tableWorkingPath;

  private Configuration config;

  private DruidStorageHandler druidStorageHandler;

  private DataSegment createSegment(String location) throws IOException {
    return createSegment(location, new Interval(100, 170, DateTimeZone.UTC), "v1", new LinearShardSpec(0));
  }

  private DataSegment createSegment(String location, Interval interval, String version,
          ShardSpec shardSpec) throws IOException {
    FileUtils.writeStringToFile(new File(location), "dummySegmentData");
    DataSegment dataSegment = DataSegment.builder().dataSource(DATA_SOURCE_NAME).version(version)
            .interval(interval).shardSpec(shardSpec)
            .loadSpec(ImmutableMap.of("path", location)).build();
    return dataSegment;
  }

  @Before
  public void before() throws Throwable {
    tableWorkingPath = temporaryFolder.newFolder().getAbsolutePath();
    segmentsTable = derbyConnectorRule.metadataTablesConfigSupplier().get().getSegmentsTable();
    HashMap<String, String> params = new HashMap<>();
    params.put("external.table.purge", "TRUE");
    Mockito.when(tableMock.getParameters()).thenReturn(params);
    Mockito.when(tableMock.getPartitionKeysSize()).thenReturn(0);
    StorageDescriptor storageDes = Mockito.mock(StorageDescriptor.class);
    Mockito.when(storageDes.getBucketColsSize()).thenReturn(0);
    Mockito.when(tableMock.getSd()).thenReturn(storageDes);
    Mockito.when(tableMock.getDbName()).thenReturn(DB_NAME);
    Mockito.when(tableMock.getTableName()).thenReturn(TABLE_NAME);
    config = new Configuration();
    config.set(String.valueOf(HiveConf.ConfVars.HIVEQUERYID), "hive-" + UUID.randomUUID().toString());
    config.set(String.valueOf(HiveConf.ConfVars.DRUID_WORKING_DIR), tableWorkingPath);
    config.set(String.valueOf(HiveConf.ConfVars.DRUID_SEGMENT_DIRECTORY),
            new Path(tableWorkingPath, "finalSegmentDir").toString());
    config.set("hive.druid.maxTries", "0");
    druidStorageHandler = new DruidStorageHandler(
            derbyConnectorRule.getConnector(),
            derbyConnectorRule.metadataTablesConfigSupplier().get()
    );
    druidStorageHandler.setConf(config);
  }

  @After
  public void tearDown() {
    temporaryFolder.delete();
  }

  Table tableMock = Mockito.mock(Table.class);

  @Test
  public void testPreCreateTableWillCreateSegmentsTable() throws MetaException {

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
  public void testPreCreateTableWhenDataSourceExists() throws MetaException, IOException {
    derbyConnectorRule.getConnector().createSegmentTable();
    SQLMetadataStorageUpdaterJobHandler sqlMetadataStorageUpdaterJobHandler = new SQLMetadataStorageUpdaterJobHandler(
            derbyConnectorRule.getConnector());
    Path taskDirPath = new Path(tableWorkingPath, druidStorageHandler.makeStagingName());
    DataSegment dataSegment = createSegment(new Path(taskDirPath, "intermediatePath").toString());

    sqlMetadataStorageUpdaterJobHandler.publishSegments(segmentsTable, Arrays.asList(dataSegment),
            DruidStorageHandlerUtils.JSON_MAPPER
    );

    druidStorageHandler.preCreateTable(tableMock);
  }

  @Test
  public void testCommitCreateTablePlusCommitDropTableWithoutPurge()
          throws MetaException, IOException {
    druidStorageHandler.preCreateTable(tableMock);
    LocalFileSystem localFileSystem = FileSystem.getLocal(config);
    Path taskDirPath = new Path(tableWorkingPath, druidStorageHandler.makeStagingName());
    DataSegment dataSegment = createSegment(new Path(taskDirPath, "index.zip").toString());

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
  public void testCommitCreateTablePlusCommitDropTableWithPurge()
          throws MetaException, IOException {
    druidStorageHandler.preCreateTable(tableMock);
    LocalFileSystem localFileSystem = FileSystem.getLocal(config);
    Path taskDirPath = new Path(tableWorkingPath, druidStorageHandler.makeStagingName());
    DataSegment dataSegment = createSegment(new Path(taskDirPath, "index.zip").toString());

    Path descriptorPath = DruidStorageHandlerUtils.makeSegmentDescriptorOutputPath(dataSegment,
            new Path(taskDirPath, DruidStorageHandler.SEGMENTS_DESCRIPTOR_DIR_NAME)
    );
    DruidStorageHandlerUtils.writeSegmentDescriptor(localFileSystem, dataSegment, descriptorPath);
    druidStorageHandler.commitCreateTable(tableMock);
    Assert.assertArrayEquals(Lists.newArrayList(DATA_SOURCE_NAME).toArray(), Lists.newArrayList(
            DruidStorageHandlerUtils.getAllDataSourceNames(derbyConnectorRule.getConnector(),
                    derbyConnectorRule.metadataTablesConfigSupplier().get()
            )).toArray());
    druidStorageHandler.commitDropTable(tableMock, true);
    Assert.assertArrayEquals(Lists.newArrayList().toArray(), Lists.newArrayList(
            DruidStorageHandlerUtils.getAllDataSourceNames(derbyConnectorRule.getConnector(),
                    derbyConnectorRule.metadataTablesConfigSupplier().get()
            )).toArray());
  }

  @Test
  public void testCommitCreateEmptyTablePlusCommitDropTableWithoutPurge()
          throws MetaException, IOException {
    druidStorageHandler.preCreateTable(tableMock);
    druidStorageHandler.commitCreateTable(tableMock);
    Assert.assertArrayEquals(Lists.newArrayList().toArray(), Lists.newArrayList(
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
  public void testCommitCreateEmptyTablePlusCommitDropTableWithPurge()
          throws MetaException, IOException {
    druidStorageHandler.preCreateTable(tableMock);
    druidStorageHandler.commitCreateTable(tableMock);
    Assert.assertArrayEquals(Lists.newArrayList().toArray(), Lists.newArrayList(
            DruidStorageHandlerUtils.getAllDataSourceNames(derbyConnectorRule.getConnector(),
                    derbyConnectorRule.metadataTablesConfigSupplier().get()
            )).toArray());
    druidStorageHandler.commitDropTable(tableMock, true);
    Assert.assertArrayEquals(Lists.newArrayList().toArray(), Lists.newArrayList(
            DruidStorageHandlerUtils.getAllDataSourceNames(derbyConnectorRule.getConnector(),
                    derbyConnectorRule.metadataTablesConfigSupplier().get()
            )).toArray());
  }

  @Test
  public void testCommitInsertTable() throws MetaException, IOException {
    druidStorageHandler.preCreateTable(tableMock);
    LocalFileSystem localFileSystem = FileSystem.getLocal(config);
    Path taskDirPath = new Path(tableWorkingPath, druidStorageHandler.makeStagingName());
    DataSegment dataSegment = createSegment(new Path(taskDirPath, "index.zip").toString());
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
  public void testCommitEmptyInsertTable() throws MetaException, IOException {
    druidStorageHandler.preCreateTable(tableMock);
    druidStorageHandler.commitCreateTable(tableMock);
    Assert.assertArrayEquals(Lists.newArrayList().toArray(), Lists.newArrayList(
            DruidStorageHandlerUtils.getAllDataSourceNames(derbyConnectorRule.getConnector(),
                    derbyConnectorRule.metadataTablesConfigSupplier().get()
            )).toArray());
  }

  @Test
  public void testDeleteSegment() throws IOException, SegmentLoadingException {
    String segmentRootPath = temporaryFolder.newFolder().getAbsolutePath();
    LocalFileSystem localFileSystem = FileSystem.getLocal(config);
    Path taskDirPath = new Path(tableWorkingPath, druidStorageHandler.makeStagingName());
    DataSegment dataSegment = createSegment(new Path(taskDirPath, "index.zip").toString());
    HdfsDataSegmentPusherConfig hdfsDSPConfig = new HdfsDataSegmentPusherConfig();
    hdfsDSPConfig.setStorageDirectory(segmentRootPath);
    HdfsDataSegmentPusher hdfsDataSegmentPusher = new HdfsDataSegmentPusher(hdfsDSPConfig, config,
            DruidStorageHandlerUtils.JSON_MAPPER
    );
    Path segmentOutputPath = JobHelper
            .makeFileNamePath(new Path(segmentRootPath), localFileSystem, dataSegment,
                    JobHelper.INDEX_ZIP, hdfsDataSegmentPusher
            );
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

  @Test
  public void testCommitInsertOverwriteTable() throws MetaException, IOException {
    DerbyConnectorTestUtility connector = derbyConnectorRule.getConnector();
    MetadataStorageTablesConfig metadataStorageTablesConfig = derbyConnectorRule
            .metadataTablesConfigSupplier().get();
    druidStorageHandler.preCreateTable(tableMock);
    LocalFileSystem localFileSystem = FileSystem.getLocal(config);
    Path taskDirPath = new Path(tableWorkingPath, druidStorageHandler.makeStagingName());
    HdfsDataSegmentPusherConfig pusherConfig = new HdfsDataSegmentPusherConfig();
    pusherConfig.setStorageDirectory(config.get(String.valueOf(HiveConf.ConfVars.DRUID_SEGMENT_DIRECTORY)));
    DataSegmentPusher dataSegmentPusher = new HdfsDataSegmentPusher(pusherConfig, config, DruidStorageHandlerUtils.JSON_MAPPER);

    // This create and publish the segment to be overwritten
    List<DataSegment> existingSegments = Arrays
            .asList(createSegment(new Path(taskDirPath, DruidStorageHandlerUtils.INDEX_ZIP).toString(),
                    new Interval(100, 150, DateTimeZone.UTC), "v0", new LinearShardSpec(0)));
    DruidStorageHandlerUtils
            .publishSegmentsAndCommit(connector, metadataStorageTablesConfig, DATA_SOURCE_NAME,
                    existingSegments,
                    true,
                    config,
                    dataSegmentPusher
            );

    // This creates and publish new segment
    DataSegment dataSegment = createSegment(new Path(taskDirPath, DruidStorageHandlerUtils.INDEX_ZIP).toString(),
            new Interval(180, 250, DateTimeZone.UTC), "v1", new LinearShardSpec(0));

    Path descriptorPath = DruidStorageHandlerUtils.makeSegmentDescriptorOutputPath(dataSegment,
            new Path(taskDirPath, DruidStorageHandler.SEGMENTS_DESCRIPTOR_DIR_NAME)
    );
    DruidStorageHandlerUtils.writeSegmentDescriptor(localFileSystem, dataSegment, descriptorPath);

    druidStorageHandler.commitInsertTable(tableMock, true);
    Assert.assertArrayEquals(Lists.newArrayList(DATA_SOURCE_NAME).toArray(), Lists.newArrayList(
            DruidStorageHandlerUtils.getAllDataSourceNames(connector,
                    metadataStorageTablesConfig
            )).toArray());

    final List<DataSegment> dataSegmentList = getUsedSegmentsList(connector,
            metadataStorageTablesConfig);
    Assert.assertEquals(1, dataSegmentList.size());
    DataSegment persistedSegment = Iterables.getOnlyElement(dataSegmentList);
    Assert.assertEquals(dataSegment, persistedSegment);
    Assert.assertEquals(dataSegment.getVersion(), persistedSegment.getVersion());
    Path expectedFinalHadoopPath =  new Path(dataSegmentPusher.getPathForHadoop(), dataSegmentPusher
            .makeIndexPathName(persistedSegment, DruidStorageHandlerUtils.INDEX_ZIP));
    Assert.assertEquals(ImmutableMap.of("type", "hdfs", "path", expectedFinalHadoopPath.toString()),
            persistedSegment.getLoadSpec());
    Assert.assertEquals("dummySegmentData",
            FileUtils.readFileToString(new File(expectedFinalHadoopPath.toUri())));
  }

  @Test
  public void testCommitMultiInsertOverwriteTable() throws MetaException, IOException {
    DerbyConnectorTestUtility connector = derbyConnectorRule.getConnector();
    MetadataStorageTablesConfig metadataStorageTablesConfig = derbyConnectorRule
            .metadataTablesConfigSupplier().get();
    LocalFileSystem localFileSystem = FileSystem.getLocal(config);
    druidStorageHandler.preCreateTable(tableMock);
    Path taskDirPath = new Path(tableWorkingPath, druidStorageHandler.makeStagingName());
    HdfsDataSegmentPusherConfig pusherConfig = new HdfsDataSegmentPusherConfig();
    pusherConfig.setStorageDirectory(config.get(String.valueOf(HiveConf.ConfVars.DRUID_SEGMENT_DIRECTORY)));
    DataSegmentPusher dataSegmentPusher = new HdfsDataSegmentPusher(pusherConfig, config, DruidStorageHandlerUtils.JSON_MAPPER);

    // This create and publish the segment to be overwritten
    List<DataSegment> existingSegments = Arrays
            .asList(createSegment(new Path(taskDirPath, DruidStorageHandlerUtils.INDEX_ZIP).toString(),
                    new Interval(100, 150, DateTimeZone.UTC), "v0", new LinearShardSpec(0)));
    DruidStorageHandlerUtils
            .publishSegmentsAndCommit(connector, metadataStorageTablesConfig, DATA_SOURCE_NAME,
                    existingSegments,
                    true,
                    config,
                    dataSegmentPusher
            );
    // Check that there is one datasource with the published segment
    Assert.assertArrayEquals(Lists.newArrayList(DATA_SOURCE_NAME).toArray(), Lists.newArrayList(
            DruidStorageHandlerUtils.getAllDataSourceNames(connector,
                    metadataStorageTablesConfig
            )).toArray());

    // Sequence is the following:
    // 1) INSERT with no segments -> Original segment still present in the datasource
    // 2) INSERT OVERWRITE with no segments -> Datasource is empty
    // 3) INSERT OVERWRITE with no segments -> Datasource is empty
    // 4) INSERT with no segments -> Datasource is empty
    // 5) INSERT with one segment -> Datasource has one segment
    // 6) INSERT OVERWRITE with one segment -> Datasource has one segment
    // 7) INSERT with one segment -> Datasource has two segments
    // 8) INSERT OVERWRITE with no segments -> Datasource is empty

    // We start:
    // #1
    druidStorageHandler.commitInsertTable(tableMock, false);
    Assert.assertArrayEquals(Lists.newArrayList(DATA_SOURCE_NAME).toArray(), Lists.newArrayList(
            DruidStorageHandlerUtils.getAllDataSourceNames(connector,
                    metadataStorageTablesConfig
            )).toArray());
    Assert.assertEquals(1, getUsedSegmentsList(connector,
            metadataStorageTablesConfig).size());

    // #2
    druidStorageHandler.commitInsertTable(tableMock, true);
    Assert.assertEquals(0, getUsedSegmentsList(connector,
            metadataStorageTablesConfig).size());

    // #3
    druidStorageHandler.commitInsertTable(tableMock, true);
    Assert.assertEquals(0, getUsedSegmentsList(connector,
            metadataStorageTablesConfig).size());

    // #4
    druidStorageHandler.commitInsertTable(tableMock, true);
    Assert.assertEquals(0, getUsedSegmentsList(connector,
            metadataStorageTablesConfig).size());

    // #5
    DataSegment dataSegment1 = createSegment(new Path(taskDirPath, DruidStorageHandlerUtils.INDEX_ZIP).toString(),
            new Interval(180, 250, DateTimeZone.UTC), "v1", new LinearShardSpec(0));
    Path descriptorPath1 = DruidStorageHandlerUtils.makeSegmentDescriptorOutputPath(dataSegment1,
            new Path(taskDirPath, DruidStorageHandler.SEGMENTS_DESCRIPTOR_DIR_NAME)
    );
    DruidStorageHandlerUtils.writeSegmentDescriptor(localFileSystem, dataSegment1, descriptorPath1);
    druidStorageHandler.commitInsertTable(tableMock, false);
    Assert.assertArrayEquals(Lists.newArrayList(DATA_SOURCE_NAME).toArray(), Lists.newArrayList(
            DruidStorageHandlerUtils.getAllDataSourceNames(connector,
                    metadataStorageTablesConfig
            )).toArray());
    Assert.assertEquals(1, getUsedSegmentsList(connector,
            metadataStorageTablesConfig).size());

    // #6
    DataSegment dataSegment2 = createSegment(new Path(taskDirPath, DruidStorageHandlerUtils.INDEX_ZIP).toString(),
            new Interval(200, 250, DateTimeZone.UTC), "v1", new LinearShardSpec(0));
    Path descriptorPath2 = DruidStorageHandlerUtils.makeSegmentDescriptorOutputPath(dataSegment2,
            new Path(taskDirPath, DruidStorageHandler.SEGMENTS_DESCRIPTOR_DIR_NAME)
    );
    DruidStorageHandlerUtils.writeSegmentDescriptor(localFileSystem, dataSegment2, descriptorPath2);
    druidStorageHandler.commitInsertTable(tableMock, true);
    Assert.assertArrayEquals(Lists.newArrayList(DATA_SOURCE_NAME).toArray(), Lists.newArrayList(
            DruidStorageHandlerUtils.getAllDataSourceNames(connector,
                    metadataStorageTablesConfig
            )).toArray());
    Assert.assertEquals(1, getUsedSegmentsList(connector,
            metadataStorageTablesConfig).size());

    // #7
    DataSegment dataSegment3 = createSegment(new Path(taskDirPath, DruidStorageHandlerUtils.INDEX_ZIP).toString(),
            new Interval(100, 200, DateTimeZone.UTC), "v1", new LinearShardSpec(0));
    Path descriptorPath3 = DruidStorageHandlerUtils.makeSegmentDescriptorOutputPath(dataSegment3,
            new Path(taskDirPath, DruidStorageHandler.SEGMENTS_DESCRIPTOR_DIR_NAME)
    );
    DruidStorageHandlerUtils.writeSegmentDescriptor(localFileSystem, dataSegment3, descriptorPath3);
    druidStorageHandler.commitInsertTable(tableMock, false);
    Assert.assertArrayEquals(Lists.newArrayList(DATA_SOURCE_NAME).toArray(), Lists.newArrayList(
            DruidStorageHandlerUtils.getAllDataSourceNames(connector,
                    metadataStorageTablesConfig
            )).toArray());
    Assert.assertEquals(2, getUsedSegmentsList(connector,
            metadataStorageTablesConfig).size());

    // #8
    druidStorageHandler.commitInsertTable(tableMock, true);
    Assert.assertEquals(0, getUsedSegmentsList(connector,
            metadataStorageTablesConfig).size());
  }

  private List<DataSegment> getUsedSegmentsList(DerbyConnectorTestUtility connector,
          final MetadataStorageTablesConfig metadataStorageTablesConfig) {
    return connector.getDBI()
            .withHandle(new HandleCallback<List<DataSegment>>() {
              @Override
              public List<DataSegment> withHandle(Handle handle) throws Exception {
                return handle
                        .createQuery(String.format(
                                "SELECT payload FROM %s WHERE used=true ORDER BY created_date ASC",
                                metadataStorageTablesConfig.getSegmentsTable()))
                        .map(new ResultSetMapper<DataSegment>() {

                          @Override
                          public DataSegment map(int i, ResultSet resultSet,
                                  StatementContext statementContext)
                                  throws SQLException {
                            try {
                              return DruidStorageHandlerUtils.JSON_MAPPER.readValue(
                                      resultSet.getBytes("payload"),
                                      DataSegment.class
                              );
                            } catch (IOException e) {
                              throw Throwables.propagate(e);
                            }
                          }
                        }).list();
              }
            });
  }

  @Test
  public void testCommitInsertIntoTable() throws MetaException, IOException {
    DerbyConnectorTestUtility connector = derbyConnectorRule.getConnector();
    MetadataStorageTablesConfig metadataStorageTablesConfig = derbyConnectorRule
            .metadataTablesConfigSupplier().get();
    druidStorageHandler.preCreateTable(tableMock);
    LocalFileSystem localFileSystem = FileSystem.getLocal(config);
    Path taskDirPath = new Path(tableWorkingPath, druidStorageHandler.makeStagingName());
    List<DataSegment> existingSegments = Arrays
            .asList(createSegment(new Path(taskDirPath, DruidStorageHandlerUtils.INDEX_ZIP).toString(),
                    new Interval(100, 150, DateTimeZone.UTC), "v0", new LinearShardSpec(1)));
    HdfsDataSegmentPusherConfig pusherConfig = new HdfsDataSegmentPusherConfig();
    pusherConfig.setStorageDirectory(config.get(String.valueOf(HiveConf.ConfVars.DRUID_SEGMENT_DIRECTORY)));
    DataSegmentPusher dataSegmentPusher = new HdfsDataSegmentPusher(pusherConfig, config, DruidStorageHandlerUtils.JSON_MAPPER);

    DruidStorageHandlerUtils
            .publishSegmentsAndCommit(connector, metadataStorageTablesConfig, DATA_SOURCE_NAME,
                    existingSegments,
                    true,
                    config,
                    dataSegmentPusher
            );
    DataSegment dataSegment = createSegment(new Path(taskDirPath, DruidStorageHandlerUtils.INDEX_ZIP).toString(),
            new Interval(100, 150, DateTimeZone.UTC), "v1", new LinearShardSpec(0));
    Path descriptorPath = DruidStorageHandlerUtils.makeSegmentDescriptorOutputPath(dataSegment,
            new Path(taskDirPath, DruidStorageHandler.SEGMENTS_DESCRIPTOR_DIR_NAME)
    );
    DruidStorageHandlerUtils.writeSegmentDescriptor(localFileSystem, dataSegment, descriptorPath);
    druidStorageHandler.commitInsertTable(tableMock, false);
    Assert.assertArrayEquals(Lists.newArrayList(DATA_SOURCE_NAME).toArray(), Lists.newArrayList(
            DruidStorageHandlerUtils.getAllDataSourceNames(connector,
                    metadataStorageTablesConfig
            )).toArray());

    final List<DataSegment> dataSegmentList = getUsedSegmentsList(connector,
            metadataStorageTablesConfig);
    Assert.assertEquals(2, dataSegmentList.size());

    DataSegment persistedSegment = dataSegmentList.get(1);
    // Insert into appends to old version
    Assert.assertEquals("v0", persistedSegment.getVersion());
    Assert.assertTrue(persistedSegment.getShardSpec() instanceof LinearShardSpec);
    Assert.assertEquals(2, persistedSegment.getShardSpec().getPartitionNum());

    Path expectedFinalHadoopPath =  new Path(dataSegmentPusher.getPathForHadoop(), dataSegmentPusher
            .makeIndexPathName(persistedSegment, DruidStorageHandlerUtils.INDEX_ZIP));

    Assert.assertEquals(ImmutableMap.of("type", "hdfs", "path", expectedFinalHadoopPath.toString()),
            persistedSegment.getLoadSpec());
    Assert.assertEquals("dummySegmentData",
            FileUtils.readFileToString(new File(expectedFinalHadoopPath.toUri())));
  }

  @Test
  public void testInsertIntoAppendOneMorePartition() throws MetaException, IOException {
    DerbyConnectorTestUtility connector = derbyConnectorRule.getConnector();
    MetadataStorageTablesConfig metadataStorageTablesConfig = derbyConnectorRule
            .metadataTablesConfigSupplier().get();
    druidStorageHandler.preCreateTable(tableMock);
    LocalFileSystem localFileSystem = FileSystem.getLocal(config);
    Path taskDirPath = new Path(tableWorkingPath, druidStorageHandler.makeStagingName());
    HdfsDataSegmentPusherConfig pusherConfig = new HdfsDataSegmentPusherConfig();
    pusherConfig.setStorageDirectory(config.get(String.valueOf(HiveConf.ConfVars.DRUID_SEGMENT_DIRECTORY)));
    DataSegmentPusher dataSegmentPusher = new HdfsDataSegmentPusher(pusherConfig, config, DruidStorageHandlerUtils.JSON_MAPPER);

    List<DataSegment> existingSegments = Arrays
            .asList(createSegment(new Path(taskDirPath, DruidStorageHandlerUtils.INDEX_ZIP).toString(),
                    new Interval(100, 150, DateTimeZone.UTC), "v0", new LinearShardSpec(0)));
    DruidStorageHandlerUtils
            .publishSegmentsAndCommit(connector, metadataStorageTablesConfig, DATA_SOURCE_NAME,
                    existingSegments,
                    true,
                    config,
                    dataSegmentPusher
            );

    DataSegment dataSegment = createSegment(new Path(taskDirPath, DruidStorageHandlerUtils.INDEX_ZIP).toString(),
            new Interval(100, 150, DateTimeZone.UTC), "v0", new LinearShardSpec(0));
    Path descriptorPath = DruidStorageHandlerUtils.makeSegmentDescriptorOutputPath(dataSegment,
            new Path(taskDirPath, DruidStorageHandler.SEGMENTS_DESCRIPTOR_DIR_NAME)
    );
    DruidStorageHandlerUtils.writeSegmentDescriptor(localFileSystem, dataSegment, descriptorPath);
    druidStorageHandler.commitInsertTable(tableMock, false);
    Assert.assertArrayEquals(Lists.newArrayList(DATA_SOURCE_NAME).toArray(), Lists.newArrayList(
            DruidStorageHandlerUtils.getAllDataSourceNames(connector,
                    metadataStorageTablesConfig
            )).toArray());

    final List<DataSegment> dataSegmentList = getUsedSegmentsList(connector,
            metadataStorageTablesConfig);
    Assert.assertEquals(2, dataSegmentList.size());

    DataSegment persistedSegment = dataSegmentList.get(1);
    Assert.assertEquals("v0", persistedSegment.getVersion());
    Assert.assertTrue(persistedSegment.getShardSpec() instanceof LinearShardSpec);
    Assert.assertEquals(1, persistedSegment.getShardSpec().getPartitionNum());

    Path expectedFinalHadoopPath =  new Path(dataSegmentPusher.getPathForHadoop(), dataSegmentPusher
            .makeIndexPathName(persistedSegment, DruidStorageHandlerUtils.INDEX_ZIP));

    Assert.assertEquals(ImmutableMap.of("type", "hdfs", "path", expectedFinalHadoopPath.toString()),
            persistedSegment.getLoadSpec());
    Assert.assertEquals("dummySegmentData",
            FileUtils.readFileToString(new File(expectedFinalHadoopPath.toUri())));
  }

  @Test
  public void testCommitInsertIntoWhenDestinationSegmentFileExist()
          throws MetaException, IOException {
    DerbyConnectorTestUtility connector = derbyConnectorRule.getConnector();
    MetadataStorageTablesConfig metadataStorageTablesConfig = derbyConnectorRule
            .metadataTablesConfigSupplier().get();
    druidStorageHandler.preCreateTable(tableMock);
    LocalFileSystem localFileSystem = FileSystem.getLocal(config);
    Path taskDirPath = new Path(tableWorkingPath, druidStorageHandler.makeStagingName());
    List<DataSegment> existingSegments = Arrays
            .asList(createSegment(new Path(taskDirPath, "index_old.zip").toString(),
                    new Interval(100, 150, DateTimeZone.UTC), "v0", new LinearShardSpec(1)));
    HdfsDataSegmentPusherConfig pusherConfig = new HdfsDataSegmentPusherConfig();
    pusherConfig.setStorageDirectory(config.get(String.valueOf(HiveConf.ConfVars.DRUID_SEGMENT_DIRECTORY)));
    DataSegmentPusher dataSegmentPusher = new HdfsDataSegmentPusher(pusherConfig, config, DruidStorageHandlerUtils.JSON_MAPPER);
    DruidStorageHandlerUtils
            .publishSegmentsAndCommit(connector, metadataStorageTablesConfig, DATA_SOURCE_NAME,
                    existingSegments,
                    true,
                    config,
                    dataSegmentPusher
            );
    DataSegment dataSegment = createSegment(new Path(taskDirPath, "index.zip").toString(),
            new Interval(100, 150, DateTimeZone.UTC), "v1", new LinearShardSpec(0));
    Path descriptorPath = DruidStorageHandlerUtils.makeSegmentDescriptorOutputPath(dataSegment,
            new Path(taskDirPath, DruidStorageHandler.SEGMENTS_DESCRIPTOR_DIR_NAME)
    );
    DruidStorageHandlerUtils.writeSegmentDescriptor(localFileSystem, dataSegment, descriptorPath);

    // Create segment file at the destination location with LinearShardSpec(2)
    DataSegment segment = createSegment(new Path(taskDirPath, "index_conflict.zip").toString(),
            new Interval(100, 150, DateTimeZone.UTC), "v1", new LinearShardSpec(1));
    Path segmentPath = new Path(dataSegmentPusher.getPathForHadoop(), dataSegmentPusher.makeIndexPathName(segment, DruidStorageHandlerUtils.INDEX_ZIP));
    FileUtils.writeStringToFile(new File(segmentPath.toUri()), "dummy");

    druidStorageHandler.commitInsertTable(tableMock, false);
    Assert.assertArrayEquals(Lists.newArrayList(DATA_SOURCE_NAME).toArray(), Lists.newArrayList(
            DruidStorageHandlerUtils.getAllDataSourceNames(connector,
                    metadataStorageTablesConfig
            )).toArray());

    final List<DataSegment> dataSegmentList = getUsedSegmentsList(connector,
            metadataStorageTablesConfig);
    Assert.assertEquals(2, dataSegmentList.size());

    DataSegment persistedSegment = dataSegmentList.get(1);
    // Insert into appends to old version
    Assert.assertEquals("v0", persistedSegment.getVersion());
    Assert.assertTrue(persistedSegment.getShardSpec() instanceof LinearShardSpec);
    // insert into should skip and increment partition number to 3
    Assert.assertEquals(2, persistedSegment.getShardSpec().getPartitionNum());
    Path expectedFinalHadoopPath =  new Path(dataSegmentPusher.getPathForHadoop(), dataSegmentPusher
            .makeIndexPathName(persistedSegment, DruidStorageHandlerUtils.INDEX_ZIP));


    Assert.assertEquals(ImmutableMap.of("type", "hdfs", "path", expectedFinalHadoopPath.toString()),
            persistedSegment.getLoadSpec());
    Assert.assertEquals("dummySegmentData",
            FileUtils.readFileToString(new File(expectedFinalHadoopPath.toUri())));
  }

  @Test(expected = MetaException.class)
  public void testCommitInsertIntoWithConflictingIntervalSegment()
          throws MetaException, IOException {
    DerbyConnectorTestUtility connector = derbyConnectorRule.getConnector();
    MetadataStorageTablesConfig metadataStorageTablesConfig = derbyConnectorRule
            .metadataTablesConfigSupplier().get();
    druidStorageHandler.preCreateTable(tableMock);
    LocalFileSystem localFileSystem = FileSystem.getLocal(config);
    Path taskDirPath = new Path(tableWorkingPath, druidStorageHandler.makeStagingName());
    List<DataSegment> existingSegments = Arrays.asList(
            createSegment(new Path(taskDirPath, "index_old_1.zip").toString(),
                    new Interval(100, 150, DateTimeZone.UTC),
                    "v0", new LinearShardSpec(0)),
            createSegment(new Path(taskDirPath, "index_old_2.zip").toString(),
                    new Interval(150, 200, DateTimeZone.UTC),
                    "v0", new LinearShardSpec(0)),
            createSegment(new Path(taskDirPath, "index_old_3.zip").toString(),
                    new Interval(200, 300, DateTimeZone.UTC),
                    "v0", new LinearShardSpec(0)));
    HdfsDataSegmentPusherConfig pusherConfig = new HdfsDataSegmentPusherConfig();
    pusherConfig.setStorageDirectory(taskDirPath.toString());
    DataSegmentPusher dataSegmentPusher = new HdfsDataSegmentPusher(pusherConfig, config, DruidStorageHandlerUtils.JSON_MAPPER);
    DruidStorageHandlerUtils
            .publishSegmentsAndCommit(connector, metadataStorageTablesConfig, DATA_SOURCE_NAME,
                    existingSegments,
                    true,
                    config,
                    dataSegmentPusher
            );

    // Try appending segment with conflicting interval
    DataSegment conflictingSegment = createSegment(new Path(taskDirPath, DruidStorageHandlerUtils.INDEX_ZIP).toString(),
            new Interval(100, 300, DateTimeZone.UTC), "v1", new LinearShardSpec(0));
    Path descriptorPath = DruidStorageHandlerUtils
            .makeSegmentDescriptorOutputPath(conflictingSegment,
                    new Path(taskDirPath, DruidStorageHandler.SEGMENTS_DESCRIPTOR_DIR_NAME)
            );
    DruidStorageHandlerUtils
            .writeSegmentDescriptor(localFileSystem, conflictingSegment, descriptorPath);
    druidStorageHandler.commitInsertTable(tableMock, false);
  }

  @Test(expected = MetaException.class)
  public void testCommitInsertIntoWithNonExtendableSegment() throws MetaException, IOException {
    DerbyConnectorTestUtility connector = derbyConnectorRule.getConnector();
    MetadataStorageTablesConfig metadataStorageTablesConfig = derbyConnectorRule
            .metadataTablesConfigSupplier().get();
    druidStorageHandler.preCreateTable(tableMock);
    LocalFileSystem localFileSystem = FileSystem.getLocal(config);
    Path taskDirPath = new Path(tableWorkingPath, druidStorageHandler.makeStagingName());
    List<DataSegment> existingSegments = Arrays
            .asList(createSegment(new Path(taskDirPath, "index_old_1.zip").toString(),
                    new Interval(100, 150, DateTimeZone.UTC), "v0", new NoneShardSpec()),
                    createSegment(new Path(taskDirPath, "index_old_2.zip").toString(),
                            new Interval(200, 250, DateTimeZone.UTC), "v0", new LinearShardSpec(0)),
                    createSegment(new Path(taskDirPath, "index_old_3.zip").toString(),
                            new Interval(250, 300, DateTimeZone.UTC), "v0", new LinearShardSpec(0)));
    HdfsDataSegmentPusherConfig pusherConfig = new HdfsDataSegmentPusherConfig();
    pusherConfig.setStorageDirectory(taskDirPath.toString());
    DataSegmentPusher dataSegmentPusher = new HdfsDataSegmentPusher(pusherConfig, config, DruidStorageHandlerUtils.JSON_MAPPER);
    DruidStorageHandlerUtils
            .publishSegmentsAndCommit(connector, metadataStorageTablesConfig, DATA_SOURCE_NAME,
                    existingSegments,
                    true,
                    config,
                    dataSegmentPusher
            );

    // Try appending to non extendable shard spec
    DataSegment conflictingSegment = createSegment(new Path(taskDirPath, DruidStorageHandlerUtils.INDEX_ZIP).toString(),
            new Interval(100, 150, DateTimeZone.UTC), "v1", new LinearShardSpec(0));
    Path descriptorPath = DruidStorageHandlerUtils
            .makeSegmentDescriptorOutputPath(conflictingSegment,
                    new Path(taskDirPath, DruidStorageHandler.SEGMENTS_DESCRIPTOR_DIR_NAME)
            );
    DruidStorageHandlerUtils
            .writeSegmentDescriptor(localFileSystem, conflictingSegment, descriptorPath);

    druidStorageHandler.commitInsertTable(tableMock, false);

  }

}
