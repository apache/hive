/**
 * Licensed to the Apache Software Foundation (ASF) under one§
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.queryhistory.repository;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.queryhistory.repository.QueryHistoryRepository;
import org.apache.hadoop.hive.ql.queryhistory.schema.DummyRecord;
import org.apache.hadoop.hive.ql.queryhistory.schema.IcebergRecord;
import org.apache.hadoop.hive.ql.queryhistory.schema.Record;
import org.apache.hadoop.hive.ql.queryhistory.schema.Schema;
import org.apache.hadoop.hive.ql.queryhistory.schema.QueryHistorySchemaTestUtils;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hive.common.util.ReflectionUtil;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.mr.mapred.Container;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

public class TestIcebergRepository {
  private static final Logger LOG = LoggerFactory.getLogger(TestIcebergRepository.class);
  private final Queue<Record> queryHistoryQueue = new LinkedBlockingQueue<>();

  private HiveConf conf;
  private IcebergRepository repository;

  /*
   * This unit test asserts that the created record is persisted as expected and the values made their way to the
   * iceberg table.
   */
  @Test
  public void testPersistRecord() throws Exception {
    Record historyRecord = new DummyRecord();

    createIcebergRepository(conf);

    queryHistoryQueue.add(historyRecord);
    repository.flush(queryHistoryQueue);

    checkRecords(conf, repository, historyRecord);
  }

  /**
   * Useful defaults for iceberg repository testing. Can be overridden in test cases if needed.
   * @return HiveConf to be used
   */
  private HiveConf newHiveConf() {
    HiveConf testConf = new HiveConf();
    testConf.set("iceberg.engine.hive.lock-enabled", "false");
    testConf.setBoolVar(HiveConf.ConfVars.HIVE_CLI_TEZ_INITIALIZE_SESSION, false);
    testConf.setIntVar(HiveConf.ConfVars.HIVE_QUERY_HISTORY_BATCH_SIZE, 0); // sync persist on flush
    testConf.setVar(HiveConf.ConfVars.HIVE_QUERY_HISTORY_REPOSITORY_CLASS, IcebergRepositoryForTest.class.getName());
    return testConf;
  }

  private void createIcebergRepository(HiveConf conf) {
    try {
      repository = (IcebergRepository) ReflectionUtil.newInstance(
          conf.getClassByName(conf.get(HiveConf.ConfVars.HIVE_QUERY_HISTORY_REPOSITORY_CLASS.varname)), conf);
      repository.init(conf, new Schema());
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  private void checkRecords(HiveConf conf, IcebergRepository repository, Record historyRecord)
      throws Exception {
    JobConf jobConf = new JobConf(conf);
    // force table to be reloaded from Catalogs to see latest Snapshot
    jobConf.unset("iceberg.mr.serialized.table.sys.query_history");
    Container container = readRecords(repository, jobConf);

    Record deserialized = new IcebergRecord((GenericRecord) container.get());

    Assert.assertTrue("Original and deserialized records should contain equal values",
        QueryHistorySchemaTestUtils.queryHistoryRecordsAreEqual(historyRecord, deserialized));
  }

  private Container readRecords(IcebergRepository repository, JobConf jobConf) throws Exception {
    InputFormat<?, ?> inputFormat = repository.storageHandler.getInputFormatClass().newInstance();
    String tableLocation = getTableLocation(repository);
    File[] dataFiles =
        new File(tableLocation +
            "/data/cluster_id=" + DummyRecord.CLUSTER_ID).listFiles(
            file -> file.isFile() && file.getName().toLowerCase().endsWith(".orc"));
    FileInputFormat.setInputPaths(jobConf, new Path(dataFiles[0].toURI()));

    jobConf.set("iceberg.mr.table.identifier", QueryHistoryRepository.QUERY_HISTORY_DB_TABLE_NAME);
    jobConf.set("iceberg.mr.table.location", tableLocation);
    InputSplit[] splits = inputFormat.getSplits(jobConf, 1);

    Container container = null;
    try (RecordReader<Void, Container<org.apache.iceberg.data.Record>> reader =
             (RecordReader<Void, Container<org.apache.iceberg.data.Record>>) inputFormat.getRecordReader(splits[0],
                 jobConf, Reporter.NULL)) {
      container = reader.createValue();
      reader.next(reader.createKey(), container);
    }
    return container;
  }

  private String getTableLocation(IcebergRepository repository) {
    return repository.tableDesc.getProperties().get("location").toString().replaceAll("^[a-zA-Z]+:", "");
  }

  @Test
  public void testExpireSnapshots() throws Exception {
    conf.setIntVar(HiveConf.ConfVars.HIVE_QUERY_HISTORY_ICEBERG_SNAPSHOT_EXPIRY_INVERVAL_SECONDS, 1);
    conf.setVar(HiveConf.ConfVars.HIVE_QUERY_HISTORY_REPOSITORY_CLASS,
        IcebergRepositoryWithShortSnapshotAge.class.getName());
    createIcebergRepository(conf);

    String metadataDirectory = getTableLocation(repository) + "/metadata";
    assertSnapshotFiles(metadataDirectory, 0);
    Record historyRecord = new DummyRecord();

    // flush a record, 1 snapshot is visible
    queryHistoryQueue.add(historyRecord);
    repository.flush(queryHistoryQueue);
    assertSnapshotFiles(metadataDirectory, 1);

    // flush another record, 2 snapshots are visible
    queryHistoryQueue.add(historyRecord);
    repository.flush(queryHistoryQueue);
    assertSnapshotFiles(metadataDirectory, 2);

    // wait for the expiry to take effect, after which only one snapshot will be visible again.
    Thread.sleep(3000);
    assertSnapshotFiles(metadataDirectory, 1);
  }

  private void assertSnapshotFiles(String metadataDirectory, int numberForSnapshotFiles) {
    File[] matchingFiles = new File(metadataDirectory).listFiles((dir, name) -> name.startsWith("snap-"));
    List<File> files = Optional.ofNullable(matchingFiles).map(Arrays::asList).orElse(Collections.emptyList());
    LOG.debug("Snapshot files found: {}", files);
    Assert.assertEquals(numberForSnapshotFiles, files.size());
  }

  /**
   * Base testing iceberg repository class that takes care of table purge between unit test cases.
   */
  public static class IcebergRepositoryForTest extends IcebergRepository {
    @Override
    protected Table createTableObject() {
      Table table = super.createTableObject();
      table.setProperty("external.table.purge", "TRUE"); // cleanup between tests to prevent data/metadata collision
      return table;
    }
  }

  /**
   * Test class extending IcebergRepositoryForTest. This is because the snapshot age property is not exposed
   * as an easy hive configuration, instead, it's supposed to be changed by ALTER TABLE commands. We need a low value
   * here to test snapshot expiry.
   */
  public static class IcebergRepositoryWithShortSnapshotAge extends IcebergRepositoryForTest {
    @Override
    int getSnapshotMaxAge() {
      return 500;
    }
  }

  @Before
  public void before() {
    conf = newHiveConf();
    SessionState.start(conf);
  }

  @After
  public void after() throws Exception {
    if (repository != null) {
      Hive.get().dropTable(QueryHistoryRepository.QUERY_HISTORY_DB_TABLE_NAME, true);
    }
  }
}
