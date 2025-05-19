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
import org.apache.hadoop.hive.ql.ServiceContext;
import org.apache.hadoop.hive.ql.queryhistory.QueryHistoryService;
import org.apache.hadoop.hive.ql.queryhistory.schema.DummyRecord;
import org.apache.hadoop.hive.ql.queryhistory.schema.IcebergRecord;
import org.apache.hadoop.hive.ql.queryhistory.schema.Record;
import org.apache.hadoop.hive.ql.queryhistory.schema.QueryHistorySchemaTestUtils;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.mr.mapred.Container;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

public class TestIcebergRepository {
  private static final Logger LOG = LoggerFactory.getLogger(TestIcebergRepository.class);
  private static final ServiceContext serviceContext = new ServiceContext(() -> DummyRecord.SERVER_HOST,
      () ->  DummyRecord.SERVER_PORT);
  private final Queue<Record> queryHistoryQueue = new LinkedBlockingQueue<>();

  /*
   * This unit test asserts that the created record is persisted as expected and the values made their way to the
   * iceberg table.
   */
  @Test
  public void testPersistRecord() throws Exception {
    HiveConf conf = new HiveConf();
    conf.set("iceberg.engine.hive.lock-enabled", "false");

    conf.setBoolVar(HiveConf.ConfVars.HIVE_CLI_TEZ_INITIALIZE_SESSION, false);
    conf.setIntVar(HiveConf.ConfVars.HIVE_QUERY_HISTORY_BATCH_SIZE, 0);

    Record record = new DummyRecord();

    QueryHistoryService service = QueryHistoryService.newInstance(conf, serviceContext);
    service.start();
    IcebergRepository repository = (IcebergRepository) service.getRepository();

    queryHistoryQueue.add(record);
    repository.flush(queryHistoryQueue);

    checkRecords(conf, repository, record);
  }

  private void checkRecords(HiveConf conf, IcebergRepository repository, Record record)
      throws Exception {
    JobConf jobConf = new JobConf(conf);
    // force table to be reloaded from Catalogs to see latest Snapshot
    jobConf.unset("iceberg.mr.serialized.table.sys.query_history");
    Container container = readRecords(repository, jobConf);

    Record deserialized = new IcebergRecord((GenericRecord) container.get());

    Assert.assertTrue("Original and deserialized records should contain equal values",
        QueryHistorySchemaTestUtils.queryHistoryRecordsAreEqual(record, deserialized));
  }

  private Container readRecords(IcebergRepository repository, JobConf jobConf) throws Exception {
    InputFormat<?, ?> inputFormat = repository.storageHandler.getInputFormatClass().newInstance();
    String tableLocation = repository.tableDesc.getProperties().get("location").toString();
    File[] dataFiles =
        new File(tableLocation.replaceAll("^[a-zA-Z]+:", "") +
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
}
