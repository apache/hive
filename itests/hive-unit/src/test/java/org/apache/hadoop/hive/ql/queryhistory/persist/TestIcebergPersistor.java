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
package org.apache.hadoop.hive.ql.queryhistory.persist;

import org.apache.hadoop.hive.ql.queryhistory.schema.QueryHistorySchemaTestUtils;
import org.apache.iceberg.mr.mapred.Container;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.queryhistory.QueryHistoryService;
import org.apache.hadoop.hive.ql.queryhistory.persist.QueryHistoryPersistor;
import org.apache.hadoop.hive.ql.queryhistory.schema.ExampleQueryHistoryRecord;
import org.apache.hadoop.hive.ql.queryhistory.schema.QueryHistoryRecord;
import org.apache.hadoop.hive.ql.queryhistory.schema.IcebergQueryHistoryRecord;
import org.apache.hadoop.hive.ql.queryhistory.schema.QueryHistorySchema;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

public class TestIcebergPersistor {
  private static final Logger LOG = LoggerFactory.getLogger(TestIcebergPersistor.class);
  private final Queue<QueryHistoryRecord> queryHistoryQueue = new LinkedBlockingQueue<>();

  /*
   * This unit test asserts that the created record is persisted as expected and the values made their way to the
   * iceberg table.
   */
  @Test
  public void testPersistRecord() throws Exception {
    HiveConf conf = new HiveConf();
    // don't mess with the HIVE_LOCKS table and other stuff in this test
    conf.set("iceberg.engine.hive.lock-enabled", "false");
    conf.setBoolVar(HiveConf.ConfVars.HIVE_CLI_TEZ_INITIALIZE_SESSION, false);
    conf.setIntVar(HiveConf.ConfVars.HIVE_QUERY_HISTORY_SERVICE_PERSIST_MAX_BATCH_SIZE, 0);
    conf.setVar(HiveConf.ConfVars.HIVE_QUERY_HISTORY_SERVICE_PERSISTOR_CLASS, IcebergPersistorForTest.class.getName());

    QueryHistoryRecord record = new ExampleQueryHistoryRecord();

    IcebergPersistorForTest persistor = (IcebergPersistorForTest) QueryHistoryService.createPersistor(conf);
    persistor.init(conf, new QueryHistorySchema());

    queryHistoryQueue.add(record);
    persistor.persist(queryHistoryQueue);

    checkRecords(conf, persistor, record);
  }

  private void checkRecords(HiveConf conf, IcebergPersistorForTest persistor, QueryHistoryRecord record)
      throws Exception {
    JobConf jobConf = new JobConf(conf);
    // force table to be reloaded from Catalogs to see latest Snapshot
    jobConf.unset("iceberg.mr.serialized.table.sys.query_history");
    Container container = readRecords(persistor, jobConf);

    QueryHistoryRecord deserialized = new IcebergQueryHistoryRecord((GenericRecord) container.get());
    LOG.info("Deserialized record: {}", deserialized.toLongString());

    Assert.assertTrue("Original and deserialized records should contain equal values",
        QueryHistorySchemaTestUtils.queryHistoryRecordsAreEqual(record, deserialized));
  }

  private Container readRecords(IcebergPersistorForTest persistor, JobConf jobConf) throws Exception {
    InputFormat<?, ?> inputFormat = persistor.storageHandler.getInputFormatClass().newInstance();
    File[] dataFiles =
        new File(persistor.tableDesc.getProperties().get("location").toString().replaceAll("^[a-zA-Z]+:", "") +
            "/data/cluster_id=" + ExampleQueryHistoryRecord.CLUSTER_ID).listFiles(
            file -> file.isFile() && file.getName().toLowerCase().endsWith(".orc"));
    FileInputFormat.setInputPaths(jobConf, new Path(dataFiles[0].toURI()));

    InputSplit[] splits = inputFormat.getSplits(jobConf, 1);
    Container container = null;
    try (RecordReader<Void, Container<Record>> reader =
             (RecordReader<Void, Container<Record>>) inputFormat.getRecordReader(splits[0], jobConf, Reporter.NULL)) {
      container = reader.createValue();
      reader.next(reader.createKey(), container);
    }
    return container;
  }
}
