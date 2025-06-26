/*
 * Licensed to the Apache Software Foundation (ASF) under one
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
 **/
package org.apache.hadoop.hive.ql.queryhistory.repository;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.queryhistory.schema.Record;
import org.apache.hadoop.hive.ql.queryhistory.schema.Schema;
import org.apache.hadoop.hive.ql.security.authorization.HiveCustomStorageHandlerUtils;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionStateUtil;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.tez.mapreduce.hadoop.mapred.TaskAttemptContextImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class IcebergRepository extends AbstractRepository implements QueryHistoryRepository {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergRepository.class);
  private static final String ICEBERG_STORAGE_HANDLER = "org.apache.iceberg.mr.hive.HiveIcebergStorageHandler";
  private static final String ICEBERG_WORKER_THREAD_NAME_FORMAT = "query-history-iceberg-worker-pool-%d";

  private HiveOutputFormat<?, ?> outputFormat;
  private Serializer serializer;
  @VisibleForTesting
  HiveStorageHandler storageHandler;
  @VisibleForTesting
  TableDesc tableDesc;
  private ExecutorService icebergExecutor;

  @Override
  public void init(HiveConf conf, Schema schema) {
    super.init(conf, schema);
    icebergExecutor = Executors.newFixedThreadPool(2,
        (new ThreadFactoryBuilder()).setDaemon(true).setNameFormat(ICEBERG_WORKER_THREAD_NAME_FORMAT).build());
  }

  @Override
  protected Table createTable(Hive hive, Database db) throws HiveException {
    LOG.info("Creating iceberg Query History table...");

    Table table = createTableObject();

    // iceberg specific
    table.setProperty(org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE,
        ICEBERG_STORAGE_HANDLER);
    table.setProperty("table_type", "ICEBERG");
    table.setProperty("write.format.default", "orc");
    // set the default max snapshot age to 1 day for query history
    // this is applied only during table creation, it can be manually altered afterward.
    table.setProperty("history.expire.max-snapshot-age-ms", Integer.toString(24 * 60 * 60 * 1000));
    table.setProperty(hive_metastoreConstants.META_TABLE_NAME, QUERY_HISTORY_DB_TABLE_NAME);

    table.setFields(schema.getFields());

    hive.createTable(table, false);

    table = hive.getTable(QUERY_HISTORY_DB_NAME, QUERY_HISTORY_TABLE_NAME);
    table.setProperty("iceberg.mr.table.location", table.getDataLocation().toString());

    return table;
  }

  @Override
  protected void postInitTable(Table table) throws Exception {
    this.tableDesc = Utilities.getTableDesc(table);

    Map<String, String> map = new HashMap<>();

    this.storageHandler = table.getStorageHandler();
    storageHandler.configureOutputJobProperties(tableDesc, map);

    //need to obtain a serialized table for later usage
    map.forEach((key, value) -> conf.set(key, value));

    conf.set(hive_metastoreConstants.META_TABLE_NAME, QUERY_HISTORY_DB_TABLE_NAME);

    HiveCustomStorageHandlerUtils.setWriteOperation(conf, QUERY_HISTORY_DB_TABLE_NAME, Context.Operation.OTHER);
    this.outputFormat = HiveFileFormatUtils.getHiveOutputFormat(conf, tableDesc);

    this.serializer = tableDesc.getDeserializer(conf);
  }

  @Override
  public void flush(Queue<Record> records) {
    int numRecords = records.size();
    LOG.info("Persisting {} records", numRecords);
    if (numRecords == 0) {
      return;
    }

    try {
      prepareConfForWrite();
      JobConf jobConf = new JobConf(conf);

      RecordWriter writer = outputFormat.getHiveRecordWriter(jobConf, null, null, false, null, null);
      while (!records.isEmpty()) {
        writeRecord(writer, records.poll());
      }

      writer.close(false);

      TaskAttemptContext context = new TaskAttemptContextImpl(jobConf, TaskAttemptID.forName(
          conf.get("mapred.task.id")), null);
      OutputCommitter committer = storageHandler.getOutputCommitter();
      committer.commitTask(context);
      storageHandler.storageHandlerCommit(HiveConf.getProperties(conf), Context.Operation.OTHER, icebergExecutor);

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void writeRecord(RecordWriter writer, Record record) throws Exception {
    Writable w = record.serialize(serializer);
    writer.write(w);
  }

  private void prepareConfForWrite() {
    GregorianCalendar gc = new GregorianCalendar();
    String queryHistoryId = "queryhistory_" + String.format("%1$4d%2$02d%3$02d%4$02d%5$02d%6$02d",
        gc.get(Calendar.YEAR), gc.get(Calendar.MONTH) + 1, gc.get(Calendar.DAY_OF_MONTH), gc.get(Calendar.HOUR_OF_DAY),
        gc.get(Calendar.MINUTE), gc.get(Calendar.SECOND)) + "_" + UUID.randomUUID();
    conf.set(HiveConf.ConfVars.HIVE_QUERY_ID.varname, queryHistoryId);
    SessionState.getSessionConf().set(HiveConf.ConfVars.HIVE_QUERY_ID.varname, queryHistoryId);

    long currentTime = System.currentTimeMillis();
    String jobIdPostFix = String.format("%d_0000", currentTime);
    conf.set("mapred.task.id", String.format("attempt_%s_r_000000_0", jobIdPostFix));
    conf.set("iceberg.mr.output.tables", tableDesc.getTableName());

    QueryState queryState = new QueryState.Builder().withGenerateNewQueryId(false).withHiveConf(conf).build();
    SessionState.get().addQueryState(queryHistoryId, queryState);

    String jobId = String.format("job_%s", jobIdPostFix);
    SessionStateUtil.addCommitInfo(SessionState.getSessionConf(), tableDesc.getTableName(), jobId, 1,
        Maps.fromProperties(tableDesc.getProperties()));
  }
}
