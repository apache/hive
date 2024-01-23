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

package org.apache.iceberg.mr.hive;

import java.util.Properties;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.security.authorization.HiveCustomStorageHandlerUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.util.Progressable;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.mr.Catalogs;
import org.apache.iceberg.mr.hive.writer.HiveIcebergWriter;
import org.apache.iceberg.mr.hive.writer.WriterBuilder;
import org.apache.iceberg.mr.mapred.Container;
import org.apache.parquet.hadoop.ParquetOutputFormat;

public class HiveIcebergOutputFormat<T> implements OutputFormat<NullWritable, Container<Record>>,
    HiveOutputFormat<NullWritable, Container<Record>> {
  private static final String DELETE_FILE_THREAD_POOL_SIZE = "iceberg.delete.file.thread.pool.size";
  private static final int DELETE_FILE_THREAD_POOL_SIZE_DEFAULT = 10;

  @Override
  public FileSinkOperator.RecordWriter getHiveRecordWriter(JobConf jc, Path finalOutPath, Class valueClass,
      boolean isCompressed, Properties tableAndSerDeProperties, Progressable progress) {
    return writer(jc);
  }

  @Override
  public org.apache.hadoop.mapred.RecordWriter<NullWritable, Container<Record>> getRecordWriter(FileSystem ignored,
      JobConf job, String name, Progressable progress) {
    return writer(job);
  }

  @Override
  public void checkOutputSpecs(FileSystem ignored, JobConf job) {
    // Not doing any check.
  }

  private static HiveIcebergWriter writer(JobConf jc) {
    TaskAttemptID taskAttemptID = TezUtil.taskAttemptWrapper(jc);
    // It gets the config from the FileSinkOperator which has its own config for every target table
    Table table = HiveIcebergStorageHandler.table(jc, jc.get(hive_metastoreConstants.META_TABLE_NAME));
    String tableName = jc.get(Catalogs.NAME);
    int poolSize = jc.getInt(DELETE_FILE_THREAD_POOL_SIZE, DELETE_FILE_THREAD_POOL_SIZE_DEFAULT);

    setWriterLevelConfiguration(jc, table);
    return WriterBuilder.builderFor(table)
        .queryId(jc.get(HiveConf.ConfVars.HIVE_QUERY_ID.varname))
        .tableName(tableName)
        .attemptID(taskAttemptID)
        .poolSize(poolSize)
        .operation(HiveCustomStorageHandlerUtils.getWriteOperation(jc, tableName))
        .isFanoutEnabled(!HiveCustomStorageHandlerUtils.getWriteOperationIsSorted(jc, tableName))
        .build();
  }

  private static void setWriterLevelConfiguration(JobConf jc, Table table) {
    final String writeFormat = table.properties().get("write.format.default");
    if (writeFormat == null || "PARQUET".equalsIgnoreCase(writeFormat)) {
      if (table.properties().get(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES) == null &&
          jc.get(ParquetOutputFormat.BLOCK_SIZE) != null) {
        table.properties().put(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, jc.get(ParquetOutputFormat.BLOCK_SIZE));
      }
      if (table.properties().get(TableProperties.PARQUET_COMPRESSION) == null &&
          jc.get(ParquetOutputFormat.COMPRESSION) != null) {
        table.properties().put(TableProperties.PARQUET_COMPRESSION, jc.get(ParquetOutputFormat.COMPRESSION));
      }
    }
  }
}
