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

package org.apache.iceberg.mr.hive.writer;

import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hive.ql.Context.Operation;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.mr.hive.IcebergTableUtil;
import org.apache.iceberg.util.PropertyUtil;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;
import static org.apache.iceberg.TableProperties.DELETE_DEFAULT_FILE_FORMAT;

public class WriterBuilder {
  private final Table table;
  private String tableName;
  private TaskAttemptID attemptID;
  private String queryId;
  private int poolSize;
  private Operation operation;
  private boolean fanoutEnabled;
  private boolean isMergeTask;

  // A task may write multiple output files using multiple writers. Each of them must have a unique operationId.
  private static AtomicInteger operationNum = new AtomicInteger(0);

  // To specify whether to write the actual row data while writing the delete files.
  public static final String ICEBERG_DELETE_SKIPROWDATA = "iceberg.delete.skiprowdata";
  public static final String ICEBERG_DELETE_SKIPROWDATA_DEFAULT = "true";

  private WriterBuilder(Table table) {
    this.table = table;
  }

  public static WriterBuilder builderFor(Table table) {
    return new WriterBuilder(table);
  }

  public WriterBuilder tableName(String newTableName) {
    this.tableName = newTableName;
    return this;
  }

  public WriterBuilder attemptID(TaskAttemptID newAttemptID) {
    this.attemptID = newAttemptID;
    return this;
  }

  public WriterBuilder queryId(String newQueryId) {
    this.queryId = newQueryId;
    return this;
  }

  public WriterBuilder poolSize(int newPoolSize) {
    this.poolSize = newPoolSize;
    return this;
  }

  public WriterBuilder operation(Operation newOperation) {
    this.operation = newOperation;
    return this;
  }

  public WriterBuilder isFanoutEnabled(boolean isFanoutEnabled) {
    this.fanoutEnabled = isFanoutEnabled;
    return this;
  }

  public WriterBuilder isMergeTask(boolean mergeTaskEnabled) {
    this.isMergeTask = mergeTaskEnabled;
    return this;
  }

  public HiveIcebergWriter build() {
    Map<String, String> properties = table.properties();

    String dataFileFormatName = properties.getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT);
    FileFormat dataFileFormat = FileFormat.valueOf(dataFileFormatName.toUpperCase(Locale.ENGLISH));

    String deleteFileFormatName = properties.getOrDefault(DELETE_DEFAULT_FILE_FORMAT, dataFileFormatName);
    FileFormat deleteFileFormat = FileFormat.valueOf(deleteFileFormatName.toUpperCase(Locale.ENGLISH));

    long targetFileSize = PropertyUtil.propertyAsLong(table.properties(), TableProperties.WRITE_TARGET_FILE_SIZE_BYTES,
        TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);

    boolean skipRowData =
        Boolean.parseBoolean(properties.getOrDefault(ICEBERG_DELETE_SKIPROWDATA, ICEBERG_DELETE_SKIPROWDATA_DEFAULT));

    Schema dataSchema = table.schema();
    FileIO io = table.io();
    Map<Integer, PartitionSpec> specs = table.specs();
    int currentSpecId = table.spec().specId();
    int partitionId = attemptID.getTaskID().getId();
    int taskId = attemptID.getId();
    String operationId = queryId + "-" + attemptID.getJobID() + "-" + operationNum.incrementAndGet();
    OutputFileFactory outputFileFactory = OutputFileFactory.builderFor(table, partitionId, taskId)
        .format(dataFileFormat)
        .operationId("data-" + operationId)
        .build();

    OutputFileFactory deleteOutputFileFactory = OutputFileFactory.builderFor(table, partitionId, taskId)
        .format(deleteFileFormat)
        .operationId("delete-" + operationId)
        .build();

    HiveFileWriterFactory writerFactory =
        new HiveFileWriterFactory(table, dataFileFormat, dataSchema, null, deleteFileFormat, null, null, null,
            skipRowData ? null : dataSchema);

    HiveIcebergWriter writer;
    if (IcebergTableUtil.isCopyOnWriteMode(operation, properties::getOrDefault)) {
      writer = new HiveIcebergCopyOnWriteRecordWriter(dataSchema, specs, currentSpecId, writerFactory,
        outputFileFactory, io, targetFileSize);
    } else {
      switch (operation) {
        case DELETE:
          writer = new HiveIcebergDeleteWriter(dataSchema, specs, writerFactory, deleteOutputFileFactory,
            io, targetFileSize, skipRowData, isMergeTask);
          break;
        case OTHER:
          writer = new HiveIcebergRecordWriter(dataSchema, specs, currentSpecId, writerFactory, outputFileFactory,
            io, targetFileSize, fanoutEnabled);
          break;
        default:
          // Update and Merge should be splitted to inserts and deletes
          throw new IllegalArgumentException("Unsupported operation when creating IcebergRecordWriter: " +
            operation.name());
      }
    }

    WriterRegistry.registerWriter(attemptID, tableName, writer);
    return writer;
  }
}
