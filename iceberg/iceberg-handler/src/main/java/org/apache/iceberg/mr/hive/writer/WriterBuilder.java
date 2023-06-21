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
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.deletes.DeleteGranularity;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.mr.hive.IcebergTableUtil;
import org.apache.iceberg.util.PropertyUtil;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;
import static org.apache.iceberg.TableProperties.DELETE_DEFAULT_FILE_FORMAT;

public class WriterBuilder {
  private final Table table;
  private final Context context;
  private String tableName;
  private TaskAttemptID attemptID;
  private String outputId;
  private String queryId;
  private Operation operation;

  // A task may write multiple output files using multiple writers. Each of them must have a unique operationId.
  private static AtomicInteger operationNum = new AtomicInteger(0);

  // To specify whether to write the actual row data while writing the delete files.
  public static final String ICEBERG_DELETE_SKIPROWDATA = "iceberg.delete.skiprowdata";
  public static final boolean ICEBERG_DELETE_SKIPROWDATA_DEFAULT = true;

  private WriterBuilder(Table table) {
    this.table = table;
    this.context = new Context(table.properties());
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

  public WriterBuilder outputId(String identifier) {
    this.outputId = identifier;
    return this;
  }

  public WriterBuilder queryId(String newQueryId) {
    this.queryId = newQueryId;
    return this;
  }

  public WriterBuilder operation(Operation newOperation) {
    this.operation = newOperation;
    return this;
  }

  public WriterBuilder hasOrdering(boolean inputOrdered) {
    context.inputOrdered = inputOrdered;
    if (IcebergTableUtil.isFanoutEnabled(table.properties()) && !inputOrdered) {
      context.useFanoutWriter = true;
    }
    return this;
  }

  public WriterBuilder isMergeTask(boolean isMergeTaskEnabled) {
    context.isMergeTask = isMergeTaskEnabled;
    return this;
  }

  public HiveIcebergWriter build() {
    int partitionId = attemptID.getTaskID().getId();
    int taskId = attemptID.getId();
    String operationId = queryId + "-" + attemptID.getJobID() + "-" + operationNum.incrementAndGet();

    OutputFileFactory dataFileFactory = OutputFileFactory.builderFor(table, partitionId, taskId)
        .format(context.dataFileFormat())
        .operationId(operationId)
        .build();

    OutputFileFactory deleteFileFactory = OutputFileFactory.builderFor(table, partitionId, taskId)
        .format(context.deleteFileFormat())
        .operationId(operationId)
        .suffix("pos-deletes")
        .build();

    HiveFileWriterFactory writerFactory = HiveFileWriterFactory.builderFor(table)
        .dataFileFormat(context.dataFileFormat())
        .dataSchema(table.schema())
        .deleteFileFormat(context.deleteFileFormat())
        .positionDeleteRowSchema(context.skipRowData() || !context.inputOrdered() ?
            // SortingPositionOnlyDeleteWriter doesn't support rawData in delete schema
            null : table.schema())
        .build();

    HiveIcebergWriter writer;
    boolean isCOW = IcebergTableUtil.isCopyOnWriteMode(operation, table.properties()::getOrDefault);
    if (isCOW) {
      writer = new HiveIcebergCopyOnWriteRecordWriter(table, writerFactory, dataFileFactory, context);
    } else {
      switch (operation) {
        case DELETE:
          writer = new HiveIcebergDeleteWriter(table, writerFactory, deleteFileFactory, context);
          break;
        case OTHER:
          writer = new HiveIcebergRecordWriter(table, writerFactory, dataFileFactory, context);
          break;
        default:
          // Update and Merge should be splitted to inserts and deletes
          throw new IllegalArgumentException("Unsupported operation when creating IcebergRecordWriter: " +
            operation.name());
      }
    }

    WriterRegistry.registerWriter(attemptID, outputId, tableName, writer);
    return writer;
  }

  static class Context {

    private final FileFormat dataFileFormat;
    private final long targetDataFileSize;
    private final FileFormat deleteFileFormat;
    private final long targetDeleteFileSize;
    private final DeleteGranularity deleteGranularity;
    private boolean useFanoutWriter;
    private boolean inputOrdered;
    private boolean isMergeTask;
    private final boolean skipRowData;

    Context(Map<String, String> properties) {
      String dataFileFormatName =
          properties.getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT);
      this.dataFileFormat = FileFormat.valueOf(dataFileFormatName.toUpperCase(Locale.ENGLISH));

      this.targetDataFileSize = PropertyUtil.propertyAsLong(properties,
          TableProperties.WRITE_TARGET_FILE_SIZE_BYTES, TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);

      String deleteFileFormatName =
          properties.getOrDefault(DELETE_DEFAULT_FILE_FORMAT, dataFileFormatName);
      this.deleteFileFormat = FileFormat.valueOf(deleteFileFormatName.toUpperCase(Locale.ENGLISH));

      this.targetDeleteFileSize = PropertyUtil.propertyAsLong(properties,
          TableProperties.DELETE_TARGET_FILE_SIZE_BYTES, TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);

      this.skipRowData = PropertyUtil.propertyAsBoolean(properties,
        ICEBERG_DELETE_SKIPROWDATA, ICEBERG_DELETE_SKIPROWDATA_DEFAULT);

      this.deleteGranularity = DeleteGranularity.PARTITION;
    }

    FileFormat dataFileFormat() {
      return dataFileFormat;
    }

    long targetDataFileSize() {
      return targetDataFileSize;
    }

    FileFormat deleteFileFormat() {
      return deleteFileFormat;
    }

    long targetDeleteFileSize() {
      return targetDeleteFileSize;
    }

    DeleteGranularity deleteGranularity() {
      return deleteGranularity;
    }

    boolean useFanoutWriter() {
      return useFanoutWriter;
    }

    boolean inputOrdered() {
      return inputOrdered;
    }

    boolean isMergeTask() {
      return isMergeTask;
    }

    boolean skipRowData() {
      return skipRowData;
    }
  }
}
