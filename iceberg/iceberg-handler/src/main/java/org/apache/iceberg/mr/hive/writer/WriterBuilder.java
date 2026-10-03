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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import org.apache.hadoop.hive.ql.Context.Operation;
import org.apache.hadoop.hive.ql.security.authorization.HiveCustomStorageHandlerUtils;
import org.apache.hadoop.hive.ql.session.SessionStateUtil;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.deletes.DeleteGranularity;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.mr.Catalogs;
import org.apache.iceberg.mr.hive.IcebergTableUtil;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.DeleteFileSet;
import org.apache.iceberg.util.PropertyUtil;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;
import static org.apache.iceberg.TableProperties.DELETE_DEFAULT_FILE_FORMAT;

public class WriterBuilder {
  private final Table table;
  private Supplier<Map<String, DeleteFileSet>> rewritableDeletes;
  private final Context context;
  private final String tableName;
  private TaskAttemptID attemptID;
  private String queryId;
  private Operation operation;
  private final boolean copyOnWrite;

  // A task may write multiple output files using multiple writers. Each of them must have a unique operationId.
  private static AtomicInteger operationNum = new AtomicInteger(0);

  // To specify whether to write the actual row data while writing the delete files.
  private boolean shouldAddRowLineageColumns = false;

  private WriterBuilder(Table table, UnaryOperator<String> ops) {
    this.table = table;
    this.tableName = ops.apply(Catalogs.NAME);
    this.context = new Context(table.properties(), ops, tableName);
    this.operation = HiveCustomStorageHandlerUtils.getWriteOperation(ops, tableName);
    this.copyOnWrite = HiveCustomStorageHandlerUtils.isCopyOnWrite(ops, tableName);
  }

  public static WriterBuilder builderFor(Table table, UnaryOperator<String> ops) {
    return new WriterBuilder(table, ops);
  }

  public WriterBuilder attemptID(TaskAttemptID newAttemptID) {
    this.attemptID = newAttemptID;
    return this;
  }

  public WriterBuilder queryId(String newQueryId) {
    this.queryId = newQueryId;
    return this;
  }

  public WriterBuilder rewritableDeletes(Supplier<Map<String, DeleteFileSet>> newRewritableDeletes) {
    this.rewritableDeletes = newRewritableDeletes;
    return this;
  }

  // Test-only
  public WriterBuilder operation(Operation newOperation) {
    this.operation = newOperation;
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
        .dataSchema(shouldAddRowLineageColumns ? MetadataColumns.schemaWithRowLineage(table.schema()) : table.schema())
        .deleteFileFormat(context.deleteFileFormat())
        .build();

    HiveIcebergWriter writer;
    if (copyOnWrite) {
      writer = new HiveIcebergCopyOnWriteRecordWriter(table, writerFactory, dataFileFactory, shouldAddRowLineageColumns,
          context);
    } else {
      writer = switch (operation) {
        case DELETE ->
            new HiveIcebergDeleteWriter(table, rewritableDeletes, writerFactory, deleteFileFactory, context);
        case OTHER ->
            new HiveIcebergRecordWriter(table, writerFactory, dataFileFactory, context);
        default ->
            // Update and Merge should be split to inserts and deletes
            throw new IllegalArgumentException("Unsupported operation when creating IcebergRecordWriter: " +
                operation.name());
      };
    }

    WriterRegistry.registerWriter(attemptID, tableName, writer);
    return writer;
  }

  public WriterBuilder addRowLineageColumns(boolean isRowLineage) {
    this.shouldAddRowLineageColumns = isRowLineage;
    return this;
  }

  static class Context {

    private final FileFormat dataFileFormat;
    private final long targetDataFileSize;
    private final FileFormat deleteFileFormat;
    private final long targetDeleteFileSize;
    private final DeleteGranularity deleteGranularity;
    private final boolean useFanoutWriter;
    private final boolean inputOrdered;
    private final boolean useDVs;
    private final Set<String> missingColumns;

    Context(Map<String, String> properties, UnaryOperator<String> ops, String tableName) {
      String dataFileFormatName =
          properties.getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT);
      this.dataFileFormat = FileFormat.valueOf(dataFileFormatName.toUpperCase(Locale.ROOT));

      String deleteFileFormatName =
          properties.getOrDefault(DELETE_DEFAULT_FILE_FORMAT, dataFileFormatName);
      this.deleteFileFormat = FileFormat.valueOf(deleteFileFormatName.toUpperCase(Locale.ROOT));

      this.targetDataFileSize = PropertyUtil.propertyAsLong(properties,
          TableProperties.WRITE_TARGET_FILE_SIZE_BYTES, TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);
      this.targetDeleteFileSize = PropertyUtil.propertyAsLong(properties,
          TableProperties.DELETE_TARGET_FILE_SIZE_BYTES, TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);

      this.inputOrdered = HiveCustomStorageHandlerUtils.getWriteOperationIsSorted(ops, tableName);
      this.useFanoutWriter = !inputOrdered && IcebergTableUtil.isFanoutEnabled(properties);

      this.deleteGranularity = DeleteGranularity.PARTITION;
      this.useDVs = IcebergTableUtil.formatVersion(properties) > 2;


      this.missingColumns = Optional.ofNullable(ops.apply(SessionStateUtil.MISSING_COLUMNS))
          .map(columns -> Arrays.stream(columns.split(",")).collect(Collectors.toCollection(HashSet::new)))
          .orElse(Sets.newHashSet());
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

    public boolean useDVs() {
      return useDVs;
    }

    public Set<String> missingColumns() {
      return missingColumns;
    }
  }
}
