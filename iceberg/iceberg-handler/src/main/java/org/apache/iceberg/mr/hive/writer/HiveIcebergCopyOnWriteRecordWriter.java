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

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.ql.io.AffectedRowsProvidingRecordWriter;
import org.apache.hadoop.io.Writable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.io.DataWriteResult;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.mr.hive.FilesForCommit;
import org.apache.iceberg.mr.hive.IcebergAcidUtil;
import org.apache.iceberg.mr.hive.writer.WriterBuilder.Context;
import org.apache.iceberg.mr.mapred.Container;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

class HiveIcebergCopyOnWriteRecordWriter extends HiveIcebergDefaultWriter
    implements AffectedRowsProvidingRecordWriter {

  private final GenericRecord rowDataTemplate;
  private final Map<String, DataFile> replacedDataFiles;
  private long affectedRows;

  HiveIcebergCopyOnWriteRecordWriter(Table table, HiveFileWriterFactory writerFactory,
      OutputFileFactory deleteFileFactory, boolean shouldAddRowLineage, Context context) {
    super(table, writerFactory, deleteFileFactory, context);

    this.rowDataTemplate = GenericRecord.create(
        shouldAddRowLineage ? MetadataColumns.schemaWithRowLineage(table.schema()) : table.schema());
    this.replacedDataFiles = new LinkedHashMap<>();
  }

  @Override
  public void write(Writable row) throws IOException {
    Record record = ((Container<Record>) row).get();
    PositionDelete<Record> positionDelete = IcebergAcidUtil.getPositionDelete(record, rowDataTemplate);
    Record rowData = rowDataTemplate;

    if (positionDelete.pos() < 0) {
      // A negative position is a matched-row marker whose magnitude contributes to the affected-row count.
      int specId = IcebergAcidUtil.parseSpecId(record);
      String filePath = positionDelete.path().toString();
      replacedDataFiles.computeIfAbsent(filePath, ignored -> {
        affectedRows += -positionDelete.pos();
        return DataFiles.builder(specs.get(specId))
            .withPath(filePath)
            .withPartition(partition(rowData, specId))
            .withFileSizeInBytes(0)
            .withRecordCount(0)
            .build();
      });
    } else {
      if (positionDelete.path() == null) {
        affectedRows++;
      }
      write(rowData);
    }
  }

  @Override
  public long getAffectedRows() {
    return affectedRows;
  }

  @Override
  public FilesForCommit files() {
    List<DataFile> dataFiles = ((DataWriteResult) writer.result()).dataFiles();
    return FilesForCommit.onlyData(dataFiles, Lists.newArrayList(replacedDataFiles.values()));
  }
}
