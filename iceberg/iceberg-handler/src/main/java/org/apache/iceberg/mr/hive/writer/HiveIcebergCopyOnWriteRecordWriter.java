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
import java.util.List;
import java.util.Set;
import org.apache.hadoop.io.Writable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.hive.HiveSchemaUtil;
import org.apache.iceberg.io.DataWriteResult;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.mr.hive.FilesForCommit;
import org.apache.iceberg.mr.hive.IcebergAcidUtil;
import org.apache.iceberg.mr.hive.writer.WriterBuilder.Context;
import org.apache.iceberg.mr.mapred.Container;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;

class HiveIcebergCopyOnWriteRecordWriter extends HiveIcebergWriterBase {

  private final int currentSpecId;
  private final Set<String> missingColumns;
  private final List<Types.NestedField> missingOrStructFields;

  private final GenericRecord rowDataTemplate;
  private final List<DataFile> replacedDataFiles;

  private final HiveFileWriterFactory fileWriterFactory;

  HiveIcebergCopyOnWriteRecordWriter(Table table, HiveFileWriterFactory writerFactory,
      OutputFileFactory deleteFileFactory, Context context) {
    super(table, newDataWriter(table, writerFactory, deleteFileFactory, context));

    this.currentSpecId = table.spec().specId();
    this.rowDataTemplate = GenericRecord.create(table.schema());
    this.replacedDataFiles = Lists.newArrayList();

    this.missingColumns = context.missingColumns();
    this.missingOrStructFields = specs.get(currentSpecId).schema().asStruct().fields().stream()
        .filter(field -> missingColumns.contains(field.name()) || field.type().isStructType())
        .toList();
    this.fileWriterFactory = writerFactory;
  }

  @Override
  public void write(Writable row) throws IOException {
    Record record = ((Container<Record>) row).get();
    PositionDelete<Record> positionDelete = IcebergAcidUtil.getPositionDelete(record, rowDataTemplate);
    Record rowData = positionDelete.row();

    if (positionDelete.pos() < 0) {
      int specId = IcebergAcidUtil.parseSpecId(record);
      DataFile dataFile =
          DataFiles.builder(specs.get(specId))
            .withPath(positionDelete.path().toString())
            .withPartition(partition(rowData, specId))
            .withFileSizeInBytes(0)
            .withRecordCount(0)
            .build();
      replacedDataFiles.add(dataFile);
    } else {
      HiveSchemaUtil.setDefaultValues(rowData, missingOrStructFields, missingColumns);
      fileWriterFactory.initialize(() -> rowData);
      writer.write(rowData, specs.get(currentSpecId), partition(rowData, currentSpecId));
    }
  }

  @Override
  public FilesForCommit files() {
    List<DataFile> dataFiles = ((DataWriteResult) writer.result()).dataFiles();
    return FilesForCommit.onlyData(dataFiles, replacedDataFiles);
  }
}
