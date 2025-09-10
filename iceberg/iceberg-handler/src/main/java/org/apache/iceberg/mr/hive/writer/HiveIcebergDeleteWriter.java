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
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.io.Writable;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.mr.hive.FilesForCommit;
import org.apache.iceberg.mr.hive.IcebergAcidUtil;
import org.apache.iceberg.mr.hive.writer.WriterBuilder.Context;
import org.apache.iceberg.mr.mapred.Container;
import org.apache.iceberg.util.DeleteFileSet;

class HiveIcebergDeleteWriter extends HiveIcebergWriterBase {

  private final GenericRecord rowDataTemplate;
  private final boolean skipRowData;
  private final boolean isMergeTask;

  HiveIcebergDeleteWriter(
      Table table, Map<String, DeleteFileSet> rewritableDeletes,
      HiveFileWriterFactory writerFactory, OutputFileFactory deleteFileFactory,
      Context context) {
    super(table, newDeleteWriter(table, rewritableDeletes, writerFactory, deleteFileFactory, context));

    this.rowDataTemplate = GenericRecord.create(table.schema());
    this.skipRowData = context.skipRowData();
    this.isMergeTask = context.isMergeTask();
  }

  @Override
  public void write(Writable row) throws IOException {
    Record rec = ((Container<Record>) row).get();
    PositionDelete<Record> positionDelete = IcebergAcidUtil.getPositionDelete(rec, rowDataTemplate);
    int specId = IcebergAcidUtil.parseSpecId(rec);
    PartitionKey partitionKey = isMergeTask ? IcebergAcidUtil.parsePartitionKey(rec) :
        partition(positionDelete.row(), specId);
    if (skipRowData) {
      // Set null as the row data as we intend to avoid writing the actual row data in the delete file.
      positionDelete.set(positionDelete.path(), positionDelete.pos(), null);
    }
    writer.write(positionDelete, specs.get(specId), partitionKey);
  }

  @Override
  public FilesForCommit files() {
    DeleteWriteResult deleteWriteResult = (DeleteWriteResult) writer.result();
    List<DeleteFile> deleteFiles = deleteWriteResult.deleteFiles();
    Set<CharSequence> referencedDataFiles = deleteWriteResult.referencedDataFiles();
    List<DeleteFile> rewrittenDeleteFiles = deleteWriteResult.rewrittenDeleteFiles();
    return FilesForCommit.onlyDelete(deleteFiles, referencedDataFiles, rewrittenDeleteFiles);
  }
}
