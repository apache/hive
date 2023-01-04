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
import org.apache.hadoop.io.Writable;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.io.ClusteredPositionDeleteWriter;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.mr.hive.FilesForCommit;
import org.apache.iceberg.mr.hive.IcebergAcidUtil;
import org.apache.iceberg.mr.mapred.Container;

class HiveIcebergDeleteWriter extends HiveIcebergWriterBase {

  private final GenericRecord rowDataTemplate;
  private final boolean skipRowData;

  HiveIcebergDeleteWriter(Schema schema, Map<Integer, PartitionSpec> specs,
      FileWriterFactory<Record> writerFactory, OutputFileFactory fileFactory, FileIO io,
      long targetFileSize, boolean skipRowData) {
    super(schema, specs, io,
        new ClusteredPositionDeleteWriter<>(writerFactory, fileFactory, io, targetFileSize));
    rowDataTemplate = GenericRecord.create(schema);
    this.skipRowData = skipRowData;
  }

  @Override
  public void write(Writable row) throws IOException {
    Record rec = ((Container<Record>) row).get();
    PositionDelete<Record> positionDelete = IcebergAcidUtil.getPositionDelete(rec, rowDataTemplate);
    int specId = IcebergAcidUtil.parseSpecId(rec);
    Record rowData = positionDelete.row();
    if (skipRowData) {
      // Set null as the row data as we intend to avoid writing the actual row data in the delete file.
      positionDelete.set(positionDelete.path(), positionDelete.pos(), null);
    }
    writer.write(positionDelete, specs.get(specId), partition(rowData, specId));
  }

  @Override
  public FilesForCommit files() {
    List<DeleteFile> deleteFiles = ((DeleteWriteResult) writer.result()).deleteFiles();
    return FilesForCommit.onlyDelete(deleteFiles);
  }
}
