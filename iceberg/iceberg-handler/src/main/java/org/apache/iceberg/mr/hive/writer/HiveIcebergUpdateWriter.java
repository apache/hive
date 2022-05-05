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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.io.Writable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.mr.hive.FilesForCommit;
import org.apache.iceberg.mr.hive.IcebergAcidUtil;
import org.apache.iceberg.mr.mapred.Container;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;

/**
 * Hive update queries are converted to an insert statement where the result contains the updated rows.
 * The schema is defined by {@link IcebergAcidUtil#createFileReadSchemaForUpdate(List, Table)}}.
 * The rows are sorted based on the requirements of the {@link HiveIcebergRecordWriter}.
 * The {@link HiveIcebergBufferedDeleteWriter} needs to handle out of order records.
 */
class HiveIcebergUpdateWriter implements HiveIcebergWriter {

  private final HiveIcebergBufferedDeleteWriter deleteWriter;
  private final HiveIcebergRecordWriter insertWriter;
  private final Container<Record> container;

  HiveIcebergUpdateWriter(Schema schema, Map<Integer, PartitionSpec> specs, int currentSpecId,
      FileWriterFactory<Record> fileWriterFactory, OutputFileFactory fileFactory, OutputFileFactory deleteFileFactory,
      FileFormat format, FileFormat deleteFormat, FileIO io, long targetFileSize, int poolSize) {
    this.deleteWriter = new HiveIcebergBufferedDeleteWriter(schema, specs, fileWriterFactory, deleteFileFactory,
        deleteFormat, io, targetFileSize, poolSize);
    this.insertWriter = new HiveIcebergRecordWriter(schema, specs, currentSpecId, fileWriterFactory, fileFactory,
        format, io, targetFileSize);
    this.container = new Container<>();
    Record record = GenericRecord.create(schema);
    container.set(record);
  }

  @Override
  public void write(Writable row) throws IOException {
    deleteWriter.write(row);
    IcebergAcidUtil.populateWithNewValues(((Container<Record>) row).get(), container.get());
    insertWriter.write(container);
  }

  @Override
  public void close(boolean abort) throws IOException {
    deleteWriter.close(abort);
    insertWriter.close(abort);
  }

  @Override
  public FilesForCommit files() {
    Collection<DataFile> dataFiles = insertWriter.files().dataFiles();
    Collection<DeleteFile> deleteFiles = deleteWriter.files().deleteFiles();
    return new FilesForCommit(dataFiles, deleteFiles);
  }

  @VisibleForTesting
  HiveIcebergWriter deleteWriter() {
    return deleteWriter;
  }
}
