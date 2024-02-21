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
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.ClusteredDataWriter;
import org.apache.iceberg.io.DataWriteResult;
import org.apache.iceberg.io.FanoutDataWriter;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.PartitioningWriter;
import org.apache.iceberg.mr.hive.FilesForCommit;
import org.apache.iceberg.mr.mapred.Container;

class HiveIcebergRecordWriter extends HiveIcebergWriterBase {

  private final int currentSpecId;

  HiveIcebergRecordWriter(Schema schema, Map<Integer, PartitionSpec> specs, int currentSpecId,
      FileWriterFactory<Record> fileWriterFactory, OutputFileFactory fileFactory, FileIO io,
      long targetFileSize, boolean fanoutEnabled) {
    super(schema, specs, io, getIcebergDataWriter(fileWriterFactory, fileFactory, io, targetFileSize, fanoutEnabled));
    this.currentSpecId = currentSpecId;
  }

  @Override
  public void write(Writable row) throws IOException {
    Record record = ((Container<Record>) row).get();
    writer.write(record, specs.get(currentSpecId), partition(record, currentSpecId));
  }

  @Override
  public FilesForCommit files() {
    List<DataFile> dataFiles = ((DataWriteResult) writer.result()).dataFiles();
    return FilesForCommit.onlyData(dataFiles);
  }

  private static PartitioningWriter getIcebergDataWriter(FileWriterFactory<Record> fileWriterFactory,
      OutputFileFactory fileFactory, FileIO io,
      long targetFileSize, boolean fanoutEnabled) {
    return fanoutEnabled ? new FanoutDataWriter<>(fileWriterFactory, fileFactory, io, targetFileSize)
      : new ClusteredDataWriter<>(fileWriterFactory, fileFactory, io, targetFileSize);
  }
}
