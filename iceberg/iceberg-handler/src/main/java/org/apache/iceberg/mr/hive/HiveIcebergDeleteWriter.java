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

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.io.ClusteredPositionDeleteWriter;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.mr.mapred.Container;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveIcebergDeleteWriter extends HiveIcebergWriter {
  private static final Logger LOG = LoggerFactory.getLogger(HiveIcebergDeleteWriter.class);

  HiveIcebergDeleteWriter(Schema schema, PartitionSpec spec, FileFormat fileFormat,
      FileWriterFactory<Record> writerFactory, OutputFileFactory fileFactory, FileIO io, long targetFileSize,
      TaskAttemptID taskAttemptID, String tableName) {
    super(schema, spec, io, taskAttemptID, tableName,
        new ClusteredPositionDeleteWriter<>(writerFactory, fileFactory, io, fileFormat, targetFileSize));
  }

  @Override
  public void write(Writable row) throws IOException {
    Record rec = ((Container<Record>) row).get();
    PositionDelete<Record> positionDelete = IcebergAcidUtil.getPositionDelete(spec.schema(), rec);
    writer.write(positionDelete, spec, partition(positionDelete.row()));
  }

  @Override
  public FilesForCommit files() {
    List<DeleteFile> deleteFiles = ((DeleteWriteResult) writer.result()).deleteFiles();
    return FilesForCommit.onlyDelete(deleteFiles);
  }
}
