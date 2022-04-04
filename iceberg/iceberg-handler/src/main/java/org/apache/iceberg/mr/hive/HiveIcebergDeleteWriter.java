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
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.mr.mapred.Container;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveIcebergDeleteWriter extends HiveIcebergWriter {
  private static final Logger LOG = LoggerFactory.getLogger(HiveIcebergDeleteWriter.class);

  private final ClusteredPositionDeleteWriter<Record> innerWriter;

  HiveIcebergDeleteWriter(Schema schema, PartitionSpec spec, FileFormat fileFormat,
      FileWriterFactory<Record> writerFactory, OutputFileFactory fileFactory, FileIO io, long targetFileSize,
      TaskAttemptID taskAttemptID, String tableName) {
    super(schema, spec, io, taskAttemptID, tableName, true);
    this.innerWriter = new ClusteredPositionDeleteWriter<>(writerFactory, fileFactory, io, fileFormat, targetFileSize);
  }

  @Override
  public void write(Writable row) throws IOException {
    Record rec = ((Container<Record>) row).get();
    PositionDelete<Record> positionDelete = IcebergAcidUtil.getPositionDelete(spec.schema(), rec);
    innerWriter.write(positionDelete, spec, partition(positionDelete.row()));
  }

  @Override
  public void close(boolean abort) throws IOException {
    innerWriter.close();
    List<DeleteFile> deleteFiles = deleteFiles();

    // If abort then remove the unnecessary files
    if (abort) {
      Tasks.foreach(deleteFiles)
          .retry(3)
          .suppressFailureWhenFinished()
          .onFailure((file, exception) -> LOG.debug("Failed on to remove delete file {} on abort", file, exception))
          .run(deleteFile -> io.deleteFile(deleteFile.path().toString()));
    }

    LOG.info("IcebergDeleteWriter is closed with abort={}. Created {} files", abort, deleteFiles.size());
  }

  @Override
  public List<DeleteFile> deleteFiles() {
    return innerWriter.result().deleteFiles();
  }
}
