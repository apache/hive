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
import java.util.Map;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.PartitioningWriter;
import org.apache.iceberg.mr.hive.FilesForCommit;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("checkstyle:VisibilityModifier")
abstract class HiveIcebergWriterBase implements HiveIcebergWriter {
  private static final Logger LOG = LoggerFactory.getLogger(HiveIcebergWriterBase.class);

  protected final FileIO io;
  protected final InternalRecordWrapper wrapper;
  protected final Map<Integer, PartitionSpec> specs;
  protected final Map<Integer, PartitionKey> partitionKeys;
  protected final PartitioningWriter writer;

  HiveIcebergWriterBase(Schema schema, Map<Integer, PartitionSpec> specs, FileIO io,
      PartitioningWriter writer) {
    this.io = io;
    this.wrapper = new InternalRecordWrapper(schema.asStruct());
    this.specs = specs;
    this.partitionKeys = Maps.newHashMapWithExpectedSize(specs.size());
    this.writer = writer;
  }

  @Override
  public void close(boolean abort) throws IOException {
    writer.close();
    FilesForCommit result = files();

    // If abort then remove the unnecessary files
    if (abort) {
      Tasks.foreach(result.allFiles())
          .retry(3)
          .suppressFailureWhenFinished()
          .onFailure((file, exception) -> LOG.debug("Failed on to remove file {} on abort", file, exception))
          .run(file -> io.deleteFile(file.path().toString()));
    }

    LOG.info("HiveIcebergWriter is closed with abort={}. Created {} data files and {} delete files", abort,
        result.dataFiles().size(), result.deleteFiles().size());
  }

  protected PartitionKey partition(Record row, int specId) {
    PartitionKey partitionKey = partitionKeys.computeIfAbsent(specId,
        id -> new PartitionKey(specs.get(id), specs.get(id).schema()));
    partitionKey.partition(wrapper.wrap(row));
    return partitionKey;
  }
}
