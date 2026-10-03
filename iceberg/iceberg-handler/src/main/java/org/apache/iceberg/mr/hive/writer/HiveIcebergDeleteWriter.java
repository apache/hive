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
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.hadoop.io.Writable;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.mr.hive.FilesForCommit;
import org.apache.iceberg.mr.hive.IcebergAcidUtil;
import org.apache.iceberg.mr.hive.writer.WriterBuilder.Context;
import org.apache.iceberg.mr.mapred.Container;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.util.DeleteFileSet;

class HiveIcebergDeleteWriter extends HiveIcebergWriterBase {

  // the partitions of the deleted records by spec and serialized partition
  private final Map<Integer, Map<ByteBuffer, StructLike>> partitions;

  HiveIcebergDeleteWriter(
      Table table, Supplier<Map<String, DeleteFileSet>> rewritableDeletes,
      HiveFileWriterFactory writerFactory, OutputFileFactory deleteFileFactory,
      Context context) {
    super(table, newDeleteWriter(table, rewritableDeletes, writerFactory, deleteFileFactory, context));
    this.partitions = Maps.newHashMapWithExpectedSize(specs.size());
  }

  @Override
  public void write(Writable row) throws IOException {
    Record rec = ((Container<Record>) row).get();
    PartitionSpec spec = specs.get(IcebergAcidUtil.parseSpecId(rec));
    StructLike partition = partitions.computeIfAbsent(spec.specId(), id -> newPartitionCache(spec))
        .computeIfAbsent(IcebergAcidUtil.parseSerializedPartition(rec),
            serialized -> IcebergAcidUtil.deserializePartition(serialized, spec));
    writer.write(IcebergAcidUtil.getPositionDelete(rec), spec, partition);
  }

  /**
   * A delete file holds the partition of the data files it applies to, and Iceberg validates a DV's against its data
   * file's. The partition field of a dropped source column has the unknown type, which a manifest stores as null, so
   * a delete file cannot hold the value of the data files.
   */
  private Map<ByteBuffer, StructLike> newPartitionCache(PartitionSpec spec) {
    Preconditions.checkArgument(spec.partitionType().fields().stream()
            .noneMatch(field -> field.type().typeId() == Type.TypeID.UNKNOWN),
        "Cannot delete from the data files of partition spec %s: the source column of a partition field was dropped",
        spec.specId());
    return Maps.newHashMap();
  }

  @Override
  public FilesForCommit files() {
    DeleteWriteResult result = (DeleteWriteResult) writer.result();
    List<DeleteFile> deleteFiles = result.deleteFiles();
    Set<CharSequence> referencedDataFiles = result.referencedDataFiles();
    List<DeleteFile> rewrittenDeleteFiles = result.rewrittenDeleteFiles();
    return FilesForCommit.onlyDelete(deleteFiles, referencedDataFiles, rewrittenDeleteFiles);
  }
}
