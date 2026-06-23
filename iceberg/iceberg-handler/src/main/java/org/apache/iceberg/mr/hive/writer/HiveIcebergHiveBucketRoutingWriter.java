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
import java.util.Map;
import java.util.function.IntFunction;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.mr.hive.FilesForCommit;
import org.apache.iceberg.mr.mapred.Container;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * Routes writes to per-bucket Iceberg writers when Hive-native bucketing (CLUSTERED BY) is enabled
 * but reducers are capped below the bucket count.
 *
 * <p>Bucket id is expected to be carried on the incoming {@link Writable} via {@link BucketAwareContainer}.</p>
 */
class HiveIcebergHiveBucketRoutingWriter implements HiveIcebergWriter {

  private final IntFunction<HiveIcebergWriter> perBucketWriterFactory;
  private final int numBuckets;
  private final Map<Integer, HiveIcebergWriter> writersByBucket = new LinkedHashMap<>();

  HiveIcebergHiveBucketRoutingWriter(IntFunction<HiveIcebergWriter> perBucketWriterFactory, int numBuckets) {
    this.perBucketWriterFactory = perBucketWriterFactory;
    this.numBuckets = numBuckets;
  }

  @Override
  public void write(Writable w) throws IOException {
    if (!(w instanceof Container)) {
      throw new IllegalArgumentException("Expected Container<Record> but got: " + w.getClass());
    }
    @SuppressWarnings("unchecked")
    Container<Record> container = (Container<Record>) w;

    if (!(container instanceof BucketAwareContainer)) {
      throw new IllegalStateException("Bucket routing enabled but incoming row does not carry bucket id: " +
          container.getClass());
    }

    int bucketId = ((BucketAwareContainer<?>) container).bucketId();
    if (bucketId < 0 || numBuckets > 0 && bucketId >= numBuckets) {
      throw new IllegalArgumentException("Invalid bucket id for routing: " + bucketId +
          " (numBuckets=" + numBuckets + ")");
    }

    HiveIcebergWriter writer = writersByBucket.computeIfAbsent(bucketId, perBucketWriterFactory::apply);
    writer.write(w);
  }

  @Override
  public void close(boolean abort) throws IOException {
    IOException first = null;
    for (HiveIcebergWriter writer : writersByBucket.values()) {
      try {
        writer.close(abort);
      } catch (IOException ioe) {
        if (first == null) {
          first = ioe;
        }
      }
    }
    if (first != null) {
      throw first;
    }
  }

  @Override
  public FilesForCommit files() {
    if (writersByBucket.isEmpty()) {
      return FilesForCommit.empty();
    }

    java.util.List<DataFile> dataFiles = Lists.newArrayList();
    java.util.List<DeleteFile> deleteFiles = Lists.newArrayList();
    java.util.List<DataFile> replacedDataFiles = Lists.newArrayList();
    java.util.List<CharSequence> referencedDataFiles = Lists.newArrayList();
    java.util.List<DeleteFile> rewrittenDeleteFiles = Lists.newArrayList();
    java.util.List<Path> mergedAndDeletedFiles = Lists.newArrayList();

    for (HiveIcebergWriter writer : writersByBucket.values()) {
      FilesForCommit filesForCommit = writer.files();
      dataFiles.addAll(filesForCommit.dataFiles());
      deleteFiles.addAll(filesForCommit.deleteFiles());
      replacedDataFiles.addAll(filesForCommit.replacedDataFiles());
      referencedDataFiles.addAll(filesForCommit.referencedDataFiles());
      rewrittenDeleteFiles.addAll(filesForCommit.rewrittenDeleteFiles());
      mergedAndDeletedFiles.addAll(filesForCommit.mergedAndDeletedFiles());
    }

    return new FilesForCommit(dataFiles, deleteFiles, replacedDataFiles, referencedDataFiles, rewrittenDeleteFiles,
        mergedAndDeletedFiles);
  }
}
