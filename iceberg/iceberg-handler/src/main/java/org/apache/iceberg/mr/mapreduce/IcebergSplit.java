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

package org.apache.iceberg.mr.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.SerializationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Since this class extends `mapreduce.InputSplit and implements `mapred.InputSplit`, it can be returned by both MR v1
// and v2 file formats.
public class IcebergSplit extends InputSplit implements IcebergSplitContainer {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergSplit.class);

  public static final String[] ANYWHERE = new String[]{"*"};

  private ScanTaskGroup<FileScanTask> taskGroup;

  private transient String[] locations;
  private transient Configuration conf;

  // public no-argument constructor for deserialization
  public IcebergSplit() {
  }

  IcebergSplit(Configuration conf, ScanTaskGroup<FileScanTask> taskGroup) {
    this.taskGroup = taskGroup;
    this.conf = conf;
  }

  public ScanTaskGroup<FileScanTask> taskGroup() {
    return taskGroup;
  }

  @Override
  public IcebergSplit icebergSplit() {
    return this;
  }

  @Override
  public long getLength() {
    return taskGroup.tasks().stream().mapToLong(FileScanTask::length).sum();
  }

  @Override
  public String[] getLocations() {
    // The implementation of getLocations() is only meant to be used during split computation
    // getLocations() won't be accurate when called on worker nodes and will always return "*"
    if (locations == null && conf != null) {
      boolean localityPreferred = conf.getBoolean(InputFormatConfig.LOCALITY, false);
      locations = localityPreferred ? blockLocations(taskGroup, conf) : ANYWHERE;
    } else {
      locations = ANYWHERE;
    }

    return locations;
  }

  // We should move to Util.blockLocations once the following PR is merged and shipped
  // https://github.com/apache/iceberg/pull/11053
  private static String[] blockLocations(ScanTaskGroup<FileScanTask> task, Configuration conf) {
    final Set<String> locationSets = Sets.newHashSet();
    task.tasks().forEach(fileScanTask -> {
      final Path path = new Path(fileScanTask.file().path().toString());
      try {
        final FileSystem fs = path.getFileSystem(conf);
        for (BlockLocation location : fs.getFileBlockLocations(path, fileScanTask.start(), fileScanTask.length())) {
          locationSets.addAll(Arrays.asList(location.getHosts()));
        }
      } catch (IOException e) {
        LOG.warn("Failed to get block locations for path {}", path, e);
      }
    });

    return locationSets.toArray(new String[0]);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    byte[] data = SerializationUtil.serializeToBytes(this.taskGroup);
    out.writeInt(data.length);
    out.write(data);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    byte[] data = new byte[in.readInt()];
    in.readFully(data);
    this.taskGroup = SerializationUtil.deserializeFromBytes(data);
  }
}
