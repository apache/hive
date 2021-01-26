/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.impala.catalog;

import java.util.List;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.catalog.HdfsPartition;
import org.apache.impala.catalog.HdfsPartitionLocationCompressor;
import org.apache.impala.catalog.HdfsStorageDescriptor;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.thrift.TAccessLevel;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.util.ListMap;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Extension of Impala's HdfsPartition.  In this extension, the partition name and hostIndex
 * get overridden.  The parent class has dependencies on the Table object in these methods.
 * We would like to avoid this because this object can be stored in the HMS client and the table
 * object using this partition will be instantiated for each query. Because of this, we pass in
 * null for the table object.
 * Also, the fileDescriptors are tracked here.  In the parent object, they are tracked in
 * a compressed format, but here they are expanded in order to save on compilation time.
 */
public class ImpalaHdfsPartition extends HdfsPartition {

  public static final String DUMMY_PARTITION = "DUMMY";

  // Static Configuration object. On first iteration, getting a FileSystem object with a new
  // Configuration object is about 10ms, On subsequent calls to get a new FileSystem, it is much
  // quicker when using the same Configuration object.
  // Impala also declares this Configuration object as static when they fetch the FileSystem object.
  private static final Configuration CONF = new Configuration();

  private final ListMap<TNetworkAddress> hostIndex;

  private final FileSystemUtil.FsType fsType;

  private final FileSystem fs;

  private final List<HdfsPartition.FileDescriptor> fileDescriptors;

  public ImpalaHdfsPartition(
        org.apache.hadoop.hive.metastore.api.Partition msPartition,
        List<LiteralExpr> partitionKeyValues,
        HdfsStorageDescriptor fileFormatDescriptor,
        List<HdfsPartition.FileDescriptor> fileDescriptors, long id,
        HdfsPartitionLocationCompressor.Location location, TAccessLevel accessLevel,
        String partitionName, ListMap<TNetworkAddress> hostIndex, long numRows
        ) throws HiveException {
    super(null /*table*/, id, -1, partitionName, partitionKeyValues,
        fileFormatDescriptor,
        null /*encodedFileDescriptors*/,
        null /*encodedInsertFileDescriptors*/, null /*encodedDeleteFileDescriptors*/,
        location, false, accessLevel, Maps.newHashMap() /*hmsParameters*/,
        null /*cachedMsPartitionDescriptor*/, null /*partitionStats*/, false, numRows, -1L,
        null /*inFlightEvents*/);
    try {
      this.hostIndex = hostIndex;
      Preconditions.checkNotNull(getLocationPath().toUri().getScheme(),
          "Cannot get scheme from path " + getLocationPath());
      fsType = FileSystemUtil.FsType.getFsType(getLocationPath().toUri().getScheme());
      fs = getLocationPath().getFileSystem(CONF);
      this.fileDescriptors = fileDescriptors;
    } catch (Exception e) {
      throw new HiveException("Could not create ImpalaHdfsPartition.", e);
    }
  }

  @Override
  public FileSystemUtil.FsType getFsType() {
    return fsType;
  }

  @Override
  public ListMap<TNetworkAddress> getHostIndex() {
    return hostIndex;
  }

  @Override
  public FileSystem getFileSystem(Configuration conf) {
    return fs;
  }

  @Override
  public List<HdfsPartition.FileDescriptor> getFileDescriptors() {
    return fileDescriptors;
  }

  @Override
  public int getNumFileDescriptors() {
    return fileDescriptors.size();
  }

  @Override
  public boolean hasFileDescriptors() {
    return !fileDescriptors.isEmpty();
  }
}
