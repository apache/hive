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
package org.apache.hadoop.hive.impala.prune;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.fs.Path;
import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.HdfsFileFormat;
import org.apache.impala.catalog.HdfsPartition;
import org.apache.impala.catalog.HdfsStorageDescriptor;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.thrift.TAccessLevel;
import org.apache.impala.thrift.THdfsPartitionLocation;
import org.apache.impala.thrift.TPartitionStats;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
/**
 * The basic partition information contains only the partition name.
 * Most methods are overridden to throw a RuntimeException since they
 * should not be called.  Some of the methods, however, are defined.
 * This is because they are used to calculate some initial state
 * information needed by HdfsTable. A design choice was made here to
 * reuse some of the Impala HdfsTable methods that call these partition
 * methods rather than rewriting HdfsTable to keep the amount of
 * Impala changes at a minimum. In the future, we may want to refactor
 * the code.
 */
public class ImpalaBasicPartition extends HdfsPartition {

  public ImpalaBasicPartition(String partitionName, List<LiteralExpr> partitionKeyValues)
      throws MetaException, CatalogException {
    super(null /*table*/, -1L /*prevId*/, partitionName,
        partitionKeyValues, null /*fileFormatDescriptor*/,
        ImmutableList.copyOf(new ArrayList<byte[]>()) /*fileDescriptors*/,
        null /*encodedInsertFileDescriptors*/, null /*encodedDeleteFileDescriptors*/,
        null /*location*/, false, null /*accessLevel*/, Maps.newHashMap() /*hmsParameters*/,
        null /*cachedMsPartitionDescriptor*/, null /*partitionStats*/, false, -1L, -1L,
        null /*inFlightEvents*/);
  }

  @Override
  public boolean hasFileDescriptors() {
    // always return true since we don't have enough information to tell, which
    // forces the Pruner to use this partition.
    return true;
  }

  /**
   * @return the number of files in this partition
   */
  @Override
  public int getNumFileDescriptors() {
    // Return 0 because we don't have enough information to determine this with
    // the basic partition.
    return 0;
  }

  @Override // FeFsPartition
  public HdfsFileFormat getFileFormat() {
    return null;
  }

  @Override
  public long getSize() {
    // Return 0 because we don't have enough information to determine this with
    // the basic partition.
    return 0L;
  }

  @Override
  public FileSystemUtil.FsType getFsType() {
    throw new RuntimeException("not implemented");
  }

  @Override
  public List<FileDescriptor> getFileDescriptors() {
    throw new RuntimeException("not implemented");
  }

  @Override
  public String getLocation() {
    throw new RuntimeException("not implemented");
  }

  @Override
  public THdfsPartitionLocation getLocationAsThrift() {
    throw new RuntimeException("not implemented");
  }

  @Override
  public Path getLocationPath() {
    throw new RuntimeException("not implemented");
  }

  @Override
  public TAccessLevel getAccessLevel() {
    throw new RuntimeException("not implemented");
  }

  @Override
  public boolean isCacheable() {
    throw new RuntimeException("not implemented");
  }

  @Override
  public boolean isMarkedCached() {
    throw new RuntimeException("not implemented");
  }

  @Override
  public HdfsStorageDescriptor getInputFormatDescriptor() {
    throw new RuntimeException("not implemented");
  }

  @Override
  public TPartitionStats getPartitionStats() {
    throw new RuntimeException("not implemented");
  }

  @Override
  public boolean hasIncrementalStats() {
    throw new RuntimeException("not implemented");
  }

  @Override
  public byte[] getPartitionStatsCompressed() {
    throw new RuntimeException("not implemented");
  }

  @Override
  public long getNumRows() {
    throw new RuntimeException("not implemented");
  }

  @Override
  public String getConjunctSql() {
    throw new RuntimeException("not implemented");
  }

  @Override
  public List<String> getPartitionValuesAsStrings(boolean mapNullsToHiveKey) {
    throw new RuntimeException("not implemented");
  }

  @Override
  public Map<String, String> getParameters() {
    throw new RuntimeException("not implemented");
  }

  @Override
  public long getWriteId() {
    throw new RuntimeException("not implemented");
  }
}
