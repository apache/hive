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

import com.google.common.base.Preconditions;
import org.apache.hadoop.hive.metastore.localcache.HMSTableConverter;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.GetTableResult;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.impala.prune.ImpalaBasicHdfsTable;
import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.HdfsPartitionLocationCompressor;
import org.apache.impala.catalog.HdfsPartition.FileDescriptor;
import org.apache.impala.catalog.HdfsStorageDescriptor;
import org.apache.impala.catalog.metastore.CatalogHmsClientUtils;
import org.apache.impala.util.ListMap;
import org.apache.impala.thrift.TAccessLevel;
import org.apache.impala.thrift.TNetworkAddress;

import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converter for HMS Table objects. The methods here are called through the HMSConverter
 * interface from an IMetaStoreClient, A "Request" structure will be passed in along with the result
 * structure retrieved from the HMS Server and potentially a cached structure from the HMS client.
 * Either a new converted Result structure will be returned or the same Result structure will be
 * passed back if a conversion was unable to take place.
 * The result will contain one dummy partition (it is an unpartitioned table) and all the file
 * metadata.
 */
public class ImpalaTableConverter implements HMSTableConverter {

  private static final Logger LOG = LoggerFactory.getLogger(ImpalaTableConverter.class);

  private final ImpalaGetTableRequest request;

  public ImpalaTableConverter(GetTableRequest request) {
    this.request = (ImpalaGetTableRequest) request;
    Preconditions.checkState(this.request.basicHdfsTable.getNumClusteringCols() == 0);
  }

  /**
   * load and return the dummy partition for an unpartitioned table.
   */
  public GetTableResult convertTable(GetTableResult result) {
    try {
      Table hmsTable = result.getTable();
      ImpalaGetTableResult response = new ImpalaGetTableResult();
      response.setTable(hmsTable);
      ImpalaBasicHdfsTable basicHdfsTable = request.basicHdfsTable;

      HdfsPartitionLocationCompressor partitionLocationCompressor =
          new HdfsPartitionLocationCompressor(basicHdfsTable.getNumClusteringCols());
      HdfsStorageDescriptor fileFormatDescriptor =
          HdfsStorageDescriptor.fromStorageDescriptor(basicHdfsTable.getName(),
              hmsTable.getSd());

      Partition partition = null;
      List<LiteralExpr> keyValues = new ArrayList<>();
      ListMap<TNetworkAddress> hostIndex = new ListMap<>();
      List<FileDescriptor> fds =
          CatalogHmsClientUtils.extractFileDescriptors(hmsTable, hostIndex);

      String name = ImpalaHdfsPartition.DUMMY_PARTITION;
      // Put in the dummy partition.
      response.partitionInfo.hdfsPartitions.put(name,
          new ImpalaHdfsPartition(partition, keyValues,
              fileFormatDescriptor, fds, 1,
              partitionLocationCompressor.new Location(hmsTable.getSd().getLocation()),
              TAccessLevel.READ_ONLY, name, hostIndex, -1L /*numRows*/));
      response.partitionInfo.locationPrefixes = partitionLocationCompressor.getPrefixes();
      response.partitionInfo.hostIndex = hostIndex.getList();
      return response;
    } catch (Exception e) {
      LOG.error("Exception thrown in ImpalaTableConverter.", e);
    }
    return null;
  }

  public GetTableResult cloneTableResult(GetTableResult result) {
    ImpalaGetTableResult impalaResult = (ImpalaGetTableResult) result;
    return new ImpalaGetTableResult(impalaResult);
  }

  public static class ImpalaGetTableRequest extends GetTableRequest {
    public ImpalaBasicHdfsTable basicHdfsTable;
  }

  public static class ImpalaGetTableResult extends GetTableResult {
    public ImpalaPartitionConverter.PartitionInfo partitionInfo =
        new ImpalaPartitionConverter.PartitionInfo();

    public ImpalaGetTableResult() {
    }

    /**
     * Clone object (shallow clone)
     */
    public ImpalaGetTableResult(ImpalaGetTableResult other) {
      super(other);
      this.partitionInfo = other.partitionInfo;
    }
  }
}
