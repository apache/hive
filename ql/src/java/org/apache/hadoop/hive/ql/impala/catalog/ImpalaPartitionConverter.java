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

package org.apache.hadoop.hive.ql.impala.catalog;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hive.metastore.localcache.HMSPartitionConverter;
import org.apache.hadoop.hive.metastore.localcache.PartitionCacheHelper;
import org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesResult;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.impala.prune.ImpalaBasicHdfsTable;
import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.FeCatalogUtils;
import org.apache.impala.catalog.HdfsPartition;
import org.apache.impala.catalog.HdfsPartitionLocationCompressor;
import org.apache.impala.catalog.HdfsPartition.FileDescriptor;
import org.apache.impala.catalog.HdfsStorageDescriptor;
import org.apache.impala.catalog.metastore.CatalogHMSClientUtils;
import org.apache.impala.util.ListMap;
import org.apache.impala.thrift.TAccessLevel;
import org.apache.impala.thrift.TNetworkAddress;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The ImpalaPartitionConverter class converts the HMS Result into a Result that extends the
 * original ImpalaGetPartitionsByNamesResult class.  This child class will contain a conversion of
 * the result class. This will allow the HMS client to cache the conversion result specific to
 * the Impala engine.
 * The conversion of file descriptors is costly, so by caching this, we will save time on
 * compilation.  The methods of this class get called by some IMetaStoreClient through the
 * HMSConverter interface.
 */
public class ImpalaPartitionConverter implements HMSPartitionConverter {

  private static final Logger LOG = LoggerFactory.getLogger(ImpalaPartitionConverter.class);

  private final ImpalaGetPartitionsByNamesRequest rqst;

  private final Table table;

  public ImpalaPartitionConverter(GetPartitionsByNamesRequest rqst, Table table) {
    this.rqst = (ImpalaGetPartitionsByNamesRequest) rqst;
    this.table = table;
    // Make sure there are partitioned columns.
    Preconditions.checkState(this.rqst.basicHdfsTable.getNumClusteringCols() > 0);
  }

  /**
   * Creates the child converted result object.
   * @param result The final result that result that would be sent back if there was
   * no conversion necessary.
   * @param cacheValue The cache object that contains all the converted partitions.
   */
  @Override
  public GetPartitionsByNamesResult createPartitionsByNamesResult(
      GetPartitionsByNamesResult result, PartitionCacheHelper.CacheValue cacheValue
      ) throws MetaException {
    try {
      // already converted
      if (result instanceof ImpalaGetPartitionsByNamesResult) {
        return result;
      }
      ImpalaCacheValue impalaCacheValue = (ImpalaCacheValue) cacheValue;
      ImpalaGetPartitionsByNamesResult impalaResult = new ImpalaGetPartitionsByNamesResult();
      // The list of all Partition objects requested by the HMS is in the "result", so
      // it is put in the final "impalaResult" object. The dictionary is purposefully not
      // being copied over since it was not stored in the cache. If it is needed in the future,
      // we will have to figure out how to store it in the cache.
      impalaResult.setPartitions(result.getPartitions());

      ImpalaBasicHdfsTable basicHdfsTable = rqst.basicHdfsTable;

      // The partitionLocationCompressor object contains locations of all the partitions. It is
      // outside of the partition object on the table level. Each partition location is tracked
      // here.
      impalaResult.partitionInfo.locationPrefixes =
          impalaCacheValue.partitionLocationCompressor.getPrefixes();

      // Populate the cached host indexes into the response.
      impalaResult.partitionInfo.hostIndex = impalaCacheValue.hostIndex.getList();

      // Copy over the cached partitions. All partitions should have been fetched already
      // and placed in the cache. This will overwrite the basicPartition already present.
      for (String partitionName : rqst.getNames()) {
        impalaResult.partitionInfo.hdfsPartitions.put(partitionName,
            impalaCacheValue.hdfsPartitions.get(partitionName));
      }

      return impalaResult;
    } catch (Exception e) {
      MetaStoreUtils.logAndThrowMetaException(e);
    }
    return null;
  }

  /**
   * Add new result values into cache.  This method mutates the cacheValue variable that
   * is passed in.
   */
  @Override
  public void addToPartitionCache(
      GetPartitionsByNamesRequest missingNamesRqst,
      PartitionCacheHelper.CacheValue cacheValue,
      GetPartitionsByNamesResult missingNamesResult) throws MetaException {
    try {
      ImpalaCacheValue impalaCacheValue = (ImpalaCacheValue) cacheValue;
      ImpalaBasicHdfsTable basicHdfsTable = rqst.basicHdfsTable;
      // Note: While it might not be obvious, The hostIndex variable here gets populated inside
      // of the extractFileDescriptors method.  It uses the Impala ListMap method. The ListMap
      // class has a method called getIndex. This method checks for a given host, and will add it
      // if it isn't present in the ListMap.
      Map<Partition, List<FileDescriptor>> partitionFds =
          CatalogHMSClientUtils.extractFileDescriptors(missingNamesResult,
              impalaCacheValue.hostIndex);

      // Convert the newly fetched partitions and put them in the result
      Preconditions.checkState(
          missingNamesResult.getPartitions().size() == missingNamesRqst.getNames().size());

      for (Partition partition : missingNamesResult.getPartitions()) {
        String name = MetaStoreUtils.getPartitionName(table, partition);
        // After the transform, the partitionLocationCompressor will be populated. This is
        // due to the weird nature of the ListMap object inside the class.
        HdfsPartition newPartition = transformPartition(basicHdfsTable, name, partition,
            partitionFds.get(partition), impalaCacheValue.hostIndex,
            impalaCacheValue.partitionLocationCompressor);
        impalaCacheValue.hdfsPartitions.put(name, newPartition);
      }
    } catch (Exception e) {
      MetaStoreUtils.logAndThrowMetaException(e);
    }
  }

  /**
   * Instantiates a new Impala specific CacheValue object.
   */
  @Override
  public PartitionCacheHelper.CacheValue createConverterPartitionCacheValue(boolean isConcurrent) {
    return ImpalaCacheValue.create(rqst, isConcurrent);
  }

  /**
   * Convert Partition to HdfsPartition.
   */
  private static HdfsPartition transformPartition(ImpalaBasicHdfsTable basicHdfsTable,
      String partitionName, Partition partition, List<FileDescriptor> fds,
      ListMap<TNetworkAddress> cachedHostIndex,
      HdfsPartitionLocationCompressor cachedLocationCompressor
      ) throws CatalogException, HiveException {
    HdfsStorageDescriptor fileFormatDescriptor =
        HdfsStorageDescriptor.fromStorageDescriptor(basicHdfsTable.getName(), partition.getSd());
    List<LiteralExpr> keyValues = FeCatalogUtils.parsePartitionKeyValues(basicHdfsTable, partition.getValues());

    // need to reuse the same id for the newly created partition.
    Long id = basicHdfsTable.getIdFromName(partitionName);
    Preconditions.checkNotNull(id);

    HdfsPartition newPartition =  new ImpalaHdfsPartition(partition, keyValues,
        fileFormatDescriptor, fds, id,
        cachedLocationCompressor.new Location(partition.getSd().getLocation()),
        TAccessLevel.READ_ONLY, partitionName, cachedHostIndex);
    newPartition.setNumRows(FeCatalogUtils.getRowCount(partition.getParameters()));
    return newPartition;
  }


  /**
   * PartitionInfo contains all the converted partition information needed to create
   * the Impala HdfsTable object.
   */
  public static class PartitionInfo {
    // A map of partition names to their converted partition. This will contain all of the
    // partitions regardless of whether they are fetched from HMS. If a partition is not
    // fetched from HMS, it will be of type ImpalaBasicPartition, which doesn't contain any
    // HMS partition information.
    public Map<String, HdfsPartition> hdfsPartitions = new HashMap<>();
    // A list of the hosts for all of the partitions.
    public List<TNetworkAddress> hostIndex;
    // An object holding the locations of all the partitions.
    public List<String> locationPrefixes;
  }

  public static class ImpalaGetPartitionsByNamesRequest extends GetPartitionsByNamesRequest {
    public ImpalaBasicHdfsTable basicHdfsTable;
  }

  public static class ImpalaGetPartitionsByNamesResult extends GetPartitionsByNamesResult {
    public PartitionInfo partitionInfo = new PartitionInfo();
  }

  private static class ImpalaCacheValue extends PartitionCacheHelper.CacheValue {
    public final Map<String, HdfsPartition> hdfsPartitions;
    public final ListMap<TNetworkAddress> hostIndex = new ListMap<TNetworkAddress>();
    public final HdfsPartitionLocationCompressor partitionLocationCompressor;

    /**
     * Constructor
     * @param rqst HMS request containing the table which is used to get the number of partitioned
     * columns.
     * @param isConcurrent True if we need a thread safe map that will be used across queries.
     */
    private ImpalaCacheValue(ImpalaGetPartitionsByNamesRequest rqst, boolean isConcurrent) {
      this.hdfsPartitions = isConcurrent ? new ConcurrentHashMap<>() : new HashMap<>();
      this.partitionLocationCompressor =
          new HdfsPartitionLocationCompressor(rqst.basicHdfsTable.getNumClusteringCols());
    }

    public static ImpalaCacheValue create(GetPartitionsByNamesRequest rqst, boolean isConcurrent) {
      return new ImpalaCacheValue((ImpalaGetPartitionsByNamesRequest) rqst, isConcurrent);
    }
  }
}
