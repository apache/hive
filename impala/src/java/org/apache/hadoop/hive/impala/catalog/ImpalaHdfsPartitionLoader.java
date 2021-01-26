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
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesRequest;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.impala.catalog.ImpalaTableConverter.ImpalaGetTableRequest;
import org.apache.hadoop.hive.impala.catalog.ImpalaTableConverter.ImpalaGetTableResult;
import org.apache.hadoop.hive.impala.catalog.ImpalaPartitionConverter.ImpalaGetPartitionsByNamesRequest;
import org.apache.hadoop.hive.impala.catalog.ImpalaPartitionConverter.ImpalaGetPartitionsByNamesResult;
import org.apache.hadoop.hive.impala.catalog.ImpalaPartitionConverter.PartitionInfo;
import org.apache.hadoop.hive.impala.prune.ImpalaBasicHdfsTable;
import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.catalog.HdfsStorageDescriptor;
import org.apache.impala.catalog.HdfsPartition.FileDescriptor;
import org.apache.impala.catalog.HdfsPartitionLocationCompressor;
import org.apache.impala.util.ListMap;
import org.apache.impala.thrift.TAccessLevel;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Class responsible for getting the PartitionInfo structure fetched from HMS. The PartitionInfo
 * structure will contain converted Partition objects into an Impala HdfsPartition.
 */
public class ImpalaHdfsPartitionLoader {

  /**
   * Fetch the PartitionInfo structure from HMS. The PartitionInfo is a shared structure
   * used by ImpalaGetTableResult and ImpalaGetPartitionsByNamesResult. The GetTableResult
   * version is for when the table isn't partitioned. In that case, one dummy partition
   * will be returned.
   */
  public static PartitionInfo fetchPartitionInfoFromHMS (
      HiveConf conf, ImpalaBasicHdfsTable basicHdfsTable, Set<String> partitionNames,
      IMetaStoreClient client, ValidWriteIdList compileTimeWriteIdList) throws HiveException {
    return (basicHdfsTable.getNumClusteringCols() > 0)
        ? getPartitionInfoFromPartitionedTable(conf, basicHdfsTable, partitionNames, client,
            compileTimeWriteIdList)
        : getPartitionInfoFromNonPartitionedTable(conf, basicHdfsTable, client,
            compileTimeWriteIdList);
  }

  /**
   * Load the partition and file metadata from HMS for a partitioned table
   */
  private static PartitionInfo getPartitionInfoFromPartitionedTable(
      HiveConf conf, ImpalaBasicHdfsTable basicHdfsTable, Set<String> partitionNames,
      IMetaStoreClient client, ValidWriteIdList compileTimeWriteIdList) throws HiveException {
    try {
      GetPartitionsByNamesRequest request = getPartitionsByNamesRequest(
          partitionNames, basicHdfsTable, conf, true, compileTimeWriteIdList);
      // CDPD-16617: HIVE_IN_TEST mode, we avoid the call to HMS and return
      // an empty partition list.
      ImpalaGetPartitionsByNamesResult result = conf.getBoolVar(ConfVars.HIVE_IN_TEST)
          ? getTestPartitionsByNamesResult(basicHdfsTable)
          : (ImpalaGetPartitionsByNamesResult) client.getPartitionsByNames(request);

      return result.partitionInfo;
    } catch (TException e) {
      throw new HiveException(e);
    }
  }

  /**
   * Load and return the dummy partition for an unpartitioned table.
   */
  private static PartitionInfo getPartitionInfoFromNonPartitionedTable(
      HiveConf conf, ImpalaBasicHdfsTable basicHdfsTable, IMetaStoreClient client,
      ValidWriteIdList compileTimeWriteIdList) throws HiveException {
    ImpalaGetTableRequest request = getTableRequest(basicHdfsTable, compileTimeWriteIdList);
    try {
      // CDPD-16617: HIVE_IN_TEST mode, we avoid the call to HMS and return an empty table.
      ImpalaGetTableResult result = conf.getBoolVar(ConfVars.HIVE_IN_TEST)
          ? getTestTableResult(basicHdfsTable)
          : (ImpalaGetTableResult) client.getTable(request);

      return result.partitionInfo;
    } catch (TException e) {
      throw new HiveException(e);
    }
  }

  /**
   * Method used only in testing for GetPartitionsByNamesResult.
   */
  public static ImpalaGetPartitionsByNamesResult getTestPartitionsByNamesResult(
      ImpalaBasicHdfsTable table) throws HiveException {
    try {
      ImpalaGetPartitionsByNamesResult r = new ImpalaGetPartitionsByNamesResult();
      r.partitionInfo.hostIndex = new ArrayList<>();
      r.partitionInfo.locationPrefixes = new ArrayList<>();
      return r;
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  /**
   * Method used only in testing for GetTableResult.
   */
  public static ImpalaGetTableResult getTestTableResult(
      ImpalaBasicHdfsTable table) throws HiveException {
    try {
      ImpalaGetTableResult r = new ImpalaGetTableResult();
      String name = ImpalaHdfsPartition.DUMMY_PARTITION;
      List<LiteralExpr> keyValues = new ArrayList<>();
      HdfsStorageDescriptor fileFormatDescriptor =
          HdfsStorageDescriptor.fromStorageDescriptor(table.getName(),
              table.getMetaStoreTable().getSd());
      HdfsPartitionLocationCompressor partitionLocationCompressor =
          new HdfsPartitionLocationCompressor(0);
      r.partitionInfo.hostIndex = new ArrayList<>();
      r.partitionInfo.locationPrefixes = new ArrayList<>();
      List<FileDescriptor> fds = new ArrayList<>();
      ListMap<TNetworkAddress> hostIndex = new ListMap<>();
      r.partitionInfo.hdfsPartitions.put(name,
            new ImpalaHdfsPartition(null, keyValues,
                fileFormatDescriptor, fds, 1,
                partitionLocationCompressor.new Location(
                table.getMetaStoreTable().getSd().getLocation()),
                TAccessLevel.READ_ONLY, name, hostIndex, -1L /*numRows*/));
      return r;
    } catch (Exception e ) {
      throw new HiveException(e);
    }
  }

  /**
   * Helper method that returns the request structure.
   */
  public static ImpalaGetPartitionsByNamesRequest getPartitionsByNamesRequest(Set<String> names,
      ImpalaBasicHdfsTable basicHdfsTable, HiveConf conf, boolean fetchFileMetadata,
      ValidWriteIdList writeIdList) {
    Table msTbl = basicHdfsTable.getMetaStoreTable();
    Preconditions.checkState(
        !AcidUtils.isTransactionalTable(msTbl) || writeIdList != null,
        "Transaction tables must provide a ValidWriteIdList");
    //TODO: CDPD-17400: Need to populate tableId once it is a field in the request structure.
    ImpalaGetPartitionsByNamesRequest request = new ImpalaGetPartitionsByNamesRequest();
    request.setDb_name(MetaStoreUtils.prependCatalogToDbName(msTbl.getDbName(), conf));
    request.setTbl_name(msTbl.getTableName());
    request.setNames(new ArrayList<>(names));
    if (writeIdList != null) {
      request.setValidWriteIdList(writeIdList.toString());
    }
    request.setGetFileMetadata(fetchFileMetadata);
    request.basicHdfsTable = basicHdfsTable;
    return request;
  }

  private static ImpalaGetTableRequest getTableRequest(ImpalaBasicHdfsTable basicHdfsTable,
      ValidWriteIdList compileTimeWriteIdList) {
    Table msTable = basicHdfsTable.getMetaStoreTable();
    Preconditions.checkState(
        !AcidUtils.isTransactionalTable(msTable) || compileTimeWriteIdList != null,
        "Transaction tables must provide a ValidWriteIdList");
    ImpalaGetTableRequest request = new ImpalaGetTableRequest();
    request.setDbName(msTable.getDbName());
    request.setTblName(msTable.getTableName());
    request.setGetFileMetadata(true);
    request.setId(msTable.getId());
    if (compileTimeWriteIdList != null) {
      request.setValidWriteIdList(compileTimeWriteIdList.toString());
    }
    request.basicHdfsTable = basicHdfsTable;
    return request;
  }
}

