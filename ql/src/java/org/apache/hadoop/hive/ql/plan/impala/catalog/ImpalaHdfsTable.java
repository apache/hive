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
package org.apache.hadoop.hive.ql.plan.impala.catalog;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FileMetadata;
import org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesResult;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.impala.prune.ImpalaBasicHdfsTable;
import org.apache.hadoop.hive.ql.plan.impala.prune.ImpalaBasicPartition;
import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.FeCatalogUtils;
import org.apache.impala.catalog.FeFsPartition;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.catalog.HdfsPartition;
import org.apache.impala.catalog.HdfsPartition.FileDescriptor;
import org.apache.impala.catalog.HdfsStorageDescriptor;
import org.apache.impala.catalog.metastore.CatalogHMSClientUtils;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.util.ListMap;
import org.apache.impala.thrift.TAccessLevel;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.thrift.THdfsFileDesc;
import org.apache.thrift.TException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ImpalaHdfsTable extends HdfsTable {

  private final HiveConf conf;

  private final String nullPartitionKeyValue;

  // a dummy partition needed for an unparttiioned table.
  private final FeFsPartition dummyPartition;
  // This is a "compile time" ValidWriteIdList. Typically HMS clients will auto-set this
  // for a variety of HMS calls automatically - but only after compilation (this is
  // usualy setup via Driver#recordValidWriteIds). Since we are making HMS calls
  // that require a ValidWriteIdList, we have to explicitly set this on the HMS calls.
  private final ValidWriteIdList compileTimeWriteIdList;

  public ImpalaHdfsTable(HiveConf conf, Table msTbl, Db db, String name, String owner)
      throws HiveException {
    super(msTbl, db, name, owner);
    this.conf = conf;
    this.dummyPartition = null;
    this.compileTimeWriteIdList = null;
    try {
      loadSchema(msTbl);
      initializePartitionMetadata(msTbl);
      updateMdFromHmsTable(msTbl);
      nullPartitionKeyValue = conf.getVar(HiveConf.ConfVars.DEFAULTPARTITIONNAME);
    } catch (CatalogException|IOException e) {
      throw new HiveException(e);
    }
  }

  public ImpalaHdfsTable(ImpalaBasicHdfsTable basicHdfsTable,
      IMetaStoreClient client, ValidWriteIdList compileTimeWriteIdList) throws HiveException, ImpalaException, MetaException {
    super(basicHdfsTable.getMetaStoreTable(), basicHdfsTable.getDb(),
        basicHdfsTable.getName(), basicHdfsTable.getOwnerUser());
    try {
      this.conf = basicHdfsTable.getConf();
      this.compileTimeWriteIdList = compileTimeWriteIdList;
      Table msTbl = getMetaStoreTable();
      // initialize variables needed in parent HdfsTable
      loadSchema(msTbl);
      // CDPD-16908 the initial msTbl should have all column stats and metadata info,
      // there should be no need to refetch these.
      loadAllColumnStats(client);
      loadConstraintsInfo(client, msTbl);
      // CDPD-16964: Is this the proper way to get valid writeIds?
      loadValidWriteIdList(client);
      initializePartitionMetadata(msTbl);
      updateMdFromHmsTable(msTbl);
      nullPartitionKeyValue = conf.getVar(HiveConf.ConfVars.DEFAULTPARTITIONNAME);

      // load in partition data from HMS
      HdfsPartition tmpDummyPartition = null;
      if (getNumClusteringCols() > 0) {
        loadPartitionedTable(basicHdfsTable, client);
      } else {
        tmpDummyPartition = loadNoPartitionData(client);
        addPartition(tmpDummyPartition);
      }
      dummyPartition = tmpDummyPartition;
      setTableStats(msTable_);
    } catch (IOException|ImpalaException e) {
      throw new HiveException(e);
    }
  }

  /**
   * Retrieve the full partitions (including partition metadata and file metadata) given
   * a list of partitions.  If the table is not partitioned, return back the dummyPartition.
   */
  public List<FeFsPartition> getPartitions(List<ImpalaBasicPartition> partitions)
      throws HiveException {
    if (getNumClusteringCols() == 0) {
      return Lists.newArrayList(dummyPartition);
    }

    List<FeFsPartition> impalaPartitions = Lists.newArrayList();
    // The "id" for both this table object and the wrapped table object containing the
    // partition and filemetadata use the same partition ids.
    for (ImpalaBasicPartition p : partitions) {
      impalaPartitions.add((FeFsPartition) getPartitionMap().get(p.getId()));
    }
    return impalaPartitions;
  }

  @Override
  public String getNullPartitionKeyValue() {
    return nullPartitionKeyValue;
  }

  /**
   * For FENG, this always returns true.
   */
  @Override
  public boolean isStoredInImpaladCatalogCache() {
    return true;
  }

  /**
   * load the partition and file metadata from HMS for a partitioned table
   */
  private void loadPartitionedTable(ImpalaBasicHdfsTable basicHdfsTable, IMetaStoreClient client
      ) throws HiveException {
    Preconditions.checkState(getNumClusteringCols() > 0);
    try {
      GetPartitionsByNamesRequest request =
          getPartitionsByNamesRequest(basicHdfsTable.getNamesToLoad());
      // CDPD-16617: HIVE_IN_TEST mode, we avoid the call to HMS and return
      // an empty partition list.
      GetPartitionsByNamesResult result = conf.getBoolVar(ConfVars.HIVE_IN_TEST)
        ? new GetPartitionsByNamesResult()
        : client.getPartitionsByNames(request);

      Map<Partition, List<FileDescriptor>> partitionFds = conf.getBoolVar(ConfVars.HIVE_IN_TEST)
          ? Maps.newHashMap()
          : CatalogHMSClientUtils.extractFileDescriptors(result, getHostIndex());

      if (result.getPartitions() != null) {
        for (Partition p : result.getPartitions()) {
          // Need to call "transformPartition" to convert Partition to HdfsPartition.
          addPartition(transformPartition(basicHdfsTable, p, partitionFds.get(p)));
        }
        for (HdfsPartition p : basicHdfsTable.getPartitionsNotToLoad()) {
          addPartition(p);
        }
      }
    } catch (CatalogException|TException e) {
      throw new HiveException(e);
    }
  }

  /**
   * load and return the dummy partition for an unpartitioned table.
   */
  private HdfsPartition loadNoPartitionData(IMetaStoreClient client) throws HiveException {
    Preconditions.checkState(getNumClusteringCols() == 0);
    try {
      GetTableRequest request = getTableRequest();
      // CDPD-16617: HIVE_IN_TEST mode, we avoid the call to HMS and return an empty table.
      Table result = conf.getBoolVar(ConfVars.HIVE_IN_TEST)
        ? new Table()
        : client.getTable(request);

      HdfsStorageDescriptor fileFormatDescriptor =
          HdfsStorageDescriptor.fromStorageDescriptor(this.getName(), msTable_.getSd());
      List<LiteralExpr> keyValues = Lists.newArrayList();
      List<FileDescriptor> fds = conf.getBoolVar(ConfVars.HIVE_IN_TEST)
          ? Lists.newArrayList()
          : CatalogHMSClientUtils.extractFileDescriptors(result, getHostIndex());

      return new HdfsPartition(this, null, keyValues, fileFormatDescriptor, fds, 1,
          getPartitionLocationCompressor().new Location(msTable_.getSd().getLocation()),
          TAccessLevel.READ_ONLY);
    } catch (CatalogException|TException e) {
      throw new HiveException(e);
    }
  }

  private GetPartitionsByNamesRequest getPartitionsByNamesRequest(Set<String> names) {
    GetPartitionsByNamesRequest request = new GetPartitionsByNamesRequest();
    request.setDb_name(MetaStoreUtils.prependCatalogToDbName(msTable_.getDbName(), conf));
    request.setTbl_name(msTable_.getTableName());
    request.setNames(Lists.newArrayList(names));
    request.setGetFileMetadata(true);
    Preconditions.checkState(
        !AcidUtils.isTransactionalTable(msTable_) || compileTimeWriteIdList != null,
        "Transaction tables must provide a ValidWriteIdList");
    if (compileTimeWriteIdList != null) {
      request.setValidWriteIdList(compileTimeWriteIdList.toString());
    }
    return request;
  }

  private GetTableRequest getTableRequest() {
    GetTableRequest request = new GetTableRequest();
    request.setDbName(msTable_.getDbName());
    request.setTblName(msTable_.getTableName());
    request.setGetFileMetadata(true);
    Preconditions.checkState(
        !AcidUtils.isTransactionalTable(msTable_) || compileTimeWriteIdList != null,
        "Transaction tables must provide a ValidWriteIdList");
    if (compileTimeWriteIdList != null) {
      request.setValidWriteIdList(compileTimeWriteIdList.toString());
    }
    return request;
  }

  /**
   * Convert Partition to HdfsPartition.
   */
  private HdfsPartition transformPartition(ImpalaBasicHdfsTable basicHdfsTable,
      Partition partition, List<FileDescriptor> fds) throws CatalogException {
    HdfsStorageDescriptor fileFormatDescriptor =
        HdfsStorageDescriptor.fromStorageDescriptor(this.getName(), partition.getSd());
    List<LiteralExpr> keyValues = FeCatalogUtils.parsePartitionKeyValues(this, partition.getValues());

    String partitionName = MetaStoreUtils.getPartitionName(msTable_, partition);
    // need to reuse the same id for the newly created partition.
    Long id = basicHdfsTable.getIdFromName(partitionName);
    Preconditions.checkNotNull(id);
    HdfsPartition newPartition =  new HdfsPartition(this, partition, keyValues,
        fileFormatDescriptor, fds, id,
        getPartitionLocationCompressor().new Location(partition.getSd().getLocation()),
        TAccessLevel.READ_ONLY);
    newPartition.setNumRows(FeCatalogUtils.getRowCount(partition.getParameters()));
    return newPartition;
  }
}
