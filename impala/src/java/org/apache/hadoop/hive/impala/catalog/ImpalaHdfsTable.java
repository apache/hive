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
import com.google.common.collect.Lists;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.impala.catalog.ImpalaPartitionConverter.PartitionInfo;
import org.apache.hadoop.hive.impala.prune.ImpalaBasicHdfsTable;
import org.apache.hadoop.hive.impala.prune.ImpalaBasicPartition;
import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.FeCatalogUtils;
import org.apache.impala.catalog.FeFsPartition;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.catalog.HdfsPartition;
import org.apache.impala.catalog.HdfsPartitionLocationCompressor;
import org.apache.impala.catalog.HdfsPartition.FileDescriptor;
import org.apache.impala.catalog.HdfsStorageDescriptor;
import org.apache.impala.catalog.PrunablePartition;
import org.apache.impala.catalog.metastore.CatalogHMSClientUtils;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.util.ListMap;
import org.apache.impala.thrift.TAccessLevel;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.thrift.THdfsFileDesc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ImpalaHdfsTable extends HdfsTable {

  private final HiveConf conf;

  private final String nullPartitionKeyValue;

  private final Map<String, FeFsPartition> namePartitionMap = new HashMap<>();

  // This is a "compile time" ValidWriteIdList. Typically HMS clients will auto-set this
  // for a variety of HMS calls automatically - but only after compilation (this is
  // usualy setup via Driver#recordValidWriteIds). Since we are making HMS calls
  // that require a ValidWriteIdList, we have to explicitly set this on the HMS calls.
  private final ValidWriteIdList compileTimeWriteIdList;

  public ImpalaHdfsTable(HiveConf conf, Table msTbl, Db db, String name, String owner)
      throws HiveException {
    super(msTbl, db, name, owner);
    this.conf = conf;
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

  private ImpalaHdfsTable(ImpalaBasicHdfsTable basicHdfsTable,
      IMetaStoreClient client, ValidWriteIdList compileTimeWriteIdList,
      PartitionInfo partitionInfo, HiveConf conf)
      throws HiveException, ImpalaException, MetaException {
    super(basicHdfsTable.getMetaStoreTable(), basicHdfsTable.getDb(),
        basicHdfsTable.getName(), basicHdfsTable.getOwnerUser());
    // host indexes are populated by the partitions fetched from HMS.
    getHostIndex().populate(partitionInfo.hostIndex);
    try {
      this.conf = conf;
      this.compileTimeWriteIdList = compileTimeWriteIdList;
      Table msTbl = getMetaStoreTable();
      // initialize variables needed in parent HdfsTable
      loadSchema(msTbl);
      // CDPD-16908 the initial msTbl should have all column stats and metadata info,
      // there should be no need to refetch these.
      loadAllColumnStats(client);
      loadConstraintsInfo(client, msTbl);
      validWriteIds_ = compileTimeWriteIdList;
      initializePartitionMetadata(msTbl);
      updateMdFromHmsTable(msTbl);
      this.nullPartitionKeyValue = conf.getVar(HiveConf.ConfVars.DEFAULTPARTITIONNAME);

      List<HdfsPartition> partitions = new ArrayList<>();
      for (String name : basicHdfsTable.getPartitionNames()) {
        if (partitionInfo.hdfsPartitions.containsKey(name)) {
          partitions.add(partitionInfo.hdfsPartitions.get(name));
        } else {
          partitions.add(basicHdfsTable.getPartition(name));
        }
      }

      // load in partition data from HMS
      for (HdfsPartition partition : partitions) {
        addPartition(partition);
        namePartitionMap.put(partition.getPartitionName(), partition);
      }
      // Locations are populated by the partitions fetched from HMS.
      this.partitionLocationCompressor_ = new HdfsPartitionLocationCompressor(
          basicHdfsTable.getNumClusteringCols(), partitionInfo.locationPrefixes);

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
      Preconditions.checkState(getPartitions().size() == 1);
      return Lists.newArrayList(
          (FeFsPartition) namePartitionMap.get(ImpalaHdfsPartition.DUMMY_PARTITION));
    }

    List<FeFsPartition> impalaPartitions = Lists.newArrayList();
    for (ImpalaBasicPartition p : partitions) {
      impalaPartitions.add(namePartitionMap.get(p.getPartitionName()));
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

  public static ImpalaHdfsTable create(HiveConf conf, ImpalaBasicHdfsTable basicHdfsTable,
      Set<String> partitionNames, IMetaStoreClient client, ValidWriteIdList compileTimeWriteIdList)
      throws HiveException, ImpalaException, MetaException {
    PartitionInfo partitionInfo = ImpalaHdfsPartitionLoader.fetchPartitionInfoFromHMS(conf,
        basicHdfsTable, partitionNames, client, compileTimeWriteIdList);
    return new ImpalaHdfsTable(basicHdfsTable, client, compileTimeWriteIdList, partitionInfo, conf);
  }
}
