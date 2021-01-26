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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesResult;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.impala.catalog.ImpalaHdfsPartitionLoader;
import org.apache.hadoop.hive.impala.catalog.ImpalaHdfsPartition;
import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.FeCatalogUtils;
import org.apache.impala.catalog.FeFsPartition;
import org.apache.impala.catalog.HdfsPartition;
import org.apache.impala.catalog.HdfsTable;
import org.apache.thrift.TException;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * ImpalaBasicHdfsTable
 *
 * This class is an HdfsTable class that allows the creation of the HdfsTable at optimization time,
 * but allows the retrieval of FileMetaData to be deferred to translation time.
 * At constructor time, the partition names will be fetched and placed into "basic" partitions
 * (partitions without any additional metadata other than the name). At translation time the real
 * partitions and filemetadata will be returned via the getImpalaPartitions() method.
 * For nonpartitioned tables, a dummy partition will be created which is needed for the Impala
 * request.
 * The lifetime of this object is per query (unlike the HdfsTable in Impala).
 */
public class ImpalaBasicHdfsTable extends HdfsTable {

  // helper map to allow retrieval of the id through the name.
  private final Map<String, Long> nameToIdMap;

  private final Map<String, ImpalaBasicPartition> basicPartitionMap;

  private final String nullPartitionKeyValue;

  private final ValidWriteIdList validWriteIdList;

  public ImpalaBasicHdfsTable(
      org.apache.hadoop.hive.metastore.api.Table msTbl, Database msDb,
      ValidWriteIdList validWriteIdList) throws HiveException {
    super(msTbl, new Db(msTbl.getDbName(), msDb), msTbl.getTableName(), msTbl.getOwner());
    this.nullPartitionKeyValue = null;
    this.validWriteIdList = validWriteIdList;
    this.nameToIdMap = ImmutableMap.of();
    this.basicPartitionMap = new HashMap<>();
    this.basicPartitionMap.put(ImpalaHdfsPartition.DUMMY_PARTITION, null);
  }

  public ImpalaBasicHdfsTable(HiveConf conf, IMetaStoreClient client,
      org.apache.hadoop.hive.metastore.api.Table msTbl, Database msDb,
      ValidWriteIdList validWriteIdList) throws HiveException {
    super(msTbl, new Db(msTbl.getDbName(), msDb), msTbl.getTableName(), msTbl.getOwner());
    try {
      loadSchema(msTable_);
      this.nullPartitionKeyValue = null;
      this.validWriteIdList = validWriteIdList;
      this.nameToIdMap = ImmutableMap.of();
      this.basicPartitionMap = new HashMap<>();
      this.basicPartitionMap.put(ImpalaHdfsPartition.DUMMY_PARTITION, null);
    } catch (CatalogException e) {
      throw new HiveException(e);
    }
  }

  public ImpalaBasicHdfsTable(org.apache.hadoop.hive.metastore.api.Table msTbl, Database msDb,
      List<String> partitionNames, ValidWriteIdList validWriteIdList,
      String defaultPartitionName) throws HiveException {
    super(msTbl, new Db(msTbl.getDbName(), msDb), msTbl.getTableName(), msTbl.getOwner());
    try {
      this.validWriteIdList = validWriteIdList;
      // some hdfs table initialization needed since we are using partitions.
      loadSchema(msTable_);
      this.nullPartitionKeyValue = defaultPartitionName;
      initializePartitionMetadata(msTbl);

      Preconditions.checkState(getNumClusteringCols() > 0);
      ImmutableMap.Builder<String, Long> nameToIdMapBuilder = ImmutableMap.builder();
      ImmutableMap.Builder<String, ImpalaBasicPartition> basicPartitionMapBuilder =
          ImmutableMap.builder();
      for (String partitionName : partitionNames) {
        List<LiteralExpr> partitionExprs = FeCatalogUtils.parsePartitionKeyValues(this,
            Warehouse.getPartValuesFromPartName(partitionName));
        ImpalaBasicPartition partition =
            new ImpalaBasicPartition(partitionName, partitionExprs);
        partitionMap_.put(partition.getId(), partition);
        updatePartitionMdAndColStats(partition);
        nameToIdMapBuilder.put(partitionName, partition.getId());
        basicPartitionMapBuilder.put(partitionName, partition);
      }
      this.nameToIdMap = nameToIdMapBuilder.build();
      this.basicPartitionMap = basicPartitionMapBuilder.build();
    } catch (CatalogException|TException e) {
      throw new HiveException(e);
    }
  }

  @Override
  public String getNullPartitionKeyValue() {
    return nullPartitionKeyValue;
  }

  public Long getIdFromName(String partitionName) {
    return nameToIdMap.get(partitionName);
  }

  public Collection<String> getPartitionNames() {
    return basicPartitionMap.keySet();
  }

  public HdfsPartition getPartition(String name) {
    return basicPartitionMap.get(name);
  }

  public Map<String, ImpalaBasicPartition> getBasicPartitionMap() {
    return basicPartitionMap;
  }

  public List<ImpalaBasicPartition> getAllPartitions() {
    return new ArrayList(basicPartitionMap.values());
  }

  /**
   * Called from Impala's HdfsPartitionPruner
   */
  @Override
  public List<FeFsPartition> loadPartitions(Collection<Long> ids) {
    Preconditions.checkState(getNumClusteringCols() > 0);
    List<FeFsPartition> partitions = Lists.newArrayList();
    for (Long id : ids) {
      FeFsPartition partition = (FeFsPartition) getPartitionMap().get(id);
      partitions.add(partition);
    }
    return partitions;
  }

  public Set<Partition> fetchPartitions(IMetaStoreClient client, Table tableMD,
      Collection<ImpalaBasicPartition> partitions, HiveConf conf) throws HiveException {
    Set<Partition> msPartitions = Sets.newHashSet();
    Set<String> namesToFetch = Sets.newHashSet();
    for (ImpalaBasicPartition partition : partitions) {
      namesToFetch.add(partition.getPartitionName());
    }

    List<Partition> newPartitions = fetchPartitionsFromHMS(client, tableMD, namesToFetch, conf);
    for (Partition p : newPartitions) {
      msPartitions.add(p);
    }
    return msPartitions;
  }

  public List<Partition> fetchPartitionsFromHMS(IMetaStoreClient client,
      Table table, Set<String> partitionNames, HiveConf conf) throws HiveException {
    try {
      GetPartitionsByNamesRequest request = ImpalaHdfsPartitionLoader.getPartitionsByNamesRequest(
          partitionNames, this, conf, false, validWriteIdList);
      // CDPD-16617: HIVE_IN_TEST mode, we avoid the call to HMS and return
      // an empty partition list.
      GetPartitionsByNamesResult result = conf.getBoolVar(ConfVars.HIVE_IN_TEST)
        ? new GetPartitionsByNamesResult()
        : client.getPartitionsByNames(request);
      List<Partition> partitions = Lists.newArrayList();
      if (result.getPartitions() != null) {
        for (org.apache.hadoop.hive.metastore.api.Partition p : result.getPartitions()) {
          partitions.add(new Partition(table, p));
        }
      }
      return partitions;
    } catch (TException e) {
      throw new HiveException(e);
    }
  }

  /**
   * For FENG, this always returns true.
   */
  @Override
  public boolean isStoredInImpaladCatalogCache() {
    return true;
  }

  public static class TableWithPartitionNames {
    private final ImpalaBasicHdfsTable table;
    private final Set<String> partitionNames = new HashSet<>();

    public TableWithPartitionNames(ImpalaBasicHdfsTable table) {
      this.table = table;
    }

    public void addPartitionNames(Collection<ImpalaBasicPartition> partitions) {
      for (ImpalaBasicPartition partition : partitions) {
        partitionNames.add(partition.getPartitionName());
      }
    }

    public Set<String> getPartitionNames() {
      return ImmutableSet.copyOf(partitionNames);
    }

    public ImpalaBasicHdfsTable getTable() {
      return table;
    }
  }
}
