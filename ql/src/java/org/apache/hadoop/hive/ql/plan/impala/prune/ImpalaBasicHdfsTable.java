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
package org.apache.hadoop.hive.ql.plan.impala.prune;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.FeFsPartition;
import org.apache.impala.catalog.HdfsStorageDescriptor;
import org.apache.impala.catalog.HdfsTable;
import org.apache.thrift.TException;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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

  private final HiveConf conf;

  // helper map to allow retrieval of the id through the name.
  private final Map<String, Long> nameToIdMap = Maps.newHashMap();

  // track the partition names that need to be retrieved from HMS
  private final Set<String> namesToLoad = Sets.newHashSet();

  private final String nullPartitionKeyValue;

  public ImpalaBasicHdfsTable(HiveConf conf, IMetaStoreClient client, Table msTbl, Database msDb
      ) throws HiveException {
    super(msTbl, new Db(msTbl.getDbName(), msDb), msTbl.getTableName(), msTbl.getOwner());
    try {
      this.conf = conf;
      // some hdfs table initialization needed since we are using partitions.
      loadSchema(msTable_);
      this.nullPartitionKeyValue = conf.getVar(HiveConf.ConfVars.DEFAULTPARTITIONNAME);
      initializePartitionMetadata(msTbl);
      updateMdFromHmsTable(msTbl);

      // initialize basic partitions
      if (getNumClusteringCols() > 0) {
        List<String> partitionNames =
            client.listPartitionNames(msTable_.getDbName(), msTable_.getTableName(), (short) -1);
        HdfsStorageDescriptor fileFormatDescriptor =
            HdfsStorageDescriptor.fromStorageDescriptor(this.getName(), msTable_.getSd());
        for (String partitionName : partitionNames) {
          ImpalaBasicPartition partition = new ImpalaBasicPartition(this, partitionName,
              fileFormatDescriptor);
          addPartition(partition);
          this.nameToIdMap.put(partitionName, partition.getId());
        }
      }
    } catch (CatalogException|IOException|TException e) {
      throw new HiveException(e);
    }
  }

  @Override
  public String getNullPartitionKeyValue() {
    return nullPartitionKeyValue;
  }

  public HiveConf getConf() {
    return conf;
  }

  public Set<String> getNamesToLoad() {
    return namesToLoad;
  }

  public Long getIdFromName(String partitionName) {
    return nameToIdMap.get(partitionName);
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
      // track this name as one we will need to retrieve from HMS at translation time.
      namesToLoad.add(partition.getPartitionName());
    }
    return partitions;
  }

  /**
   * For FENG, this always returns true.
   */
  @Override
  public boolean isStoredInImpaladCatalogCache() {
    return true;
  }
}
