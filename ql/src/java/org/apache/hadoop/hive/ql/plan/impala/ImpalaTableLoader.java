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
package org.apache.hadoop.hive.ql.plan.impala;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.RelNode;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.plan.impala.node.ImpalaHdfsScanRel;
import org.apache.hadoop.hive.ql.plan.impala.node.ImpalaPlanRel;
import org.apache.hadoop.hive.ql.plan.impala.catalog.ImpalaHdfsTable;
import org.apache.hadoop.hive.ql.plan.impala.node.ImpalaRelUtil;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.common.ImpalaException;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class manages the loading of table and partitions metadata (which includes file
 * metadata for all files within that partition) within a single query. A table may be
 * referenced multiple times in a query and each instance may have its own set of pruned
 * partitions (for partition tables). This centralized loader ensures that for each table
 * the list of partitions are combined into a unique set before asking Impala to load the
 * metadata for those partitions. For un-partitioned tables, the entire table's metadata
 * is loaded.
 */
public class ImpalaTableLoader {
  private Map<HdfsTable, Set<String>> tablePartitionMap;
  private Map<org.apache.hadoop.hive.metastore.api.Table, HdfsTable> tableMap;
  private Map<String, org.apache.hadoop.hive.metastore.api.Database> dbMap;
  private boolean loaded = false;

  public ImpalaTableLoader() {
    this.tablePartitionMap = new HashMap<>();
    this.tableMap = new HashMap<>();
    this.dbMap = new HashMap<>();
  }

  /**
   * Collect the list of table scans and for each scan the unique list of partitions.
   * Then load the table and partition metadata
   */
  public void loadTablesAndPartitions(Hive db, ImpalaPlanRel rootRelNode)
      throws ImpalaException, HiveException, MetaException {
    List<ImpalaHdfsScanRel> tableScans = Lists.newArrayList();
    ImpalaRelUtil.gatherTableScans(rootRelNode, tableScans);

    for (ImpalaHdfsScanRel hdfsScanRel : tableScans) {
      addTableAndPartitions(hdfsScanRel.getDb(), hdfsScanRel.getScan());
    }

    for (Map.Entry e : tableMap.entrySet()) {
      org.apache.hadoop.hive.metastore.api.Table msTbl =
          (org.apache.hadoop.hive.metastore.api.Table) e.getKey();
      HdfsTable hdfsTable = (HdfsTable) e.getValue();
      Set<String> partitionNames = tablePartitionMap.get(hdfsTable);
      if (msTbl.getPartitionKeysSize() > 0) {
        hdfsTable.loadSchema(msTbl);
        hdfsTable.initializePartitionMetadata(msTbl);
        String reason = "";
        // load the table and its selected partitions
        hdfsTable.load(true /* reuseMetadata */, db.getMSC(), msTbl,
            true /* loadPartitionFileMetadata */, true /* loadTableSchema */,
            partitionNames, reason);
      } else {
        // for un-partitioned tables, load metadata of the entire table
        // (note: reuseMetadata is false for such tables)
        hdfsTable.load(false /* reuseMetadata */, db.getMSC(), msTbl, "");
      }
    }
    loaded = true;
  }

  public HdfsTable loadHdfsTable(Hive db, org.apache.hadoop.hive.metastore.api.Table msTbl) throws HiveException {
    org.apache.hadoop.hive.metastore.api.Database msDb = dbMap.get(msTbl.getDbName());
    if (msDb == null) {
      // cache the Database object to avoid rpc to the metastore for future requests
      msDb = db.getDatabase(msTbl.getDbName());
      dbMap.put(msTbl.getDbName(), msDb);
    }
    HdfsTable hdfsTable = tableMap.get(msTbl);
    if (hdfsTable == null) {
      org.apache.impala.catalog.Db impalaDb = new Db(msTbl.getDbName(), msDb);
      hdfsTable = new ImpalaHdfsTable(msTbl, impalaDb, msTbl.getTableName(), msTbl.getOwner());
      tableMap.put(msTbl, hdfsTable);
      tablePartitionMap.put(hdfsTable, new HashSet<>());
    }
    return hdfsTable;
  }

  /**
   * Add the database and table to internal structures if not previously populated
   */
  public void addTableAndPartitions(Hive db, HiveTableScan scan) throws HiveException, CatalogException, MetaException {
    Table table = ((RelOptHiveTable) scan.getTable()).getHiveTableMD();

    // get the corresponding metastore Table object
    org.apache.hadoop.hive.metastore.api.Table msTbl = table.getTTable();

    HdfsTable hdfsTable = loadHdfsTable(db, msTbl);

    if (msTbl.getPartitionKeysSize() > 0) {
      // propagate Hive's statically pruned partition list to Impala
      PrunedPartitionList prunedPartList = ((RelOptHiveTable) scan.getTable()).getPrunedPartitionList();
      Set<String> partitionNames = tablePartitionMap.get(hdfsTable);
      for (org.apache.hadoop.hive.ql.metadata.Partition p : prunedPartList.getPartitions()) {
        // create the partition names in the format that Impala expects (with a trailing "/")
        String partName = p.getName() + "/";
        if (!partitionNames.contains(partName)) {
          partitionNames.add(partName);
        }
      }
    }
  }

  public HdfsTable getHdfsTable(org.apache.hadoop.hive.metastore.api.Table table) {
    Preconditions.checkState(loaded);
    return tableMap.get(table);
  }

}
