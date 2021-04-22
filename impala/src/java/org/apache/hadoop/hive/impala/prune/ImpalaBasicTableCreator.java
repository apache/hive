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

import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.GetPartitionNamesPsRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionNamesPsResponse;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.impala.plan.ImpalaQueryContext;
import org.apache.hadoop.hive.impala.catalog.ImpalaPartitionNamesConverter.ImpalaPartitionNamesRequest;
import org.apache.hadoop.hive.impala.catalog.ImpalaPartitionNamesConverter.ImpalaPartitionNamesResult;

/**
 * Class used to create an ImpalaBasicTable object. The static factory
 * method also caches the created basic table in the QueryContext.
 */
public class ImpalaBasicTableCreator {

  public static ImpalaBasicHdfsTable createNonPartitionedTable(RelOptHiveTable table,
      ImpalaQueryContext context) throws HiveException {
    Table msTbl = table.getHiveTableMD().getTTable();
    return new ImpalaBasicHdfsTable(msTbl, context.getDb(table), getValidWriteIdList(msTbl, context));
  }

  public static ImpalaBasicHdfsTable createPartitionedTable(RelOptHiveTable table,
      ImpalaQueryContext context) throws HiveException {
    try {
      Table msTbl = table.getHiveTableMD().getTTable();
      Database msDb = context.getDb(table);

      String tableName = msTbl.getDbName() + "." + msTbl.getTableName();
      ValidWriteIdList validWriteIdList = getValidWriteIdList(msTbl, context);

      HiveConf conf = context.getConf();
      GetPartitionNamesPsRequest request = new ImpalaPartitionNamesRequest(msTbl, msDb,
        validWriteIdList, conf.getVar(HiveConf.ConfVars.DEFAULTPARTITIONNAME),
        table.getPartColumns().size());
      GetPartitionNamesPsResponse psResponse = table.getMSC().listPartitionNamesRequest(request);
      ImpalaPartitionNamesResult impalaResult = (ImpalaPartitionNamesResult) psResponse;
      return impalaResult.basicTable;
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  private static ValidWriteIdList getValidWriteIdList(Table msTbl,
      ImpalaQueryContext context) throws HiveException {
    HiveConf conf = context.getConf();
    String tableName = msTbl.getDbName() + "." + msTbl.getTableName();
    return AcidUtils.isTransactionalTable(msTbl) ? context.getValidWriteIdList(conf, tableName) : null;
  }
}
