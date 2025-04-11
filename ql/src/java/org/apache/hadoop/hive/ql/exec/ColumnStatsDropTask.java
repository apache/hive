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

package org.apache.hadoop.hive.ql.exec;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.ColumnStatsDropWork;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ColumnStatsDropTask implementation. Examples:
 * ALTER TABLE src_stat DROP STATISTICS for column key;
 * ALTER TABLE src_stat_part PARTITION(partitionId=100) DROP STATISTICS for column value;
 **/

public class ColumnStatsDropTask extends Task<ColumnStatsDropWork> {
  private static final long serialVersionUID = 1L;
  private static transient final Logger LOG = LoggerFactory
      .getLogger(ColumnStatsDropTask.class);

  private int dropColumnStats(Hive db) throws HiveException, MetaException, IOException {
    String dbName = work.dbName();
    String tblName = work.getTableName();
    Table tbl = db.getTable(dbName, tblName);
    long writeId = work.getWriteId();
    String validWriteIdList = null;
    if (AcidUtils.isTransactionalTable(tbl)) {
      ValidWriteIdList writeIds = AcidUtils.getTableValidWriteIdList(conf, TableName.getDbTable(dbName, tblName));
      validWriteIdList = writeIds.toString();
    }
    if (work.getPartName() == null) {
      db.deleteTableColumnStatistics(dbName, tblName, work.getColName(), validWriteIdList, writeId);
    } else {
      db.deletePartitionColumnStatistics(dbName, tblName, work.getPartName(), work.getColName());
    }
    return 0;
  }

  @Override
  public int execute() {
    try {
      Hive db = getHive();
      return dropColumnStats(db);
    } catch (Exception e) {
      setException(e);
      LOG.info("Failed to drop stats in metastore", e);
      return ReplUtils.handleException(work.isReplication(), e, work.getDumpDirectory(), work.getMetricCollector(),
                                       getName(), conf);
    }
  }

  @Override
  public StageType getType() {
    return StageType.COLUMNSTATS;
  }

  @Override
  public String getName() {
    return "COLUMNSTATS DELETE TASK";
  }
}
