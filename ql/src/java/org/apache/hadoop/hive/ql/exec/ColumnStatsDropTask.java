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

import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.plan.ColumnStatsDropWork;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ColumnStatsDropTask implementation. Examples:
 * ALTER TABLE src_stat DROP STATISTICS for columns [comma separated list of columns];
 * ALTER TABLE src_stat_part PARTITION(partitionId=100) DROP STATISTICS for columns [comma separated list of columns];
 **/

public class ColumnStatsDropTask extends Task<ColumnStatsDropWork> {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(ColumnStatsDropTask.class);

  @Override
  public int execute() {
    try {
      getHive()
          .deleteColumnStatistics(work.getDbName(), work.getTableName(), work.getPartName(), work.getColNames());
      return 0;
    } catch (Exception e) {
      setException(e);
      LOG.info("Failed to drop column stats in metastore", e);
      return ErrorMsg.getErrorMsg(e.getMessage()).getErrorCode();
    }
  }

  @Override
  public StageType getType() {
    return StageType.COLUMNSTATS;
  }

  @Override
  public String getName() {
    return "COLUMN STATS DELETE TASK";
  }
}
