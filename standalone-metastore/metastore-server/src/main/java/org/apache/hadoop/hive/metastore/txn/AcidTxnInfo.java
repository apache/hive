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
package org.apache.hadoop.hive.metastore.txn;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.hadoop.hive.common.ValidCompactorWriteIdList;
import org.apache.hadoop.hive.metastore.api.TableValidWriteIds;

import java.util.Set;

/**
 * A class used for encapsulating information of abort-cleanup activities and compaction activities.
 */
public class AcidTxnInfo {
  public String dbname;
  public String tableName;
  public String partName;
  public String runAs;
  public String fullTableName;
  public String fullPartitionName;
  /**
   * The highest write id that the compaction job will pay attention to.
   * {@code 0} means it wasn't set (e.g. in case of upgrades, since ResultSet.getLong() will return 0 if field is NULL)
   * See also {@link TxnUtils#createValidCompactWriteIdList(TableValidWriteIds)} and
   * {@link ValidCompactorWriteIdList#highWatermark}.
   */
  public long highestWriteId;
  public long txnId = 0;
  public boolean hasUncompactedAborts;
  public Set<Long> writeIds;

  public void setWriteIds(boolean hasUncompactedAborts, Set<Long> writeIds) {
    this.hasUncompactedAborts = hasUncompactedAborts;
    this.writeIds = writeIds;
  }

  public String getFullPartitionName() {
    if (fullPartitionName == null) {
      StringBuilder buf = new StringBuilder(dbname);
      buf.append('.');
      buf.append(tableName);
      if (partName != null) {
        buf.append('.');
        buf.append(partName);
      }
      fullPartitionName = buf.toString();
    }
    return fullPartitionName;
  }

  public String getFullTableName() {
    if (fullTableName == null) {
      StringBuilder buf = new StringBuilder(dbname);
      buf.append('.');
      buf.append(tableName);
      fullTableName = buf.toString();
    }
    return fullTableName;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
            .append("dbname", dbname)
            .append("tableName", tableName)
            .append("partName", partName)
            .append("runAs", runAs)
            .append("highestWriteId", highestWriteId)
            .append("txnId", txnId)
            .append("hasUncompactedAborts", hasUncompactedAborts)
            .build();
  }
}
