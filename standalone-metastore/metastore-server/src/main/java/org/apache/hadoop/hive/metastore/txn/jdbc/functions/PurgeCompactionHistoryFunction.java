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
package org.apache.hadoop.hive.metastore.txn.jdbc.functions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionInfo;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.txn.jdbc.InClauseBatchCommand;
import org.apache.hadoop.hive.metastore.txn.jdbc.MultiDataSourceJdbcResource;
import org.apache.hadoop.hive.metastore.txn.jdbc.TransactionalFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hive.metastore.txn.TxnStore.DID_NOT_INITIATE;
import static org.apache.hadoop.hive.metastore.txn.TxnStore.FAILED_STATE;
import static org.apache.hadoop.hive.metastore.txn.TxnStore.REFUSED_STATE;
import static org.apache.hadoop.hive.metastore.txn.TxnStore.SUCCEEDED_STATE;

public class PurgeCompactionHistoryFunction implements TransactionalFunction<Void> {

  private static final Logger LOG = LoggerFactory.getLogger(PurgeCompactionHistoryFunction.class);
  
  @Override
  public Void execute(MultiDataSourceJdbcResource jdbcResource) throws MetaException {
    NamedParameterJdbcTemplate jdbcTemplate = jdbcResource.getJdbcTemplate();
    Configuration conf = jdbcResource.getConf();
    List<Long> deleteSet = new ArrayList<>();
    long timeoutThreshold = System.currentTimeMillis() -
        MetastoreConf.getTimeVar(conf, MetastoreConf.ConfVars.COMPACTOR_HISTORY_RETENTION_TIMEOUT, TimeUnit.MILLISECONDS);
    int didNotInitiateRetention = MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.COMPACTOR_HISTORY_RETENTION_DID_NOT_INITIATE);
    int failedRetention = getFailedCompactionRetention(conf);
    int succeededRetention = MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.COMPACTOR_HISTORY_RETENTION_SUCCEEDED);
    int refusedRetention = MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.COMPACTOR_HISTORY_RETENTION_REFUSED);
        /* cc_id is monotonically increasing so for any entity sorts in order of compaction history,
        thus this query groups by entity and withing group sorts most recent first */
    jdbcTemplate.query("SELECT \"CC_ID\", \"CC_DATABASE\", \"CC_TABLE\", \"CC_PARTITION\", "
        + "\"CC_STATE\" , \"CC_START\", \"CC_TYPE\" "
        + "FROM \"COMPLETED_COMPACTIONS\" ORDER BY \"CC_DATABASE\", \"CC_TABLE\", \"CC_PARTITION\"," +
        "\"CC_ID\" DESC", rs -> {
      String lastCompactedEntity = null;
      RetentionCounters counters = null;
      /* In each group, walk from most recent and count occurrences of each state type.  Once you
       * have counted enough (for each state) to satisfy retention policy, delete all other
       * instances of this status, plus timed-out entries (see this method's JavaDoc).
       */
      while (rs.next()) {
        CompactionInfo ci = new CompactionInfo(
            rs.getLong(1), rs.getString(2), rs.getString(3),
            rs.getString(4), rs.getString(5).charAt(0));
        ci.start = rs.getLong(6);
        ci.type = TxnUtils.dbCompactionType2ThriftType(rs.getString(7).charAt(0));
        if (!ci.getFullPartitionName().equals(lastCompactedEntity)) {
          lastCompactedEntity = ci.getFullPartitionName();
          counters = new RetentionCounters(didNotInitiateRetention, failedRetention, succeededRetention, refusedRetention);
        }
        checkForDeletion(deleteSet, ci, counters, timeoutThreshold);
      }
      return null;
    });

    if (deleteSet.size() == 0) {
      return null;
    }

    int totalCount = jdbcResource.execute(new InClauseBatchCommand<>(
        "DELETE FROM \"COMPLETED_COMPACTIONS\" WHERE \"CC_ID\" in (:ids)",
        new MapSqlParameterSource().addValue("ids", deleteSet), "ids", Long::compareTo));
    LOG.debug("Removed {} records from COMPLETED_COMPACTIONS", totalCount);
    return null;
  }  

  private void checkForDeletion(List<Long> deleteSet, CompactionInfo ci, RetentionCounters rc, long timeoutThreshold) {
    switch (ci.state) {
      case DID_NOT_INITIATE:
        if(--rc.didNotInitiateRetention < 0 || timedOut(ci, rc, timeoutThreshold)) {
          deleteSet.add(ci.id);
        }
        break;
      case FAILED_STATE:
        if(--rc.failedRetention < 0 || timedOut(ci, rc, timeoutThreshold)) {
          deleteSet.add(ci.id);
        }
        break;
      case SUCCEEDED_STATE:
        if(--rc.succeededRetention < 0) {
          deleteSet.add(ci.id);
        }
        if (ci.type == CompactionType.MAJOR) {
          rc.hasSucceededMajorCompaction = true;
        } else {
          rc.hasSucceededMinorCompaction = true;
        }
        break;
      case REFUSED_STATE:
        if(--rc.refusedRetention < 0 || timedOut(ci, rc, timeoutThreshold)) {
          deleteSet.add(ci.id);
        }
        break;
      default:
        //do nothing to handle future RU/D where we may want to add new state types
    }
  }

  private static boolean timedOut(CompactionInfo ci, RetentionCounters rc, long pastTimeout) {
    return ci.start < pastTimeout
        && (rc.hasSucceededMajorCompaction || (rc.hasSucceededMinorCompaction && ci.type == CompactionType.MINOR));
  }

  /**
   * this ensures that the number of failed compaction entries retained is > than number of failed
   * compaction threshold which prevents new compactions from being scheduled.
   */
  private int getFailedCompactionRetention(Configuration conf) {
    int failedThreshold = MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.COMPACTOR_INITIATOR_FAILED_THRESHOLD);
    int failedRetention = MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.COMPACTOR_HISTORY_RETENTION_FAILED);
    if(failedRetention < failedThreshold) {
      LOG.warn("Invalid configuration {}={} < {}={}.  Will use {}={}",
          MetastoreConf.ConfVars.COMPACTOR_INITIATOR_FAILED_THRESHOLD.getVarname(), failedRetention,
          MetastoreConf.ConfVars.COMPACTOR_HISTORY_RETENTION_FAILED, failedRetention,
          MetastoreConf.ConfVars.COMPACTOR_INITIATOR_FAILED_THRESHOLD.getVarname(), failedRetention);
      failedRetention = failedThreshold;
    }
    return failedRetention;
  }  

  private static class RetentionCounters {
    int didNotInitiateRetention;
    int failedRetention;
    int succeededRetention;
    int refusedRetention;
    boolean hasSucceededMajorCompaction = false;
    boolean hasSucceededMinorCompaction = false;

    RetentionCounters(int didNotInitiateRetention, int failedRetention, int succeededRetention, int refusedRetention) {
      this.didNotInitiateRetention = didNotInitiateRetention;
      this.failedRetention = failedRetention;
      this.succeededRetention = succeededRetention;
      this.refusedRetention = refusedRetention;
    }
  }
  
}
