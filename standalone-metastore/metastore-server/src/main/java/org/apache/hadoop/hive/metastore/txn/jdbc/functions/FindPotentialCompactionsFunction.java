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
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionInfo;
import org.apache.hadoop.hive.metastore.txn.jdbc.queries.AbortedTxnHandler;
import org.apache.hadoop.hive.metastore.txn.jdbc.queries.CompactionCandidateHandler;
import org.apache.hadoop.hive.metastore.txn.jdbc.MultiDataSourceJdbcResource;
import org.apache.hadoop.hive.metastore.txn.jdbc.TransactionalFunction;

import java.util.HashSet;
import java.util.Set;

public class FindPotentialCompactionsFunction implements TransactionalFunction<Set<CompactionInfo>> {
  
  private final int fetchSize;
  private final int abortedThreshold;
  private final long abortedTimeThreshold;
  private final long lastChecked;
  private final boolean collectAbortedTxns;

  public FindPotentialCompactionsFunction(Configuration conf, int abortedThreshold, long abortedTimeThreshold, long lastChecked) {
    this.fetchSize = MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.COMPACTOR_FETCH_SIZE);
    this.abortedThreshold = abortedThreshold;
    this.abortedTimeThreshold = abortedTimeThreshold;
    this.lastChecked = lastChecked;
    this.collectAbortedTxns = !MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.COMPACTOR_CLEAN_ABORTS_USING_CLEANER);
  }

  @Override
  public Set<CompactionInfo> execute(MultiDataSourceJdbcResource jdbcResource) throws MetaException {
    Set<CompactionInfo> candidates = new HashSet<>(jdbcResource.execute(
        new CompactionCandidateHandler(lastChecked, fetchSize)));
    int remaining = fetchSize - candidates.size();
    if (collectAbortedTxns && remaining > 0) {
      candidates.addAll(jdbcResource.execute(new AbortedTxnHandler(abortedTimeThreshold, abortedThreshold, remaining)));
    }
    return candidates;
  }
}
