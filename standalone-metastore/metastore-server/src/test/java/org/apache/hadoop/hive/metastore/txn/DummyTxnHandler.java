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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.FindNextCompactRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;

import java.util.List;
import java.util.Optional;
import java.util.Set;

public class DummyTxnHandler extends TxnHandler {

    public synchronized static void init(Configuration conf) {
    }

    @Override
    public Set<CompactionInfo> findPotentialCompactions(int abortedThreshold, long abortedTimeThreshold) throws MetaException {
        return null;
    }

    @Override
    public Set<CompactionInfo> findPotentialCompactions(int abortedThreshold, long abortedTimeThreshold, long checkInterval) throws MetaException {
        return null;
    }

    @Override
    public void updateCompactorState(CompactionInfo ci, long compactionTxnId) throws MetaException {

    }

    @Override
    public CompactionInfo findNextToCompact(String workerId) throws MetaException {
        return null;
    }

    @Override
    public CompactionInfo findNextToCompact(FindNextCompactRequest rqst) throws MetaException {
        return null;
    }

    @Override
    public void markCompacted(CompactionInfo info) throws MetaException {

    }

    @Override
    public List<CompactionInfo> findReadyToClean(long minOpenTxnWaterMark, long retentionTime) throws MetaException {
        return null;
    }

    @Override
    public void markCleaned(CompactionInfo info) throws MetaException {

    }

    @Override
    public void markFailed(CompactionInfo info) throws MetaException {

    }

    @Override
    public void cleanTxnToWriteIdTable() throws MetaException {

    }

    @Override
    public void cleanEmptyAbortedAndCommittedTxns() throws MetaException {

    }

    @Override
    public void revokeFromLocalWorkers(String hostname) throws MetaException {

    }

    @Override
    public void revokeTimedoutWorkers(long timeout) throws MetaException {

    }

    @Override
    public List<String> findColumnsWithStats(CompactionInfo ci) throws MetaException {
        return null;
    }

    @Override
    public void purgeCompactionHistory() throws MetaException {

    }

    @Override
    public boolean checkFailedCompactions(CompactionInfo ci) throws MetaException {
        return false;
    }

    @Override
    public void setHadoopJobId(String hadoopJobId, long id) {

    }

    @Override
    public long findMinOpenTxnIdForCleaner() throws MetaException {
        return 0;
    }

    @Override
    public Optional<CompactionInfo> getCompactionByTxnId(long txnId) throws MetaException {
        return Optional.empty();
    }

    @Override
    public long findMinTxnIdSeenOpen() throws MetaException {
        return 0;
    }
}
