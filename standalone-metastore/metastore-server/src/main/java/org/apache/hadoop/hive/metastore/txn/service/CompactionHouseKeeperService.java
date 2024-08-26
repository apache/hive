/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.txn.service;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.function.FailableRunnable;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;

import java.util.concurrent.TimeUnit;

/**
 * Performs background tasks for Transaction management in Hive.
 * Runs inside Hive Metastore Service.
 */
public class CompactionHouseKeeperService extends AcidHouseKeeperService {

  public CompactionHouseKeeperService() {
    serviceName = this.getClass().getSimpleName();
  }

  @Override
  protected void initTasks(){
    tasks = ImmutableMap.<FailableRunnable<MetaException>, String>builder()
            .put(txnHandler::removeDuplicateCompletedTxnComponents,
                    "Cleaning duplicate COMPLETED_TXN_COMPONENTS entries")
            .put(txnHandler::purgeCompactionHistory, "Cleaning obsolete compaction history entries")
            .build();
  }

  @Override
  public long runFrequency(TimeUnit unit) {
    return MetastoreConf.getTimeVar(getConf(), MetastoreConf.ConfVars.COMPACTION_HOUSEKEEPER_SERVICE_INTERVAL,
        unit);
  }
}
