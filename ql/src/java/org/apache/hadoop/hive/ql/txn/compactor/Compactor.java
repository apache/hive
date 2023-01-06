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
package org.apache.hadoop.hive.ql.txn.compactor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.ValidReadTxnList;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.txn.CompactionInfo;
import org.apache.hadoop.hive.ql.io.AcidDirectory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.parquet.Strings;

import java.io.IOException;

public interface Compactor {

  String FINAL_LOCATION = "hive.compactor.input.dir";

  static long getCompactorTxnId(Configuration jobConf) {
    String snapshot = jobConf.get(ValidTxnList.VALID_TXNS_KEY);
    if(Strings.isNullOrEmpty(snapshot)) {
      throw new IllegalStateException(ValidTxnList.VALID_TXNS_KEY + " not found for writing to "
          + jobConf.get(FINAL_LOCATION));
    }
    ValidTxnList validTxnList = new ValidReadTxnList();
    validTxnList.readFromString(snapshot);
    //this is id of the current (compactor) txn
    return validTxnList.getHighWatermark();
  }

  /**
   * Start a compaction.
   * @param hiveConf hive configuration
   * @param table the table, where the compaction should run
   * @param partition the partition, where the compaction should run
   * @param storageDescriptor this is the resolved storage descriptor
   * @param writeIds valid write IDs used to filter rows while they're being read for compaction
   * @param compactionInfo provides info about the type of compaction
   * @param dir provides ACID directory layout information
   * @throws IOException compaction cannot be finished.
   */
  boolean run(HiveConf hiveConf, Table table, Partition partition, StorageDescriptor storageDescriptor,
           ValidWriteIdList writeIds, CompactionInfo compactionInfo, AcidDirectory dir)
      throws IOException, HiveException, InterruptedException;

}
