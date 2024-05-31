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

import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionInfo;
import org.apache.hadoop.hive.ql.io.AcidDirectory;

/**
 * A class which contains all information required for MR/Query based compaction.
 */
public class CompactorContext {

  private final HiveConf conf;
  private final Table table;
  private final Partition partition;
  private final StorageDescriptor sd;
  private final ValidWriteIdList validWriteIdList;
  private final CompactionInfo compactionInfo;
  private final AcidDirectory dir;

  public CompactorContext(HiveConf conf, Table table, Partition p, StorageDescriptor sd, ValidWriteIdList tblValidWriteIds, CompactionInfo ci, AcidDirectory dir) {
    this.conf = conf;
    this.table = table;
    this.partition = p;
    this.sd = sd;
    this.validWriteIdList = tblValidWriteIds;
    this.compactionInfo = ci;
    this.dir = dir;
  }

  public CompactorContext(HiveConf conf, Table table, CompactionInfo ci) {
    this(conf, table, null, null, null, ci, null);
  }

  public HiveConf getConf() {
    return conf;
  }

  public Table getTable() {
    return table;
  }

  public Partition getPartition() {
    return partition;
  }

  public StorageDescriptor getSd() {
    return sd;
  }

  public ValidWriteIdList getValidWriteIdList() {
    return validWriteIdList;
  }

  public CompactionInfo getCompactionInfo() {
    return compactionInfo;
  }

  public AcidDirectory getAcidDirectory() {
    return dir;
  }
}
