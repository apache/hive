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

import com.google.common.collect.Lists;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.txn.CompactionInfo;
import org.apache.hadoop.hive.ql.io.AcidDirectory;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

final class RebalanceQueryCompactor extends QueryCompactor {

  @Override
  public void run(HiveConf hiveConf, Table table, Partition partition, StorageDescriptor storageDescriptor,
           ValidWriteIdList writeIds, CompactionInfo compactionInfo, AcidDirectory dir)
      throws IOException, HiveException {
    AcidUtils.setAcidOperationalProperties(hiveConf, true, AcidUtils.getAcidOperationalProperties(table.getParameters()));

    // Set up the session for driver.
    HiveConf conf = new HiveConf(hiveConf);

    String tmpTableName = getTempTableName(table);
    Path tmpTablePath = QueryCompactor.Util.getCompactionResultDir(storageDescriptor, writeIds,
        conf, true, false, false, null);

    //TODO: This is quite expensive, a better way should be found to get the number of buckets for an implicitly bucketed table
    int numBuckets = FileUtils.asStream(FileUtils.listFiles(dir.getFs(), dir.getPath(), true, AcidUtils.bucketFileFilter))
        .map(f -> AcidUtils.parseBucketId(f.getPath()))
        .collect(Collectors.toSet()).size();

    List<String> createQueries = getCreateQueries(tmpTableName, table, tmpTablePath.toString());
    List<String> compactionQueries = getCompactionQueries(table, partition, tmpTableName, numBuckets);
    List<String> dropQueries = getDropQueries(tmpTableName);
    runCompactionQueries(conf, tmpTableName, storageDescriptor, writeIds, compactionInfo,
        Lists.newArrayList(tmpTablePath), createQueries, compactionQueries, dropQueries,
        table.getParameters());
  }

  private List<String> getCreateQueries(String fullName, Table t, String tmpTableLocation) {
    return Lists.newArrayList(new CompactionQueryBuilder(
        CompactionType.REBALANCE,
        CompactionQueryBuilder.Operation.CREATE,
        false,
        fullName)
        .setSourceTab(t)
        .setLocation(tmpTableLocation)
        .build());
  }

  private List<String> getCompactionQueries(Table t, Partition p, String tmpName, int numberOfBuckets) {
    return Lists.newArrayList(
        new CompactionQueryBuilder(
            CompactionType.REBALANCE,
            CompactionQueryBuilder.Operation.INSERT,
            false,
            tmpName)
            .setSourceTab(t)
            .setSourcePartition(p)
            .setNumberOfBuckets(numberOfBuckets)
            .build());
  }

  private List<String> getDropQueries(String tmpTableName) {
    return Lists.newArrayList(
        new CompactionQueryBuilder(
            CompactionType.REBALANCE,
            CompactionQueryBuilder.Operation.DROP,
            false,
            tmpTableName).build());
  }
}
