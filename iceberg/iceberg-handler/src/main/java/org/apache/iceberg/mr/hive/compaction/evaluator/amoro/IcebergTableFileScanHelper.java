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

package org.apache.iceberg.mr.hive.compaction.evaluator.amoro;

import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

public class IcebergTableFileScanHelper implements TableFileScanHelper {
  private final Table table;
  private Expression partitionFilter = Expressions.alwaysTrue();
  private final long snapshotId;

  public IcebergTableFileScanHelper(Table table, long snapshotId) {
    this.table = table;
    this.snapshotId = snapshotId;
  }

  @Override
  public CloseableIterable<FileScanResult> scan() {
    return CloseableIterable.transform(
        table.newScan().useSnapshot(snapshotId).filter(partitionFilter).planFiles(),
        this::buildFileScanResult);
  }

  protected FileScanResult buildFileScanResult(FileScanTask fileScanTask) {
    return new FileScanResult(fileScanTask.file(), Lists.newArrayList(fileScanTask.deletes()));
  }

  @Override
  public TableFileScanHelper withPartitionFilter(Expression filter) {
    this.partitionFilter = filter;
    return this;
  }
}
