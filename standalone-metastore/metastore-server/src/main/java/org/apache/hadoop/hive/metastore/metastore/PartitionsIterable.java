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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.metastore.metastore;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.api.GetPartitionsFilterSpec;
import org.apache.hadoop.hive.metastore.api.GetPartitionsRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionFilterMode;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.builder.GetPartitionsArgs;
import org.apache.hadoop.hive.metastore.metastore.iface.TableStore;

/**
 * PartitionsIterable - effectively a lazy Iterable&lt;List&lt;Partition&gt;&gt;
 * Similar to {@link org.apache.hadoop.hive.metastore.PartitionIterable}, but
 * each iteration step returns up to {@code batchSize} partitions. Because a metastore
 * lookup by name can return fewer objects than names requested (missing partitions),
 * the iterator may perform multiple fetches in one {@code next()} until the batch
 * is full or partition names are exhausted.
 * Note that remove() is an illegal call on this, and will result in an
 * IllegalStateException.
 */
public class PartitionsIterable implements Iterable<List<Partition>> {

  private final TableStore tableStore;
  private final int batchSize;
  private final TableName tableName;
  private final List<String> partNames;
  private GetPartitionsRequest request;
  private Boolean skipColumnSchemaForPartition;

  @Override
  public Iterator<List<Partition>> iterator() {
    if (partNames.isEmpty()) {
      return Collections.emptyIterator();
    }

    return new Iterator<>() {
      private final Iterator<String> partitionNamesIter = partNames.iterator();
      private Table table;
      private List<Partition> leftParts;

      @Override
      public boolean hasNext() {
        return hasPendingPartitions() || partitionNamesIter.hasNext();
      }

      private boolean hasPendingPartitions() {
        return leftParts != null && !leftParts.isEmpty();
      }

      @Override
      public List<Partition> next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        List<Partition> result = new ArrayList<>(batchSize);
        if (hasPendingPartitions()) {
          appendPartitions(result, leftParts);
          leftParts = null;
        }
        while (result.size() < batchSize && partitionNamesIter.hasNext()) {
          List<String> nameBatch = nextNameBatch();
          appendPartitions(result, fetchBatch(nameBatch));
        }
        return result;
      }

      private void appendPartitions(List<Partition> result, List<Partition> fetched) {
        if (fetched.isEmpty()) {
          return;
        }
        int space = batchSize - result.size();
        if (fetched.size() <= space) {
          result.addAll(fetched);
        } else {
          result.addAll(fetched.subList(0, space));
          leftParts = new ArrayList<>(fetched.subList(space, fetched.size()));
        }
      }

      private List<String> nextNameBatch() {
        List<String> nameBatch = new ArrayList<>(batchSize);
        for (int i = 0; i < batchSize && partitionNamesIter.hasNext(); i++) {
          nameBatch.add(partitionNamesIter.next());
        }
        return nameBatch;
      }

      private List<Partition> fetchBatch(List<String> nameBatch) {
        try {
          List<Partition> partitions;
          if (request != null) {
            if (table == null) {
              table = tableStore.getTable(tableName, null, -1);
            }
            GetPartitionsFilterSpec filterSpec = new GetPartitionsFilterSpec();
            filterSpec.setFilterMode(PartitionFilterMode.BY_NAMES);
            filterSpec.setFilters(nameBatch);
            partitions = tableStore.getPartitionSpecsByFilterAndProjection(
                table, request.getProjectionSpec(), filterSpec);
          } else {
            GetPartitionsArgs.GetPartitionsArgsBuilder argsBuilder =
                new GetPartitionsArgs.GetPartitionsArgsBuilder().partNames(nameBatch);
            if (skipColumnSchemaForPartition != null) {
              argsBuilder.skipColumnSchemaForPartition(skipColumnSchemaForPartition);
            }
            partitions = tableStore.getPartitionsByNames(tableName, argsBuilder.build());
          }
          return partitions == null ? Collections.emptyList() : partitions;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public void remove() {
        throw new IllegalStateException(
            "PartitionsIterable is a read-only iterable and remove() is unsupported");
      }
    };
  }

  private PartitionsIterable(TableStore tableStore,
                             TableName tableName,
                             int batchSize,
                             List<String> partNames) {
    this.tableStore = Objects.requireNonNull(tableStore);
    this.tableName = Objects.requireNonNull(tableName);
    this.partNames = Objects.requireNonNull(partNames);
    this.batchSize = batchSize < 0 ? 1000 : batchSize;
  }

  public PartitionsIterable withProjectSpec(GetPartitionsRequest request) {
    this.request = request;
    return this;
  }

  public PartitionsIterable withSkipColumnSchemaForPartition(boolean skipColumnSchemaForPartition) {
    this.skipColumnSchemaForPartition = skipColumnSchemaForPartition;
    return this;
  }

  public static PartitionsIterable create(RawStore ms, TableName tableName, int batchSize)
      throws MetaException {
    List<String> partNames = ms.listPartitionNames(
        tableName.getCat(), tableName.getDb(), tableName.getTable(), (short) -1);
    return create(ms.unwrap(TableStore.class), tableName, batchSize, partNames);
  }

  public static PartitionsIterable create(TableStore tableStore, TableName tableName,
      int batchSize, List<String> partNames) throws MetaException {
    return new PartitionsIterable(tableStore, tableName, batchSize, partNames);
  }
}
