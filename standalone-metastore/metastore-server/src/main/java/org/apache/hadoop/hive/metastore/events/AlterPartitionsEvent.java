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
package org.apache.hadoop.hive.metastore.events;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

import static java.util.Objects.requireNonNull;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class AlterPartitionsEvent extends ListenerEvent {

  private final List<Partition> old_parts;
  private final List<Partition> new_parts;
  private final Table table;
  private final boolean isTruncateOp;

  public AlterPartitionsEvent(List<Partition> old_parts, List<Partition> new_parts, Table table,
      boolean isTruncateOp, boolean status, IHMSHandler handler) {
    super(status, handler);
    this.old_parts = requireNonNull(old_parts, "old_parts is null");
    this.new_parts = requireNonNull(new_parts, "new_parts is null");
    this.table = requireNonNull(table, "table is null");
    this.isTruncateOp = isTruncateOp;
  }

  /**
   * @return The table.
   */
  public Table getTable() {
    return table;
  }

  /**
   * @return Iterator for new partitions.
   */
  public Iterator<List<Partition>> getNewPartsIterator(int batchSize) {
    return iteratePartitions(new_parts, batchSize);
  }

  /**
   * The partitions might have different write ids, groups the partitions by write id first,
   * then iterate each group, for each iteration, there is a maximum number(e.g, the batchSize)
   * of the partitions returned.
   * @param partitions partitions in this alter event
   * @param batchSize the maximum number of partitions returned in each iteration
   * @return iterator of partitions in bulk
   */
  private Iterator<List<Partition>> iteratePartitions(List<Partition> partitions,
      final int batchSize) {
    Map<Long, List<Partition>> writeIdToParts = new HashMap<>();
    partitions.forEach(part ->
        writeIdToParts.computeIfAbsent(part.getWriteId(), k -> new ArrayList<>()).add(part));
    Iterator<Map.Entry<Long, List<Partition>>> iterator = writeIdToParts.entrySet().iterator();
    return new Iterator<List<Partition>>() {
      Map.Entry<Long, List<Partition>> mapEntry;
      Iterator<Partition> current;
      @Override
      public boolean hasNext() {
        return iterator.hasNext() || (current != null && current.hasNext());
      }
      @Override
      public List<Partition> next() {
        List<Partition> result = new ArrayList<>();
        if (mapEntry == null && iterator.hasNext()) {
          mapEntry = iterator.next();
        }
        if (current == null) {
          current = mapEntry.getValue().iterator();
        } else if (!current.hasNext() && iterator.hasNext()) {
          mapEntry = iterator.next();
          current = mapEntry.getValue().iterator();
        }

        int i = 0;
        while (current != null && current.hasNext() &&
            (batchSize <= 0 || i++ < batchSize)) {
          result.add(current.next());
        }
        return result;
      }
    };
  }

  /**
   * @return Iterator for old partitions.
   */
  public Iterator<List<Partition>> getOldPartsIterator(int batchSize) {
    return iteratePartitions(old_parts, batchSize);
  }

  /**
   * Get the truncate table flag
   */
  public boolean getIsTruncateOp() {
    return isTruncateOp;
  }

}
