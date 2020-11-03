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
package org.apache.hadoop.hive.ql.plan.impala.prune;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.calcite.rex.RexNode;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.plan.impala.funcmapper.ImpalaConjuncts;
import org.apache.impala.catalog.HdfsPartition;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Extension of PrunedPartitionList containing objects specific for Impala.
 * It is recommended that callers don't pre-sort the impalaBasicPartitions
 * list supplied in the constructor because this class does the sorting the
 * first time the list is accessed and is maintained sorted thereafter.
 * Note: This class is not thread safe.
 */
public class ImpalaPrunedPartitionList extends PrunedPartitionList {
  // "Basic" partitions is an Impala HdfsPartition object that only
  // contains the name (no data other than the name was fetched from
  // HMS). See comments above about sortedness of this list
  private final List<ImpalaBasicPartition> impalaBasicPartitions;

  private boolean isPartitionListSorted = false;

  // The Impala HdfsTable wrapper containing the real HdfsTable catalog
  // table.  Used to defer loading partition and filemetadata information
  // until translation time.
  private final ImpalaBasicHdfsTable table;

  // ImpalaConjuncts object holding partitioned and unpartitioned conjuncts
  private final ImpalaConjuncts impalaConjuncts;

  public ImpalaPrunedPartitionList(Table source, String key, Set<Partition> partitions,
      boolean hasUnknowns, List<ImpalaBasicPartition> impalaBasicPartitions,
      ImpalaBasicHdfsTable table, ImpalaConjuncts impalaConjuncts) {
    super(source, key, partitions, Lists.newArrayList(), hasUnknowns);
    this.impalaBasicPartitions = impalaBasicPartitions;
    this.table = table;
    this.impalaConjuncts = impalaConjuncts;
  }

  /**
   * Return an unmodifiable 'view' of the list of basic partitions sorted
   * by their value
   */
  public List<ImpalaBasicPartition> getSortedBasicPartitions() {
    if (!isPartitionListSorted) {
      Collections.sort(impalaBasicPartitions, HdfsPartition.KV_COMPARATOR);
      isPartitionListSorted = true;
    }
    return Collections.unmodifiableList(impalaBasicPartitions);
  }

  public ImpalaBasicHdfsTable getTable() throws HiveException {
    return table;
  }

  public List<RexNode> getPartitionConjuncts() {
    return impalaConjuncts.getPartitionConjuncts();
  }
}
