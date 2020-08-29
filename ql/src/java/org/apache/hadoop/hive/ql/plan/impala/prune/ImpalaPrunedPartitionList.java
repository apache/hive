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

import org.apache.calcite.rex.RexNode;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.plan.impala.funcmapper.ImpalaConjuncts;

import java.util.List;
import java.util.Set;

/**
 * Extension of PrunedPartitionList containing objects specific for Impala.
 */
public class ImpalaPrunedPartitionList extends PrunedPartitionList {
  // "Basic" partitions is an Impala HdfsPartition object that only
  // contains the name (no data other than the name was fetched from
  // HMS).
  private final List<ImpalaBasicPartition> impalaBasicPartitions;

  // The Impala HdfsTable wrapper containing the real HdfsTable catalog
  // table.  Used to defer loading partition and filemetadata information
  // until translation time.
  private final ImpalaBasicHdfsTable table;

  // ImpalaConjuncts object holding partitioned and unpartitioned conjuncts
  private final ImpalaConjuncts impalaConjuncts;

  public ImpalaPrunedPartitionList(Table source, String key, Set<Partition> partitions,
      List<String> referred, boolean hasUnknowns, List<ImpalaBasicPartition> impalaBasicPartitions,
      ImpalaBasicHdfsTable table, ImpalaConjuncts impalaConjuncts) {
    super(source, createKey(impalaConjuncts), partitions, referred, hasUnknowns);
    this.impalaBasicPartitions = impalaBasicPartitions;
    this.table = table;
    this.impalaConjuncts = impalaConjuncts;
  }

  public List<ImpalaBasicPartition> getBasicPartitions() throws HiveException {
    return impalaBasicPartitions;
  }

  public ImpalaBasicHdfsTable getTable() throws HiveException {
    return table;
  }

  public List<RexNode> getPartitionConjuncts() {
    return impalaConjuncts.getPartitionConjuncts();
  }

  private static String createKey(ImpalaConjuncts conjuncts) {
    String key = "";
    for (RexNode r : conjuncts.getPartitionConjuncts()) {
      key += r.toString() + ":";
    }
    return key;
  }
}
