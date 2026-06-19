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
package org.apache.hadoop.hive.ql.optimizer.calcite.stats;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;
import org.apache.hadoop.hive.ql.optimizer.calcite.Bug;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;

public class HiveRelMdUniqueKeys implements MetadataHandler<BuiltInMetadata.UniqueKeys> {

  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          BuiltInMethod.UNIQUE_KEYS.method, new HiveRelMdUniqueKeys());
  /**
   * A limit about the number of unique keys returned by the handler.
   * The limit must be in the range [0, Integer.MAX_VALUE].
   */
  private final int limit;

  private HiveRelMdUniqueKeys() {
    if (Bug.CALCITE_6704_FIXED) {
      throw new IllegalStateException("Remove constructor and limit once we upgrade to version with CALCITE-6704");
    }
    this.limit = 1000;
  }

  @Override
  public MetadataDef<BuiltInMetadata.UniqueKeys> getDef() {
    return BuiltInMetadata.UniqueKeys.DEF;
  }

  public Set<ImmutableBitSet> getUniqueKeys(Project rel, RelMetadataQuery mq,
      boolean ignoreNulls) {
    return getProjectUniqueKeys(rel, mq, ignoreNulls, rel.getProjects());
  }

  private Set<ImmutableBitSet> getProjectUniqueKeys(SingleRel rel, RelMetadataQuery mq,
      boolean ignoreNulls, List<RexNode> projExprs) {
    if (Bug.CALCITE_6704_FIXED) {
      throw new IllegalStateException("Method is redundant once we upgrade to version with CALCITE-6704");

    }
    // LogicalProject maps a set of rows to a different set;
    // Without knowledge of the mapping function(whether it
    // preserves uniqueness), it is only safe to derive uniqueness
    // info from the child of a project when the mapping is f(a) => a.
    //
    // Further more, the unique bitset coming from the child needs
    // to be mapped to match the output of the project.

    // Single input can be mapped to multiple outputs
    ImmutableMultimap.Builder<Integer, Integer> inToOutPosBuilder = ImmutableMultimap.builder();
    ImmutableBitSet.Builder mappedInColumnsBuilder = ImmutableBitSet.builder();

    // Build an input to output position map.
    for (int i = 0; i < projExprs.size(); i++) {
      RexNode projExpr = projExprs.get(i);
      if (projExpr instanceof RexInputRef) {
        int inputIndex = ((RexInputRef) projExpr).getIndex();
        inToOutPosBuilder.put(inputIndex, i);
        mappedInColumnsBuilder.set(inputIndex);
      }
    }
    ImmutableBitSet inColumnsUsed = mappedInColumnsBuilder.build();

    if (inColumnsUsed.isEmpty()) {
      // if there's no RexInputRef in the projected expressions
      // return empty set.
      return ImmutableSet.of();
    }

    Set<ImmutableBitSet> childUniqueKeySet =
        mq.getUniqueKeys(rel.getInput(), ignoreNulls);

    if (childUniqueKeySet == null) {
      return ImmutableSet.of();
    }

    Multimap<Integer, Integer> mapInToOutPos = inToOutPosBuilder.build();

    Set<ImmutableBitSet> resultBuilder = new HashSet<>();
    // Now add to the projUniqueKeySet the child keys that are fully
    // projected.
    outerLoop:
    for (ImmutableBitSet colMask : childUniqueKeySet) {
      if (!inColumnsUsed.contains(colMask)) {
        // colMask contains a column that is not projected as RexInput => the key is not unique
        continue;
      }
      // colMask is mapped to output project, however, the column can be mapped more than once:
      // select key1, key1, val1, val2, key2 from ...
      // the resulting unique keys would be {{0},{4}}, {{1},{4}}

      Iterable<List<Integer>> product = Linq4j.product(Util.transform(colMask.toList(), mapInToOutPos::get));
      for (List<Integer> passKey : product) {
        if (resultBuilder.size() == limit) {
          break outerLoop;
        }
        resultBuilder.add(ImmutableBitSet.of(passKey));
      }
    }
    return resultBuilder;
  }

  public Set<ImmutableBitSet> getUniqueKeys(HiveTableScan rel, RelMetadataQuery mq,
                                            boolean ignoreNulls) {
    RelOptHiveTable tbl = (RelOptHiveTable) rel.getTable();
    List<ImmutableBitSet> keyList = tbl.getNonNullableKeys();
    if (keyList != null) {
      Set<ImmutableBitSet> keySet = new HashSet<>(keyList);
      return keySet;
    }
    return null;
  }
}
