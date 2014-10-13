/**
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

package org.apache.hadoop.hive.ql.optimizer.optiq.stats;

import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.hydromatic.optiq.BuiltinMethod;
import net.hydromatic.optiq.util.BitSets;

import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveTableScanRel;
import org.apache.hadoop.hive.ql.plan.ColStatistics;
import org.eigenbase.rel.FilterRelBase;
import org.eigenbase.rel.ProjectRelBase;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.metadata.BuiltInMetadata;
import org.eigenbase.rel.metadata.Metadata;
import org.eigenbase.rel.metadata.ReflectiveRelMetadataProvider;
import org.eigenbase.rel.metadata.RelMdUniqueKeys;
import org.eigenbase.rel.metadata.RelMetadataProvider;
import org.eigenbase.relopt.hep.HepRelVertex;
import org.eigenbase.rex.RexInputRef;
import org.eigenbase.rex.RexNode;

import com.google.common.base.Function;

public class HiveRelMdUniqueKeys {

  public static final RelMetadataProvider SOURCE = ReflectiveRelMetadataProvider
      .reflectiveSource(BuiltinMethod.UNIQUE_KEYS.method,
          new HiveRelMdUniqueKeys());

  /*
   * Infer Uniquenes if: - rowCount(col) = ndv(col) - TBD for numerics: max(col)
   * - min(col) = rowCount(col)
   * 
   * Why are we intercepting ProjectRelbase and not TableScan? Because if we
   * have a method for TableScan, it will not know which columns to check for.
   * Inferring Uniqueness for all columns is very expensive right now. The flip
   * side of doing this is, it only works post Field Trimming.
   */
  public Set<BitSet> getUniqueKeys(ProjectRelBase rel, boolean ignoreNulls) {

    HiveTableScanRel tScan = getTableScan(rel.getChild(), false);

    if ( tScan == null ) {
      Function<RelNode, Metadata> fn = RelMdUniqueKeys.SOURCE.apply(
          rel.getClass(), BuiltInMetadata.UniqueKeys.class);
      return ((BuiltInMetadata.UniqueKeys) fn.apply(rel))
          .getUniqueKeys(ignoreNulls);
    }

    Map<Integer, Integer> posMap = new HashMap<Integer, Integer>();
    int projectPos = 0;
    int colStatsPos = 0;

    BitSet projectedCols = new BitSet();
    for (RexNode r : rel.getProjects()) {
      if (r instanceof RexInputRef) {
        projectedCols.set(((RexInputRef) r).getIndex());
        posMap.put(colStatsPos, projectPos);
        colStatsPos++;
      }
      projectPos++;
    }

    double numRows = tScan.getRows();
    List<ColStatistics> colStats = tScan.getColStat(BitSets
        .toList(projectedCols));
    Set<BitSet> keys = new HashSet<BitSet>();

    colStatsPos = 0;
    for (ColStatistics cStat : colStats) {
      boolean isKey = false;
      if (cStat.getCountDistint() >= numRows) {
        isKey = true;
      }
      if ( !isKey && cStat.getRange() != null &&
          cStat.getRange().maxValue != null  &&
          cStat.getRange().minValue != null) {
        double r = cStat.getRange().maxValue.doubleValue() - 
            cStat.getRange().minValue.doubleValue() + 1;
        isKey = (numRows == r);
      }
      if ( isKey ) {
        BitSet key = new BitSet();
        key.set(posMap.get(colStatsPos));
        keys.add(key);
      }
      colStatsPos++;
    }

    return keys;
  }

  /*
   * traverse a path of Filter, Projects to get to the TableScan.
   * In case of Unique keys, stop if you reach a Project, it will be handled
   * by the invocation on the Project.
   * In case of getting the base rowCount of a Path, keep going past a Project.
   */
  static HiveTableScanRel getTableScan(RelNode r, boolean traverseProject) {

    while (r != null && !(r instanceof HiveTableScanRel)) {
      if (r instanceof HepRelVertex) {
        r = ((HepRelVertex) r).getCurrentRel();
      } else if (r instanceof FilterRelBase) {
        r = ((FilterRelBase) r).getChild();
      } else if (traverseProject && r instanceof ProjectRelBase) {
        r = ((ProjectRelBase) r).getChild();
      } else {
        r = null;
      }
    }
    return r == null ? null : (HiveTableScanRel) r;
  }

}
