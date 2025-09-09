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
package org.apache.hadoop.hive.ql.optimizer.calcite;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.hadoop.hive.ql.plan.ColStatistics;

import java.util.List;
import java.util.Map;

public class HiveRelJsonImplWithStats extends HiveRelJsonImpl{
  @Override
  protected void explain_(RelNode rel, List<Pair<String, Object>> values) {
    super.explain_(rel, values);
    RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    Map<String, Object> map = (Map<String, Object>) relList.get(relList.size() - 1);
    map.put("rowCount", mq.getRowCount(rel));

    // This is a leaf, we will print the average row size and schema
    // We also include column stats
    if (rel.getInputs().isEmpty()) {
      RelOptHiveTable table = (RelOptHiveTable) rel.getTable();
      if (table == null) {
        return;
      }

      map.put("avgRowSize", mq.getAverageRowSize(rel));
      List<ColStatistics> colStats = table.getColStat(
          ImmutableBitSet.range(0, table.getNoOfNonVirtualCols()).asList(), true);
      List<Object> list = jsonBuilder.list();
      for (ColStatistics cs : colStats) {
        final Map<String, Object> csMap = jsonBuilder.map();
        csMap.put("name", cs.getColumnName());
        csMap.put("ndv", cs.getCountDistint());
        if (cs.getRange() != null) {
          csMap.put("minValue", cs.getRange().minValue);
          csMap.put("maxValue", cs.getRange().maxValue);
        }
        list.add(csMap);
      }
      if (!list.isEmpty()) {
        map.put("colStats", list);
      }
    }
  }
}
