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

package org.apache.hadoop.hive.ql.optimizer;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.ppr.PartitionPruner;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/*
 * This is a pluggable policy to choose the candidate map-join table for converting a join to a
 * sort merge join. The largest table is chosen based on the size of the tables.
 */
public class AvgPartitionSizeBasedBigTableSelectorForAutoSMJ
    extends SizeBasedBigTableSelectorForAutoSMJ
    implements BigTableSelectorForAutoSMJ {

  private static final Log LOG = LogFactory
      .getLog(AvgPartitionSizeBasedBigTableSelectorForAutoSMJ.class.getName());

  public int getBigTablePosition(ParseContext parseCtx, JoinOperator joinOp,
      Set<Integer> bigTableCandidates)
    throws SemanticException {
    int bigTablePos = -1;
    long maxSize = -1;
    int numPartitionsCurrentBigTable = 0; // number of partitions for the chosen big table
    HiveConf conf = parseCtx.getConf();

    try {
      List<TableScanOperator> topOps = new ArrayList<TableScanOperator>();
      getListTopOps(joinOp, topOps);
      int currentPos = 0;
      for (TableScanOperator topOp : topOps) {

        if (topOp == null) {
          return -1;
        }

        if (!bigTableCandidates.contains(currentPos)) {
          currentPos++;
          continue;
        }

        int numPartitions = 1; // in case the sizes match, preference is
                               // given to the table with fewer partitions
        Table table = topOp.getConf().getTableMetadata();
        long averageSize = 0;

        if (!table.isPartitioned()) {
          averageSize = getSize(conf, table);
        }
        else {
          // For partitioned tables, get the size of all the partitions
          PrunedPartitionList partsList = PartitionPruner.prune(topOp, parseCtx, null);
          numPartitions = partsList.getNotDeniedPartns().size();
          long totalSize = 0;
          for (Partition part : partsList.getNotDeniedPartns()) {
            totalSize += getSize(conf, part);
          }
          averageSize = numPartitions == 0 ? 0 : totalSize/numPartitions;
        }

        if (averageSize > maxSize) {
          maxSize = averageSize;
          bigTablePos = currentPos;
          numPartitionsCurrentBigTable = numPartitions;
        }
        // If the sizes match, prefer the table with fewer partitions
        else if (averageSize == maxSize) {
          if (numPartitions < numPartitionsCurrentBigTable) {
            bigTablePos = currentPos;
            numPartitionsCurrentBigTable = numPartitions;
          }
        }

        currentPos++;
      }
    } catch (HiveException e) {
      throw new SemanticException(e.getMessage());
    }

    return bigTablePos;
  }
}
