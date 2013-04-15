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
 * This is a pluggable policy to chose the candidate map-join table for converting a join to a
 * sort merge join. The largest table is chosen based on the size of the tables.
 */
public class TableSizeBasedBigTableSelectorForAutoSMJ extends SizeBasedBigTableSelectorForAutoSMJ
implements BigTableSelectorForAutoSMJ {
  public int getBigTablePosition(ParseContext parseCtx, JoinOperator joinOp)
    throws SemanticException {
    int bigTablePos = 0;
    long maxSize = 0;
    HiveConf conf = parseCtx.getConf();

    try {
      List<TableScanOperator> topOps = new ArrayList<TableScanOperator>();
      getListTopOps(joinOp, topOps);
      int currentPos = 0;
      for (TableScanOperator topOp : topOps) {
        Table table = parseCtx.getTopToTable().get(topOp);
        long currentSize = 0;

        if (!table.isPartitioned()) {
          currentSize = getSize(conf, table);
        }
        else {
          // For partitioned tables, get the size of all the partitions
          PrunedPartitionList partsList =
            PartitionPruner.prune(parseCtx.getTopToTable().get(topOp),
              parseCtx.getOpToPartPruner().get(topOp), parseCtx.getConf(),
              null, parseCtx.getPrunedPartitions());
          for (Partition part : partsList.getNotDeniedPartns()) {
            currentSize += getSize(conf, part);
          }
        }

        if (currentSize > maxSize) {
          maxSize = currentSize;
          bigTablePos = currentPos;
        }
        currentPos++;
      }
    } catch (HiveException e) {
      throw new SemanticException(e.getMessage());
    }

    return bigTablePos;
  }
}
