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

import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.CommonJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

/*
 * This is a pluggable policy to chose the candidate map-join table for converting a join to a
 * sort merge join. The largest table is chosen based on the size of the tables.
 */
public abstract class SizeBasedBigTableSelectorForAutoSMJ {
  protected void getListTopOps(
    Operator<? extends OperatorDesc> op, List<TableScanOperator> topOps) {
    if ((op.getParentOperators() == null) ||
        (op.getParentOperators().isEmpty())) {
      return;
    }

    for (Operator<? extends OperatorDesc> parentOp : op.getParentOperators()) {
      if (parentOp instanceof TableScanOperator) {
        topOps.add((TableScanOperator) parentOp);
      } else if (parentOp instanceof CommonJoinOperator) {
        topOps.add(null);
      } else {
        getListTopOps(parentOp, topOps);
      }
    }
  }

  private long getSize(HiveConf conf, String size, Path path) {
    // If the size is present in the metastore, use it
    if (size != null) {
      try {
        return Long.parseLong(size);
      } catch (NumberFormatException e) {
        return -1;
      }
    }

    try {
      FileSystem fs = path.getFileSystem(conf);
      return fs.getContentSummary(path).getLength();
    } catch (Exception e) {
      return -1;
    }
  }

  protected long getSize(HiveConf conf, Table table) {
    Path path = table.getPath();
    String size = table.getProperty("totalSize");
    return getSize(conf, size, path);
  }

  protected long getSize(HiveConf conf, Partition partition) {
    Path path = partition.getDataLocation();
    String size = partition.getParameters().get("totalSize");

    return getSize(conf, size, path);
  }
}
