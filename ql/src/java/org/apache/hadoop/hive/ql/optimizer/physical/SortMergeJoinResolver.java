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
package org.apache.hadoop.hive.ql.optimizer.physical;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.lib.SemanticDispatcher;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.TaskGraphWalker;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/*
 * If a join has been automatically converted into a sort-merge join, create a conditional
 * task to try map-side join with each table as the big table. It is similar to
 * hive.auto.convert.join, but is only applicable to joins which have been automatically
 * converted to sort-merge joins. For hive.auto.convert.join, the backup task is the
 * map-reduce join, whereas here, the backup task is the sort-merge join.
 *
 * Depending on the inputs, a sort-merge join may be faster or slower than the map-side join.
 * The other advantage of sort-merge join is that the output is also bucketed and sorted.
 * Consider a very big table, say 1TB with 10 buckets being joined with a very small table, say
 * 10MB with 10 buckets, the sort-merge join may perform slower since it will be restricted to
 * 10 mappers.
 */
public class SortMergeJoinResolver implements PhysicalPlanResolver {
  @Override
  public PhysicalContext resolve(PhysicalContext pctx) throws SemanticException {

    // create dispatcher and graph walker
    SemanticDispatcher disp = new SortMergeJoinTaskDispatcher(pctx);
    TaskGraphWalker ogw = new TaskGraphWalker(disp);

    // get all the tasks nodes from root task
    List<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pctx.rootTasks);

    // begin to walk through the task tree.
    ogw.startWalking(topNodes, null);
    return pctx;
  }
}
