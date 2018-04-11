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
package org.apache.hadoop.hive.ql.optimizer;

import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorUtils;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.repl.ReplStateLogWork;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.ReplLoadWork;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.parse.GenTezProcContext;
import org.apache.hadoop.hive.ql.parse.GenTezWork;
import org.apache.hadoop.hive.ql.parse.spark.GenSparkWork;
import org.apache.hadoop.hive.ql.plan.ArchiveWork;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.BasicStatsNoJobWork;
import org.apache.hadoop.hive.ql.plan.BasicStatsWork;
import org.apache.hadoop.hive.ql.plan.ColumnStatsUpdateWork;
import org.apache.hadoop.hive.ql.plan.ConditionalWork;
import org.apache.hadoop.hive.ql.plan.CopyWork;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.plan.DependencyCollectionWork;
import org.apache.hadoop.hive.ql.plan.ExplainSQRewriteWork;
import org.apache.hadoop.hive.ql.plan.ExplainWork;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.FunctionWork;
import org.apache.hadoop.hive.ql.plan.MapredLocalWork;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.MoveWork;
import org.apache.hadoop.hive.ql.plan.SparkWork;
import org.apache.hadoop.hive.ql.plan.StatsWork;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Finds Acid FileSinkDesc objects which can be created in the physical (disconnected) plan, e.g.
 * {@link org.apache.hadoop.hive.ql.parse.GenTezUtils#removeUnionOperators(GenTezProcContext, BaseWork, int)}
 * so that statementId can be properly assigned to ensure unique ROW__IDs
 * {@link org.apache.hadoop.hive.ql.optimizer.unionproc.UnionProcFactory} is another example where
 * Union All optimizations create new FileSinkDescS
 */
public class QueryPlanPostProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(QueryPlanPostProcessor.class);

  public QueryPlanPostProcessor(List<Task<?>> rootTasks, Set<FileSinkDesc> acidSinks, String executionId) {
    for(Task<?> t : rootTasks) {
      //Work
      Object work = t.getWork();
      if(work instanceof TezWork) {
        for(BaseWork bw : ((TezWork)work).getAllWorkUnsorted()) {
          collectFileSinkDescs(bw.getAllLeafOperators(), acidSinks);
        }
      }
      else if(work instanceof BaseWork) {
        collectFileSinkDescs(((BaseWork)work).getAllLeafOperators(), acidSinks);
      }
      else if(work instanceof MapredWork) {
        MapredWork w = (MapredWork)work;
        if(w.getMapWork() != null) {
          collectFileSinkDescs(w.getMapWork().getAllLeafOperators(), acidSinks);
        }
        if(w.getReduceWork() != null) {
          collectFileSinkDescs(w.getReduceWork().getAllLeafOperators(), acidSinks);
        }
      }
      else if(work instanceof SparkWork) {
        for(BaseWork bw : ((SparkWork)work).getRoots()) {
          collectFileSinkDescs(bw.getAllLeafOperators(), acidSinks);
        }
      }
      else if(work instanceof MapredLocalWork) {
        //I don't think this can have any FileSinkOperatorS - more future proofing
        Set<FileSinkOperator> fileSinkOperatorSet = OperatorUtils.findOperators(((MapredLocalWork) work).getAliasToWork().values(), FileSinkOperator.class);
        for(FileSinkOperator fsop : fileSinkOperatorSet) {
          collectFileSinkDescs(fsop, acidSinks);
        }
      }
      else if(work instanceof ExplainWork) {
        new QueryPlanPostProcessor(((ExplainWork)work).getRootTasks(), acidSinks, executionId);
      }
      else if(work instanceof ReplLoadWork ||
        work instanceof ReplStateLogWork ||
        work instanceof GenTezWork ||
        work instanceof GenSparkWork ||
        work instanceof ArchiveWork ||
        work instanceof ColumnStatsUpdateWork ||
        work instanceof BasicStatsWork ||
        work instanceof ConditionalWork ||
        work instanceof CopyWork ||
        work instanceof DDLWork ||
        work instanceof DependencyCollectionWork ||
        work instanceof ExplainSQRewriteWork ||
        work instanceof FetchWork ||
        work instanceof FunctionWork ||
        work instanceof MoveWork ||
        work instanceof BasicStatsNoJobWork ||
        work instanceof StatsWork) {
        LOG.debug("Found " + work.getClass().getName() + " - no FileSinkOperation can be present.  executionId=" + executionId);
      }
      else {
        //if here, someone must have added new Work object - should it be walked to find FileSinks?
        throw new IllegalArgumentException("Unexpected Work object: " + work.getClass() + " executionId=" + executionId);
      }
    }
  }
  private void collectFileSinkDescs(Operator<?> leaf, Set<FileSinkDesc> acidSinks) {
    if(leaf instanceof FileSinkOperator) {
      FileSinkDesc fsd = ((FileSinkOperator) leaf).getConf();
      if(fsd.getWriteType() != AcidUtils.Operation.NOT_ACID) {
        if(acidSinks.add(fsd)) {
          if(LOG.isDebugEnabled()) {
            LOG.debug("Found Acid Sink: " + fsd.getDirName());
          }
        }
      }
    }
  }
  private void collectFileSinkDescs(Set<Operator<?>> leaves, Set<FileSinkDesc> acidSinks) {
    for(Operator<?> leaf : leaves) {
      collectFileSinkDescs(leaf, acidSinks);
    }
  }
}
