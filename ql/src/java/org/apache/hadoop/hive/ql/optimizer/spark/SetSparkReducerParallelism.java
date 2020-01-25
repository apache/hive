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

package org.apache.hadoop.hive.ql.optimizer.spark;

import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.Stack;

import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.LimitOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorUtils;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.TerminalOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.spark.SparkUtilities;
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSession;
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSessionManager;
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSessionManagerImpl;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.SemanticNodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.spark.GenSparkUtils;
import org.apache.hadoop.hive.ql.parse.spark.OptimizeSparkProcContext;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.stats.StatsUtils;

import static org.apache.hadoop.hive.ql.plan.ReduceSinkDesc.ReducerTraits.UNIFORM;

/**
 * SetSparkReducerParallelism determines how many reducers should
 * be run for a given reduce sink, clone from SetReducerParallelism.
 */
public class SetSparkReducerParallelism implements SemanticNodeProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(SetSparkReducerParallelism.class.getName());

  private static final String SPARK_DYNAMIC_ALLOCATION_ENABLED = "spark.dynamicAllocation.enabled";

  // Spark memory per task, and total number of cores
  private Pair<Long, Integer> sparkMemoryAndCores;
  private final boolean useOpStats;

  public SetSparkReducerParallelism(HiveConf conf) {
    sparkMemoryAndCores = null;
    useOpStats = conf.getBoolVar(HiveConf.ConfVars.SPARK_USE_OP_STATS);
  }

  @Override
  public Object process(Node nd, Stack<Node> stack,
                        NodeProcessorCtx procContext, Object... nodeOutputs)
      throws SemanticException {

    OptimizeSparkProcContext context = (OptimizeSparkProcContext) procContext;

    ReduceSinkOperator sink = (ReduceSinkOperator) nd;
    ReduceSinkDesc desc = sink.getConf();
    Set<ReduceSinkOperator> parentSinks = null;

    int maxReducers = context.getConf().getIntVar(HiveConf.ConfVars.MAXREDUCERS);
    int constantReducers = context.getConf().getIntVar(HiveConf.ConfVars.HADOOPNUMREDUCERS);

    if (!useOpStats) {
      parentSinks = OperatorUtils.findOperatorsUpstream(sink, ReduceSinkOperator.class);
      parentSinks.remove(sink);
      if (!context.getVisitedReduceSinks().containsAll(parentSinks)) {
        // We haven't processed all the parent sinks, and we need
        // them to be done in order to compute the parallelism for this sink.
        // In this case, skip. We should visit this again from another path.
        LOG.debug("Skipping sink " + sink + " for now as we haven't seen all its parents.");
        return false;
      }
    }

    if (context.getVisitedReduceSinks().contains(sink)) {
      // skip walking the children
      LOG.debug("Already processed reduce sink: " + sink.getName());
      return true;
    }
    context.getVisitedReduceSinks().add(sink);

    if (needSetParallelism(sink, context.getConf())) {
      if (constantReducers > 0) {
        LOG.info("Parallelism for reduce sink " + sink + " set by user to " + constantReducers);
        desc.setNumReducers(constantReducers);
      } else {
        //If it's a FileSink to bucketed files, use the bucket count as the reducer number
        FileSinkOperator fso = GenSparkUtils.getChildOperator(sink, FileSinkOperator.class);
        if (fso != null) {
          String bucketCount = fso.getConf().getTableInfo().getProperties().getProperty(
            hive_metastoreConstants.BUCKET_COUNT);
          int numBuckets = bucketCount == null ? 0 : Integer.parseInt(bucketCount);
          if (numBuckets > 0) {
            LOG.info("Set parallelism for reduce sink " + sink + " to: " + numBuckets + " (buckets)");
            desc.setNumReducers(numBuckets);
            return false;
          }
        }

        if (useOpStats || parentSinks.isEmpty()) {
          long numberOfBytes = 0;
          if (useOpStats) {
            // we need to add up all the estimates from the siblings of this reduce sink
            for (Operator<? extends OperatorDesc> sibling
                : sink.getChildOperators().get(0).getParentOperators()) {
              if (sibling.getStatistics() != null) {
                numberOfBytes = StatsUtils.safeAdd(numberOfBytes, sibling.getStatistics().getDataSize());
                if (LOG.isDebugEnabled()) {
                  LOG.debug("Sibling " + sibling + " has stats: " + sibling.getStatistics());
                }
              } else {
                LOG.warn("No stats available from: " + sibling);
              }
            }
          } else {
            // Not using OP stats and this is the first sink in the path, meaning that
            // we should use TS stats to infer parallelism
            for (Operator<? extends OperatorDesc> sibling
                : sink.getChildOperators().get(0).getParentOperators()) {
              Set<TableScanOperator> sources =
                  OperatorUtils.findOperatorsUpstream(sibling, TableScanOperator.class);
              for (TableScanOperator source : sources) {
                if (source.getStatistics() != null) {
                  numberOfBytes = StatsUtils.safeAdd(numberOfBytes, source.getStatistics().getDataSize());
                  if (LOG.isDebugEnabled()) {
                    LOG.debug("Table source " + source + " has stats: " + source.getStatistics());
                  }
                } else {
                  LOG.warn("No stats available from table source: " + source);
                }
              }
            }
            LOG.debug("Gathered stats for sink " + sink + ". Total size is "
                + numberOfBytes + " bytes.");
          }

          // Divide it by 2 so that we can have more reducers
          long bytesPerReducer = context.getConf().getLongVar(HiveConf.ConfVars.BYTESPERREDUCER) / 2;
          int numReducers = Utilities.estimateReducers(numberOfBytes, bytesPerReducer,
              maxReducers, false);

          getSparkMemoryAndCores(context);
          if (sparkMemoryAndCores != null &&
              sparkMemoryAndCores.getLeft() > 0 && sparkMemoryAndCores.getRight() > 0) {
            // warn the user if bytes per reducer is much larger than memory per task
            if ((double) sparkMemoryAndCores.getLeft() / bytesPerReducer < 0.5) {
              LOG.warn("Average load of a reducer is much larger than its available memory. " +
                  "Consider decreasing hive.exec.reducers.bytes.per.reducer");
            }

            // If there are more cores, use the number of cores
            numReducers = Math.max(numReducers, sparkMemoryAndCores.getRight());
          }
          numReducers = Math.min(numReducers, maxReducers);
          LOG.info("Set parallelism for reduce sink " + sink + " to: " + numReducers +
              " (calculated)");
          desc.setNumReducers(numReducers);
        } else {
          // Use the maximum parallelism from all parent reduce sinks
          int numberOfReducers = 0;
          for (ReduceSinkOperator parent : parentSinks) {
            numberOfReducers = Math.max(numberOfReducers, parent.getConf().getNumReducers());
          }
          desc.setNumReducers(numberOfReducers);
          LOG.debug("Set parallelism for sink " + sink + " to " + numberOfReducers
              + " based on its parents");
        }
        final Collection<ExprNodeDesc.ExprNodeDescEqualityWrapper> keyCols =
            ExprNodeDesc.ExprNodeDescEqualityWrapper.transform(desc.getKeyCols());
        final Collection<ExprNodeDesc.ExprNodeDescEqualityWrapper> partCols =
            ExprNodeDesc.ExprNodeDescEqualityWrapper.transform(desc.getPartitionCols());
        if (keyCols != null && keyCols.equals(partCols)) {
          desc.setReducerTraits(EnumSet.of(UNIFORM));
        }
      }
    } else {
      LOG.info("Number of reducers for sink " + sink + " was already determined to be: " + desc.getNumReducers());
    }

    return false;
  }

  // tests whether the RS needs automatic setting parallelism
  private boolean needSetParallelism(ReduceSinkOperator reduceSink, HiveConf hiveConf) {
    ReduceSinkDesc desc = reduceSink.getConf();
    if (desc.getNumReducers() <= 0) {
      return true;
    }
    if (desc.getNumReducers() == 1 && desc.hasOrderBy() &&
        hiveConf.getBoolVar(HiveConf.ConfVars.HIVESAMPLINGFORORDERBY) && !desc.isDeduplicated()) {
      Stack<Operator<? extends OperatorDesc>> descendants = new Stack<Operator<? extends OperatorDesc>>();
      List<Operator<? extends OperatorDesc>> children = reduceSink.getChildOperators();
      if (children != null) {
        for (Operator<? extends OperatorDesc> child : children) {
          descendants.push(child);
        }
      }
      while (descendants.size() != 0) {
        Operator<? extends OperatorDesc> descendant = descendants.pop();
        //If the decendants contains LimitOperator,return false
        if (descendant instanceof LimitOperator) {
          return false;
        }
        boolean reachTerminalOperator = (descendant instanceof TerminalOperator);
        if (!reachTerminalOperator) {
          List<Operator<? extends OperatorDesc>> childrenOfDescendant = descendant.getChildOperators();
          if (childrenOfDescendant != null) {
            for (Operator<? extends OperatorDesc> childOfDescendant : childrenOfDescendant) {
              descendants.push(childOfDescendant);
            }
          }
        }
      }
      return true;
    }
    return false;

  }

  private void getSparkMemoryAndCores(OptimizeSparkProcContext context) throws SemanticException {
    if (sparkMemoryAndCores != null) {
      return;
    }
    if (context.getConf().getBoolean(SPARK_DYNAMIC_ALLOCATION_ENABLED, false)) {
      // If dynamic allocation is enabled, numbers for memory and cores are meaningless. So, we don't
      // try to get it.
      sparkMemoryAndCores = null;
      return;
    }

    SparkSessionManager sparkSessionManager = null;
    SparkSession sparkSession = null;
    try {
      sparkSessionManager = SparkSessionManagerImpl.getInstance();
      sparkSession = SparkUtilities.getSparkSession(
          context.getConf(), sparkSessionManager);
      sparkMemoryAndCores = sparkSession.getMemoryAndCores();
    } catch (HiveException e) {
      throw new SemanticException("Failed to get a Hive on Spark session", e);
    } catch (Exception e) {
      LOG.warn("Failed to get spark memory/core info, reducer parallelism may be inaccurate", e);
    } finally {
      if (sparkSession != null && sparkSessionManager != null) {
        try {
          sparkSessionManager.returnSession(sparkSession);
        } catch (HiveException ex) {
          LOG.error("Failed to return the session to SessionManager: " + ex, ex);
        }
      }
    }
  }

}
