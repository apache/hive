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

package org.apache.hadoop.hive.ql.reexec;

import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.mapjoin.MapJoinMemoryExhaustionError;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext.HookType;
import org.apache.hadoop.hive.ql.plan.mapper.PlanMapper;
import org.apache.hadoop.hive.ql.plan.mapper.StatsSources;
import org.apache.hadoop.hive.ql.stats.OperatorStatsReaderHook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class ReOptimizePlugin implements IReExecutionPlugin {

  private static final Logger LOG = LoggerFactory.getLogger(ReOptimizePlugin.class);

  private boolean retryPossible;

  private Driver coreDriver;

  private OperatorStatsReaderHook statsReaderHook;

  private boolean alwaysCollectStats;

  class LocalHook implements ExecuteWithHookContext {

    @Override
    public void run(HookContext hookContext) throws Exception {
      if (hookContext.getHookType() == HookType.ON_FAILURE_HOOK) {
        Throwable exception = hookContext.getException();
        if (exception != null) {
          {
            String message = exception.getMessage();
            if (message != null) {
              boolean isOOM = message.contains(MapJoinMemoryExhaustionError.class.getName())
                  || message.contains(OutOfMemoryError.class.getName());
              if (message.contains("Vertex failed,") && isOOM) {
                retryPossible = true;
              }
            }
          }
        }
        LOG.info("ReOptimization: retryPossible: {}", retryPossible);
      }
    }
  }

  @Override
  public void initialize(Driver driver) {
    coreDriver = driver;
    coreDriver.getHookRunner().addOnFailureHook(new LocalHook());
    statsReaderHook = new OperatorStatsReaderHook();
    coreDriver.getHookRunner().addOnFailureHook(statsReaderHook);
    coreDriver.getHookRunner().addPostHook(statsReaderHook);
    alwaysCollectStats = driver.getConf().getBoolVar(ConfVars.HIVE_QUERY_REEXECUTION_ALWAYS_COLLECT_OPERATOR_STATS);
    statsReaderHook.setCollectOnSuccess(alwaysCollectStats);

    coreDriver.setStatsSource(StatsSources.getStatsSource(driver.getConf()));
  }

  @Override
  public boolean shouldReExecute(int executionNum) {
    return retryPossible;
  }

  @Override
  public void prepareToReExecute() {
    statsReaderHook.setCollectOnSuccess(true);
    retryPossible = false;
    if (!alwaysCollectStats) {
      coreDriver.setStatsSource(
          StatsSources.getStatsSourceContaining(coreDriver.getStatsSource(), coreDriver.getPlanMapper()));
    }
  }

  @Override
  public boolean shouldReExecuteAfterCompile(int executionNum, PlanMapper oldPlanMapper, PlanMapper newPlanMapper) {
    boolean planDidChange = !planEquals(oldPlanMapper, newPlanMapper);
    LOG.info("planDidChange: {}", planDidChange);
    return planDidChange;
  }

  private boolean planEquals(PlanMapper pmL, PlanMapper pmR) {
    List<Operator> opsL = getRootOps(pmL);
    List<Operator> opsR = getRootOps(pmR);
    for (Iterator<Operator> itL = opsL.iterator(); itL.hasNext();) {
      Operator<?> opL = itL.next();
      for (Iterator<Operator> itR = opsR.iterator(); itR.hasNext();) {
        Operator<?> opR = itR.next();
        if (opL.logicalEqualsTree(opR)) {
          itL.remove();
          itR.remove();
          break;
        }
      }
    }
    return opsL.isEmpty() && opsR.isEmpty();
  }

  private List<Operator> getRootOps(PlanMapper pmL) {
    List<Operator> ops = pmL.getAll(Operator.class);
    for (Iterator<Operator> iterator = ops.iterator(); iterator.hasNext();) {
      Operator o = iterator.next();
      if (o.getNumChild() != 0) {
        iterator.remove();
      }
    }
    return ops;
  }

  @Override
  public void beforeExecute(int executionIndex, boolean explainReOptimization) {
    if (explainReOptimization) {
      statsReaderHook.setCollectOnSuccess(true);
    }
  }

  @Override
  public void afterExecute(PlanMapper planMapper, boolean success) {
    if (alwaysCollectStats) {
      coreDriver.setStatsSource(
          StatsSources.getStatsSourceContaining(coreDriver.getStatsSource(), planMapper));
    }
  }

}
