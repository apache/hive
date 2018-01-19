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

package org.apache.hadoop.hive.ql.plan;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.hive.ql.exec.CommonMergeJoinOperator;
import org.apache.hadoop.hive.ql.exec.HashTableDummyOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.plan.Explain.Level;
import org.apache.hadoop.mapred.JobConf;

public class MergeJoinWork extends BaseWork {

  private CommonMergeJoinOperator mergeJoinOp = null;
  private final List<BaseWork> mergeWorkList = new ArrayList<BaseWork>();
  private BaseWork bigTableWork;

  public MergeJoinWork() {
    super();
  }

  @Override
  public void replaceRoots(Map<Operator<?>, Operator<?>> replacementMap) {
    getMainWork().replaceRoots(replacementMap);
  }

  @Override
  public Set<Operator<?>> getAllRootOperators() {
    Set<Operator<?>> set = new HashSet<>();
    set.addAll(getMainWork().getAllRootOperators());
    for (BaseWork w : mergeWorkList) {
      set.addAll(w.getAllRootOperators());
    }
    return set;
  }

  @Override
  public Operator<?> getAnyRootOperator() {
    return getMainWork().getAnyRootOperator();
  }

  @Override
  public void configureJobConf(JobConf job) {
  }

  public CommonMergeJoinOperator getMergeJoinOperator() {
    return this.mergeJoinOp;
  }

  public void setMergeJoinOperator(CommonMergeJoinOperator mergeJoinOp) {
    this.mergeJoinOp = mergeJoinOp;
  }

  public void addMergedWork(BaseWork work, BaseWork connectWork,
      Map<Operator<?>, BaseWork> leafOperatorToFollowingWork) {
    if (work != null) {
      if ((bigTableWork != null) && (bigTableWork != work)) {
        assert false;
      }
      this.bigTableWork = work;
      setName(work.getName());
    }

    if (connectWork != null) {
      this.mergeWorkList.add(connectWork);
      if ((connectWork instanceof ReduceWork) && (bigTableWork != null)) {
        /*
         * For tez to route data from an up-stream vertex correctly to the following vertex, the
         * output name in the reduce sink needs to be setup appropriately. In the case of reduce
         * side merge work, we need to ensure that the parent work that provides data to this merge
         * work is setup to point to the right vertex name - the main work name.
         * 
         * In this case, if the big table work has already been created, we can hook up the merge
         * work items for the small table correctly.
         */
        setReduceSinkOutputName(connectWork, leafOperatorToFollowingWork, bigTableWork.getName());
      }
    }

    if (work != null) {
      /*
       * Same reason as above. This is the case when we have the main work item after the merge work
       * has been created for the small table side.
       */
      for (BaseWork mergeWork : mergeWorkList) {
        if (mergeWork instanceof ReduceWork) {
          setReduceSinkOutputName(mergeWork, leafOperatorToFollowingWork, work.getName());
        }
      }
    }
  }

  private void setReduceSinkOutputName(BaseWork mergeWork,
      Map<Operator<?>, BaseWork> leafOperatorToFollowingWork, String name) {
    for (Entry<Operator<?>, BaseWork> entry : leafOperatorToFollowingWork.entrySet()) {
      if (entry.getValue() == mergeWork) {
        ((ReduceSinkOperator) entry.getKey()).getConf().setOutputName(name);
      }
    }
  }

  @Explain(skipHeader=true, displayName = "Join", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public List<BaseWork> getBaseWorkList() {
    return mergeWorkList;
  }

  public String getBigTableAlias() {
    return ((MapWork) bigTableWork).getAliasToWork().keySet().iterator().next();
  }

  @Explain(skipHeader=true, displayName = "Main", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public BaseWork getMainWork() {
    return bigTableWork;
  }

  @Override
  public void setDummyOps(List<HashTableDummyOperator> dummyOps) {
    getMainWork().setDummyOps(dummyOps);
  }

  @Override
  public List<HashTableDummyOperator> getDummyOps() {
    return getMainWork().getDummyOps();
  }

  @Override
  public void setVectorMode(boolean vectorMode) {
    getMainWork().setVectorMode(vectorMode);
  }

  @Override
  public boolean getVectorMode() {
    return getMainWork().getVectorMode();
  }

  @Override
  public void setUberMode(boolean uberMode) {
    getMainWork().setUberMode(uberMode);
  }

  @Override
  public boolean getUberMode() {
    return getMainWork().getUberMode();
  }

  @Override
  public void setLlapMode(boolean llapMode) {
    getMainWork().setLlapMode(llapMode);
  }

  @Override
  public boolean getLlapMode() {
    return getMainWork().getLlapMode();
  }
  
  public void addDummyOp(HashTableDummyOperator dummyOp) {
    getMainWork().addDummyOp(dummyOp);
  }
}
