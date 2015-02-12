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

package org.apache.hadoop.hive.ql.plan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.ql.exec.CommonMergeJoinOperator;
import org.apache.hadoop.hive.ql.exec.HashTableDummyOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.mapred.JobConf;

public class MergeJoinWork extends BaseWork {

  private CommonMergeJoinOperator mergeJoinOp = null;
  private final List<BaseWork> mergeWorkList = new ArrayList<BaseWork>();
  private BaseWork bigTableWork;

  public MergeJoinWork() {
    super();
  }

  @Override
  public String getName() {
    return super.getName();
  }

  @Override
  public void replaceRoots(Map<Operator<?>, Operator<?>> replacementMap) {
    getMainWork().replaceRoots(replacementMap);
  }

  @Override
  public Set<Operator<?>> getAllRootOperators() {
    return getMainWork().getAllRootOperators();
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

  public void addMergedWork(BaseWork work, BaseWork connectWork) {
    if (work != null) {
      if ((bigTableWork != null) && (bigTableWork != work)) {
        assert false;
      }
      this.bigTableWork = work;
      setName(work.getName());
    }

    if (connectWork != null) {
      this.mergeWorkList.add(connectWork);
    }
  }

  @Explain(skipHeader=true, displayName = "Join")
  public List<BaseWork> getBaseWorkList() {
    return mergeWorkList;
  }

  public String getBigTableAlias() {
    return ((MapWork) bigTableWork).getAliasToWork().keySet().iterator().next();
  }

  @Explain(skipHeader=true, displayName = "Main")
  public BaseWork getMainWork() {
    return bigTableWork;
  }

  @Override
  public void setDummyOps(List<HashTableDummyOperator> dummyOps) {
    getMainWork().setDummyOps(dummyOps);
  }

  @Override
  public void addDummyOp(HashTableDummyOperator dummyOp) {
    getMainWork().addDummyOp(dummyOp);
  }
}
