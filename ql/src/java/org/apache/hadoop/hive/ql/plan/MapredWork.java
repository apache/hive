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
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.hive.ql.plan.Explain.Level;
import org.apache.hadoop.hive.ql.plan.Explain.Vectorization;


/**
 * MapredWork.
 *
 */
@Explain(displayName = "Map Reduce", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED },
    vectorization = Vectorization.SUMMARY_PATH)
public class MapredWork extends AbstractOperatorDesc {
  private static final long serialVersionUID = 1L;

  private MapWork mapWork = new MapWork();
  private ReduceWork reduceWork = null;

  private boolean finalMapRed;

  @Explain(skipHeader = true, displayName = "Map", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED },
      vectorization = Vectorization.SUMMARY_PATH)
  public MapWork getMapWork() {
    return mapWork;
  }

  public void setMapWork(MapWork mapWork) {
    this.mapWork = mapWork;
  }

  @Explain(skipHeader = true, displayName = "Reduce", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED },
      vectorization = Vectorization.SUMMARY_PATH)
  public ReduceWork getReduceWork() {
    return reduceWork;
  }

  public void setReduceWork(ReduceWork reduceWork) {
    this.reduceWork = reduceWork;
  }

  public boolean isFinalMapRed() {
    return finalMapRed;
  }

  public void setFinalMapRed(boolean finalMapRed) {
    this.finalMapRed = finalMapRed;
  }

  public void configureJobConf(JobConf job) {
    mapWork.configureJobConf(job);
    if (reduceWork != null) {
      reduceWork.configureJobConf(job);
    }
  }

  public List<Operator<?>> getAllOperators() {
    List<Operator<?>> ops = new ArrayList<Operator<?>>();
    ops.addAll(mapWork.getAllOperators());
    if (reduceWork != null) {
      ops.addAll(reduceWork.getAllOperators());
    }

    return ops;
  }

  public Operator<?> getAnyOperator() {
    Operator<?> result = mapWork.getAnyRootOperator();
    if (result != null) return result;
    return (reduceWork != null) ? reduceWork.getAnyRootOperator() : null;
  }}
