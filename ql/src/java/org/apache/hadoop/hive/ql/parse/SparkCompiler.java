/**
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.hive.ql.parse;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.mr.MapRedTask;
import org.apache.hadoop.hive.ql.exec.spark.SparkTask;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.MoveWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.SparkWork;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;

/**
 * SparkCompiler translates the operator plan into SparkTask.
 * TODO: currently extending MapReduceCompiler in order to make POC work. It will
 *       stand alone parallel to MapReduceCompiler.
 */
public class SparkCompiler extends MapReduceCompiler {

  protected final Log LOG = LogFactory.getLog(SparkCompiler.class);

  public SparkCompiler() {
  }

  @Override
  public void init(HiveConf conf, LogHelper console, Hive db) {
    super.init(conf, console, db);
    // Any Spark specific configuration
  }

  @Override
  protected void optimizeOperatorPlan(ParseContext pCtx, Set<ReadEntity> inputs,
      Set<WriteEntity> outputs) throws SemanticException {
  }

  private static int counter = 0;
  
  @Override
  protected void generateTaskTree(List<Task<? extends Serializable>> rootTasks, ParseContext pCtx,
      List<Task<MoveWork>> mvTask, Set<ReadEntity> inputs, Set<WriteEntity> outputs)
      throws SemanticException {
    super.generateTaskTree(rootTasks, pCtx, mvTask, inputs, outputs);

    MapRedTask mrTask = (MapRedTask) rootTasks.get(0);
    MapWork mapWork = mrTask.getWork().getMapWork();
    ReduceWork redWork = mrTask.getWork().getReduceWork();
    SparkWork sparkWork = new SparkWork("first spark #" + counter++);
    sparkWork.setMapWork(mapWork);
    sparkWork.setReduceWork(redWork);
    SparkTask task = new SparkTask();
    task.setWork(sparkWork);
    task.setId(sparkWork.getName());
    rootTasks.clear();
    rootTasks.add(task);

    // finally make sure the file sink operators are set up right
    breakTaskTree(task);
  }

  private void breakTaskTree(Task<? extends Serializable> task) {
    if (task instanceof SparkTask) {
      SparkTask st = (SparkTask) task;
      SparkWork sw = st.getWork();
      MapWork mw = sw.getMapWork();
      HashMap<String, Operator<? extends OperatorDesc>> opMap = mw.getAliasToWork();
      if (!opMap.isEmpty()) {
        for (Operator<? extends OperatorDesc> op : opMap.values()) {
          breakOperatorTree(op);
        }
      }
    }
  }

  @Override
  protected void decideExecMode(List<Task<? extends Serializable>> rootTasks, Context ctx,
      GlobalLimitCtx globalLimitCtx)
      throws SemanticException {
    // currently all Spark work is on the cluster
    return;
  }

  @Override
  protected void optimizeTaskPlan(List<Task<? extends Serializable>> rootTasks, ParseContext pCtx,
      Context ctx) throws SemanticException {
  }

}
