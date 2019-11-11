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
package org.apache.hadoop.hive.ql.hooks;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.CommonMergeJoinOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskRunner;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;

public class MapJoinCounterHook implements ExecuteWithHookContext {

  public void run(HookContext hookContext) {
    HiveConf conf = hookContext.getConf();
    boolean enableConvert = HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVECONVERTJOIN);
    if (!enableConvert) {
      return;
    }

    QueryPlan plan = hookContext.getQueryPlan();
    LogHelper console = SessionState.getConsole();
    String engine = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE);
    final String MR = "mr", TEZ = "tez", SPARK = "spark";

    int commonJoin = 0;
    int hintedMapJoin = 0;
    int convertedMapJoin = 0;
    int hintedMapJoinLocal = 0;
    int convertedMapJoinLocal = 0;
    int backupCommonJoin = 0;
    int bucketMapJoin = 0;
    int hybridHashJoin = 0;
    int dynamicPartitionHashJoin = 0;

    switch (engine) {
      case MR:
        List<TaskRunner> list = hookContext.getCompleteTaskList();
        for (TaskRunner tskRunner : list) {
          Task tsk = tskRunner.getTask();
          int tag = tsk.getTaskTag();
          switch (tag) {
            case Task.COMMON_JOIN:
              commonJoin++;
              break;
            case Task.HINTED_MAPJOIN:
              hintedMapJoin++;
              break;
            case Task.HINTED_MAPJOIN_LOCAL:
              hintedMapJoinLocal++;
              break;
            case Task.CONVERTED_MAPJOIN:
              convertedMapJoin++;
              break;
            case Task.CONVERTED_MAPJOIN_LOCAL:
              convertedMapJoinLocal++;
              break;
            case Task.BACKUP_COMMON_JOIN:
              backupCommonJoin++;
              break;
          }
        }
        break;

      case TEZ:
        for(Task<? extends Serializable>  tezTask: plan.getRootTasks()) {
          TezWork work = (TezWork) tezTask.getWork();
          Map<String, BaseWork> workGraph = work.getWorkMap();
          for(Map.Entry<String, BaseWork> baseWorkEntry : workGraph.entrySet()) {
            Set<Operator<?>> operatorSet = baseWorkEntry.getValue().getAllOperators();
            for(Operator<?> operator : operatorSet) {
              if(operator instanceof MapJoinOperator) {
                MapJoinDesc mapJoinDesc = (MapJoinDesc) operator.getConf();
                if (mapJoinDesc.isBucketMapJoin()) {
                  bucketMapJoin++;
                }
                if (mapJoinDesc.isHybridHashJoin()) {
                  hybridHashJoin++;
                }
                if (mapJoinDesc.isDynamicPartitionHashJoin()) {
                  dynamicPartitionHashJoin++;
                }
                if (mapJoinDesc.isMapSideJoin()) {
                  convertedMapJoin++;
                }
              } else if(operator instanceof CommonMergeJoinOperator) {
                commonJoin++;
              }
            }
          }
        }
        break;

      case SPARK:
        //todo
        break;
    }
    console.printError("[MapJoinCounter PostHook] COMMON_JOIN: " + commonJoin
            + " HINTED_MAPJOIN: " + hintedMapJoin + " HINTED_MAPJOIN_LOCAL: " + hintedMapJoinLocal
            + " CONVERTED_MAPJOIN: " + convertedMapJoin + " CONVERTED_MAPJOIN_LOCAL: " + convertedMapJoinLocal
            + " BACKUP_COMMON_JOIN: " + backupCommonJoin +" BUCKET_MAP_JOIN: " + bucketMapJoin +
            " HYBRID_HASH_JOIN: " + hybridHashJoin + " DYNAMIC_PARTITION_HASH_JOIN: " + dynamicPartitionHashJoin );
  }
}
