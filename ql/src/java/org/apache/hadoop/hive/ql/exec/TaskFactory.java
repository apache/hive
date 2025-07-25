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

package org.apache.hadoop.hive.ql.exec;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.ddl.DDLTask;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.exec.mr.MapRedTask;
import org.apache.hadoop.hive.ql.exec.mr.MapredLocalTask;
import org.apache.hadoop.hive.ql.exec.repl.AtlasDumpTask;
import org.apache.hadoop.hive.ql.exec.repl.AtlasDumpWork;
import org.apache.hadoop.hive.ql.exec.repl.AtlasLoadTask;
import org.apache.hadoop.hive.ql.exec.repl.AtlasLoadWork;
import org.apache.hadoop.hive.ql.exec.repl.ClearDanglingTxnTask;
import org.apache.hadoop.hive.ql.exec.repl.ClearDanglingTxnWork;
import org.apache.hadoop.hive.ql.exec.repl.ReplDumpTask;
import org.apache.hadoop.hive.ql.exec.repl.ReplDumpWork;
import org.apache.hadoop.hive.ql.exec.repl.ReplLoadTask;
import org.apache.hadoop.hive.ql.exec.repl.ReplLoadWork;
import org.apache.hadoop.hive.ql.exec.repl.AckTask;
import org.apache.hadoop.hive.ql.exec.repl.AckWork;
import org.apache.hadoop.hive.ql.exec.repl.ReplStateLogTask;
import org.apache.hadoop.hive.ql.exec.repl.ReplStateLogWork;
import org.apache.hadoop.hive.ql.exec.repl.DirCopyTask;
import org.apache.hadoop.hive.ql.exec.repl.DirCopyWork;
import org.apache.hadoop.hive.ql.exec.repl.RangerLoadWork;
import org.apache.hadoop.hive.ql.exec.repl.RangerLoadTask;
import org.apache.hadoop.hive.ql.exec.repl.RangerDumpWork;
import org.apache.hadoop.hive.ql.exec.repl.RangerDumpTask;
import org.apache.hadoop.hive.ql.exec.repl.RangerDenyTask;
import org.apache.hadoop.hive.ql.exec.repl.RangerDenyWork;
import org.apache.hadoop.hive.ql.exec.schq.ScheduledQueryMaintenanceTask;
import org.apache.hadoop.hive.ql.exec.tez.TezTask;
import org.apache.hadoop.hive.ql.io.merge.MergeFileTask;
import org.apache.hadoop.hive.ql.io.merge.MergeFileWork;
import org.apache.hadoop.hive.ql.plan.ColumnStatsUpdateWork;
import org.apache.hadoop.hive.ql.plan.ConditionalWork;
import org.apache.hadoop.hive.ql.plan.CopyWork;
import org.apache.hadoop.hive.ql.plan.DependencyCollectionWork;
import org.apache.hadoop.hive.ql.plan.ExplainSQRewriteWork;
import org.apache.hadoop.hive.ql.plan.ExplainWork;
import org.apache.hadoop.hive.ql.plan.ExportWork;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.plan.MapredLocalWork;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.MoveWork;
import org.apache.hadoop.hive.ql.plan.ReplCopyWork;
import org.apache.hadoop.hive.ql.plan.ReplTxnWork;
import org.apache.hadoop.hive.ql.plan.StatsWork;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.apache.hadoop.hive.ql.scheduled.ScheduledQueryMaintenanceWork;

import com.google.common.annotations.VisibleForTesting;


/**
 * TaskFactory implementation.
 **/
public final class TaskFactory {

  /**
   * taskTuple.
   *
   * @param <T>
   */
  public static final class TaskTuple<T extends Serializable> {
    public Class<T> workClass;
    public Class<? extends Task<T>> taskClass;

    public TaskTuple(Class<T> workClass, Class<? extends Task<T>> taskClass) {
      this.workClass = workClass;
      this.taskClass = taskClass;
    }
  }

  public static ArrayList<TaskTuple<? extends Serializable>> taskvec;
  static {
    taskvec = new ArrayList<TaskTuple<? extends Serializable>>();
    taskvec.add(new TaskTuple<MoveWork>(MoveWork.class, MoveTask.class));
    taskvec.add(new TaskTuple<FetchWork>(FetchWork.class, FetchTask.class));
    taskvec.add(new TaskTuple<CopyWork>(CopyWork.class, CopyTask.class));
    taskvec.add(new TaskTuple<ReplCopyWork>(ReplCopyWork.class, ReplCopyTask.class));
    taskvec.add(new TaskTuple<DDLWork>(DDLWork.class, DDLTask.class));
    taskvec
        .add(new TaskTuple<ExplainWork>(ExplainWork.class, ExplainTask.class));
    taskvec
        .add(new TaskTuple<ExplainSQRewriteWork>(ExplainSQRewriteWork.class, ExplainSQRewriteTask.class));
    taskvec.add(new TaskTuple<ConditionalWork>(ConditionalWork.class,
        ConditionalTask.class));
    taskvec.add(new TaskTuple<MapredWork>(MapredWork.class,
                                          MapRedTask.class));

    taskvec.add(new TaskTuple<MapredLocalWork>(MapredLocalWork.class,
        MapredLocalTask.class));
    taskvec.add(new TaskTuple<StatsWork>(StatsWork.class, StatsTask.class));
    taskvec.add(new TaskTuple<ColumnStatsUpdateWork>(ColumnStatsUpdateWork.class, ColumnStatsUpdateTask.class));
    taskvec.add(new TaskTuple<MergeFileWork>(MergeFileWork.class,
        MergeFileTask.class));
    taskvec.add(new TaskTuple<DependencyCollectionWork>(DependencyCollectionWork.class,
        DependencyCollectionTask.class));
    taskvec.add(new TaskTuple<TezWork>(TezWork.class, TezTask.class));
    taskvec.add(new TaskTuple<>(ReplDumpWork.class, ReplDumpTask.class));
    taskvec.add(new TaskTuple<>(ReplLoadWork.class, ReplLoadTask.class));
    taskvec.add(new TaskTuple<>(ReplStateLogWork.class, ReplStateLogTask.class));
    taskvec.add(new TaskTuple<AckWork>(AckWork.class, AckTask.class));
    taskvec.add(new TaskTuple<RangerDumpWork>(RangerDumpWork.class, RangerDumpTask.class));
    taskvec.add(new TaskTuple<RangerLoadWork>(RangerLoadWork.class, RangerLoadTask.class));
    taskvec.add(new TaskTuple<RangerDenyWork>(RangerDenyWork.class, RangerDenyTask.class));
    taskvec.add(new TaskTuple<AtlasDumpWork>(AtlasDumpWork.class, AtlasDumpTask.class));
    taskvec.add(new TaskTuple<AtlasLoadWork>(AtlasLoadWork.class, AtlasLoadTask.class));
    taskvec.add(new TaskTuple<ExportWork>(ExportWork.class, ExportTask.class));
    taskvec.add(new TaskTuple<ReplTxnWork>(ReplTxnWork.class, ReplTxnTask.class));
    taskvec.add(new TaskTuple<DirCopyWork>(DirCopyWork.class, DirCopyTask.class));
    taskvec.add(new TaskTuple<ScheduledQueryMaintenanceWork>(ScheduledQueryMaintenanceWork.class,
            ScheduledQueryMaintenanceTask.class));
    taskvec.add(new TaskTuple<>(ClearDanglingTxnWork.class,
            ClearDanglingTxnTask.class));
  }

  private static ThreadLocal<Integer> tid = new ThreadLocal<Integer>() {
    @Override
    protected Integer initialValue() {
      return Integer.valueOf(0);
    }
  };

  public static int getAndIncrementId() {
    int curValue = tid.get().intValue();
    tid.set(Integer.valueOf(curValue + 1));
    return curValue;
  }

  public static void resetId() {
    tid.set(Integer.valueOf(0));
  }

  @SuppressWarnings("unchecked")
  @VisibleForTesting
  static <T extends Serializable> Task<T> get(Class<T> workClass) {

    for (TaskTuple<? extends Serializable> t : taskvec) {
      if (t.workClass == workClass) {
        try {
          Task<T> ret = (Task<T>) t.taskClass.newInstance();
          ret.setId("Stage-" + Integer.toString(getAndIncrementId()));
          return ret;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }

    throw new RuntimeException("No task for work class " + workClass.getName());
  }

  public static <T extends Serializable> Task<T> get(T work, HiveConf conf) {
    @SuppressWarnings("unchecked")
    Task<T> ret = get((Class<T>) work.getClass());
    ret.setWork(work);
    if (null != conf) {
      ret.setConf(conf);
    }
    return ret;
  }

  public static <T extends Serializable> Task<T> get(T work) {
    return get(work, null);
  }

  @SafeVarargs
  public static <T extends Serializable> Task<T> getAndMakeChild(T work,
      HiveConf conf, Task<? extends Serializable>... tasklist) {
    Task<T> ret = get(work);
    if (tasklist.length == 0) {
      return (ret);
    }

    makeChild(ret, tasklist);

    return (ret);
  }


  @SafeVarargs
  public static  void makeChild(Task<?> ret,
      Task<?>... tasklist) {
    // Add the new task as child of each of the passed in tasks
    for (Task<?> tsk : tasklist) {
      List<Task<?>> children = tsk.getChildTasks();
      if (children == null) {
        children = new ArrayList<Task<?>>();
      }
      children.add(ret);
      tsk.setChildTasks(children);
    }
  }

  private TaskFactory() {
    // prevent instantiation
  }

}
