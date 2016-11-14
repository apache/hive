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

package org.apache.hadoop.hive.ql.exec;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.mr.MapRedTask;
import org.apache.hadoop.hive.ql.exec.mr.MapredLocalTask;
import org.apache.hadoop.hive.ql.exec.spark.SparkTask;
import org.apache.hadoop.hive.ql.exec.tez.TezTask;
import org.apache.hadoop.hive.ql.index.IndexMetadataChangeTask;
import org.apache.hadoop.hive.ql.index.IndexMetadataChangeWork;
import org.apache.hadoop.hive.ql.io.merge.MergeFileTask;
import org.apache.hadoop.hive.ql.io.merge.MergeFileWork;
import org.apache.hadoop.hive.ql.io.rcfile.stats.PartialScanTask;
import org.apache.hadoop.hive.ql.io.rcfile.stats.PartialScanWork;
import org.apache.hadoop.hive.ql.plan.ColumnStatsUpdateWork;
import org.apache.hadoop.hive.ql.plan.ColumnStatsWork;
import org.apache.hadoop.hive.ql.plan.ConditionalWork;
import org.apache.hadoop.hive.ql.plan.CopyWork;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.plan.DependencyCollectionWork;
import org.apache.hadoop.hive.ql.plan.ExplainSQRewriteWork;
import org.apache.hadoop.hive.ql.plan.ExplainWork;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.plan.FunctionWork;
import org.apache.hadoop.hive.ql.plan.MapredLocalWork;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.MoveWork;
import org.apache.hadoop.hive.ql.plan.ReplCopyWork;
import org.apache.hadoop.hive.ql.plan.SparkWork;
import org.apache.hadoop.hive.ql.plan.StatsNoJobWork;
import org.apache.hadoop.hive.ql.plan.StatsWork;
import org.apache.hadoop.hive.ql.plan.TezWork;

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
    taskvec.add(new TaskTuple<FunctionWork>(FunctionWork.class,
        FunctionTask.class));
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
    taskvec.add(new TaskTuple<StatsWork>(StatsWork.class,
        StatsTask.class));
    taskvec.add(new TaskTuple<StatsNoJobWork>(StatsNoJobWork.class, StatsNoJobTask.class));
    taskvec.add(new TaskTuple<ColumnStatsWork>(ColumnStatsWork.class, ColumnStatsTask.class));
    taskvec.add(new TaskTuple<ColumnStatsUpdateWork>(ColumnStatsUpdateWork.class, ColumnStatsUpdateTask.class));
    taskvec.add(new TaskTuple<MergeFileWork>(MergeFileWork.class,
        MergeFileTask.class));
    taskvec.add(new TaskTuple<DependencyCollectionWork>(DependencyCollectionWork.class,
        DependencyCollectionTask.class));
    taskvec.add(new TaskTuple<PartialScanWork>(PartialScanWork.class,
        PartialScanTask.class));
    taskvec.add(new TaskTuple<IndexMetadataChangeWork>(IndexMetadataChangeWork.class,
        IndexMetadataChangeTask.class));
    taskvec.add(new TaskTuple<TezWork>(TezWork.class, TezTask.class));
    taskvec.add(new TaskTuple<SparkWork>(SparkWork.class, SparkTask.class));

  }

  private static ThreadLocal<Integer> tid = new ThreadLocal<Integer>() {
    @Override
    protected Integer initialValue() {
      return Integer.valueOf(0);
    }
  };

  public static int getAndIncrementId() {
    int curValue = tid.get().intValue();
    tid.set(new Integer(curValue + 1));
    return curValue;
  }

  public static void resetId() {
    tid.set(Integer.valueOf(0));
  }

  @SuppressWarnings("unchecked")
  public static <T extends Serializable> Task<T> get(Class<T> workClass,
      HiveConf conf) {

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

  public static <T extends Serializable> Task<T> get(T work, HiveConf conf,
      Task<? extends Serializable>... tasklist) {
    Task<T> ret = get((Class<T>) work.getClass(), conf);
    ret.setWork(work);
    if (tasklist.length == 0) {
      return (ret);
    }

    ArrayList<Task<? extends Serializable>> clist = new ArrayList<Task<? extends Serializable>>();
    for (Task<? extends Serializable> tsk : tasklist) {
      clist.add(tsk);
    }
    ret.setChildTasks(clist);
    return (ret);
  }

  public static <T extends Serializable> Task<T> getAndMakeChild(T work,
      HiveConf conf, Task<? extends Serializable>... tasklist) {
    Task<T> ret = get((Class<T>) work.getClass(), conf);
    ret.setWork(work);
    if (tasklist.length == 0) {
      return (ret);
    }

    makeChild(ret, tasklist);

    return (ret);
  }


  public static  void makeChild(Task<?> ret,
      Task<? extends Serializable>... tasklist) {
    // Add the new task as child of each of the passed in tasks
    for (Task<? extends Serializable> tsk : tasklist) {
      List<Task<? extends Serializable>> children = tsk.getChildTasks();
      if (children == null) {
        children = new ArrayList<Task<? extends Serializable>>();
      }
      children.add(ret);
      tsk.setChildTasks(children);
    }
  }

  private TaskFactory() {
    // prevent instantiation
  }

}
