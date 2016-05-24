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

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.Utilities;

/**
 * ConditionalResolverSkewJoin.
 *
 */
public class ConditionalResolverSkewJoin implements ConditionalResolver, Serializable {
  private static final long serialVersionUID = 1L;

  /**
   * ConditionalResolverSkewJoinCtx.
   *
   */
  public static class ConditionalResolverSkewJoinCtx implements Serializable {
    private static final long serialVersionUID = 1L;
    // we store big keys in one table into one dir, and same keys in other
    // tables into corresponding different dirs (one dir per table).
    // this map stores mapping from "big key dir" to its corresponding mapjoin
    // task.
    private HashMap<Path, Task<? extends Serializable>> dirToTaskMap;
    private List<Task<? extends Serializable>> noSkewTask;

    /**
     * For serialization use only.
     */
    public ConditionalResolverSkewJoinCtx() {
    }

    public ConditionalResolverSkewJoinCtx(
        HashMap<Path, Task<? extends Serializable>> dirToTaskMap,
        List<Task<? extends Serializable>> noSkewTask) {
      super();
      this.dirToTaskMap = dirToTaskMap;
      this.noSkewTask = noSkewTask;
    }

    public HashMap<Path, Task<? extends Serializable>> getDirToTaskMap() {
      return dirToTaskMap;
    }

    public void setDirToTaskMap(
        HashMap<Path, Task<? extends Serializable>> dirToTaskMap) {
      this.dirToTaskMap = dirToTaskMap;
    }

    public List<Task<? extends Serializable>> getNoSkewTask() {
      return noSkewTask;
    }

    public void setNoSkewTask(List<Task<? extends Serializable>> noSkewTask) {
      this.noSkewTask = noSkewTask;
    }
  }

  public ConditionalResolverSkewJoin() {
  }

  @Override
  public List<Task<? extends Serializable>> getTasks(HiveConf conf,
      Object objCtx) {
    ConditionalResolverSkewJoinCtx ctx = (ConditionalResolverSkewJoinCtx) objCtx;
    List<Task<? extends Serializable>> resTsks = new ArrayList<Task<? extends Serializable>>();

    Map<Path, Task<? extends Serializable>> dirToTaskMap = ctx
        .getDirToTaskMap();
    Iterator<Entry<Path, Task<? extends Serializable>>> bigKeysPathsIter = dirToTaskMap
        .entrySet().iterator();
    try {
      while (bigKeysPathsIter.hasNext()) {
        Entry<Path, Task<? extends Serializable>> entry = bigKeysPathsIter.next();
        Path dirPath = entry.getKey();
        FileSystem inpFs = dirPath.getFileSystem(conf);
        FileStatus[] fstatus = Utilities.listStatusIfExists(dirPath, inpFs);
        if (fstatus != null && fstatus.length > 0) {
          Task <? extends Serializable> task = entry.getValue();
          List<Task <? extends Serializable>> parentOps = task.getParentTasks();
          if(parentOps!=null){
            for(Task <? extends Serializable> parentOp: parentOps){
              //right now only one parent
              resTsks.add(parentOp);
            }
          }else{
            resTsks.add(task);
          }
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    if (resTsks.isEmpty() && ctx.getNoSkewTask() != null) {
      resTsks.addAll(ctx.getNoSkewTask());
    }
    return resTsks;
  }

}
