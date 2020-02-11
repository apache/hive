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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ConditionalResolverSkewJoin.
 *
 */
public class ConditionalResolverSkewJoin implements ConditionalResolver, Serializable {
  private static final long serialVersionUID = 1L;

  static final protected Logger LOG = LoggerFactory.getLogger(ConditionalResolverSkewJoin.class);

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
    private HashMap<Path, Task<?>> dirToTaskMap;
    private List<Task<?>> noSkewTask;

    /**
     * For serialization use only.
     */
    public ConditionalResolverSkewJoinCtx() {
    }

    public ConditionalResolverSkewJoinCtx(
        HashMap<Path, Task<?>> dirToTaskMap,
        List<Task<?>> noSkewTask) {
      super();
      this.dirToTaskMap = dirToTaskMap;
      this.noSkewTask = noSkewTask;
    }

    public HashMap<Path, Task<?>> getDirToTaskMap() {
      return dirToTaskMap;
    }

    public void setDirToTaskMap(
        HashMap<Path, Task<?>> dirToTaskMap) {
      this.dirToTaskMap = dirToTaskMap;
    }

    public List<Task<?>> getNoSkewTask() {
      return noSkewTask;
    }

    public void setNoSkewTask(List<Task<?>> noSkewTask) {
      this.noSkewTask = noSkewTask;
    }
  }

  public ConditionalResolverSkewJoin() {
  }

  @Override
  public List<Task<?>> getTasks(HiveConf conf,
      Object objCtx) {
    ConditionalResolverSkewJoinCtx ctx = (ConditionalResolverSkewJoinCtx) objCtx;
    List<Task<?>> resTsks = new ArrayList<Task<?>>();

    Map<Path, Task<?>> dirToTaskMap = ctx
        .getDirToTaskMap();
    Iterator<Entry<Path, Task<?>>> bigKeysPathsIter = dirToTaskMap
        .entrySet().iterator();
    try {
      while (bigKeysPathsIter.hasNext()) {
        Entry<Path, Task<?>> entry = bigKeysPathsIter.next();
        Path dirPath = entry.getKey();
        FileSystem inpFs = dirPath.getFileSystem(conf);
        FileStatus[] fstatus = Utilities.listStatusIfExists(dirPath, inpFs);
        if (fstatus != null && fstatus.length > 0) {
          Task <?> task = entry.getValue();
          List<Task <?>> parentOps = task.getParentTasks();
          if(parentOps!=null){
            for(Task <?> parentOp: parentOps){
              //right now only one parent
              resTsks.add(parentOp);
            }
          }else{
            resTsks.add(task);
          }
        }
      }
    } catch (IOException e) {
      LOG.warn("Exception while getting tasks", e);
    }
    if (resTsks.isEmpty() && ctx.getNoSkewTask() != null) {
      resTsks.addAll(ctx.getNoSkewTask());
    }
    return resTsks;
  }

}
