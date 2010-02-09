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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Task;

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
    Map<String, Task<? extends Serializable>> dirToTaskMap;

    public ConditionalResolverSkewJoinCtx(
        Map<String, Task<? extends Serializable>> dirToTaskMap) {
      super();
      this.dirToTaskMap = dirToTaskMap;
    }

    public Map<String, Task<? extends Serializable>> getDirToTaskMap() {
      return dirToTaskMap;
    }

    public void setDirToTaskMap(
        Map<String, Task<? extends Serializable>> dirToTaskMap) {
      this.dirToTaskMap = dirToTaskMap;
    }
  }

  public ConditionalResolverSkewJoin() {
  }

  @Override
  public List<Task<? extends Serializable>> getTasks(HiveConf conf,
      Object objCtx) {
    ConditionalResolverSkewJoinCtx ctx = (ConditionalResolverSkewJoinCtx) objCtx;
    List<Task<? extends Serializable>> resTsks = new ArrayList<Task<? extends Serializable>>();

    Map<String, Task<? extends Serializable>> dirToTaskMap = ctx
        .getDirToTaskMap();
    Iterator<Entry<String, Task<? extends Serializable>>> bigKeysPathsIter = dirToTaskMap
        .entrySet().iterator();
    try {
      while (bigKeysPathsIter.hasNext()) {
        Entry<String, Task<? extends Serializable>> entry = bigKeysPathsIter
            .next();
        String path = entry.getKey();
        Path dirPath = new Path(path);
        FileSystem inpFs = dirPath.getFileSystem(conf);
        FileStatus[] fstatus = inpFs.listStatus(dirPath);
        if (fstatus.length > 0) {
          resTsks.add(entry.getValue());
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return resTsks;
  }

}
