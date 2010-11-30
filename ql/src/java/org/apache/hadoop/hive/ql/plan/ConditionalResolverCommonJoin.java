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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Task;

/**
 * ConditionalResolverSkewJoin.
 *
 */
public class ConditionalResolverCommonJoin implements ConditionalResolver, Serializable {
  private static final long serialVersionUID = 1L;

  /**
   * ConditionalResolverSkewJoinCtx.
   *
   */
  public static class ConditionalResolverCommonJoinCtx implements Serializable {
    private static final long serialVersionUID = 1L;

    private HashMap<String, Task<? extends Serializable>> aliasToTask;
    private HashMap<String, String> aliasToPath;
    private Task<? extends Serializable> commonJoinTask;



    public ConditionalResolverCommonJoinCtx() {
    }

    public HashMap<String, Task<? extends Serializable>> getAliasToTask() {
      return aliasToTask;
    }

    public void setAliasToTask(HashMap<String, Task<? extends Serializable>> aliasToTask) {
      this.aliasToTask = aliasToTask;
    }

    public HashMap<String, String> getAliasToPath() {
      return aliasToPath;
    }

    public void setAliasToPath(HashMap<String, String> aliasToPath) {
      this.aliasToPath = aliasToPath;
    }

    public Task<? extends Serializable> getCommonJoinTask() {
      return commonJoinTask;
    }

    public void setCommonJoinTask(Task<? extends Serializable> commonJoinTask) {
      this.commonJoinTask = commonJoinTask;
    }

  }

  public ConditionalResolverCommonJoin() {
  }

  @Override
  public List<Task<? extends Serializable>> getTasks(HiveConf conf, Object objCtx) {
    ConditionalResolverCommonJoinCtx ctx = (ConditionalResolverCommonJoinCtx) objCtx;
    List<Task<? extends Serializable>> resTsks = new ArrayList<Task<? extends Serializable>>();

    // get aliasToPath and pass it to the heuristic
    HashMap<String, String> aliasToPath = ctx.getAliasToPath();
    String bigTableAlias = this.resolveMapJoinTask(aliasToPath, conf);

    if (bigTableAlias == null) {
      // run common join task
      resTsks.add(ctx.getCommonJoinTask());
    } else {
      // run the map join task
      Task<? extends Serializable> task = ctx.getAliasToTask().get(bigTableAlias);
      //set task tag
      if(task.getTaskTag() == Task.CONVERTED_LOCAL_MAPJOIN) {
        task.getBackupTask().setTaskTag(Task.BACKUP_COMMON_JOIN);
      }
      resTsks.add(task);

    }

    return resTsks;
  }

  private String resolveMapJoinTask(HashMap<String, String> aliasToPath, HiveConf conf) {
    // for the full out join; return null directly
    if (aliasToPath.size() == 0) {
      return null;
    }

    // generate file size to alias mapping; but not set file size as key,
    // because different file may have the same file size.
    List<String> aliasList = new ArrayList<String>();
    List<Long> fileSizeList = new ArrayList<Long>();

    try {
      for (Map.Entry<String, String> entry : aliasToPath.entrySet()) {
        String alias = entry.getKey();
        String pathStr = entry.getValue();

        Path path = new Path(pathStr);
        FileSystem fs = path.getFileSystem(conf);
        FileStatus[] fstatus = fs.listStatus(path);
        long fileSize = 0;
        for (int i = 0; i < fstatus.length; i++) {
          fileSize += fstatus[i].getLen();
        }

        // put into list and sorted set
        aliasList.add(alias);
        fileSizeList.add(fileSize);

      }
      // sorted based file size
      List<Long> sortedList = new ArrayList<Long>(fileSizeList);
      Collections.sort(sortedList);

      // get big table file size and small table file size summary
      long bigTableFileSize = 0;
      long smallTablesFileSizeSum = 0;
      String bigTableFileAlias = null;
      int size = sortedList.size();
      int tmpIndex;
      // Iterate the sorted_set to get big/small table file size
      for (int index = 0; index < sortedList.size(); index++) {
        Long key = sortedList.get(index);
        if (index != (size - 1)) {
          smallTablesFileSizeSum += key.longValue();
        } else {
          tmpIndex = fileSizeList.indexOf(key);
          String alias = aliasList.get(tmpIndex);
          bigTableFileSize += key.longValue();
          bigTableFileAlias = alias;
        }
      }

      // compare with threshold
      long threshold = HiveConf.getLongVar(conf, HiveConf.ConfVars.HIVESMALLTABLESFILESIZE);
      if (smallTablesFileSizeSum <= threshold) {
        return bigTableFileAlias;
      } else {
        return null;
      }
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }
}
