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
    HashMap<String, ArrayList<String>> pathToAliases;
    HashMap<String, Long> aliasToKnownSize;
    private Task<? extends Serializable> commonJoinTask;

    private String localTmpDir;
    private String hdfsTmpDir;

    public ConditionalResolverCommonJoinCtx() {
    }

    public HashMap<String, Task<? extends Serializable>> getAliasToTask() {
      return aliasToTask;
    }

    public void setAliasToTask(HashMap<String, Task<? extends Serializable>> aliasToTask) {
      this.aliasToTask = aliasToTask;
    }

    public Task<? extends Serializable> getCommonJoinTask() {
      return commonJoinTask;
    }

    public void setCommonJoinTask(Task<? extends Serializable> commonJoinTask) {
      this.commonJoinTask = commonJoinTask;
    }

    public HashMap<String, Long> getAliasToKnownSize() {
      return aliasToKnownSize == null ? new HashMap<String, Long>() : aliasToKnownSize;
    }

    public void setAliasToKnownSize(HashMap<String, Long> aliasToKnownSize) {
      this.aliasToKnownSize = aliasToKnownSize;
    }

    public HashMap<String, ArrayList<String>> getPathToAliases() {
      return pathToAliases;
    }

    public void setPathToAliases(HashMap<String, ArrayList<String>> pathToAliases) {
      this.pathToAliases = pathToAliases;
    }

    public String getLocalTmpDir() {
      return localTmpDir;
    }

    public void setLocalTmpDir(String localTmpDir) {
      this.localTmpDir = localTmpDir;
    }

    public String getHdfsTmpDir() {
      return hdfsTmpDir;
    }

    public void setHdfsTmpDir(String hdfsTmpDir) {
      this.hdfsTmpDir = hdfsTmpDir;
    }
  }

  public ConditionalResolverCommonJoin() {
  }

  @Override
  public List<Task<? extends Serializable>> getTasks(HiveConf conf, Object objCtx) {
    ConditionalResolverCommonJoinCtx ctx = (ConditionalResolverCommonJoinCtx) objCtx;
    List<Task<? extends Serializable>> resTsks = new ArrayList<Task<? extends Serializable>>();

    // get aliasToPath and pass it to the heuristic
    HashMap<String, ArrayList<String>> pathToAliases = ctx.getPathToAliases();
    HashMap<String, Long> aliasToKnownSize = ctx.getAliasToKnownSize();
    String bigTableAlias = this.resolveMapJoinTask(pathToAliases, ctx
        .getAliasToTask(), aliasToKnownSize, ctx.getHdfsTmpDir(), ctx
        .getLocalTmpDir(), conf);

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

  static class AliasFileSizePair implements Comparable<AliasFileSizePair> {
    String alias;
    long size;
    AliasFileSizePair(String alias, long size) {
      super();
      this.alias = alias;
      this.size = size;
    }
    @Override
    public int compareTo(AliasFileSizePair o) {
      if (o == null) {
        return 1;
      }
      return (size < o.size) ? -1 : ((size > o.size) ? 1 : 0);
    }
  }

  private String resolveMapJoinTask(
      HashMap<String, ArrayList<String>> pathToAliases,
      HashMap<String, Task<? extends Serializable>> aliasToTask,
      HashMap<String, Long> aliasToKnownSize, String hdfsTmpDir,
      String localTmpDir, HiveConf conf) {

    String bigTableFileAlias = null;
    long smallTablesFileSizeSum = 0;

    Map<String, AliasFileSizePair> aliasToFileSizeMap = new HashMap<String, AliasFileSizePair>();
    for (Map.Entry<String, Long> entry : aliasToKnownSize.entrySet()) {
      String alias = entry.getKey();
      AliasFileSizePair pair = new AliasFileSizePair(alias, entry.getValue());
      aliasToFileSizeMap.put(alias, pair);
    }

    try {
      // need to compute the input size at runtime, and select the biggest as
      // the big table.
      for (Map.Entry<String, ArrayList<String>> oneEntry : pathToAliases
          .entrySet()) {
        String p = oneEntry.getKey();
        // this path is intermediate data
        if (p.startsWith(hdfsTmpDir) || p.startsWith(localTmpDir)) {
          ArrayList<String> aliasArray = oneEntry.getValue();
          if (aliasArray.size() <= 0) {
            continue;
          }
          Path path = new Path(p);
          FileSystem fs = path.getFileSystem(conf);
          long fileSize = fs.getContentSummary(path).getLength();
          for (String alias : aliasArray) {
            AliasFileSizePair pair = aliasToFileSizeMap.get(alias);
            if (pair == null) {
              pair = new AliasFileSizePair(alias, 0);
              aliasToFileSizeMap.put(alias, pair);
            }
            pair.size += fileSize;
          }
        }
      }
      // generate file size to alias mapping; but not set file size as key,
      // because different file may have the same file size.

      List<AliasFileSizePair> aliasFileSizeList = new ArrayList<AliasFileSizePair>(
          aliasToFileSizeMap.values());

      Collections.sort(aliasFileSizeList);
      // iterating through this list from the end to beginning, trying to find
      // the big table for mapjoin
      int idx = aliasFileSizeList.size() - 1;
      boolean bigAliasFound = false;
      while (idx >= 0) {
        AliasFileSizePair pair = aliasFileSizeList.get(idx);
        String alias = pair.alias;
        long size = pair.size;
        idx--;
        if (!bigAliasFound && aliasToTask.get(alias) != null) {
          // got the big table
          bigAliasFound = true;
          bigTableFileAlias = alias;
          continue;
        }
        smallTablesFileSizeSum += size;
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
