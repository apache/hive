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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.Utilities;

/**
 * ConditionalResolverSkewJoin.
 *
 */
public class ConditionalResolverCommonJoin implements ConditionalResolver, Serializable {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(ConditionalResolverCommonJoin.class);

  /**
   * ConditionalResolverSkewJoinCtx.
   *
   */
  public static class ConditionalResolverCommonJoinCtx implements Serializable {
    private static final long serialVersionUID = 1L;

    private HashMap<Task<? extends Serializable>, Set<String>> taskToAliases;
    HashMap<Path, ArrayList<String>> pathToAliases;
    HashMap<String, Long> aliasToKnownSize;
    private Task<? extends Serializable> commonJoinTask;

    private Path localTmpDir;
    private Path hdfsTmpDir;

    public ConditionalResolverCommonJoinCtx() {
    }

    public HashMap<Task<? extends Serializable>, Set<String>> getTaskToAliases() {
      return taskToAliases;
    }

    public void setTaskToAliases(HashMap<Task<? extends Serializable>, Set<String>> taskToAliases) {
      this.taskToAliases = taskToAliases;
    }

    public Task<? extends Serializable> getCommonJoinTask() {
      return commonJoinTask;
    }

    public void setCommonJoinTask(Task<? extends Serializable> commonJoinTask) {
      this.commonJoinTask = commonJoinTask;
    }

    public HashMap<String, Long> getAliasToKnownSize() {
      return aliasToKnownSize == null ?
          aliasToKnownSize = new HashMap<String, Long>() : aliasToKnownSize;
    }

    public void setAliasToKnownSize(HashMap<String, Long> aliasToKnownSize) {
      this.aliasToKnownSize = aliasToKnownSize;
    }

    public HashMap<Path, ArrayList<String>> getPathToAliases() {
      return pathToAliases;
    }

    public void setPathToAliases(final HashMap<Path, ArrayList<String>> pathToAliases) {
      this.pathToAliases = pathToAliases;
    }

    public Path getLocalTmpDir() {
      return localTmpDir;
    }

    public void setLocalTmpDir(Path localTmpDir) {
      this.localTmpDir = localTmpDir;
    }

    public Path getHdfsTmpDir() {
      return hdfsTmpDir;
    }

    public void setHdfsTmpDir(Path hdfsTmpDir) {
      this.hdfsTmpDir = hdfsTmpDir;
    }

    @Override
    public ConditionalResolverCommonJoinCtx clone() {
      ConditionalResolverCommonJoinCtx ctx = new ConditionalResolverCommonJoinCtx();
      ctx.setTaskToAliases(taskToAliases);
      ctx.setCommonJoinTask(commonJoinTask);
      ctx.setPathToAliases(pathToAliases);
      ctx.setHdfsTmpDir(hdfsTmpDir);
      ctx.setLocalTmpDir(localTmpDir);
      // if any of join participants is from other MR, it has alias like '[pos:]$INTNAME'
      // which of size should be caculated for each resolver.
      ctx.setAliasToKnownSize(new HashMap<String, Long>(aliasToKnownSize));
      return ctx;
    }
  }

  public ConditionalResolverCommonJoin() {
  }

  @Override
  public List<Task<? extends Serializable>> getTasks(HiveConf conf, Object objCtx) {
    ConditionalResolverCommonJoinCtx ctx = ((ConditionalResolverCommonJoinCtx) objCtx).clone();
    List<Task<? extends Serializable>> resTsks = new ArrayList<Task<? extends Serializable>>();

    // get aliasToPath and pass it to the heuristic
    Task<? extends Serializable> task = resolveDriverAlias(ctx, conf);

    if (task == null) {
      // run common join task
      resTsks.add(ctx.getCommonJoinTask());
    } else {
      // run the map join task, set task tag
      if (task.getBackupTask() != null) {
        task.getBackupTask().setTaskTag(Task.BACKUP_COMMON_JOIN);
      }
      resTsks.add(task);

    }

    return resTsks;
  }

  private Task<? extends Serializable> resolveDriverAlias(ConditionalResolverCommonJoinCtx ctx, HiveConf conf) {
    try {
      resolveUnknownSizes(ctx, conf);
      return resolveMapJoinTask(ctx, conf);
    } catch (Exception e) {
      LOG.info("Failed to resolve driver alias by exception.. Falling back to common join", e);
    }
    return null;
  }

  protected Task<? extends Serializable> resolveMapJoinTask(
      ConditionalResolverCommonJoinCtx ctx, HiveConf conf) throws Exception {

    Set<String> participants = getParticipants(ctx);

    Map<String, Long> aliasToKnownSize = ctx.getAliasToKnownSize();
    Map<Task<? extends Serializable>, Set<String>> taskToAliases = ctx.getTaskToAliases();

    long threshold = HiveConf.getLongVar(conf, HiveConf.ConfVars.HIVESMALLTABLESFILESIZE);

    Long bigTableSize = null;
    Long smallTablesSize = null;
    Map.Entry<Task<? extends Serializable>, Set<String>> nextTask = null;
    for (Map.Entry<Task<? extends Serializable>, Set<String>> entry : taskToAliases.entrySet()) {
      Set<String> aliases = entry.getValue();
      long sumOfOthers = Utilities.sumOfExcept(aliasToKnownSize, participants, aliases);
      if (sumOfOthers < 0 || sumOfOthers > threshold) {
        continue;
      }
      // at most one alias is unknown. we can safely regard it as a big alias
      long aliasSize = Utilities.sumOf(aliasToKnownSize, aliases);
      if (bigTableSize == null || aliasSize > bigTableSize) {
        nextTask = entry;
        bigTableSize = aliasSize;
        smallTablesSize = sumOfOthers;
      }
    }
    if (nextTask != null) {
      LOG.info("Driver alias is " + nextTask.getValue() + " with size " + bigTableSize
          + " (total size of others : " + smallTablesSize + ", threshold : " + threshold + ")");
      return nextTask.getKey();
    }
    LOG.info("Failed to resolve driver alias (threshold : " + threshold +
        ", length mapping : " + aliasToKnownSize + ")");
    return null;
  }

  private Set<String> getParticipants(ConditionalResolverCommonJoinCtx ctx) {
    Set<String> participants = new HashSet<String>();
    for (List<String> aliases : ctx.getPathToAliases().values()) {
      participants.addAll(aliases);
    }
    return participants;
  }

  protected void resolveUnknownSizes(ConditionalResolverCommonJoinCtx ctx, HiveConf conf)
      throws Exception {

    Set<String> aliases = getParticipants(ctx);

    Map<String, Long> aliasToKnownSize = ctx.getAliasToKnownSize();
    Map<Path, ArrayList<String>> pathToAliases = ctx.getPathToAliases();

    Set<Path> unknownPaths = new HashSet<>();
    for (Map.Entry<Path, ArrayList<String>> entry : pathToAliases.entrySet()) {
      for (String alias : entry.getValue()) {
        if (aliases.contains(alias) && !aliasToKnownSize.containsKey(alias)) {
          unknownPaths.add(entry.getKey());
          break;
        }
      }
    }
    Path hdfsTmpDir = ctx.getHdfsTmpDir();
    Path localTmpDir = ctx.getLocalTmpDir();
    // need to compute the input size at runtime, and select the biggest as
    // the big table.
    for (Path path: unknownPaths) {
      // this path is intermediate data
      if (FileUtils.isPathWithinSubtree(path,hdfsTmpDir) || FileUtils.isPathWithinSubtree(path,localTmpDir)) {
        FileSystem fs = path.getFileSystem(conf);
        long fileSize = fs.getContentSummary(path).getLength();
        for (String alias : pathToAliases.get(path)) {
          Long length = aliasToKnownSize.get(alias);
          if (length == null) {
            aliasToKnownSize.put(alias, fileSize);
          }
        }
      }
    }
  }
}
