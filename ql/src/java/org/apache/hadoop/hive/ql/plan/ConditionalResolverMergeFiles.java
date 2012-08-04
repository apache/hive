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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.Utilities;

/**
 * Conditional task resolution interface. This is invoked at run time to get the
 * task to invoke. Developers can plug in their own resolvers
 */
public class ConditionalResolverMergeFiles implements ConditionalResolver,
    Serializable {
  private static final long serialVersionUID = 1L;

  public ConditionalResolverMergeFiles() {
  }

  /**
   * ConditionalResolverMergeFilesCtx.
   *
   */
  public static class ConditionalResolverMergeFilesCtx implements Serializable {
    private static final long serialVersionUID = 1L;
    List<Task<? extends Serializable>> listTasks;
    private String dir;
    private DynamicPartitionCtx dpCtx; // merge task could be after dynamic partition insert

    public ConditionalResolverMergeFilesCtx() {
    }

    /**
     * @param dir
     */
    public ConditionalResolverMergeFilesCtx(
        List<Task<? extends Serializable>> listTasks, String dir) {
      this.listTasks = listTasks;
      this.dir = dir;
    }

    /**
     * @return the dir
     */
    public String getDir() {
      return dir;
    }

    /**
     * @param dir
     *          the dir to set
     */
    public void setDir(String dir) {
      this.dir = dir;
    }

    /**
     * @return the listTasks
     */
    public List<Task<? extends Serializable>> getListTasks() {
      return listTasks;
    }

    /**
     * @param listTasks
     *          the listTasks to set
     */
    public void setListTasks(List<Task<? extends Serializable>> listTasks) {
      this.listTasks = listTasks;
    }

    public DynamicPartitionCtx getDPCtx() {
      return dpCtx;
    }

    public void setDPCtx(DynamicPartitionCtx dp) {
      dpCtx = dp;
    }
  }

  public List<Task<? extends Serializable>> getTasks(HiveConf conf,
      Object objCtx) {
    ConditionalResolverMergeFilesCtx ctx = (ConditionalResolverMergeFilesCtx) objCtx;
    String dirName = ctx.getDir();

    List<Task<? extends Serializable>> resTsks = new ArrayList<Task<? extends Serializable>>();
    // check if a map-reduce job is needed to merge the files
    // If the current size is smaller than the target, merge
    long trgtSize = conf.getLongVar(HiveConf.ConfVars.HIVEMERGEMAPFILESSIZE);
    long avgConditionSize = conf
        .getLongVar(HiveConf.ConfVars.HIVEMERGEMAPFILESAVGSIZE);
    trgtSize = Math.max(trgtSize, avgConditionSize);

    Task<? extends Serializable> mvTask = ctx.getListTasks().get(0);
    Task<? extends Serializable> mrTask = ctx.getListTasks().get(1);
    Task<? extends Serializable> mrAndMvTask = ctx.getListTasks().get(2);

    try {
      Path dirPath = new Path(dirName);
      FileSystem inpFs = dirPath.getFileSystem(conf);
      DynamicPartitionCtx dpCtx = ctx.getDPCtx();

      if (inpFs.exists(dirPath)) {
        // For each dynamic partition, check if it needs to be merged.
        MapredWork work = (MapredWork) mrTask.getWork();

        // Dynamic partition: replace input path (root to dp paths) with dynamic partition
        // input paths.
        if (dpCtx != null &&  dpCtx.getNumDPCols() > 0) {

          // get list of dynamic partitions
          FileStatus[] status = Utilities.getFileStatusRecurse(dirPath,
              dpCtx.getNumDPCols(), inpFs);

          // cleanup pathToPartitionInfo
          Map<String, PartitionDesc> ptpi = work.getPathToPartitionInfo();
          assert ptpi.size() == 1;
          String path = ptpi.keySet().iterator().next();
          TableDesc tblDesc = ptpi.get(path).getTableDesc();
          ptpi.remove(path); // the root path is not useful anymore

          // cleanup pathToAliases
          Map<String, ArrayList<String>> pta = work.getPathToAliases();
          assert pta.size() == 1;
          path = pta.keySet().iterator().next();
          ArrayList<String> aliases = pta.get(path);
          pta.remove(path); // the root path is not useful anymore

          // populate pathToPartitionInfo and pathToAliases w/ DP paths
          long totalSz = 0;
          boolean doMerge = false;
          // list of paths that don't need to merge but need to move to the dest location
          List<String> toMove = new ArrayList<String>();
          for (int i = 0; i < status.length; ++i) {
            long len = getMergeSize(inpFs, status[i].getPath(), avgConditionSize);
            if (len >= 0) {
              doMerge = true;
              totalSz += len;
              Map<String, String> fullPartSpec = new LinkedHashMap<String, String>(
                  dpCtx.getPartSpec());
              Warehouse.makeSpecFromName(fullPartSpec, status[i].getPath());
              PartitionDesc pDesc = new PartitionDesc(tblDesc, (LinkedHashMap) fullPartSpec);

              work.resolveDynamicPartitionMerge(conf, status[i].getPath(), tblDesc,
                  aliases, pDesc);
            } else {
              toMove.add(status[i].getPath().toString());
            }
          }
          if (doMerge) {
            // add the merge MR job
            setupMapRedWork(conf, work, trgtSize, totalSz);

            // add the move task for those partitions that do not need merging
            if (toMove.size() > 0) {
          	  // modify the existing move task as it is already in the candidate running tasks

          	  // running the MoveTask and MR task in parallel may
              // cause the mvTask write to /ds=1 and MR task write
              // to /ds=1_1 for the same partition.
              // make the MoveTask as the child of the MR Task
          	  resTsks.add(mrAndMvTask);

          	  MoveWork mvWork = (MoveWork) mvTask.getWork();
          	  LoadFileDesc lfd = mvWork.getLoadFileWork();

          	  String targetDir = lfd.getTargetDir();
          	  List<String> targetDirs = new ArrayList<String>(toMove.size());
          	  int numDPCols = dpCtx.getNumDPCols();

              for (int i = 0; i < toMove.size(); i++) {
                String toMoveStr = toMove.get(i);
                if (toMoveStr.endsWith(Path.SEPARATOR)) {
                  toMoveStr = toMoveStr.substring(0, toMoveStr.length() - 1);
                }
                String [] moveStrSplits = toMoveStr.split(Path.SEPARATOR);
                int dpIndex = moveStrSplits.length - numDPCols;
                String target = targetDir;
                while (dpIndex < moveStrSplits.length) {
                  target = target + Path.SEPARATOR + moveStrSplits[dpIndex];
                  dpIndex ++;
                }

                targetDirs.add(target);
              }

          	  LoadMultiFilesDesc lmfd = new LoadMultiFilesDesc(toMove,
          	      targetDirs, lfd.getIsDfsDir(), lfd.getColumns(), lfd.getColumnTypes());
          	  mvWork.setLoadFileWork(null);
          	  mvWork.setLoadTableWork(null);
          	  mvWork.setMultiFilesDesc(lmfd);
          	} else {
          	  resTsks.add(mrTask);
          	}
          } else { // add the move task
            resTsks.add(mvTask);
          }
        } else { // no dynamic partitions
          long totalSz = getMergeSize(inpFs, dirPath, avgConditionSize);
          if (totalSz >= 0) { // add the merge job
            setupMapRedWork(conf, work, trgtSize, totalSz);
            resTsks.add(mrTask);
          } else { // don't need to merge, add the move job
            resTsks.add(mvTask);
          }
        }
      } else {
        resTsks.add(mvTask);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    // Only one of the tasks should ever be added to resTsks
    assert(resTsks.size() == 1);

    return resTsks;
  }

  private void setupMapRedWork(HiveConf conf, MapredWork work, long targetSize, long totalSize) {
    if (work.getNumReduceTasks() > 0) {
      int maxReducers = conf.getIntVar(HiveConf.ConfVars.MAXREDUCERS);
      int reducers = (int) ((totalSize + targetSize - 1) / targetSize);
      reducers = Math.max(1, reducers);
      reducers = Math.min(maxReducers, reducers);
      work.setNumReduceTasks(reducers);
    }
    work.setMaxSplitSize(targetSize);
    work.setMinSplitSize(targetSize);
    work.setMinSplitSizePerNode(targetSize);
    work.setMinSplitSizePerRack(targetSize);
  }

  /**
   * Whether to merge files inside directory given the threshold of the average file size.
   *
   * @param inpFs input file system.
   * @param dirPath input file directory.
   * @param avgSize threshold of average file size.
   * @return -1 if not need to merge (either because of there is only 1 file or the
   * average size is larger than avgSize). Otherwise the size of the total size of files.
   * If return value is 0 that means there are multiple files each of which is an empty file.
   * This could be true when the table is bucketized and all buckets are empty.
   */
  private long getMergeSize(FileSystem inpFs, Path dirPath, long avgSize) {
    try {
      FileStatus[] fStats = inpFs.listStatus(dirPath);
      if (fStats.length <= 1) {
        return -1;
      }
      long totalSz = 0;
      for (FileStatus fStat : fStats) {
        totalSz += fStat.getLen();
      }

      if (totalSz < avgSize * fStats.length) {
        return totalSz;
      } else {
        return -1;
      }
    } catch (IOException e) {
      return -1;
    }
  }
}
