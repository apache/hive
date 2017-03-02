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
import org.apache.hadoop.hive.common.HiveStatsUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.ql.exec.Task;

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
    private ListBucketingCtx lbCtx;

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

    /**
     * @return the lbCtx
     */
    public ListBucketingCtx getLbCtx() {
      return lbCtx;
    }

    /**
     * @param lbCtx the lbCtx to set
     */
    public void setLbCtx(ListBucketingCtx lbCtx) {
      this.lbCtx = lbCtx;
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
        MapWork work;
        if (mrTask.getWork() instanceof MapredWork) {
          work = ((MapredWork) mrTask.getWork()).getMapWork();
        } else if (mrTask.getWork() instanceof TezWork){
          work = (MapWork) ((TezWork) mrTask.getWork()).getAllWork().get(0);
        } else if (mrTask.getWork() instanceof SparkWork) {
          work = (MapWork) ((SparkWork) mrTask.getWork()).getAllWork().get(0);
        } else {
          work = (MapWork) mrTask.getWork();
        }

        int lbLevel = (ctx.getLbCtx() == null) ? 0 : ctx.getLbCtx().calculateListBucketingLevel();

        /**
         * In order to make code easier to read, we write the following in the way:
         * 1. the first if clause to differ dynamic partition and static partition
         * 2. with static partition, we differ list bucketing from non-list bucketing.
         * Another way to write it is to merge static partition w/ LB wit DP. In that way,
         * we still need to further differ them, since one uses lbLevel and
         * another lbLevel+numDPCols.
         * The first one is selected mainly for easy to read.
         */
        // Dynamic partition: replace input path (root to dp paths) with dynamic partition
        // input paths.
        if (dpCtx != null &&  dpCtx.getNumDPCols() > 0) {
          int numDPCols = dpCtx.getNumDPCols();
          int dpLbLevel = numDPCols + lbLevel;

          generateActualTasks(conf, resTsks, trgtSize, avgConditionSize, mvTask, mrTask,
              mrAndMvTask, dirPath, inpFs, ctx, work, dpLbLevel);
        } else { // no dynamic partitions
          if(lbLevel == 0) {
            // static partition without list bucketing
            long totalSz = getMergeSize(inpFs, dirPath, avgConditionSize);
            if (totalSz >= 0) { // add the merge job
              setupMapRedWork(conf, work, trgtSize, totalSz);
              resTsks.add(mrTask);
            } else { // don't need to merge, add the move job
              resTsks.add(mvTask);
            }
          } else {
            // static partition and list bucketing
            generateActualTasks(conf, resTsks, trgtSize, avgConditionSize, mvTask, mrTask,
                mrAndMvTask, dirPath, inpFs, ctx, work, lbLevel);
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

  /**
   * This method generates actual task for conditional tasks. It could be
   * 1. move task only
   * 2. merge task only
   * 3. merge task followed by a move task.
   * It used to be true for dynamic partition only since static partition doesn't have #3.
   * It changes w/ list bucketing. Static partition has #3 since it has sub-directories.
   * For example, if a static partition is defined as skewed and stored-as-directores,
   * instead of all files in one directory, it will create a sub-dir per skewed value plus
   * default directory. So #3 is required for static partition.
   * So, we move it to a method so that it can be used by both SP and DP.
   * @param conf
   * @param resTsks
   * @param trgtSize
   * @param avgConditionSize
   * @param mvTask
   * @param mrTask
   * @param mrAndMvTask
   * @param dirPath
   * @param inpFs
   * @param ctx
   * @param work
   * @param dpLbLevel
   * @throws IOException
   */
  private void generateActualTasks(HiveConf conf, List<Task<? extends Serializable>> resTsks,
      long trgtSize, long avgConditionSize, Task<? extends Serializable> mvTask,
      Task<? extends Serializable> mrTask, Task<? extends Serializable> mrAndMvTask, Path dirPath,
      FileSystem inpFs, ConditionalResolverMergeFilesCtx ctx, MapWork work, int dpLbLevel)
      throws IOException {
    DynamicPartitionCtx dpCtx = ctx.getDPCtx();
    // get list of dynamic partitions
    FileStatus[] status = HiveStatsUtils.getFileStatusRecurse(dirPath, dpLbLevel, inpFs);

    // cleanup pathToPartitionInfo
    Map<Path, PartitionDesc> ptpi = work.getPathToPartitionInfo();
    assert ptpi.size() == 1;
    Path path = ptpi.keySet().iterator().next();
    PartitionDesc partDesc = ptpi.get(path);
    TableDesc tblDesc = partDesc.getTableDesc();
    work.removePathToPartitionInfo(path); // the root path is not useful anymore

    // cleanup pathToAliases
    LinkedHashMap<Path, ArrayList<String>> pta = work.getPathToAliases();
    assert pta.size() == 1;
    path = pta.keySet().iterator().next();
    ArrayList<String> aliases = pta.get(path);
    work.removePathToAlias(path); // the root path is not useful anymore

    // populate pathToPartitionInfo and pathToAliases w/ DP paths
    long totalSz = 0;
    boolean doMerge = false;
    // list of paths that don't need to merge but need to move to the dest location
    List<Path> toMove = new ArrayList<Path>();
    for (int i = 0; i < status.length; ++i) {
      long len = getMergeSize(inpFs, status[i].getPath(), avgConditionSize);
      if (len >= 0) {
        doMerge = true;
        totalSz += len;
        PartitionDesc pDesc = (dpCtx != null) ? generateDPFullPartSpec(dpCtx, status, tblDesc, i)
            : partDesc;
        work.resolveDynamicPartitionStoredAsSubDirsMerge(conf, status[i].getPath(), tblDesc,
            aliases, pDesc);
      } else {
        toMove.add(status[i].getPath());
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

        Path targetDir = lfd.getTargetDir();
        List<Path> targetDirs = new ArrayList<Path>(toMove.size());

        for (int i = 0; i < toMove.size(); i++) {
          String[] moveStrSplits = toMove.get(i).toUri().toString().split(Path.SEPARATOR);
          int dpIndex = moveStrSplits.length - dpLbLevel;
          Path target = targetDir;
          while (dpIndex < moveStrSplits.length) {
            target = new Path(target, moveStrSplits[dpIndex]);
            dpIndex++;
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
  }

  private PartitionDesc generateDPFullPartSpec(DynamicPartitionCtx dpCtx, FileStatus[] status,
      TableDesc tblDesc, int i) {
    LinkedHashMap<String, String> fullPartSpec = new LinkedHashMap<>(dpCtx.getPartSpec());
    Warehouse.makeSpecFromName(fullPartSpec, status[i].getPath());
    PartitionDesc pDesc = new PartitionDesc(tblDesc, fullPartSpec);
    return pDesc;
  }

  private void setupMapRedWork(HiveConf conf, MapWork mWork, long targetSize, long totalSize) {
    mWork.setMaxSplitSize(targetSize);
    mWork.setMinSplitSize(targetSize);
    mWork.setMinSplitSizePerNode(targetSize);
    mWork.setMinSplitSizePerRack(targetSize);
  }

  private static class AverageSize {
    private final long totalSize;
    private final int numFiles;

    public AverageSize(long totalSize, int numFiles) {
      this.totalSize = totalSize;
      this.numFiles  = numFiles;
    }

    public long getTotalSize() {
      return totalSize;
    }

    public int getNumFiles() {
      return numFiles;
    }
  }

  private AverageSize getAverageSize(FileSystem inpFs, Path dirPath) {
    AverageSize error = new AverageSize(-1, -1);
    try {
      FileStatus[] fStats = inpFs.listStatus(dirPath);

      long totalSz = 0;
      int numFiles = 0;
      for (FileStatus fStat : fStats) {
        if (fStat.isDir()) {
          AverageSize avgSzDir = getAverageSize(inpFs, fStat.getPath());
          if (avgSzDir.getTotalSize() < 0) {
            return error;
          }
          totalSz += avgSzDir.getTotalSize();
          numFiles += avgSzDir.getNumFiles();
        }
        else {
          totalSz += fStat.getLen();
          numFiles++;
        }
      }

      return new AverageSize(totalSz, numFiles);
    } catch (IOException e) {
      return error;
    }
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
    AverageSize averageSize = getAverageSize(inpFs, dirPath);
    if (averageSize.getTotalSize() <= 0) {
      return -1;
    }

    if (averageSize.getNumFiles() <= 1) {
      return -1;
    }

    if (averageSize.getTotalSize()/averageSize.getNumFiles() < avgSize) {
      return averageSize.getTotalSize();
    }
    return -1;
  }
}
