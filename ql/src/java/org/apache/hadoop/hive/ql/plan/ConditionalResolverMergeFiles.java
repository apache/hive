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
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.HiveStatsUtils;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.utils.FileUtils;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Conditional task resolution interface. This is invoked at run time to get the
 * task to invoke. Developers can plug in their own resolvers
 */
public class ConditionalResolverMergeFiles implements ConditionalResolver,
    Serializable {
  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(ConditionalResolverMergeFiles.class);

  public ConditionalResolverMergeFiles() {
  }

  /**
   * ConditionalResolverMergeFilesCtx.
   *
   */
  public static class ConditionalResolverMergeFilesCtx implements Serializable {
    private static final long serialVersionUID = 1L;
    List<Task<?>> listTasks;
    private String dir;
    private DynamicPartitionCtx dpCtx; // merge task could be after dynamic partition insert
    private ListBucketingCtx lbCtx;
    private Properties properties;
    private String storageHandlerClass;

    public ConditionalResolverMergeFilesCtx() {
    }

    /**
     * @param dir
     */
    public ConditionalResolverMergeFilesCtx(
        List<Task<?>> listTasks, String dir) {
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
     * @return the listTasks
     */
    public List<Task<?>> getListTasks() {
      return listTasks;
    }

    /**
     * @param listTasks
     *          the listTasks to set
     */
    public void setListTasks(List<Task<?>> listTasks) {
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

    public void setTaskProperties(Properties properties) {
      this.properties = properties;
    }

    public Properties getTaskProperties() {
      return properties;
    }

    public void setStorageHandlerClass(String className) {
      this.storageHandlerClass = className;
    }

    public String getStorageHandlerClass() {
      return storageHandlerClass;
    }
  }

  public List<Task<?>> getTasks(HiveConf conf, Object objCtx) {
    ConditionalResolverMergeFilesCtx ctx = (ConditionalResolverMergeFilesCtx) objCtx;
    String dirName = ctx.getDir();

    List<Task<?>> resTsks = new ArrayList<Task<?>>();
    // check if a map-reduce job is needed to merge the files
    // If the current size is smaller than the target, merge
    long trgtSize = conf.getLongVar(HiveConf.ConfVars.HIVE_MERGE_MAP_FILES_SIZE);
    long avgConditionSize = conf
        .getLongVar(HiveConf.ConfVars.HIVE_MERGE_MAP_FILES_AVG_SIZE);
    trgtSize = Math.max(trgtSize, avgConditionSize);

    Task<?> mvTask = ctx.getListTasks().get(0);
    Task<?> mrTask = ctx.getListTasks().get(1);
    Task<?> mrAndMvTask = ctx.getListTasks().get(2);

    try {
      Path dirPath = new Path(dirName);
      FileSystem inpFs = dirPath.getFileSystem(conf);
      DynamicPartitionCtx dpCtx = ctx.getDPCtx();
      HiveStorageHandler storageHandler = HiveUtils.getStorageHandler(conf, ctx.getStorageHandlerClass());
      boolean dirExists = inpFs.exists(dirPath);
      boolean useCustomStorageHandler = storageHandler != null && storageHandler.supportsMergeFiles();

      MapWork work = null;
      // For each dynamic partition, check if it needs to be merged.
      if (mrTask.getWork() instanceof MapredWork) {
        work = ((MapredWork) mrTask.getWork()).getMapWork();
      } else if (mrTask.getWork() instanceof TezWork){
        work = (MapWork) ((TezWork) mrTask.getWork()).getAllWork().get(0);
      } else {
        work = (MapWork) mrTask.getWork();
      }

      if (dirExists) {
        int lbLevel = (ctx.getLbCtx() == null) ? 0 : ctx.getLbCtx().calculateListBucketingLevel();
        boolean manifestFilePresent = false;
        FileSystem manifestFs = dirPath.getFileSystem(conf);
        if (manifestFs.exists(new Path(dirPath, Utilities.BLOB_MANIFEST_FILE))) {
          manifestFilePresent = true;
        }

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
              mrAndMvTask, dirPath, inpFs, ctx, work, dpLbLevel, manifestFilePresent, storageHandler);
        } else { // no dynamic partitions
          if(lbLevel == 0) {
            // static partition without list bucketing
            List<FileStatus> manifestFilePaths = Lists.newArrayList();
            long totalSize;
            if (manifestFilePresent) {
              manifestFilePaths = getManifestFilePaths(conf, dirPath);
              totalSize = getMergeSize(manifestFilePaths, avgConditionSize);
            } else {
              totalSize = getMergeSize(inpFs, dirPath, avgConditionSize);
              Utilities.FILE_OP_LOGGER.debug("merge resolve simple case - totalSize " + totalSize + " from " + dirPath);
            }

            if (totalSize >= 0) { // add the merge job
              if (manifestFilePresent) {
                setupWorkWhenUsingManifestFile(work, manifestFilePaths, dirPath, true);
              }
              setupMapRedWork(conf, work, trgtSize, totalSize);
              resTsks.add(mrTask);
            } else { // don't need to merge, add the move job
              resTsks.add(mvTask);
            }
          } else {
            // static partition and list bucketing
            generateActualTasks(conf, resTsks, trgtSize, avgConditionSize, mvTask, mrTask,
                mrAndMvTask, dirPath, inpFs, ctx, work, lbLevel, manifestFilePresent, storageHandler);
          }
        }
      } else if (useCustomStorageHandler) {
        generateActualTasks(conf, resTsks, trgtSize, avgConditionSize, mvTask, mrTask,
                mrAndMvTask, dirPath, inpFs, ctx, work, 0, false, storageHandler);
      } else {
        Utilities.FILE_OP_LOGGER.info("Resolver returning movetask for " + dirPath);
        resTsks.add(mvTask);
      }
    } catch (IOException e) {
      LOG.warn("Exception while getting tasks", e);
    } catch (ClassNotFoundException | HiveException e) {
      throw new RuntimeException("Failed to load storage handler: {}" + e.getMessage());
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
   * For example, if a static partition is defined as skewed and stored-as-directories,
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
  private void generateActualTasks(HiveConf conf, List<Task<?>> resTsks,
      long trgtSize, long avgConditionSize, Task<?> mvTask,
      Task<?> mrTask, Task<?> mrAndMvTask, Path dirPath,
      FileSystem inpFs, ConditionalResolverMergeFilesCtx ctx, MapWork work, int dpLbLevel,
      boolean manifestFilePresent, HiveStorageHandler storageHandler)
      throws IOException, ClassNotFoundException {
    DynamicPartitionCtx dpCtx = ctx.getDPCtx();
    List<FileStatus> statusList;
    Map<FileStatus, List<FileStatus>> parentDirToFile = new HashMap<>();
    boolean useCustomStorageHandler = storageHandler != null && storageHandler.supportsMergeFiles();
    MergeTaskProperties mergeProperties = useCustomStorageHandler ?
            storageHandler.getMergeTaskProperties(ctx.getTaskProperties()) : null;
    if (manifestFilePresent) {
      // Get the list of files from manifest file.
      List<FileStatus> fileStatuses = getManifestFilePaths(conf, dirPath);
      // Setup the work to include all the files present in the manifest.
      setupWorkWhenUsingManifestFile(work, fileStatuses, dirPath, false);
      parentDirToFile = getParentDirToFileMap(inpFs, fileStatuses);
      statusList = Lists.newArrayList(parentDirToFile.keySet());
    } else if (useCustomStorageHandler) {
      List<FileStatus> fileStatuses = storageHandler.getMergeTaskInputFiles(ctx.getTaskProperties());
      setupWorkWithCustomHandler(work, dirPath, mergeProperties);
      parentDirToFile = getParentDirToFileMap(inpFs, fileStatuses);
      statusList = Lists.newArrayList(parentDirToFile.keySet());
    } else {
      statusList = HiveStatsUtils.getFileStatusRecurse(dirPath, dpLbLevel, inpFs);
    }
    FileStatus[] status = statusList.toArray(new FileStatus[statusList.size()]);

    // cleanup pathToPartitionInfo
    Map<Path, PartitionDesc> ptpi = work.getPathToPartitionInfo();
    assert ptpi.size() == 1;
    Path path = ptpi.keySet().iterator().next();
    PartitionDesc partDesc = ptpi.get(path);
    TableDesc tblDesc = partDesc.getTableDesc();
    Utilities.FILE_OP_LOGGER.debug("merge resolver removing " + path);
    work.removePathToPartitionInfo(path); // the root path is not useful anymore

    // cleanup pathToAliases
    Map<Path, List<String>> pta = work.getPathToAliases();
    assert pta.size() == 1;
    path = pta.keySet().iterator().next();
    List<String> aliases = pta.get(path);
    work.removePathToAlias(path); // the root path is not useful anymore

    // populate pathToPartitionInfo and pathToAliases w/ DP paths
    long totalSize = 0;
    boolean doMerge = false;
    // list of paths that don't need to merge but need to move to the dest location
    List<Path> toMove = new ArrayList<>();
    List<Path> toMerge = new ArrayList<>();
    for (int i = 0; i < status.length; ++i) {
      long len;
      if (manifestFilePresent || useCustomStorageHandler) {
        len = getMergeSize(parentDirToFile.get(status[i]), avgConditionSize);
      } else {
        len = getMergeSize(inpFs, status[i].getPath(), avgConditionSize);
      }
      if (len >= 0) {
        doMerge = true;
        totalSize += len;
        PartitionDesc pDesc = (dpCtx != null) ? generateDPFullPartSpec(dpCtx, status, tblDesc, i)
            : partDesc;
        if (pDesc == null) {
          Utilities.FILE_OP_LOGGER.warn("merger ignoring invalid DP path " + status[i].getPath());
          continue;
        }
        Utilities.FILE_OP_LOGGER.debug("merge resolver will merge " + status[i].getPath());
        work.resolveDynamicPartitionStoredAsSubDirsMerge(conf, status[i].getPath(), tblDesc,
            aliases, pDesc);
        // Do not add input file since its already added when the manifest file is present.
        if (manifestFilePresent || useCustomStorageHandler) {
          toMerge.addAll(parentDirToFile.get(status[i])
              .stream().map(FileStatus::getPath).collect(Collectors.toList()));
        } else {
          toMerge.add(status[i].getPath());
        }
      } else {
        Utilities.FILE_OP_LOGGER.debug("merge resolver will move " + status[i].getPath());

        toMove.add(status[i].getPath());
      }
    }
    if (doMerge) {
      // Set paths appropriately.
      if (work.getInputPaths() != null && !work.getInputPaths().isEmpty()) {
        toMerge.addAll(work.getInputPaths());
      }
      work.setInputPaths(toMerge);
      // add the merge MR job
      setupMapRedWork(conf, work, trgtSize, totalSize);

      // add the move task for those partitions that do not need merging
      if (toMove.size() > 0) {
        // Note: this path should be specific to concatenate; never executed in a select query.
        // modify the existing move task as it is already in the candidate running tasks

        // running the MoveTask and MR task in parallel may
        // cause the mvTask write to /ds=1 and MR task write
        // to /ds=1_1 for the same partition.
        // make the MoveTask as the child of the MR Task
        resTsks.add(mrAndMvTask);

        // Originally the mvTask and the child move task of the mrAndMvTask contain the same
        // MoveWork object.
        // If the blobstore optimizations are on and the input/output paths are merged
        // in the move only MoveWork, the mvTask and the child move task of the mrAndMvTask
        // will contain different MoveWork objects, which causes problems.
        // Not just in this case, but also in general the child move task of the mrAndMvTask should
        // be used, because that is the correct move task for the "merge and move" use case.
        Task<?> mergeAndMoveMoveTask = mrAndMvTask.getChildTasks().get(0);
        MoveWork mvWork = (MoveWork) mergeAndMoveMoveTask.getWork();

        LoadFileDesc lfd = mvWork.getLoadFileWork();

        Path targetDir = lfd.getTargetDir();
        List<Path> targetDirs = new ArrayList<Path>(toMove.size());

        for (int i = 0; i < toMove.size(); i++) {
          // Here directly the path name is used, instead of the uri because the uri contains the
          // serialized version of the path. For dynamic partition, we need the non serialized
          // version of the path as this value is used directly as partition name to create the partition.
          // For example, if the dp name is "part=2022-01-16 04:35:56.732" then the uri
          // will contain "part=2022-01-162022-01-16%2004%253A35%253A56.732". When we convert it to
          // partition name, it will come as "part=2022-01-16 04%3A35%3A56.732". But the path will have
          // value "part=2022-01-16 04%3A35%3A56.732", which will get converted to proper name by
          // function escapePathName.
          String[] moveStrSplits = toMove.get(i).toString().split(Path.SEPARATOR);
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
    LinkedHashMap<String, String> fullPartSpec = new LinkedHashMap<>( dpCtx.getPartSpec());
    // Require all the directories to be present with some values.
    if (!Warehouse.makeSpecFromName(fullPartSpec, status[i].getPath(),
        new HashSet<>(dpCtx.getPartSpec().keySet()))) {
      return null;
    }
    return new PartitionDesc(tblDesc, fullPartSpec);
  }

  private void setupMapRedWork(HiveConf conf, MapWork mWork, long targetSize, long totalSize) {
    mWork.setMaxSplitSize(targetSize);
    mWork.setMinSplitSize(targetSize);
    mWork.setMinSplitSizePerNode(targetSize);
    mWork.setMinSplitSizePerRack(targetSize);
    mWork.setIsMergeFromResolver(true);
  }

  private static class FileSummary {
    private final long totalSize;
    private final long numFiles;

    public FileSummary(long totalSize, long numFiles) {
      this.totalSize = totalSize;
      this.numFiles  = numFiles;
    }

    public long getTotalSize() {
      return totalSize;
    }

    public long getNumFiles() {
      return numFiles;
    }
  }

  private FileSummary getFileSummary(List<FileStatus> fileStatusList) {
    LongSummaryStatistics stats = fileStatusList.stream().filter(FileStatus::isFile)
        .mapToLong(FileStatus::getLen).summaryStatistics();
    return new FileSummary(stats.getSum(), stats.getCount());
  }

  private List<FileStatus> getManifestFilePaths(HiveConf conf, Path dirPath) throws IOException {
    FileSystem manifestFs = dirPath.getFileSystem(conf);
    List<String> filesKept;
    List<FileStatus> pathsKept = new ArrayList<>();
    try (FSDataInputStream inStream = manifestFs.open(new Path(dirPath, Utilities.BLOB_MANIFEST_FILE))) {
      String paths = IOUtils.toString(inStream, Charset.defaultCharset());
      filesKept = Lists.newArrayList(paths.split(System.lineSeparator()));
    }
    // The first string contains the directory information. Not useful.
    filesKept.remove(0);

    for (String file : filesKept) {
      pathsKept.add(manifestFs.getFileStatus(new Path(file)));
    }
    return pathsKept;
  }

  private long getMergeSize(FileSystem inpFs, Path dirPath, long avgSize) {
    List<FileStatus> result = FileUtils.getFileStatusRecurse(dirPath, inpFs);
    return getMergeSize(result, avgSize);
  }

  /**
   * Whether to merge files inside directory given the threshold of the average file size.
   *
   * @param fileStatuses a list of FileStatus instances.
   * @param avgSize threshold of average file size.
   * @return -1 if not need to merge (either because of there is only 1 file or the
   * average size is larger than avgSize). Otherwise the size of the total size of files.
   * If return value is 0 that means there are multiple files each of which is an empty file.
   * This could be true when the table is bucketized and all buckets are empty.
   */
  private long getMergeSize(List<FileStatus> fileStatuses, long avgSize) {
    FileSummary fileSummary = getFileSummary(fileStatuses);
    if (fileSummary.getTotalSize() <= 0) {
      return -1;
    }

    if (fileSummary.getNumFiles() <= 1) {
      return -1;
    }

    if (fileSummary.getTotalSize() / fileSummary.getNumFiles() < avgSize) {
      return fileSummary.getTotalSize();
    }
    return -1;
  }

  private void setupWorkWhenUsingManifestFile(MapWork mapWork, List<FileStatus> fileStatuses, Path dirPath,
                                              boolean isTblLevel) {
    Map<String, Operator<? extends OperatorDesc>> aliasToWork = mapWork.getAliasToWork();
    Map<Path, PartitionDesc> pathToPartitionInfo = mapWork.getPathToPartitionInfo();
    Operator<? extends OperatorDesc> op = aliasToWork.get(dirPath.toString());
    PartitionDesc partitionDesc = pathToPartitionInfo.get(dirPath);
    Path tmpDirPath = Utilities.toTempPath(dirPath);
    if (op != null) {
      aliasToWork.remove(dirPath.toString());
      aliasToWork.put(tmpDirPath.toString(), op);
      mapWork.setAliasToWork(aliasToWork);
    }
    if (partitionDesc != null) {
      pathToPartitionInfo.remove(dirPath);
      pathToPartitionInfo.put(tmpDirPath, partitionDesc);
      mapWork.setPathToPartitionInfo(pathToPartitionInfo);
    }
    mapWork.removePathToAlias(dirPath);
    mapWork.addPathToAlias(tmpDirPath, tmpDirPath.toString());
    if (isTblLevel) {
      List<Path> inputPaths = fileStatuses.stream()
          .filter(FileStatus::isFile)
          .map(FileStatus::getPath).collect(Collectors.toList());
      mapWork.setInputPaths(inputPaths);
    }
    mapWork.setUseInputPathsDirectly(true);
  }

  private void setupWorkWithCustomHandler(MapWork mapWork, Path dirPath,
                                          MergeTaskProperties mergeProperties) throws IOException, ClassNotFoundException {
    Map<String, Operator<? extends OperatorDesc>> aliasToWork = mapWork.getAliasToWork();
    Map<Path, PartitionDesc> pathToPartitionInfo = mapWork.getPathToPartitionInfo();
    Operator<? extends OperatorDesc> op = aliasToWork.get(dirPath.toString());
    PartitionDesc partitionDesc = pathToPartitionInfo.get(dirPath);
    Path tmpDir = mergeProperties.getTmpLocation();
    if (op != null) {
      aliasToWork.remove(dirPath.toString());
      aliasToWork.put(tmpDir.toString(), op);
      mapWork.setAliasToWork(aliasToWork);
    }
    if (partitionDesc != null) {
      pathToPartitionInfo.remove(dirPath);
      pathToPartitionInfo.put(tmpDir, partitionDesc);
      mapWork.setPathToPartitionInfo(pathToPartitionInfo);
    }
    mapWork.setMergeSplitProperties(mergeProperties.getSplitProperties());
    mapWork.removePathToAlias(dirPath);
    mapWork.addPathToAlias(tmpDir, tmpDir.toString());
    mapWork.setUseInputPathsDirectly(true);
  }

  private Map<FileStatus, List<FileStatus>> getParentDirToFileMap(FileSystem inpFs, List<FileStatus> fileStatuses)
      throws IOException {
    Map<FileStatus, List<FileStatus>> manifestDirsToPaths = new HashMap<>();
    for (FileStatus fileStatus : fileStatuses) {
      if (!fileStatus.isDirectory()) {
        FileStatus parentDir = inpFs.getFileStatus(fileStatus.getPath().getParent());
        List<FileStatus> fileStatusList = Lists.newArrayList(fileStatus);
        manifestDirsToPaths.merge(parentDir, fileStatusList, (oldValue, newValue) -> {
          oldValue.addAll(newValue);
          return oldValue;
        });
      }
    }
    return manifestDirsToPaths;
  }
}
