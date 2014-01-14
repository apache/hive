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

import java.io.IOException;
import java.io.Serializable;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.HiveStatsUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.exec.mr.MapRedTask;
import org.apache.hadoop.hive.ql.exec.mr.MapredLocalTask;
import org.apache.hadoop.hive.ql.hooks.LineageInfo.DataContainer;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.io.rcfile.merge.BlockMergeTask;
import org.apache.hadoop.hive.ql.lockmgr.HiveLock;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockManager;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockObj;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.physical.BucketingSortingCtx.BucketCol;
import org.apache.hadoop.hive.ql.optimizer.physical.BucketingSortingCtx.SortCol;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.plan.DynamicPartitionCtx;
import org.apache.hadoop.hive.ql.plan.LoadFileDesc;
import org.apache.hadoop.hive.ql.plan.LoadMultiFilesDesc;
import org.apache.hadoop.hive.ql.plan.LoadTableDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.MoveWork;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.util.StringUtils;

/**
 * MoveTask implementation.
 **/
public class MoveTask extends Task<MoveWork> implements Serializable {

  private static final long serialVersionUID = 1L;
  private static transient final Log LOG = LogFactory.getLog(MoveTask.class);

  public MoveTask() {
    super();
  }

  private void moveFile(Path sourcePath, Path targetPath, boolean isDfsDir)
      throws Exception {
    FileSystem fs = sourcePath.getFileSystem(conf);
    if (isDfsDir) {
      // Just do a rename on the URIs, they belong to the same FS
      String mesg = "Moving data to: " + targetPath.toString();
      String mesg_detail = " from " + sourcePath.toString();
      console.printInfo(mesg, mesg_detail);

      // delete the output directory if it already exists
      fs.delete(targetPath, true);
      // if source exists, rename. Otherwise, create a empty directory
      if (fs.exists(sourcePath)) {
        Path deletePath = null;
        // If it multiple level of folder are there fs.rename is failing so first
        // create the targetpath.getParent() if it not exist
        if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_INSERT_INTO_MULTILEVEL_DIRS)) {
        deletePath = createTargetPath(targetPath, fs);
        }
        if (!fs.rename(sourcePath, targetPath)) {
          try {
            if (deletePath != null) {
              fs.delete(deletePath, true);
            }
          } catch (IOException e) {
            LOG.info("Unable to delete the path created for facilitating rename"
                + deletePath);
          }
          throw new HiveException("Unable to rename: " + sourcePath
              + " to: " + targetPath);
        }
      } else if (!fs.mkdirs(targetPath)) {
        throw new HiveException("Unable to make directory: " + targetPath);
      }
    } else {
      // This is a local file
      String mesg = "Copying data to local directory " + targetPath.toString();
      String mesg_detail = " from " + sourcePath.toString();
      console.printInfo(mesg, mesg_detail);

      // delete the existing dest directory
      LocalFileSystem dstFs = FileSystem.getLocal(conf);

      if (dstFs.delete(targetPath, true) || !dstFs.exists(targetPath)) {
        console.printInfo(mesg, mesg_detail);
        // if source exists, rename. Otherwise, create a empty directory
        if (fs.exists(sourcePath)) {
          fs.copyToLocalFile(sourcePath, targetPath);
        } else {
          if (!dstFs.mkdirs(targetPath)) {
            throw new HiveException("Unable to make local directory: "
                + targetPath);
          }
        }
      } else {
        throw new AccessControlException(
            "Unable to delete the existing destination directory: "
            + targetPath);
      }
    }
  }

  private Path createTargetPath(Path targetPath, FileSystem fs) throws IOException {
    Path deletePath = null;
    Path mkDirPath = targetPath.getParent();
    if (mkDirPath != null & !fs.exists(mkDirPath)) {
      Path actualPath = mkDirPath;
      // targetPath path is /x/y/z/1/2/3 here /x/y/z is present in the file system
      // create the structure till /x/y/z/1/2 to work rename for multilevel directory
      // and if rename fails delete the path /x/y/z/1
      // If targetPath have multilevel directories like /x/y/z/1/2/3 , /x/y/z/1/2/4
      // the renaming of the directories are not atomic the execution will happen one
      // by one
      while (actualPath != null && !fs.exists(actualPath)) {
        deletePath = actualPath;
        actualPath = actualPath.getParent();
      }
      fs.mkdirs(mkDirPath);
    }
    return deletePath;
  }

  // Release all the locks acquired for this object
  // This becomes important for multi-table inserts when one branch may take much more
  // time than the others. It is better to release the lock for this particular insert.
  // The other option is to wait for all the branches to finish, or set
  // hive.multi.insert.move.tasks.share.dependencies to true, which will mean that the
  // first multi-insert results will be available when all of the branches of multi-table
  // inserts are done.
  private void releaseLocks(LoadTableDesc ltd) throws HiveException {
    // nothing needs to be done
    if (!conf.getBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY)) {
      return;
    }

    Context ctx = driverContext.getCtx();
    HiveLockManager lockMgr = ctx.getHiveLockMgr();
    WriteEntity output = ctx.getLoadTableOutputMap().get(ltd);
    List<HiveLockObj> lockObjects = ctx.getOutputLockObjects().get(output);
    if (lockObjects == null) {
      return;
    }

    for (HiveLockObj lockObj : lockObjects) {
      List<HiveLock> locks = lockMgr.getLocks(lockObj.getObj(), false, true);
      for (HiveLock lock : locks) {
        if (lock.getHiveLockMode() == lockObj.getMode()) {
          LOG.info("about to release lock for output: " + output.toString() +
              " lock: " + lock.getHiveLockObject().getName());
          lockMgr.unlock(lock);
          ctx.getHiveLocks().remove(lock);
        }
      }
    }
  }

  @Override
  public int execute(DriverContext driverContext) {

    try {

      // Do any hive related operations like moving tables and files
      // to appropriate locations
      LoadFileDesc lfd = work.getLoadFileWork();
      if (lfd != null) {
        Path targetPath = lfd.getTargetDir();
        Path sourcePath = lfd.getSourcePath();
        moveFile(sourcePath, targetPath, lfd.getIsDfsDir());
      }

      // Multi-file load is for dynamic partitions when some partitions do not
      // need to merge and they can simply be moved to the target directory.
      LoadMultiFilesDesc lmfd = work.getLoadMultiFilesWork();
      if (lmfd != null) {
        boolean isDfsDir = lmfd.getIsDfsDir();
        int i = 0;
        while (i <lmfd.getSourceDirs().size()) {
          Path srcPath = lmfd.getSourceDirs().get(i);
          Path destPath = lmfd.getTargetDirs().get(i);
          FileSystem fs = destPath.getFileSystem(conf);
          if (!fs.exists(destPath.getParent())) {
            fs.mkdirs(destPath.getParent());
          }
          moveFile(srcPath, destPath, isDfsDir);
          i++;
        }
      }

      // Next we do this for tables and partitions
      LoadTableDesc tbd = work.getLoadTableWork();
      if (tbd != null) {
        StringBuilder mesg = new StringBuilder("Loading data to table ")
            .append( tbd.getTable().getTableName());
        if (tbd.getPartitionSpec().size() > 0) {
          mesg.append(" partition (");
          Map<String, String> partSpec = tbd.getPartitionSpec();
          for (String key: partSpec.keySet()) {
            mesg.append(key).append('=').append(partSpec.get(key)).append(", ");
          }
          mesg.setLength(mesg.length()-2);
          mesg.append(')');
        }
        String mesg_detail = " from " + tbd.getSourcePath();
        console.printInfo(mesg.toString(), mesg_detail);
        Table table = db.getTable(tbd.getTable().getTableName());

        if (work.getCheckFileFormat()) {
          // Get all files from the src directory
          FileStatus[] dirs;
          ArrayList<FileStatus> files;
          FileSystem fs;
          try {
            fs = table.getDataLocation().getFileSystem(conf);
            dirs = fs.globStatus(tbd.getSourcePath());
            files = new ArrayList<FileStatus>();
            for (int i = 0; (dirs != null && i < dirs.length); i++) {
              files.addAll(Arrays.asList(fs.listStatus(dirs[i].getPath())));
              // We only check one file, so exit the loop when we have at least
              // one.
              if (files.size() > 0) {
                break;
              }
            }
          } catch (IOException e) {
            throw new HiveException(
                "addFiles: filesystem error in check phase", e);
          }
          if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVECHECKFILEFORMAT)) {
            // Check if the file format of the file matches that of the table.
            boolean flag = HiveFileFormatUtils.checkInputFormat(
                fs, conf, tbd.getTable().getInputFileFormatClass(), files);
            if (!flag) {
              throw new HiveException(
                  "Wrong file format. Please check the file's format.");
            }
          }
        }

        // Create a data container
        DataContainer dc = null;
        if (tbd.getPartitionSpec().size() == 0) {
          dc = new DataContainer(table.getTTable());
          db.loadTable(tbd.getSourcePath(), tbd.getTable()
              .getTableName(), tbd.getReplace(), tbd.getHoldDDLTime());
          if (work.getOutputs() != null) {
            work.getOutputs().add(new WriteEntity(table));
          }
        } else {
          LOG.info("Partition is: " + tbd.getPartitionSpec().toString());

          // Check if the bucketing and/or sorting columns were inferred
          List<BucketCol> bucketCols = null;
          List<SortCol> sortCols = null;
          int numBuckets = -1;
          Task task = this;
          String path = tbd.getSourcePath().toUri().toString();
          // Find the first ancestor of this MoveTask which is some form of map reduce task
          // (Either standard, local, or a merge)
          while (task.getParentTasks() != null && task.getParentTasks().size() == 1) {
            task = (Task)task.getParentTasks().get(0);
            // If it was a merge task or a local map reduce task, nothing can be inferred
            if (task instanceof BlockMergeTask || task instanceof MapredLocalTask) {
              break;
            }

            // If it's a standard map reduce task, check what, if anything, it inferred about
            // the directory this move task is moving
            if (task instanceof MapRedTask) {
              MapredWork work = (MapredWork)task.getWork();
              MapWork mapWork = work.getMapWork();
              bucketCols = mapWork.getBucketedColsByDirectory().get(path);
              sortCols = mapWork.getSortedColsByDirectory().get(path);
              if (work.getReduceWork() != null) {
                numBuckets = work.getReduceWork().getNumReduceTasks();
              }

              if (bucketCols != null || sortCols != null) {
                // This must be a final map reduce task (the task containing the file sink
                // operator that writes the final output)
                assert work.isFinalMapRed();
              }
              break;
            }

            // If it's a move task, get the path the files were moved from, this is what any
            // preceding map reduce task inferred information about, and moving does not invalidate
            // those assumptions
            // This can happen when a conditional merge is added before the final MoveTask, but the
            // condition for merging is not met, see GenMRFileSink1.
            if (task instanceof MoveTask) {
              if (((MoveTask)task).getWork().getLoadFileWork() != null) {
                path = ((MoveTask)task).getWork().getLoadFileWork().getSourcePath().toUri().toString();
              }
            }
          }
          // deal with dynamic partitions
          DynamicPartitionCtx dpCtx = tbd.getDPCtx();
          if (dpCtx != null && dpCtx.getNumDPCols() > 0) { // dynamic partitions

            List<LinkedHashMap<String, String>> dps = Utilities.getFullDPSpecs(conf, dpCtx);

            // publish DP columns to its subscribers
            if (dps != null && dps.size() > 0) {
              pushFeed(FeedType.DYNAMIC_PARTITIONS, dps);
            }

            // load the list of DP partitions and return the list of partition specs
            // TODO: In a follow-up to HIVE-1361, we should refactor loadDynamicPartitions
            // to use Utilities.getFullDPSpecs() to get the list of full partSpecs.
            // After that check the number of DPs created to not exceed the limit and
            // iterate over it and call loadPartition() here.
            // The reason we don't do inside HIVE-1361 is the latter is large and we
            // want to isolate any potential issue it may introduce.
            ArrayList<LinkedHashMap<String, String>> dp =
              db.loadDynamicPartitions(
                  tbd.getSourcePath(),
                  tbd.getTable().getTableName(),
                	tbd.getPartitionSpec(),
                	tbd.getReplace(),
                	dpCtx.getNumDPCols(),
                	tbd.getHoldDDLTime(),
                	isSkewedStoredAsDirs(tbd));

            if (dp.size() == 0 && conf.getBoolVar(HiveConf.ConfVars.HIVE_ERROR_ON_EMPTY_PARTITION)) {
              throw new HiveException("This query creates no partitions." +
              		" To turn off this error, set hive.error.on.empty.partition=false.");
            }

            // for each partition spec, get the partition
            // and put it to WriteEntity for post-exec hook
            for (LinkedHashMap<String, String> partSpec: dp) {
              Partition partn = db.getPartition(table, partSpec, false);

              if (bucketCols != null || sortCols != null) {
                updatePartitionBucketSortColumns(table, partn, bucketCols, numBuckets, sortCols);
              }

              WriteEntity enty = new WriteEntity(partn);
              if (work.getOutputs() != null) {
                work.getOutputs().add(enty);
              }
              // Need to update the queryPlan's output as well so that post-exec hook get executed.
              // This is only needed for dynamic partitioning since for SP the the WriteEntity is
              // constructed at compile time and the queryPlan already contains that.
              // For DP, WriteEntity creation is deferred at this stage so we need to update
              // queryPlan here.
              if (queryPlan.getOutputs() == null) {
                queryPlan.setOutputs(new HashSet<WriteEntity>());
              }
              queryPlan.getOutputs().add(enty);

              // update columnar lineage for each partition
              dc = new DataContainer(table.getTTable(), partn.getTPartition());

              if (SessionState.get() != null) {
                SessionState.get().getLineageState().setLineage(tbd.getSourcePath(), dc,
                    table.getCols());
              }

              console.printInfo("\tLoading partition " + partSpec);
            }
            dc = null; // reset data container to prevent it being added again.
          } else { // static partitions
            List<String> partVals = MetaStoreUtils.getPvals(table.getPartCols(),
                tbd.getPartitionSpec());
            db.validatePartitionNameCharacters(partVals);
            db.loadPartition(tbd.getSourcePath(), tbd.getTable().getTableName(),
                tbd.getPartitionSpec(), tbd.getReplace(), tbd.getHoldDDLTime(),
                tbd.getInheritTableSpecs(), isSkewedStoredAsDirs(tbd));
          	Partition partn = db.getPartition(table, tbd.getPartitionSpec(), false);

          	if (bucketCols != null || sortCols != null) {
          	  updatePartitionBucketSortColumns(table, partn, bucketCols, numBuckets, sortCols);
            }

          	dc = new DataContainer(table.getTTable(), partn.getTPartition());
          	// add this partition to post-execution hook
          	if (work.getOutputs() != null) {
          	  work.getOutputs().add(new WriteEntity(partn));
          	}
         }
        }
        if (SessionState.get() != null && dc != null) {
          SessionState.get().getLineageState().setLineage(tbd.getSourcePath(), dc,
              table.getCols());
        }
        releaseLocks(tbd);
      }

      return 0;
    } catch (Exception e) {
      console.printError("Failed with exception " + e.getMessage(), "\n"
          + StringUtils.stringifyException(e));
      return (1);
    }
  }

  private boolean isSkewedStoredAsDirs(LoadTableDesc tbd) {
    return (tbd.getLbCtx() == null) ? false : tbd.getLbCtx()
        .isSkewedStoredAsDir();
  }

  /**
   * Alters the bucketing and/or sorting columns of the partition provided they meet some
   * validation criteria, e.g. the number of buckets match the number of files, and the
   * columns are not partition columns
   * @param table
   * @param partn
   * @param bucketCols
   * @param numBuckets
   * @param sortCols
   * @throws IOException
   * @throws InvalidOperationException
   * @throws HiveException
   */
  private void updatePartitionBucketSortColumns(Table table, Partition partn,
      List<BucketCol> bucketCols, int numBuckets, List<SortCol> sortCols)
          throws IOException, InvalidOperationException, HiveException {

    boolean updateBucketCols = false;
    if (bucketCols != null) {
      FileSystem fileSys = partn.getDataLocation().getFileSystem(conf);
      FileStatus[] fileStatus = HiveStatsUtils.getFileStatusRecurse(
          partn.getDataLocation(), 1, fileSys);
      // Verify the number of buckets equals the number of files
      // This will not hold for dynamic partitions where not every reducer produced a file for
      // those partitions.  In this case the table is not bucketed as Hive requires a files for
      // each bucket.
      if (fileStatus.length == numBuckets) {
        List<String> newBucketCols = new ArrayList<String>();
        updateBucketCols = true;
        for (BucketCol bucketCol : bucketCols) {
          if (bucketCol.getIndexes().get(0) < partn.getCols().size()) {
            newBucketCols.add(partn.getCols().get(
                bucketCol.getIndexes().get(0)).getName());
          } else {
            // If the table is bucketed on a partition column, not valid for bucketing
            updateBucketCols = false;
            break;
          }
        }
        if (updateBucketCols) {
          partn.getBucketCols().clear();
          partn.getBucketCols().addAll(newBucketCols);
          partn.getTPartition().getSd().setNumBuckets(numBuckets);
        }
      }
    }

    boolean updateSortCols = false;
    if (sortCols != null) {
      List<Order> newSortCols = new ArrayList<Order>();
      updateSortCols = true;
      for (SortCol sortCol : sortCols) {
        if (sortCol.getIndexes().get(0) < partn.getCols().size()) {
          newSortCols.add(new Order(
            partn.getCols().get(sortCol.getIndexes().get(0)).getName(),
            sortCol.getSortOrder() == '+' ? BaseSemanticAnalyzer.HIVE_COLUMN_ORDER_ASC :
              BaseSemanticAnalyzer.HIVE_COLUMN_ORDER_DESC));
        } else {
          // If the table is sorted on a partition column, not valid for sorting
          updateSortCols = false;
          break;
        }
      }
      if (updateSortCols) {
        partn.getSortCols().clear();
        partn.getSortCols().addAll(newSortCols);
      }
    }

    if (updateBucketCols || updateSortCols) {
      db.alterPartition(table.getDbName(), table.getTableName(), partn);
    }
  }

  /*
   * Does the move task involve moving to a local file system
   */
  public boolean isLocal() {
    LoadTableDesc tbd = work.getLoadTableWork();
    if (tbd != null) {
      return false;
    }

    LoadFileDesc lfd = work.getLoadFileWork();
    if (lfd != null) {
      if (lfd.getIsDfsDir()) {
        return false;
      } else {
        return true;
      }
    }

    return false;
  }

  @Override
  public StageType getType() {
    return StageType.MOVE;
  }

  @Override
  public String getName() {
    return "MOVE";
  }
}
