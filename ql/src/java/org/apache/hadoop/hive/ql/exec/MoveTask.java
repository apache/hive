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
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.ql.hooks.LineageInfo.DataContainer;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.DynamicPartitionCtx;
import org.apache.hadoop.hive.ql.plan.LoadFileDesc;
import org.apache.hadoop.hive.ql.plan.LoadTableDesc;
import org.apache.hadoop.hive.ql.plan.MoveWork;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.util.StringUtils;

/**
 * MoveTask implementation.
 **/
public class MoveTask extends Task<MoveWork> implements Serializable {

  private static final long serialVersionUID = 1L;

  public MoveTask() {
    super();
  }

  @Override
  public int execute() {

    try {
      // Do any hive related operations like moving tables and files
      // to appropriate locations
      LoadFileDesc lfd = work.getLoadFileWork();
      if (lfd != null) {
        Path targetPath = new Path(lfd.getTargetDir());
        Path sourcePath = new Path(lfd.getSourceDir());
        FileSystem fs = sourcePath.getFileSystem(conf);
        if (lfd.getIsDfsDir()) {
          // Just do a rename on the URIs, they belong to the same FS
          String mesg = "Moving data to: " + lfd.getTargetDir();
          String mesg_detail = " from " + lfd.getSourceDir();
          console.printInfo(mesg, mesg_detail);

          // delete the output directory if it already exists
          fs.delete(targetPath, true);
          // if source exists, rename. Otherwise, create a empty directory
          if (fs.exists(sourcePath)) {
            if (!fs.rename(sourcePath, targetPath)) {
              throw new HiveException("Unable to rename: " + sourcePath
                  + " to: " + targetPath);
            }
          } else if (!fs.mkdirs(targetPath)) {
            throw new HiveException("Unable to make directory: " + targetPath);
          }
        } else {
          // This is a local file
          String mesg = "Copying data to local directory " + lfd.getTargetDir();
          String mesg_detail = " from " + lfd.getSourceDir();
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
        String mesg_detail = " from " + tbd.getSourceDir();
        console.printInfo(mesg.toString(), mesg_detail);
        Table table = db.getTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, tbd
            .getTable().getTableName());

        if (work.getCheckFileFormat()) {
          // Get all files from the src directory
          FileStatus[] dirs;
          ArrayList<FileStatus> files;
          FileSystem fs;
          try {
            fs = FileSystem.get(table.getDataLocation(), conf);
            dirs = fs.globStatus(new Path(tbd.getSourceDir()));
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
          db.loadTable(new Path(tbd.getSourceDir()), tbd.getTable()
              .getTableName(), tbd.getReplace(), new Path(tbd.getTmpDir()));
          if (work.getOutputs() != null) {
            work.getOutputs().add(new WriteEntity(table));
          }
        } else {
          LOG.info("Partition is: " + tbd.getPartitionSpec().toString());
          // deal with dynamic partitions
          DynamicPartitionCtx dpCtx = tbd.getDPCtx();
          if (dpCtx != null && dpCtx.getNumDPCols() > 0) { // dynamic partitions
            // load the list of DP partitions and return the list of partition specs
            ArrayList<LinkedHashMap<String, String>> dp =
              db.loadDynamicPartitions(
                  new Path(tbd.getSourceDir()),
                  tbd.getTable().getTableName(),
                	tbd.getPartitionSpec(),
                	tbd.getReplace(),
                	new Path(tbd.getTmpDir()),
                	dpCtx.getNumDPCols());
            // for each partition spec, get the partition
            // and put it to WriteEntity for post-exec hook
            for (LinkedHashMap<String, String> partSpec: dp) {
              Partition partn = db.getPartition(table, partSpec, false);

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
                SessionState.get().getLineageState().setLineage(tbd.getSourceDir(), dc,
                    table.getCols());
              }

              console.printInfo("\tLoading partition " + partSpec);
            }
            dc = null; // reset data container to prevent it being added again.
          } else { // static partitions
            db.loadPartition(new Path(tbd.getSourceDir()), tbd.getTable().getTableName(),
                tbd.getPartitionSpec(), tbd.getReplace(), new Path(tbd.getTmpDir()));
          	Partition partn = db.getPartition(table, tbd.getPartitionSpec(), false);
          	dc = new DataContainer(table.getTTable(), partn.getTPartition());
          	// add this partition to post-execution hook
          	if (work.getOutputs() != null) {
          	  work.getOutputs().add(new WriteEntity(partn));
          	}
         }
        }
        if (SessionState.get() != null && dc != null) {
          SessionState.get().getLineageState().setLineage(tbd.getSourceDir(), dc,
              table.getCols());
        }
      }

      return 0;
    } catch (Exception e) {
      console.printError("Failed with exception " + e.getMessage(), "\n"
          + StringUtils.stringifyException(e));
      return (1);
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
  public int getType() {
    return StageType.MOVE;
  }

  @Override
  public String getName() {
    return "MOVE";
  }
}
