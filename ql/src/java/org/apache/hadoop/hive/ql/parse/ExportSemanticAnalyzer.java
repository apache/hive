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

package org.apache.hadoop.hive.ql.parse;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.HashSet;
import java.util.List;

import org.antlr.runtime.tree.Tree;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.ReplCopyTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.InvalidTableException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.PartitionIterable;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.slf4j.Logger;

/**
 * ExportSemanticAnalyzer.
 *
 */
public class ExportSemanticAnalyzer extends BaseSemanticAnalyzer {

  private ReplicationSpec replicationSpec;

  public ExportSemanticAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  public void analyzeInternal(ASTNode ast) throws SemanticException {
    Tree tableTree = ast.getChild(0);
    Tree toTree = ast.getChild(1);

    if (ast.getChildCount() > 2) {
      replicationSpec = new ReplicationSpec((ASTNode) ast.getChild(2));
    } else {
      replicationSpec = new ReplicationSpec();
    }

    // initialize export path
    String tmpPath = stripQuotes(toTree.getText());
    URI toURI = EximUtil.getValidatedURI(conf, tmpPath);

    // initialize source table/partition
    TableSpec ts;

    try {
      ts = new TableSpec(db, conf, (ASTNode) tableTree, false, true);
    } catch (SemanticException sme){
      if ((replicationSpec.isInReplicationScope()) &&
            ((sme.getCause() instanceof InvalidTableException)
            || (sme instanceof Table.ValidationFailureSemanticException)
            )
          ){
        // If we're in replication scope, it's possible that we're running the export long after
        // the table was dropped, so the table not existing currently or being a different kind of
        // table is not an error - it simply means we should no-op, and let a future export
        // capture the appropriate state
        ts = null;
      } else {
        throw sme;
      }
    }

    // All parsing is done, we're now good to start the export process.
    prepareExport(ast, toURI, ts, replicationSpec, db, conf, ctx, rootTasks, inputs, outputs, LOG);

  }

  // FIXME : Move to EximUtil - it's okay for this to stay here for a little while more till we finalize the statics
  public static void prepareExport(
      ASTNode ast, URI toURI, TableSpec ts,
      ReplicationSpec replicationSpec, Hive db, HiveConf conf,
      Context ctx, List<Task<? extends Serializable>> rootTasks, HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
      Logger LOG) throws SemanticException {

    if (ts != null) {
      try {
        EximUtil.validateTable(ts.tableHandle);
        if (replicationSpec.isInReplicationScope()
            && ts.tableHandle.isTemporary()){
          // No replication for temporary tables either
          ts = null;
        } else if (ts.tableHandle.isView()) {
          replicationSpec.setIsMetadataOnly(true);
        }

      } catch (SemanticException e) {
        // table was a non-native table or an offline table.
        // ignore for replication, error if not.
        if (replicationSpec.isInReplicationScope()){
          ts = null; // null out ts so we can't use it.
        } else {
          throw e;
        }
      }
    }

    try {

      FileSystem fs = FileSystem.get(toURI, conf);
      Path toPath = new Path(toURI.getScheme(), toURI.getAuthority(), toURI.getPath());
      try {
        FileStatus tgt = fs.getFileStatus(toPath);
        // target exists
        if (!tgt.isDir()) {
          throw new SemanticException(ErrorMsg.INVALID_PATH.getMsg(ast,
                    "Target is not a directory : " + toURI));
        } else {
          FileStatus[] files = fs.listStatus(toPath, FileUtils.HIDDEN_FILES_PATH_FILTER);
          if (files != null && files.length != 0) {
            throw new SemanticException(ErrorMsg.INVALID_PATH.getMsg(ast,
                          "Target is not an empty directory : " + toURI));
          }
        }
      } catch (FileNotFoundException e) {
      }
    } catch (IOException e) {
      throw new SemanticException(ErrorMsg.INVALID_PATH.getMsg(ast), e);
    }

    PartitionIterable partitions = null;
    try {
      replicationSpec.setCurrentReplicationState(String.valueOf(db.getMSC().getCurrentNotificationEventId().getEventId()));
      if ( (ts != null) && (ts.tableHandle.isPartitioned())){
        if (ts.specType == TableSpec.SpecType.TABLE_ONLY){
          // TABLE-ONLY, fetch partitions if regular export, don't if metadata-only
          if (replicationSpec.isMetadataOnly()){
            partitions = null;
          } else {
            partitions = new PartitionIterable(db,ts.tableHandle,null,conf.getIntVar(
                HiveConf.ConfVars.METASTORE_BATCH_RETRIEVE_MAX));
          }
        } else {
          // PARTITIONS specified - partitions inside tableSpec
          partitions = new PartitionIterable(ts.partitions);
        }
      } else {
        // Either tableHandle isn't partitioned => null, or repl-export after ts becomes null => null.
        // or this is a noop-replication export, so we can skip looking at ptns.
        partitions = null;
      }

      Path path = new Path(ctx.getLocalTmpPath(), EximUtil.METADATA_NAME);
      EximUtil.createExportDump(
          FileSystem.getLocal(conf),
          path,
          (ts != null ? ts.tableHandle : null),
          partitions,
          replicationSpec);

      Task<? extends Serializable> rTask = ReplCopyTask.getDumpCopyTask(replicationSpec, path, new Path(toURI), conf);

      rootTasks.add(rTask);
      LOG.debug("_metadata file written into " + path.toString()
          + " and then copied to " + toURI.toString());
    } catch (Exception e) {
      throw new SemanticException(
          ErrorMsg.IO_ERROR
              .getMsg("Exception while writing out the local file"), e);
    }

    if (!(replicationSpec.isMetadataOnly() || (ts == null))) {
      Path parentPath = new Path(toURI);
      if (ts.tableHandle.isPartitioned()) {
        for (Partition partition : partitions) {
          Path fromPath = partition.getDataLocation();
          Path toPartPath = new Path(parentPath, partition.getName());
          Task<? extends Serializable> rTask =
              ReplCopyTask.getDumpCopyTask(replicationSpec, fromPath, toPartPath, conf);
          rootTasks.add(rTask);
          inputs.add(new ReadEntity(partition));
        }
      } else {
        Path fromPath = ts.tableHandle.getDataLocation();
        Path toDataPath = new Path(parentPath, EximUtil.DATA_PATH_NAME);
        Task<? extends Serializable> rTask =
                ReplCopyTask.getDumpCopyTask(replicationSpec, fromPath, toDataPath, conf);
        rootTasks.add(rTask);
        inputs.add(new ReadEntity(ts.tableHandle));
      }
      outputs.add(toWriteEntity(parentPath, conf));
    }
  }


}
