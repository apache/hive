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
import java.util.List;

import org.antlr.runtime.tree.Tree;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.plan.CopyWork;

/**
 * ExportSemanticAnalyzer.
 *
 */
public class ExportSemanticAnalyzer extends BaseSemanticAnalyzer {

  public ExportSemanticAnalyzer(HiveConf conf) throws SemanticException {
    super(conf);
  }

  @Override
  public void analyzeInternal(ASTNode ast) throws SemanticException {
    Tree tableTree = ast.getChild(0);
    Tree toTree = ast.getChild(1);

    // initialize export path
    String tmpPath = stripQuotes(toTree.getText());
    URI toURI = EximUtil.getValidatedURI(conf, tmpPath);

    // initialize source table/partition
    tableSpec ts = new tableSpec(db, conf, (ASTNode) tableTree, false, true);
    EximUtil.validateTable(ts.tableHandle);
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
          FileStatus[] files = fs.listStatus(toPath);
          if (files != null) {
            throw new SemanticException(ErrorMsg.INVALID_PATH.getMsg(ast,
                          "Target is not an empty directory : " + toURI));
          }
        }
      } catch (FileNotFoundException e) {
      }
    } catch (IOException e) {
      throw new SemanticException(ErrorMsg.INVALID_PATH.getMsg(ast), e);
    }

    List<Partition> partitions = null;
    try {
      partitions = null;
      if (ts.tableHandle.isPartitioned()) {
        partitions = (ts.partitions != null) ? ts.partitions : db.getPartitions(ts.tableHandle);
      }
      String tmpfile = ctx.getLocalTmpFileURI();
      Path path = new Path(tmpfile, "_metadata");
      EximUtil.createExportDump(FileSystem.getLocal(conf), path, ts.tableHandle, partitions);
      Task<? extends Serializable> rTask = TaskFactory.get(new CopyWork(
          path, new Path(toURI), false), conf);
      rootTasks.add(rTask);
      LOG.debug("_metadata file written into " + path.toString()
          + " and then copied to " + toURI.toString());
    } catch (Exception e) {
      throw new SemanticException(
          ErrorMsg.GENERIC_ERROR
              .getMsg("Exception while writing out the local file"), e);
    }

    Path parentPath = new Path(toURI);

    if (ts.tableHandle.isPartitioned()) {
      for (Partition partition : partitions) {
        Path fromPath = partition.getDataLocation();
        Path toPartPath = new Path(parentPath, partition.getName());
        Task<? extends Serializable> rTask = TaskFactory.get(
            new CopyWork(fromPath, toPartPath, false),
            conf);
        rootTasks.add(rTask);
        inputs.add(new ReadEntity(partition));
      }
    } else {
      Path fromPath = ts.tableHandle.getDataLocation();
      Path toDataPath = new Path(parentPath, "data");
      Task<? extends Serializable> rTask = TaskFactory.get(new CopyWork(
          fromPath, toDataPath, false), conf);
      rootTasks.add(rTask);
      inputs.add(new ReadEntity(ts.tableHandle));
    }
    outputs.add(new WriteEntity(parentPath, toURI.getScheme().equals("hdfs")));
  }
}
