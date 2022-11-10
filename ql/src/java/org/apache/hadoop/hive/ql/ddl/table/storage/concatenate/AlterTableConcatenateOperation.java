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

package org.apache.hadoop.hive.ql.ddl.table.storage.concatenate;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.TaskQueue;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.tez.TezTask;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.merge.MergeFileTask;
import org.apache.hadoop.hive.ql.io.merge.MergeFileWork;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.FileMergeDesc;
import org.apache.hadoop.hive.ql.plan.ListBucketingCtx;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.OrcFileMergeDesc;
import org.apache.hadoop.hive.ql.plan.RCFileMergeDesc;
import org.apache.hadoop.hive.ql.plan.TezWork;

import com.google.common.collect.Lists;

/**
 * Operation process of concatenating the files of a table/partition.
 */
public class AlterTableConcatenateOperation extends DDLOperation<AlterTableConcatenateDesc> {
  public AlterTableConcatenateOperation(DDLOperationContext context, AlterTableConcatenateDesc desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws HiveException {
    Context generalContext = context.getContext();

    MergeFileWork mergeWork = getMergeFileWork(generalContext.getOpContext());
    Task<?> task = getTask(mergeWork);
    return executeTask(generalContext, task);
  }

  private MergeFileWork getMergeFileWork(CompilationOpContext opContext) throws HiveException {
    List<Path> inputDirList = Lists.newArrayList(desc.getInputDir());

    // merge work only needs input and output.
    MergeFileWork mergeWork = new MergeFileWork(inputDirList, desc.getOutputDir(),
        desc.getInputFormatClass().getName(), desc.getTableDesc());
    mergeWork.setListBucketingCtx(desc.getLbCtx());
    mergeWork.resolveConcatenateMerge(context.getDb().getConf());
    mergeWork.setMapperCannotSpanPartns(true);
    mergeWork.setSourceTableInputFormat(desc.getInputFormatClass().getName());

    Map<Path, List<String>> pathToAliases = new LinkedHashMap<>();
    List<String> inputDirStr = Lists.newArrayList(inputDirList.toString());
    pathToAliases.put(desc.getInputDir(), inputDirStr);
    mergeWork.setPathToAliases(pathToAliases);

    FileMergeDesc fmd = getFileMergeDesc();
    Operator<? extends OperatorDesc> mergeOp = OperatorFactory.get(opContext, fmd);
    Map<String, Operator<? extends  OperatorDesc>> aliasToWork =
        new LinkedHashMap<String, Operator<? extends OperatorDesc>>();
    aliasToWork.put(inputDirList.toString(), mergeOp);
    mergeWork.setAliasToWork(aliasToWork);

    return mergeWork;
  }

  private FileMergeDesc getFileMergeDesc() {
    // safe to assume else is ORC as semantic analyzer will check for RC/ORC
    FileMergeDesc fmd = (desc.getInputFormatClass().equals(RCFileInputFormat.class)) ?
        new RCFileMergeDesc() :
        new OrcFileMergeDesc();

    ListBucketingCtx lbCtx = desc.getLbCtx();
    boolean lbatc = lbCtx == null ? false : lbCtx.isSkewedStoredAsDir();
    int lbd = lbCtx == null ? 0 : lbCtx.calculateListBucketingLevel();

    fmd.setDpCtx(null);
    fmd.setHasDynamicPartitions(false);
    fmd.setListBucketingAlterTableConcatenate(lbatc);
    fmd.setListBucketingDepth(lbd);
    fmd.setOutputPath(desc.getOutputDir());

    return fmd;
  }

  private Task<?> getTask(MergeFileWork mergeWork) {
    if (context.getConf().getVar(ConfVars.HIVE_EXECUTION_ENGINE).equals("tez")) {
      TezWork tezWork = new TezWork(context.getQueryState().getQueryId(), context.getConf());
      mergeWork.setName("File Merge");
      tezWork.add(mergeWork);
      Task<?> task = new TezTask();
      ((TezTask) task).setWork(tezWork);
      return task;
    } else {
      Task<?> task = new MergeFileTask();
      ((MergeFileTask) task).setWork(mergeWork);
      return task;
    }
  }

  private int executeTask(Context generalContext, Task<?> task) {
    TaskQueue taskQueue = new TaskQueue();
    task.initialize(context.getQueryState(), context.getQueryPlan(), taskQueue, generalContext);
    int ret = task.execute();
    if (task.getException() != null) {
      context.getTask().setException(task.getException());
    }
    return ret;
  }
}
