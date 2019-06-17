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

package org.apache.hadoop.hive.ql.ddl.table.storage;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.DriverContext;
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

/**
 * Operation process of concatenating the files of a table/partition.
 */
public class AlterTableConcatenateOperation extends DDLOperation {
  private final AlterTableConcatenateDesc desc;

  public AlterTableConcatenateOperation(DDLOperationContext context, AlterTableConcatenateDesc desc) {
    super(context);
    this.desc = desc;
  }

  @Override
  public int execute() throws HiveException {
    ListBucketingCtx lbCtx = desc.getLbCtx();
    boolean lbatc = lbCtx == null ? false : lbCtx.isSkewedStoredAsDir();
    int lbd = lbCtx == null ? 0 : lbCtx.calculateListBucketingLevel();

    // merge work only needs input and output.
    MergeFileWork mergeWork = new MergeFileWork(desc.getInputDir(), desc.getOutputDir(),
        desc.getInputFormatClass().getName(), desc.getTableDesc());
    LinkedHashMap<Path, ArrayList<String>> pathToAliases = new LinkedHashMap<>();
    ArrayList<String> inputDirstr = new ArrayList<String>(1);
    inputDirstr.add(desc.getInputDir().toString());
    pathToAliases.put(desc.getInputDir().get(0), inputDirstr);
    mergeWork.setPathToAliases(pathToAliases);
    mergeWork.setListBucketingCtx(desc.getLbCtx());
    mergeWork.resolveConcatenateMerge(context.getDb().getConf());
    mergeWork.setMapperCannotSpanPartns(true);
    mergeWork.setSourceTableInputFormat(desc.getInputFormatClass().getName());
    final FileMergeDesc fmd;
    if (desc.getInputFormatClass().equals(RCFileInputFormat.class)) {
      fmd = new RCFileMergeDesc();
    } else {
      // safe to assume else is ORC as semantic analyzer will check for RC/ORC
      fmd = new OrcFileMergeDesc();
    }

    fmd.setDpCtx(null);
    fmd.setHasDynamicPartitions(false);
    fmd.setListBucketingAlterTableConcatenate(lbatc);
    fmd.setListBucketingDepth(lbd);
    fmd.setOutputPath(desc.getOutputDir());

    CompilationOpContext opContext = context.getDriverContext().getCtx().getOpContext();
    Operator<? extends OperatorDesc> mergeOp = OperatorFactory.get(opContext, fmd);

    LinkedHashMap<String, Operator<? extends  OperatorDesc>> aliasToWork =
        new LinkedHashMap<String, Operator<? extends OperatorDesc>>();
    aliasToWork.put(desc.getInputDir().toString(), mergeOp);
    mergeWork.setAliasToWork(aliasToWork);
    DriverContext driverCxt = new DriverContext();
    Task<?> task;
    if (context.getConf().getVar(ConfVars.HIVE_EXECUTION_ENGINE).equals("tez")) {
      TezWork tezWork = new TezWork(context.getQueryState().getQueryId(), context.getConf());
      mergeWork.setName("File Merge");
      tezWork.add(mergeWork);
      task = new TezTask();
      ((TezTask) task).setWork(tezWork);
    } else {
      task = new MergeFileTask();
      ((MergeFileTask) task).setWork(mergeWork);
    }

    // initialize the task and execute
    task.initialize(context.getQueryState(), context.getQueryPlan(), driverCxt, opContext);
    Task<? extends Serializable> subtask = task;
    int ret = task.execute(driverCxt);
    if (subtask.getException() != null) {
      context.getTask().setException(subtask.getException());
    }
    return ret;
  }
}
