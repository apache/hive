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
package org.apache.hadoop.hive.ql.exec.repl;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.repl.ReplLogger;
import org.apache.hadoop.hive.ql.plan.AlterTableDesc;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.plan.ImportTableDesc;
import org.apache.hadoop.hive.ql.stats.StatsUtils;

import java.util.HashMap;
import java.util.HashSet;


public class ReplUtils {

  public static final String REPL_CHECKPOINT_KEY = "hive.repl.ckpt.key";

  public static Task<?> getTableReplLogTask(ImportTableDesc tableDesc, ReplLogger replLogger, HiveConf conf)
          throws SemanticException {
    ReplStateLogWork replLogWork = new ReplStateLogWork(replLogger, tableDesc.getTableName(), tableDesc.tableType());
    return TaskFactory.get(replLogWork, conf);
  }

  public static Task<?> getTableCheckpointTask(ImportTableDesc tableDesc, HashMap<String, String> partSpec,
                                               String dumpRoot, HiveConf conf) throws SemanticException {
    HashMap<String, String> mapProp = new HashMap<>();
    mapProp.put(REPL_CHECKPOINT_KEY, dumpRoot);

    AlterTableDesc alterTblDesc =  new AlterTableDesc(AlterTableDesc.AlterTableTypes.ADDPROPS);
    alterTblDesc.setProps(mapProp);
    alterTblDesc.setOldName(
            StatsUtils.getFullyQualifiedTableName(tableDesc.getDatabaseName(), tableDesc.getTableName()));
    if (partSpec != null) {
      alterTblDesc.setPartSpec(partSpec);
    }
    return TaskFactory.get(new DDLWork(new HashSet<>(), new HashSet<>(), alterTblDesc), conf);
  }
}
