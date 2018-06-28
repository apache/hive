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
package org.apache.hadoop.hive.ql.exec.repl.util;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.repl.ReplStateLogWork;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.DDLSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.repl.ReplLogger;
import org.apache.hadoop.hive.ql.plan.AlterTableDesc;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.ImportTableDesc;
import org.apache.hadoop.hive.ql.stats.StatsUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;


public class ReplUtils {

  public static final String REPL_CHECKPOINT_KEY = "hive.repl.ckpt.key";

  /**
   * Bootstrap REPL LOAD operation type on the examined object based on ckpt state.
   */
  public enum ReplLoadOpType {
    LOAD_NEW, LOAD_SKIP, LOAD_REPLACE
  }

  public static Map<Integer, List<ExprNodeGenericFuncDesc>> genPartSpecs(
          Table table, List<Map<String, String>> partitions) throws SemanticException {
    Map<Integer, List<ExprNodeGenericFuncDesc>> partSpecs = new HashMap<>();
    int partPrefixLength = 0;
    if (partitions.size() > 0) {
      partPrefixLength = partitions.get(0).size();
      // pick the length of the first ptn, we expect all ptns listed to have the same number of
      // key-vals.
    }
    List<ExprNodeGenericFuncDesc> partitionDesc = new ArrayList<>();
    for (Map<String, String> ptn : partitions) {
      // convert each key-value-map to appropriate expression.
      ExprNodeGenericFuncDesc expr = null;
      for (Map.Entry<String, String> kvp : ptn.entrySet()) {
        String key = kvp.getKey();
        Object val = kvp.getValue();
        String type = table.getPartColByName(key).getType();
        PrimitiveTypeInfo pti = TypeInfoFactory.getPrimitiveTypeInfo(type);
        ExprNodeColumnDesc column = new ExprNodeColumnDesc(pti, key, null, true);
        ExprNodeGenericFuncDesc op = DDLSemanticAnalyzer.makeBinaryPredicate(
                "=", column, new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, val));
        expr = (expr == null) ? op : DDLSemanticAnalyzer.makeBinaryPredicate("and", expr, op);
      }
      if (expr != null) {
        partitionDesc.add(expr);
      }
    }
    if (partitionDesc.size() > 0) {
      partSpecs.put(partPrefixLength, partitionDesc);
    }
    return partSpecs;
  }

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

  public static boolean replCkptStatus(String dbName, Map<String, String> props, String dumpRoot)
          throws InvalidOperationException {
    // If ckpt property not set or empty means, bootstrap is not run on this object.
    if ((props != null) && props.containsKey(REPL_CHECKPOINT_KEY) && !props.get(REPL_CHECKPOINT_KEY).isEmpty()) {
      if (props.get(REPL_CHECKPOINT_KEY).equals(dumpRoot)) {
        return true;
      }
      throw new InvalidOperationException("REPL LOAD with Dump: " + dumpRoot
              + " is not allowed as the target DB: " + dbName
              + " is already bootstrap loaded by another Dump " + props.get(REPL_CHECKPOINT_KEY));
    }
    return false;
  }
}
