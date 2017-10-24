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
package org.apache.hadoop.hive.ql.parse.repl.load.message;

import org.apache.hadoop.hive.metastore.messaging.DropPartitionMessage;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.DDLSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.plan.DropTableDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DropPartitionHandler extends AbstractMessageHandler {
  @Override
  public List<Task<? extends Serializable>> handle(Context context)
      throws SemanticException {
    try {
      DropPartitionMessage msg = deserializer.getDropPartitionMessage(context.dmd.getPayload());
      String actualDbName = context.isDbNameEmpty() ? msg.getDB() : context.dbName;
      String actualTblName = context.isTableNameEmpty() ? msg.getTable() : context.tableName;
      Map<Integer, List<ExprNodeGenericFuncDesc>> partSpecs =
          genPartSpecs(new Table(msg.getTableObj()),
              msg.getPartitions());
      if (partSpecs.size() > 0) {
        DropTableDesc dropPtnDesc = new DropTableDesc(actualDbName + "." + actualTblName,
            partSpecs, null, true, context.eventOnlyReplicationSpec());
        Task<DDLWork> dropPtnTask = TaskFactory.get(
            new DDLWork(readEntitySet, writeEntitySet, dropPtnDesc),
            context.hiveConf
        );
        context.log.debug("Added drop ptn task : {}:{},{}", dropPtnTask.getId(),
            dropPtnDesc.getTableName(), msg.getPartitions());
        updatedMetadata.set(context.dmd.getEventTo().toString(), actualDbName, actualTblName, null);
        return Collections.singletonList(dropPtnTask);
      } else {
        throw new SemanticException(
            "DROP PARTITION EVENT does not return any part descs for event message :"
                + context.dmd.getPayload());
      }
    } catch (Exception e) {
      throw (e instanceof SemanticException)
          ? (SemanticException) e
          : new SemanticException("Error reading message members", e);
    }
  }

  private Map<Integer, List<ExprNodeGenericFuncDesc>> genPartSpecs(Table table,
      List<Map<String, String>> partitions) throws SemanticException {
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
}
