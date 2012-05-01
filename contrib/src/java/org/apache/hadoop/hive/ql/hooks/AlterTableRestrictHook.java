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

package org.apache.hadoop.hive.ql.hooks;

import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.hooks.conf.FBHiveConf;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * Adding a hook to stop platinum tables from getting modified in silver
 */
public class AlterTableRestrictHook implements ExecuteWithHookContext {
  static final private Log LOG = LogFactory.getLog("hive.ql.hooks.AlterTableRestrictHook");

  String current_cluster = null;

  public AlterTableRestrictHook() throws Exception {
    HiveConf conf = new HiveConf(AlterTableRestrictHook.class);
    current_cluster = conf.get(FBHiveConf.FB_CURRENT_CLUSTER);
  }

  /**
   * Restrict the alter table command if the current cluster is not the same
   * as the creation cluster
   *
   */
  public void run(HookContext hookContext) throws Exception {
    SessionState ss = SessionState.get();

    if ((current_cluster == null) || (ss == null)) {
      return;
    }

    HiveOperation commandType = ss.getHiveOperation();

    // This check is only for alter table
    if (!((commandType == HiveOperation.ALTERTABLE_ADDCOLS) ||
          (commandType == HiveOperation.ALTERTABLE_REPLACECOLS) ||
          (commandType == HiveOperation.ALTERTABLE_RENAMECOL) ||
          (commandType == HiveOperation.ALTERTABLE_RENAMEPART) ||
          (commandType == HiveOperation.ALTERTABLE_RENAME) ||
          (commandType == HiveOperation.ALTERTABLE_PROPERTIES) ||
          (commandType == HiveOperation.ALTERTABLE_SERIALIZER) ||
          (commandType == HiveOperation.ALTERTABLE_SERDEPROPERTIES) ||
          (commandType == HiveOperation.ALTERTABLE_CLUSTER_SORT) ||
          (commandType == HiveOperation.ALTERTABLE_FILEFORMAT))) {
      return;
    }

    // If the creation cluster is being modified to be the current cluster the alter should not be
    // restricted
    if (commandType == HiveOperation.ALTERTABLE_PROPERTIES) {
      Map<String, String> newProps =
        ((DDLWork)(hookContext.getQueryPlan().getRootTasks().get(0).getWork()))
        .getAlterTblDesc().getProps();
      if (newProps.containsKey(HookUtils.TABLE_CREATION_CLUSTER) &&
          (newProps.get(HookUtils.TABLE_CREATION_CLUSTER).equals(current_cluster))) {
        return;
      }
    }

    Set<WriteEntity> outputs = hookContext.getOutputs();
    for (WriteEntity output : outputs) {
      Table table = output.getT();
      if (table != null) {
        String tableCreationCluster = table.getProperty(HookUtils.TABLE_CREATION_CLUSTER);
        if (tableCreationCluster != null &&
            !tableCreationCluster.isEmpty() &&
            !tableCreationCluster.equals(current_cluster)) {
          String exception = "Table " + table.getTableName() + " cannot be modified.";
          exception += " Table's cluster is " + tableCreationCluster + ",";
          exception += "whereas current package is " + current_cluster;
          throw new Exception(exception);
        }
      }
    }
  }
}
