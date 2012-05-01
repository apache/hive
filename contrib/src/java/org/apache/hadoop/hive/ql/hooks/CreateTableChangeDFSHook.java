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

import java.io.Serializable;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.hooks.conf.FBHiveConf;
import org.apache.hadoop.hive.ql.plan.CreateTableDesc;
import org.apache.hadoop.hive.ql.plan.CreateTableLikeDesc;
import org.apache.hadoop.hive.ql.plan.DDLWork;

/**
 * Implementation of a pre execute hook that is used to change
 * the location of the DFS.
 * This is only applicable to new tables - this can be used to
 * eventually spread the load evenly on more than 1 DFS.
 */
public class CreateTableChangeDFSHook implements ExecuteWithHookContext {
  static final private Log LOG = LogFactory.getLog(CreateTableChangeDFSHook.class.getName());

  public void run(HookContext hookContext) throws Exception {
    assert(hookContext.getHookType() == HookContext.HookType.PRE_EXEC_HOOK);

    QueryPlan queryPlan = hookContext.getQueryPlan();

    // This change is only needed when a new table is being created
    ArrayList<Task<? extends Serializable>> rootTasks = queryPlan.getRootTasks();

    if ((rootTasks == null) || (rootTasks.size() != 1)) {
      return;
    }

    Task<? extends Serializable> tsk = rootTasks.get(0);

    if (!(tsk.getWork() instanceof DDLWork)) {
      return;
    }

    HiveConf conf = hookContext.getConf();
    DDLWork ddlWork = (DDLWork)tsk.getWork();

    float pubPercent = conf.getFloat(FBHiveConf.ENABLE_PARTIAL_CHANGEDFS, 0);

    // if pubPercent == 0, make sure it returns.
    if (!HookUtils.rollDice(pubPercent)) {
      return;
    }

    String newDir = conf.get(FBHiveConf.SECONDARYMETASTOREWAREHOUSE);

    if (ddlWork.getCreateTblDesc() != null) {
      CreateTableDesc crtTblDesc = ddlWork.getCreateTblDesc();
      // The user has already specified the location
      if (crtTblDesc.getLocation() != null) {
        return;
      }

      // This is only for tmp tables right now
      if ((crtTblDesc.getTableName() == null) ||
          ((!crtTblDesc.getTableName().startsWith("tmp_")) &&
           (!crtTblDesc.getTableName().startsWith("temp_")))) {
        return;
      }

      String locn =
        (new Warehouse(conf)).getTablePath(newDir, crtTblDesc.getTableName()).toString();
      crtTblDesc.setLocation(locn);
      LOG.info("change location for table " + crtTblDesc.getTableName());
      return;
    }

    if (ddlWork.getCreateTblLikeDesc() != null) {
      CreateTableLikeDesc crtTblLikeDesc = ddlWork.getCreateTblLikeDesc();
      // The user has already specified the location
      if (crtTblLikeDesc.getLocation() != null) {
        return;
      }

      // This is only for tmp tables right now
      if ((crtTblLikeDesc.getTableName() == null) ||
          ((!crtTblLikeDesc.getTableName().startsWith("tmp_")) &&
           (!crtTblLikeDesc.getTableName().startsWith("temp_")))) {
        return;
      }

      String locn =
        (new Warehouse(conf)).getTablePath(newDir, crtTblLikeDesc.getTableName()).toString();
      crtTblLikeDesc.setLocation(locn);
      LOG.info("change location for table " + crtTblLikeDesc.getTableName());
      return;
    }
  }
}
