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
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.DDLTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.hooks.conf.FBHiveConf;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.CreateTableDesc;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.security.UserGroupInformation;


/**
 * Implementation of a pre execute hook that checks the table RETENTION is set.
 */
public class CheckRetentionsHook {

  private static final Log LOG = LogFactory.getLog(CheckRetentionsHook.class.getName());
  private static HiveConf conf;

  // If warningOnly = true, we print out some warnnings without fail
  // the CREATE TABLE DDL.
  private static boolean warningOnly = false;

  // required table parameters
  private static final String RETENTION_FLAG = "RETENTION";
  private static final String RETENTION_PLATINUM_FLAG = "RETENTION_PLATINUM";

  // wiki page URL that explains the policies
  private static final String wikiURL =
    "https://www.intern.facebook.com/intern/wiki/index.php/Data/Hive/" +
    "Retention_on_new_tables";

  private static String retentionKey = null;

  private static String ErrMsg(String str) {
    return str + "\n  Here's how to add retention: " + wikiURL;
  }

  public static class PreExec implements ExecuteWithHookContext {

    public void run(HookContext hookContext)
      throws Exception {

      assert(hookContext.getHookType() == HookContext.HookType.PRE_EXEC_HOOK);

      SessionState sess = SessionState.get();
      Set<ReadEntity> inputs = hookContext.getInputs();
      Set<WriteEntity> outputs = hookContext.getOutputs();
      UserGroupInformation ugi = hookContext.getUgi();
      conf = sess.getConf();

      warningOnly = conf.getBoolean(FBHiveConf.NO_RETENTION_WARNING_ONLY, true);

      // figure out if we are on silver or platinum
      String whDir = HiveConf.getVar(conf, HiveConf.ConfVars.METASTOREWAREHOUSE);
      if (whDir == null) {
        throw new Exception(ErrMsg("Cannot determine which cluster this query is running on: " +
          "hive.metastore.warehouse.dir is not set!"));
      }

      Path p = new Path(whDir);
      String hostName =  p.toUri().getHost();

      if (hostName.equals(conf.get(FBHiveConf.FBHIVE_SILVER_DFS_PREFIX)) ||
          hostName.equals(conf.get(FBHiveConf.FBHIVE_SILVER_DFS_PREFIX2)) ||
          hostName.equals(conf.get(FBHiveConf.FBHIVE_SILVER_DFS_PREFIX3))) {
        retentionKey = RETENTION_FLAG;
      } else if (hostName.equals(conf.get(FBHiveConf.FBHIVE_PLATINUM_DFS_PREFIX))) {
        retentionKey = RETENTION_PLATINUM_FLAG;
      } else {
        throw new Exception(ErrMsg("Cannot determine which cluster this query is running on: " +
          "hive.metastore.warehouse.dir=" + whDir +
         "; does not seems to belong to either silver or platinum!"));
      }

      Set<Task<? extends Serializable>> tasks = new HashSet<Task<? extends Serializable>>();
      getReachableTasks(tasks, hookContext.getQueryPlan());

      for (Task task: tasks) {
        if (task instanceof DDLTask) {
          DDLWork work = (DDLWork) task.getWork();
          if (work.getCreateTblDesc() != null) {
            checkRetention(work.getCreateTblDesc(), retentionKey);
          }
        }
      }
    }

    private void getReachableTasks(Set<Task<? extends Serializable>> tasks, QueryPlan qp) {
      ArrayList<Task<? extends Serializable>> rootTasks = qp.getRootTasks();
      for (Task<? extends Serializable> task: rootTasks) {
        getReachableTasks(tasks, task);
      }
    }

    /**
     * Recursively traverse the task dependence tree and gather all tasks into
     * the set.
     */
    private void getReachableTasks(Set<Task<? extends Serializable>> tasks,
            Task<? extends Serializable> rootTask) {
      if (!tasks.contains(rootTask)) {
        tasks.add(rootTask);
        if (rootTask.getDependentTasks() != null) {
          for (Task<? extends Serializable> child: rootTask.getDependentTasks()) {
            getReachableTasks(tasks, child);
          }
        }
      }
    }

    private void warnOrFail(boolean warning, String mesg) throws Exception {
      if (warning) {
        // shout loud on stderr!
        System.err.println("\n ----------");
        System.err.println("| WARNING: | ");
        System.err.println(" ----------");
        System.err.println("  This command does NOT comply with the RETENTION " +
            "policies. This command will fail in the near future. \n" +
            mesg);
      } else {
        throw new Exception(mesg);
      }
    }


    /**
     * Check if the CREATE TABLE statement has retention and data growth
     * estimation set. If not throw an exception.
     */
    private void checkRetention(CreateTableDesc desc, String retentionKey)
        throws Exception {

      // exclude EXTERNAL tables
      if (desc.isExternal()) {
        return;
      }

      // TODO: remove this whenever it becomes feasible
      // exclude table name starts with tmp, temp, or test: tmp tables should be set
      // default retention
      if (desc.getTableName().startsWith("tmp") ||
          desc.getTableName().startsWith("temp") ||
          desc.getTableName().startsWith("test")) {
        return;
      }

      // check if table already exists
      if (tableExists(desc.getTableName())) {
        return;
      }

      String tableNeedsRetention = "Newly created tables have to have " + retentionKey +
              " set unless the table name has one of the prefixes \"tmp\", \"temp\", or \"test\".";

      String tableRetentionFormat = "The value of the " + retentionKey + " parameter must be an " +
              "integer greater than or equal to -1, i.e. -1,0,1,...";

      // check 'RETENTION' parameter exists
      String retentionValue = "";
      if (desc.getTblProps() == null ||
          (retentionValue = desc.getTblProps().get(retentionKey)) == null) {
        warnOrFail(warningOnly, ErrMsg("Table " + desc.getTableName() + " does not have " +
                retentionKey + " parameter set. "
                + tableNeedsRetention + "  " + tableRetentionFormat));
        return;
      }

      // check 'RETENTION' parameter is set to a value in the range -1,0,1,...
      int retentionIntValue;
      try {
        retentionIntValue = Integer.parseInt(retentionValue);
      } catch (Exception e) {
        // retentionValue is not a valid integer, set retentionIntValue to an invalid value
        retentionIntValue = Integer.MIN_VALUE;
      }

      if (retentionIntValue < -1) {
        warnOrFail(warningOnly, ErrMsg("Table " + desc.getTableName() + " has an invalid value " +
                retentionValue + " for the parameter " + retentionKey + ".  " +
                tableRetentionFormat + "  " + tableNeedsRetention));
      }
    }

    private boolean tableExists(String tabName) throws Exception {
      Hive db = Hive.get(conf);
      Table table = db.getTable("default", tabName, false);
      return table != null;
    }
  }
}
