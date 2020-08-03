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

package org.apache.hadoop.hive.ql.hooks;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.hooks.HookContext.HookType;
import org.apache.hadoop.hive.ql.hooks.LineageInfo.BaseColumnInfo;
import org.apache.hadoop.hive.ql.hooks.LineageInfo.Dependency;
import org.apache.hadoop.hive.ql.hooks.LineageInfo.DependencyKey;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Implementation of a post execute hook that simply prints out its parameters
 * to standard output.
 */
public class PostExecutePrinter implements ExecuteWithHookContext {

  public class DependencyKeyComp implements
    Comparator<Map.Entry<DependencyKey, Dependency>> {

    @Override
    public int compare(Map.Entry<DependencyKey, Dependency> o1,
        Map.Entry<DependencyKey, Dependency> o2) {
      if (o1 == null && o2 == null) {
        return 0;
      }
      else if (o1 == null && o2 != null) {
        return -1;
      }
      else if (o1 != null && o2 == null) {
        return 1;
      }
      else {
        // Both are non null.
        // First compare the table names.
        int ret = o1.getKey().getDataContainer().getTable().getTableName()
          .compareTo(o2.getKey().getDataContainer().getTable().getTableName());

        if (ret != 0) {
          return ret;
        }

        // The table names match, so check on the partitions
        if (!o1.getKey().getDataContainer().isPartition() &&
            o2.getKey().getDataContainer().isPartition()) {
          return -1;
        }
        else if (o1.getKey().getDataContainer().isPartition() &&
            !o2.getKey().getDataContainer().isPartition()) {
          return 1;
        }

        if (o1.getKey().getDataContainer().isPartition() &&
            o2.getKey().getDataContainer().isPartition()) {
          // Both are partitioned tables.
          ret = o1.getKey().getDataContainer().getPartition().toString()
          .compareTo(o2.getKey().getDataContainer().getPartition().toString());

          if (ret != 0) {
            return ret;
          }
        }

        // The partitons are also the same so check the fieldschema
        return (o1.getKey().getFieldSchema().getName().compareTo(
            o2.getKey().getFieldSchema().getName()));
      }
    }
  }

  @Override
  public void run(HookContext hookContext) throws Exception {
    assert(hookContext.getHookType() == HookType.POST_EXEC_HOOK);
    Set<ReadEntity> inputs = hookContext.getInputs();
    Set<WriteEntity> outputs = hookContext.getOutputs();
    LineageInfo linfo = hookContext.getLinfo();
    UserGroupInformation ugi = hookContext.getUgi();
    this.run(hookContext.getQueryState(),inputs,outputs,linfo,ugi);
  }

  public void run(QueryState queryState, Set<ReadEntity> inputs,
      Set<WriteEntity> outputs, LineageInfo linfo,
      UserGroupInformation ugi) throws Exception {

    LogHelper console = SessionState.getConsole();

    if (console == null) {
      return;
    }

    if (queryState != null) {
      console.printInfo("POSTHOOK: query: " + queryState.getQueryString().trim(), false);
      console.printInfo("POSTHOOK: type: " + queryState.getCommandType(), false);
    }

    PreExecutePrinter.printEntities(console, inputs, "POSTHOOK: Input: ");
    PreExecutePrinter.printEntities(console, outputs, "POSTHOOK: Output: ");

    // Also print out the generic lineage information if there is any
    if (linfo != null) {
      LinkedList<Map.Entry<DependencyKey, Dependency>> entry_list =
        new LinkedList<Map.Entry<DependencyKey, Dependency>>(linfo.entrySet());
      Collections.sort(entry_list, new DependencyKeyComp());
      Iterator<Map.Entry<DependencyKey, Dependency>> iter = entry_list.iterator();
      while(iter.hasNext()) {
        Map.Entry<DependencyKey, Dependency> it = iter.next();
        Dependency dep = it.getValue();
        DependencyKey depK = it.getKey();

        if(dep == null) {
          continue;
        }

        StringBuilder sb = new StringBuilder();
        sb.append("POSTHOOK: Lineage: ");
        if (depK.getDataContainer().isPartition()) {
          Partition part = depK.getDataContainer().getPartition();
          sb.append(part.getTableName());
          sb.append(" PARTITION(");
          int i = 0;
          for (FieldSchema fs : depK.getDataContainer().getTable().getPartitionKeys()) {
            if (i != 0) {
              sb.append(",");
            }
            sb.append(fs.getName() + "=" + part.getValues().get(i++));
          }
          sb.append(")");
        }
        else {
          sb.append(depK.getDataContainer().getTable().getTableName());
        }
        sb.append("." + depK.getFieldSchema().getName() + " " +
            dep.getType() + " ");

        sb.append("[");
        for(BaseColumnInfo col: dep.getBaseCols()) {
          sb.append("("+col.getTabAlias().getTable().getTableName() + ")"
              + col.getTabAlias().getAlias() + "."
              + col.getColumn() + ", ");
        }
        sb.append("]");

        console.printInfo(sb.toString(), false);
      }
    }
  }

}
